import AppKit
import QuartzCore
import SwiftUI

/// In-progress label. Sweeps a highlight across the text; static under Reduce Motion.
struct ShimmerText: View {
    private let text: String
    private let font: NSFont
    @Environment(\.accessibilityReduceMotion) private var systemReduceMotion
    @Environment(\.memexReduceMotion) private var appReduceMotion

    init(_ text: String, font: NSFont = .systemFont(ofSize: NSFont.systemFontSize)) {
        self.text = text
        self.font = font
    }

    var body: some View {
        // The hidden Text gives SwiftUI the size, truncation width and text baseline it uses for
        // alignment; the AppKit view drawn over it renders the same line with the sweep.
        Text(text).font(Font(font)).lineLimit(1).truncationMode(.tail)
            .hidden()
            .overlay {
                ShimmerLabel(text: text, font: font, systemReduceMotion: systemReduceMotion, appReduceMotion: appReduceMotion)
            }
            .accessibilityElement(children: .ignore)
            .accessibilityLabel(text)
            .accessibilityAddTraits(.isStaticText)
    }
}

/// Sweep timing and geometry, free of AppKit state so it can be tested directly.
enum ShimmerSweep {
    /// Seconds per pass, between FlowDown's 1.5 s and Codex's 2 s.
    static let period: CFTimeInterval = 1.7
    /// Opacity of the resting text under the moving highlight.
    static let restingOpacity: CGFloat = 0.5
    static let bandFraction: CGFloat = 0.3
    /// Short labels still get a band wide enough to read as a sweep rather than a flicker.
    static let minimumBandWidth: CGFloat = 24

    enum Mode: Equatable {
        /// Plain secondary text: Reduce Motion is on.
        case still
        /// Dimmed like the sweep but not moving, because nothing on screen would show it.
        case paused
        case sweeping
    }

    static func mode(systemReduceMotion: Bool, appReduceMotion: Bool, onScreen: Bool) -> Mode {
        if systemReduceMotion || appReduceMotion { return .still }
        return onScreen ? .sweeping : .paused
    }

    /// Seconds into the current pass at `time` on the shared media clock. Every label computes the
    /// same value at the same instant, so labels stay in step and a rebuilt or re-texted label
    /// resumes mid-sweep instead of restarting from the leading edge.
    static func phase(at time: CFTimeInterval) -> CFTimeInterval {
        let elapsed = time.truncatingRemainder(dividingBy: period)
        return elapsed < 0 ? elapsed + period : elapsed
    }

    /// Highlight width in points for text `textWidth` points wide.
    static func bandWidth(forTextWidth textWidth: CGFloat) -> CGFloat {
        max(minimumBandWidth, textWidth * bandFraction)
    }

    /// Gradient stops, in units of the text width, at the start and end of a pass: the band begins
    /// wholly before the text and ends wholly after it, so every glyph is swept whatever the width.
    static func locations(forTextWidth textWidth: CGFloat) -> (from: [Double], to: [Double]) {
        let band = textWidth > 0 ? Double(bandWidth(forTextWidth: textWidth) / textWidth) : 1
        return ([-band, -band / 2, 0], [1, 1 + band / 2, 1 + band])
    }
}

private struct ShimmerLabel: NSViewRepresentable {
    let text: String
    let font: NSFont
    let systemReduceMotion: Bool
    let appReduceMotion: Bool

    func makeNSView(context: Context) -> ShimmerTextView { ShimmerTextView() }

    func updateNSView(_ view: ShimmerTextView, context: Context) {
        view.update(text: text, font: font, systemReduceMotion: systemReduceMotion, appReduceMotion: appReduceMotion)
    }

    /// Fills the frame of the hidden Text it overlays.
    func sizeThatFits(_ proposal: ProposedViewSize, nsView view: ShimmerTextView, context: Context) -> CGSize? {
        proposal.replacingUnspecifiedDimensions(by: view.intrinsicContentSize)
    }
}

/// AppKit body of `ShimmerText`: dim text with a brighter copy revealed through a moving gradient
/// mask. The sweep is a Core Animation animation, so it keeps moving while the main thread is busy.
final class ShimmerTextView: NSView {
    let restingText = ShimmerGlyphs()
    let highlightText = ShimmerGlyphs()
    let sweepLayer = CAGradientLayer()
    private static let sweepKey = "shimmer.sweep"
    private var systemReduceMotion = false
    private var appReduceMotion = false
    /// Untruncated width of the text, measured when the text or font changes.
    private var measuredWidth: CGFloat = 0
    /// Text width the running animation was built for; nil while no animation is attached.
    private var sweptWidth: CGFloat?
    private let accessibilityObservers = NotificationObservers()
    private let windowObservers = NotificationObservers()

    override init(frame: NSRect) {
        super.init(frame: frame)
        wantsLayer = true
        addSubview(restingText)
        addSubview(highlightText)
        highlightText.wantsLayer = true
        sweepLayer.startPoint = CGPoint(x: 0, y: 0.5)
        sweepLayer.endPoint = CGPoint(x: 1, y: 0.5)
        let clear = CGColor(gray: 1, alpha: 0)
        sweepLayer.colors = [clear, CGColor(gray: 1, alpha: 1), clear]
        // At rest the band sits before the text, so a removed animation leaves no highlight.
        sweepLayer.locations = [0, 0, 0]
        sweepLayer.actions = ["bounds": NSNull(), "position": NSNull(), "locations": NSNull()]
        highlightText.layer?.mask = sweepLayer
        // The SwiftUI wrapper is the accessibility element; these views only draw.
        setAccessibilityElement(false)
        accessibilityObservers.add(NSWorkspace.accessibilityDisplayOptionsDidChangeNotification,
                                   center: NSWorkspace.shared.notificationCenter) { [weak self] in self?.updateSweep() }
    }

    @available(*, unavailable) required init?(coder: NSCoder) { fatalError() }

    func update(text: String, font: NSFont, systemReduceMotion: Bool, appReduceMotion: Bool) {
        if restingText.text != text || restingText.font != font {
            for glyphs in [restingText, highlightText] {
                glyphs.text = text
                glyphs.font = font
            }
            measuredWidth = ceil(restingText.attributedText.size().width)
            invalidateIntrinsicContentSize()
            needsLayout = true
        }
        self.systemReduceMotion = systemReduceMotion
        self.appReduceMotion = appReduceMotion
        updateSweep()
    }

    override var intrinsicContentSize: NSSize {
        let font = restingText.font
        return NSSize(width: measuredWidth, height: ceil(font.ascender - font.descender + font.leading))
    }

    /// The swept span: the whole text, or the visible part of it when truncated.
    private var textWidth: CGFloat { min(bounds.width, measuredWidth) }

    override func layout() {
        super.layout()
        restingText.frame = bounds
        highlightText.frame = bounds
        CATransaction.begin()
        CATransaction.setDisableActions(true)
        sweepLayer.frame = CGRect(x: 0, y: 0, width: textWidth, height: bounds.height)
        if highlightText.layer?.mask !== sweepLayer { highlightText.layer?.mask = sweepLayer }
        CATransaction.commit()
        updateSweep()
    }

    override func viewWillMove(toWindow newWindow: NSWindow?) {
        super.viewWillMove(toWindow: newWindow)
        windowObservers.removeAll()
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        if let window {
            windowObservers.add(NSWindow.didChangeOcclusionStateNotification, object: window) { [weak self] in
                self?.updateSweep()
            }
        }
        updateSweep()
    }

    override func viewDidHide() { super.viewDidHide(); updateSweep() }
    override func viewDidUnhide() { super.viewDidUnhide(); updateSweep() }

    private var isOnScreen: Bool {
        guard let window, !isHiddenOrHasHiddenAncestor else { return false }
        return window.occlusionState.contains(.visible)
    }

    private func updateSweep() {
        let mode = ShimmerSweep.mode(
            systemReduceMotion: systemReduceMotion || NSWorkspace.shared.accessibilityDisplayShouldReduceMotion,
            appReduceMotion: appReduceMotion, onScreen: isOnScreen)
        restingText.alphaValue = mode == .still ? 1 : ShimmerSweep.restingOpacity
        highlightText.isHidden = mode == .still
        let width = textWidth
        guard mode == .sweeping, width > 0 else {
            sweepLayer.removeAnimation(forKey: Self.sweepKey)
            sweptWidth = nil
            return
        }
        guard sweptWidth != width || sweepLayer.animation(forKey: Self.sweepKey) == nil else { return }
        let stops = ShimmerSweep.locations(forTextWidth: width)
        let sweep = CABasicAnimation(keyPath: "locations")
        sweep.fromValue = stops.from
        sweep.toValue = stops.to
        sweep.duration = ShimmerSweep.period
        sweep.timingFunction = CAMediaTimingFunction(name: .linear)
        sweep.repeatCount = .infinity
        sweep.isRemovedOnCompletion = false
        // Start the pass at the last period boundary of the media clock, so a new width or a new
        // view picks the sweep up where every other label currently is.
        let now = CACurrentMediaTime()
        sweep.beginTime = sweepLayer.convertTime(now, from: nil) - ShimmerSweep.phase(at: now)
        sweepLayer.add(sweep, forKey: Self.sweepKey)
        sweptWidth = width
    }
}

/// One line of secondary-colour text, tail-truncated to the view's width. Drawing it directly keeps
/// its metrics identical to the SwiftUI Text it overlays and resolves the colour per appearance.
final class ShimmerGlyphs: NSView {
    var text = "" { didSet { needsDisplay = true } }
    var font = NSFont.systemFont(ofSize: NSFont.systemFontSize) { didSet { needsDisplay = true } }

    override var isFlipped: Bool { true }

    var attributedText: NSAttributedString {
        let paragraph = NSMutableParagraphStyle()
        paragraph.lineBreakMode = .byTruncatingTail
        return NSAttributedString(string: text, attributes: [
            .font: font, .foregroundColor: NSColor.secondaryLabelColor, .paragraphStyle: paragraph,
        ])
    }

    override func draw(_ dirtyRect: NSRect) {
        attributedText.draw(with: bounds, options: [.usesLineFragmentOrigin, .truncatesLastVisibleLine])
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }
}

/// Block observers that unregister when their owner is released; `deinit` needs no actor.
private final class NotificationObservers {
    private var tokens: [(center: NotificationCenter, token: NSObjectProtocol)] = []

    func add(_ name: Notification.Name, object: AnyObject? = nil, center: NotificationCenter = .default,
             perform action: @escaping @MainActor @Sendable () -> Void) {
        let token = center.addObserver(forName: name, object: object, queue: .main) { _ in
            MainActor.assumeIsolated { action() }
        }
        tokens.append((center, token))
    }

    func removeAll() {
        for entry in tokens { entry.center.removeObserver(entry.token) }
        tokens = []
    }

    deinit { removeAll() }
}
