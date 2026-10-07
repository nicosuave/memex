import SwiftUI

/// A quiet index beside the transcript. Previewing never changes the reading position.
struct ConversationOutlineRail: View {
    // With a 4pt leading inset, this fits inside the reading lane's 30pt margin.
    static let railWidth: CGFloat = 24
    let outline: ConversationOutline
    var previewWidth: CGFloat = 300
    let reveal: (ConversationPrompt) -> Void
    @State private var hoveredID: String?
    @FocusState private var focusedID: String?

    private var previewID: String? { hoveredID ?? focusedID }

    var body: some View {
        ScrollViewReader { proxy in
            ScrollView(.vertical) {
                LazyVStack(alignment: .leading, spacing: 0) {
                    ForEach(outline.prompts) { prompt in
                        let emphasized = prompt.id == outline.selectedID || prompt.id == previewID
                        Button { reveal(prompt) } label: {
                            Capsule()
                                .fill(.primary.opacity(emphasized ? 0.85 : 0.25))
                                .frame(width: emphasized ? 20 : 12, height: 2)
                                .frame(width: Self.railWidth, height: 12, alignment: .leading)
                                .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)
                        .focused($focusedID, equals: prompt.id)
                        .onHover { hovering in
                            if hovering { hoveredID = prompt.id }
                            else if hoveredID == prompt.id { hoveredID = nil }
                        }
                        .anchorPreference(key: OutlinePreviewAnchor.self, value: .bounds) {
                            prompt.id == previewID ? $0 : nil
                        }
                        .accessibilityLabel("Go to prompt: \(prompt.preview)")
                        .accessibilityValue(prompt.id == outline.selectedID ? "Current prompt" : "")
                        .id(prompt.id)
                    }
                }
                .padding(.vertical, 8)
            }
            .scrollIndicators(.hidden)
            .onChange(of: outline.selectedID) { _, id in
                // No animated scrolling: this also respects Reduce Motion.
                if let id { proxy.scrollTo(id, anchor: .center) }
            }
        }
        .frame(width: Self.railWidth)
        .overlayPreferenceValue(OutlinePreviewAnchor.self) { anchor in
            GeometryReader { geometry in
                if let anchor, let prompt = outline.prompts.first(where: { $0.id == previewID }) {
                    Text(prompt.preview)
                        .font(.callout)
                        .lineLimit(1)
                        .padding(.horizontal, 12)
                        .frame(width: max(80, previewWidth), height: 38, alignment: .leading)
                        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 12))
                        .overlay {
                            RoundedRectangle(cornerRadius: 12).strokeBorder(.primary.opacity(0.08))
                        }
                        .shadow(color: .black.opacity(0.12), radius: 8, y: 3)
                        .position(x: Self.railWidth + 8 + max(80, previewWidth) / 2,
                                  y: min(max(19, geometry[anchor].midY), max(19, geometry.size.height - 19)))
                        .allowsHitTesting(false)
                        .accessibilityHidden(true)
                }
            }
            .allowsHitTesting(false)
        }
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Conversation outline")
    }
}

private struct OutlinePreviewAnchor: PreferenceKey {
    static let defaultValue: Anchor<CGRect>? = nil
    static func reduce(value: inout Anchor<CGRect>?, nextValue: () -> Anchor<CGRect>?) {
        if let next = nextValue() { value = next }
    }
}
