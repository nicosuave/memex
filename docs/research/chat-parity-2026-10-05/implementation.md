# Implementation follow-through

This follows the 24 actionable Memex rows in [findings.json](findings.json).
The original inventory remains a record of the inspected baseline. Its comparator
defects, unknown cloud/performance comparisons, and excluded sidebar snooze are
not silently reclassified as implemented features.

The code is in the native Memex app, its execution host and web client, and the
pinned SQACP runtime. Features depend on the runtime-enabled build and advertised
provider/host capabilities where stated below. Component acceptance has passed;
the final immutable package and installed-app checks are recorded below.

| Finding | Implemented behavior and boundary | Regression evidence |
| --- | --- | --- |
| `codex-conversation-11` | Typed MCP form controls and schema validation; accept carries values. Unsupported schemas expose decline/cancel, never empty acceptance. | Runtime `AgentElicitationTests`, Codex typed form wire-response test. |
| `remote-panels` | Authenticated host files, conflict-checked atomic text saves, staged/unstaged diff, and host-owned PTY panels. Remote identifiers never become viewer filesystem paths. The PTY viewer is plain text, not a full-screen terminal emulator. | `RemoteWorkspaceAccessTests`, `RemoteWorkspaceDraftsTests`, existing host authorization/adapter suites. |
| `codex-conversation-14` | Native interactive MCP result host with exact call/server/session authority, explicit tool approval, reconnect invalidation, and saved-result fallback. | Native `NativeMcpAppTests`; runtime service relay tests and real WebKit bridge tests. |
| `cx-p01` | Per-installation Codex/Claude MCP and plugin inventory, configuration, authentication, installation/removal; native provider contracts and scopes retained. ACP configuration remains provider-owned. | `ProviderToolsAdministrationTests`, `ClaudeNativeConfigurationTests`, native CLI contract inspection. |
| `codex-conversation-03` | Native on-device dictation with retained audio, retry and recovery, inserted into the draft without sending. Microphone/speech permissions remain explicit. | `ConversationMediaTests`; packaged permission descriptions and entitlements. |
| `codex-workspace-11` | Same-chat workspace relocation and explicit host handoff preserve native identity, Git/index/working files, captured state, and exclusive ownership. Interrupted transfers require state inspection/recovery. | `HostHandoffTests`, `NativeConversationRetirementTests`, runtime workspace relocation tests; native transfer stages history outside provider lookup and rebinds the exact original path on return. |
| `app-control` | Existing opt-in execution and browser control MCP retained. Custom native panel, preferences, and organization controls and their permission UI were removed at the user's request on October 6. | `ExecutionHostTests.testDesktopControlOperationsAreUnavailable`; existing browser and host API tests. |
| `codex-conversation-02` | Captured attachment annotations retain comment identity, source/location and bytes. Editing an annotation updates local draft context. | `ConversationMediaTests`, attachment/composer regressions. |
| `browser` | Granted navigation/history/reload/wait/DOM keys/tab selection plus separately granted, bounded WebKit viewport recording to real MP4. Recording stays on the desktop host. | `WorkspaceBrowserTests`, `WorkspaceBrowserRecordingTests` with real WebKit and video decoding. |
| `codex-workspace-02` | Explicit recoverable raw-file/index/HEAD snapshots; recovery creates a separate owned checkout. Clean pruning snapshots first and still refuses dirty/ignored/referenced checkouts. No automatic destructive retention policy. | `WorkspaceSnapshotAndPRTests`, existing Git lifecycle tests. |
| `codex-conversation-07` | Full plan markdown/identity, editable local plan document, conflict-checked save and captured saved-version provenance for implementation drafts. | `ConversationPlanTests`, metadata projection tests, concurrent/failing initial publication tests. |
| `t3-i02` | Bounded image normalization supports common native image inputs while retaining exact supported small-image bytes and captured attachment identity. | `ConversationMediaTests`, existing attachment limit tests. |
| `pr-review` | Native PR details, diff, reviews and inline-thread reading with actual GitHub links and captured review context. Large/paginated responses disclose limits. No external review posting. | `WorkspaceSnapshotAndPRTests`; read-only GitHub command validation. |
| `codex-workspace-08` | Custom external-app Accessibility automation and its permission UI were removed at the user's request on October 6. No native desktop-app control integration is provided. | `ExecutionHostTests.testDesktopControlOperationsAreUnavailable`. |
| `cx-p03` | Custom sections and read/unread state extend existing library metadata, sidebar menus, and bulk actions. Removing a section preserves its conversations. | `ConversationLibraryTests`. |
| `codex-conversation-12` | Supported policy amendment decisions retain their exact native payload; approval-review lifecycle and rationale remain visible with raw evidence. Provider-resolved requests invalidate stale replies. | `AgentElicitationTests`, Codex review and resolved-question transport tests. |
| `t3-i12` | Provider setup shows exact configured installations, availability and negotiated capabilities. Native Codex/Claude and explicit ACP integrations remain distinct. Dedicated additional drivers require demonstrated demand, as the recommendation specifies; history support does not claim execution support. | Provider catalog/configuration tests; native installation contract inspection. |
| `t3-i05` | Explicitly labeled Stop and restart fallback waits for acknowledged stop/terminal state, preserves draft attachments, and never pretends to be native steering. | `ConversationQueueTests`. |
| `schedules` | Fresh-chat targets, named event triggers, durable occurrence records, read/triage inbox, and per-schedule notification policy extend the existing host scheduler. Global notification opt-in remains authoritative. | `HostScheduleRunTests`, `ExecutionScheduleDraftTests`, `HostScheduleObserverTests`, web schedule tests. |
| `codex-workspace-09` | Integrated SSH tunnel onboarding preserves exact hostname/host identity; failed pairing only stops a tunnel created by that attempt. | `RemoteWorkspaceSSHTests`, host pairing/identity tests. |
| `keyboard-settings` | Persistent shortcut editing/conflict checks and appearance preferences, including text/code size, native high contrast, and reduced motion. System accessibility settings remain effective. | `AppPreferencesTests`, `MemexAppearanceTests`. |
| `questions` | Existing native-ID grouped replies and captured UTF-8 context retained. Async provider resolution clears requests. Unsupported ordinary-question media or independent dismissal is not fabricated; typed MCP forms have their own cancel operation. | Existing native question tests and runtime grouped-answer/resolution tests. |
| `subagents` | Verified child history opens through the parent runtime before indexing; full timeline/media/tool identity and authoritative model/effort/status remain separate from parent history. | Runtime child reducer/service/transport tests; native child projection tests. |
| `codex-conversation-05` | Reviewed queue-to-side-chat transfer preserves captured bytes and command identity, creates an unsent child draft, and never duplicates uncertain creation or dispatch. | `StoreQueueTransferTests`, queue tests, durable viewer-restore regressions. |

## Acceptance status

- October 6 transcript correction removes the outline and viewport quote button.
  Selecting transcript text now offers Add to chat for the exact highlighted text,
  including code; it preserves the draft and reports the actual attachment error.
  Seven selection tests (eight cases), 15 reader tests, and seven toolbar tests
  passed. Filters hide with the sidebar and restore without clearing their state.
  The signed installed app was checked with an actual text double-click, ownership
  error display, and sidebar close/reopen; the outline and header quote are absent.
- October 6 desktop-control removal passed 15 execution-host tests and 10 browser
  and session-tool navigation tests. All five removed desktop-control methods
  return `method_not_found`; the host no longer advertises desktop controls.
  Formatting and Clippy also passed. Earlier acceptance results follow.
- Runtime agent-runtime checks passed. Final merged SQACP checks passed 383 tests
  (two existing gated skips) and five real WebKit tests. Deterministic closed-input
  regressions cover the repaired native provider SIGPIPE crash on both transports.
- The first native history-only pass verified provider tools/configuration,
  schedules, library metadata, appearance, snapshots and PR context.
- Final runtime-enabled native validation passed in complementary runs: 481
  Swift Testing cases plus 14 focused sidebar/organization cases, and 56 XCTest
  cases covering host/SSH behavior. Existing live-gated cases stayed skipped.
  Two separately reproduced AppKit baseline tests remained excluded:
  `nativeFilterPopoverUsesAccentWhileOpenAndResetsOnClose` and
  `codeWheelRoutesVerticalGesturesToTranscriptAndKeepsHorizontalPanning`.
  The 1,603-project sidebar scrolling/sorting test passed after replacing repeated
  library scans during row/menu construction with per-render lookups.
- That pass found real draft restoration data loss, now fixed: the observed draft
  text is restored only after attachments, queue and pending intent are complete.
  Follow-up durability tests retain exact bytes and command identities.
- Cross-host ownership review found stale activation and premature native lookup
  publication risks. Repaired handoff, retirement, remote file and PTY tests
  passed, including a native transfer test with no provider prompt. A separate
  disposable Codex probe verified that retired history cannot be resumed by its
  original UUID or path after restart.
- Focused web schedule tests, web typecheck/production build, Rust formatting and
  Clippy passed. CI found streaming selection loss. Font descriptor comparison
  now prevents equivalent system fonts from triggering a full reload, while real
  font-size changes still invalidate layout. Both streaming cases, including a
  recreated font, and the font-size regression passed locally.
- The final runtime lock is `1671045ec81e37a133b4e829e3d04fdf572cf11a`.
  Locked preparation, native packaging, strict code-signature verification and a
  packaged execution-host/control-MCP round trip passed. The installed
  `/Applications/Memex.app` executable matches the packaged executable and
  launched successfully; Home and the inline attachment/dictation menu were
  inspected. Appearance and keyboard settings were also inspected during this
  delivery. Providers & Tools live inspection was limited by a native UI-tool
  pipe failure; the app remained alive, and its component tests passed.
- Private runtime CI at the locked revision is blocked before execution by the
  GitHub account billing/spending limit. This is separate from the passing local
  runtime validation above.

Prior baseline UI or embedding-test failures are not attributed to these changes
without reproduction. Source-based boundaries and fixture tests are not claims
of successful live provider, OAuth, microphone, or OS permission interaction.
