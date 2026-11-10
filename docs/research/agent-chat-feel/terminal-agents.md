# Terminal coding agents: interaction and rendering conventions

Scope: Claude Code, pi, Crush, Amp, Gemini CLI, plus brief notes on Copilot CLI, Droid, Cursor CLI, Aider. Codex TUI and OpenCode are excluded (owned by other researchers). Research date: 2026-11-09. Read-only; nothing from the clones was built or run.

## Sources and labels

Every claim is tagged **[V]** (VERIFIED: path:line at the SHA below, or an official doc/changelog URL I fetched) or **[U]** (UNVERIFIED: secondary source, third-party reimplementation, or inference; URL given).

| Short name | What | SHA / URL |
|---|---|---|
| `CC-CL:<line>` | `CHANGELOG.md` in github.com/anthropics/claude-code (newest entry 2.1.321). Version in brackets. | `fd1f3c4263e54dc6128c25d6117f5c079ec9c38b` |
| `CC-docs/<page>` | https://code.claude.com/docs/en/<page> (fetched as `.md` 2026-11-09; copies in `scratchpad/research/cc-docs/`) | n/a |
| `pi:` | github.com/badlogic/pi-mono | `4ea8837927564c592b1d184c956ab7d28c88119b` |
| `crush:` | github.com/charmbracelet/crush | `1b37350c8afb8d905cfa67a941ba495d554d4141` |
| `gemini:` | github.com/google-gemini/gemini-cli | `1930f25394c4b6338b64ffdb43083fca41614798` |
| `pcs:` | github.com/ouzhenkun/pi-claude-shimmer (third-party MIT reimplementation of Claude Code's spinner; NOT Anthropic code) | `d328e8a4dacb97f157b7f64397c0dac279adb10d` |
| `amp-docs/<page>` | https://ampcode.com/docs/markdown/<page> (copies in `scratchpad/research/amp/`) | n/a |

Clones live under `/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/tui-{claude-code,pi,crush,gemini,pi-claude-shimmer}`.

Not consulted on purpose: an unofficial mirror of leaked Claude Code source exists (tangled.org/oppi.li/claude-code). The brief limited Claude Code sources to docs, changelog and issues, so Claude Code animation constants below come from the changelog (official) or from the open reimplementation `pcs:` (unverified against the real binary).

---

## 1. Claude Code

### 1.1 Working indicator

Anatomy as documented: an animated glyph, a rotating whimsical verb, a parenthesised status (elapsed, token counter, thinking state, interrupt hint), and an optional tip line.

- Verb: "the spinner shows a rotating verb" such as Accomplishing / Architecting / Baking; customisable with `spinnerVerbs` (add or replace). **[V]** CC-docs/settings-reference `#spinnerverbs`; added 2.1.23 CC-CL:8705.
- Tips: the spinner line rotates short feature tips; `spinnerTipsEnabled`, `spinnerTipsOverride` (own tips, `excludeDefault`, cooldown/priority entries). **[V]** CC-docs/settings-reference; CC-CL:9609 [1.0.112], 8524 [2.1.45], 4323 [2.1.247]. When the API is overloaded a status-page link replaces the tip. **[V]** CC-CL:5602 [2.1.198].
- Shimmer: "New shimmering spinner" [1.0.83] CC-CL:9680; thinking status text got "a subtle shimmer animation" [2.1.20] CC-CL:8754. **[V]**
- Animation clock: the spinner runs on a 50 ms animation loop that was isolated from the rest of the UI tree to cut render/CPU overhead [2.1.69] CC-CL:8260. **[V]** So the official frame budget for the indicator is 20 fps.
- Colour as state: warms to amber after 10 s of thinking with no visible progress [2.1.141] CC-CL:6528; turns red when an auto-mode permission check stalls [2.1.126] CC-CL:6845; VS Code turns red with "Not responding" after 60 s of backend silence [2.1.83] CC-CL:7803. Distinct colour count was capped twice because many colours corrupted VS Code's terminal [2.1.144] CC-CL:6410, [2.1.154] CC-CL:6216. **[V]**
- Thinking progress is inline in the spinner text ("still thinking", "thinking more", "almost done thinking") replacing a separate hint row [2.1.116] CC-CL:7092; "deep in thought" after 45 s [2.1.271] CC-CL:3525; resets to "thinking" after each tool [2.1.152] CC-CL:6289. **[V]**
- Elapsed and tokens: token counter accumulates subagent/background tokens [2.1.0] CC-CL:9028, 9047; never shows "0 tokens" before the first token [2.1.47] CC-CL:8507; values >= 1M render as "1.5m" [2.1.84] CC-CL:7701; effort level shown in the spinner ("with low effort") [2.1.69] CC-CL:8188. **[V]**
- Hook phases are named in the spinner with elapsed time ("Running PreCompact hooks…", SessionStart/UserPromptSubmit/PreToolUse) [2.1.271] CC-CL:3524. **[V]**
- Task-aware label: when a task list exists the spinner shows the current task's active form and a "Next:" line, held to one terminal row [2.1.268] CC-CL:3709; falls back to the task subject [2.1.69] CC-CL:8282. **[V]**
- Per-tool liveness: a live elapsed counter on the collapsed tool summary line so long tools "visibly tick instead of looking stuck" [2.1.210] CC-CL:5285; later dimmed so it does not compete with bold counts [2.1.234] CC-CL:4711; Bash shows timeout alongside elapsed [2.1.23] CC-CL:8713. **[V]**
- "esc to interrupt" is a footer hint (CC-CL:5965 [2.1.169], 7516 [2.1.92]). **[V]**
- Completion line: past-tense "Worked for 5s" style message after the turn [2.1.144] CC-CL:6440. **[V]**
- Layout stability: "Reduced layout jitter … when the spinner appears and disappears during streaming" [2.1.31] CC-CL:8658; terminal-title busy glyphs changed to reduce tab-bar jitter [2.1.228] CC-CL:4847. **[V]**
- Reduced motion: `prefersReducedMotion` ("Reduce motion" in /config) reduces or disables spinner, shimmer and flash; several fixes made every animated element honour it (CC-CL:8668 [2.1.30], 6086 [2.1.161], 2209 [2.1.287]). **[V]** CC-docs/settings-reference `#prefersreducedmotion`.
- Glyph cycle `· ✢ ✳ ✶ ✻ ✽` played forward then backward (ping-pong), ~120 ms per glyph. **[U]** `pcs:index.ts:42-48,357`; also described at medium.com/@kyletmartinez/reverse-engineering-claudes-ascii-spinner-animation-eec2804626e0 (403 on fetch). One official hint that the glyphs differ in visual size: "Fixed the activity spinner's pulse dwelling on the wrong glyph size in Ghostty" [2.1.181] CC-CL:5825 **[V]**.
- Shimmer algorithm and constants: see section 6 (unverified).

### 1.2 Thinking display

- Default is a collapsed stub ("Thought for 4s" style row, clickable in fullscreen CC-CL:2312 [2.1.286]); `showThinkingSummaries: true` restores summaries, expanded with Ctrl+O. **[V]** CC-docs/settings-reference `#showthinkingsummaries`; default changed in 2.1.89 CC-CL:7610.
- When shown, summaries in a collapsed group stay readable for at least 3 s, render as markdown, cap at 10 lines; "Thinking for Ns" counts up live and keeps its value on interrupt [2.1.152] CC-CL:6282-6283. **[V]**

### 1.3 Queueing and steering

Official semantics (CC-docs/interactive-mode `#queue-messages-while-claude-works`) **[V]**:

- Enter while working queues; it never interrupts. Queued entries are listed in the conversation (above the spinner since 2.1.281, CC-CL:2938) and both sent and queued messages render gray until the model starts on them (CC-CL:3195 [2.1.275]; prompts sent while idle show normal colour immediately, CC-CL:2331 [2.1.286]).
- Delivery: a plain message queued while tool calls run is passed to the model as soon as those tool calls finish, inside the same turn (steer at tool boundary). If the turn ends first, queued messages go out automatically in typed order. Slash commands and `!` shell commands are held to turn end and run one at a time.
- Send now: Ctrl+Enter (or Ctrl+X Ctrl+S; action `chat:sendNow`) sends queued messages plus the draft immediately. If the running work can move to the background (shell, subagents) it is backgrounded and the model reads the messages in the same turn; if the model is only writing a response the turn is interrupted and the messages go next (since 2.1.281; before that it always interrupted). In fullscreen the "to send now" hint under a queued message is clickable (CC-CL:633 [2.1.312]).
- Take back: Up from the first input line removes all queued entries and puts them into the input, one per line, ahead of the draft; re-Enter queues them as one entry. Queued images survive the round trip (CC-CL:8111). Esc with a queue is version-dependent and the sources disagree: current docs say Esc interrupts the turn, keeps the queue, and sends it right away, while changelog entries describe Esc pulling queued prompts back into the input without cancelling (CC-CL:8964 [2.1.0], 6977 [2.1.119] "press Esc again to interrupt", 1626 [2.1.295] "pulled back into the prompt with ↑ or Esc"). Treat "Up = take back" as the stable convention and Esc-with-queue as unsettled.
- Commands that apply immediately rather than queue: `/model` (next request in the turn), `/effort`, `/fast`, `/status`, `/rename`, `/permissions`, `/add-dir` (CC-CL:8679, 7783, 8173, 4703-4704).
- History: "Hit Enter to queue up additional messages" [0.2.75] CC-CL:10124; "send messages to Claude while it works to steer Claude in real-time" [0.2.108] CC-CL:10070. **[V]**
- Queue bugs worth learning from (each was a shipped defect): queued commands flickering during streaming (CC-CL:7757), queued message briefly shown twice during multi-tool turns (CC-CL:7224), input cleared when a queued command was processed while the user typed (CC-CL:9129), user input briefly disappearing after submit before the message rendered (CC-CL:8359), queued prompts concatenated without newline (CC-CL:7903), messages typed mid-turn not persisted to transcript (CC-CL:7442), a message sent during extended thinking interrupting the thinking (CC-CL:8621), race where Esc with a queued prompt let the next turn finish early (CC-CL:4507). **[V]**
- VS Code/desktop host conventions: queued message waits at the bottom of the conversation until Claude starts on it (CC-CL:3272); screen-reader announcement "Message queued." (CC-CL:2018); web lets you remove a queued message or press Esc/Up to return it to the box (CC-CL:3658). **[V]**

### 1.4 Interrupt, rewind, checkpoints

- Esc: stop the response or tool call mid-turn; work so far is kept; queued messages are sent next; on a dialog it closes the dialog; on a permission prompt it declines (same as No without comment). Ctrl+C also interrupts. **[V]** CC-docs/interactive-mode (General controls).
- Interrupt before any response restores the prompt to the input for editing [2.1.83] CC-CL:7782; Up after an interrupt restores the prompt and rewinds in one step [2.1.73] CC-CL:8054; partial assistant text is preserved in history on interrupt (CC-CL:7496); "Interrupted" is grey, not red [2.1.0] CC-CL:9054. **[V]**
- Esc Esc: clears a non-empty draft (saved to history, Up recalls it); with an empty input it opens the rewind menu. **[V]** CC-docs/interactive-mode, CC-docs/checkpointing.
- Rewind menu: lists the user's prompts; actions are Restore code and conversation / Restore conversation / Restore code / Summarize from here / Summarize up to here / Never mind. Code options appear only when the checkpoint has tracked file changes. After a conversation restore the selected prompt is put back in the input. **[V]** CC-docs/checkpointing.
- Checkpoints: one per prompt that starts a turn; 100 most recent kept; only edits by Claude's file tools are tracked (Bash-only changes and most subagent edits are not); a message sent mid-turn has no checkpoint of its own ("No code restore"). **[V]** CC-docs/checkpointing.

### 1.5 Plan mode and approval

- Shift+Tab cycles `default` (labelled Manual) -> `acceptEdits` -> `plan` (-> `bypassPermissions` -> `auto` when available). `/plan [description]` enters directly. **[V]** CC-docs/interactive-mode; CC-CL:8068, 8982.
- Approval card options: "Yes, and use auto mode" (or "Yes, auto-accept edits" when auto is unavailable), "Yes, manually approve edits", "No, keep planning" (free-text feedback, multi-line since 2.1.69 CC-CL:8213). Ctrl+G opens the plan in `$EDITOR` to edit before proceeding; edited plans are labelled "(edited by user)" (CC-CL:5296). Optional first entry clears planning context (`showClearContextOnPlanAccept`, hidden by default since 2.1.81). Shift+Tab on the card picks auto-accept (CC-CL:8932). **[V]** CC-docs/permission-modes `#review-and-approve-a-plan`.
- Plan approval never auto-resolves on idle. **[V]** CC-docs/tools-reference (AskUserQuestion section).
- SDK shape: `ExitPlanMode` tool; output `{plan, filePath?, planWasEdited?, …}`; reaches the host through `canUseTool`. **[V]** CC-docs/agent-sdk/typescript `#exitplanmode`.

### 1.6 AskUserQuestion

- Multiple-choice questions; each has `question`, `header` (max 12 chars), 2-4 `options` (`label`, `description`, optional `preview`), `multiSelect`; an "Other" free-text row and a notes field exist. **[V]** CC-docs/agent-sdk/user-input `#question-format`.
- Single-select auto-submits on the last question, skipping the review screen (CC-CL:9240 [2.0.55]); "Chat about this" option exists (CC-CL:6707); Ctrl+G external editor in "Other" (CC-CL:8848); Esc in notes returns to selection instead of aborting (CC-CL:6415); no auto-continue by default, optional 60s/5m/10m timeout with a 20 s countdown that pauses while the window is focused (CC-CL:5526; CC-docs/tools-reference). **[V]**
- Host integration: arrives via `canUseTool` with `toolName === "AskUserQuestion"`; `toolConfig.askUserQuestion.previewFormat: "html"` makes the model emit HTML previews per option. **[V]** CC-docs/agent-sdk/user-input.

### 1.7 Task list (todos)

- Ctrl+T toggles; shows up to five tasks with pending / in-progress / complete indicators; persists across compaction; expanded state restored on resume. **[V]** CC-docs/interactive-mode `#task-list`.
- Task tools are off by default on newer models (Opus 4.8+, Sonnet 5+, Fable) unless `CLAUDE_CODE_ENABLE_TODO_TOOLS=1` (CC-CL:4735 [2.1.233], 3736 [2.1.268]). A GUI must not assume a todo list exists. **[V]**
- Task updates no longer re-render the whole UI (CC-CL:5357); ordering fixed when several tasks are created at once (CC-CL:6391). **[V]**

### 1.8 Permission prompts

- Bash prompt layout: description, the command between dashed lines, "This command requires approval", "Do you want to proceed?" with Yes / Yes, and don't ask again for `<prefix> *` / Yes, and switch to auto mode / No; footer "Esc to cancel · Tab to amend". **[V]** CC-docs/permissions (image alt text and following paragraphs); dashed-line style unified across tools in 2.1.286-287 (CC-CL:2321-2322, 2241).
- Tab on Yes or No opens a comment field: on Yes the comment is delivered after the result; on No it becomes the denial reason and the turn continues; No without comment stops the turn. **[V]** CC-docs/permissions `#add-a-comment-…`.
- Stacking: a "2 of 5" count (CC-CL:2279) and oldest-first ordering so a new prompt never covers the one being read (CC-CL:2255). Background-subagent prompts surface in the main session and name the subagent; Esc denies that one call without stopping the subagent. **[V]** CC-docs/sub-agents.
- Fullscreen scroll rule: permission prompts scroll into view even when auto-scroll is off. **[V]** CC-docs/fullscreen `#auto-follow`.

### 1.9 Background tasks and subagents

- Ctrl+B backgrounds the running Bash command or agent; foreground commands that hit their timeout move to the background instead of dying. **[V]** CC-docs/interactive-mode `#background-bash-commands`.
- Subagents spawned from the main conversation start in the background by default; a subagent panel sits below the prompt (tree rows with `(+N)` descendant counts, Enter to view, x to stop); a successful subagent's row is removed immediately with a 30 s "/tasks to see subagents" footer hint; failed or stopped rows stay 30 s. **[V]** CC-docs/sub-agents `#finished-subagents-…`, line 1082.
- Completion notices are placed where they arrived in the transcript, not under a collapsed tool row (CC-CL:3202); live token counters and elapsed for background agents (CC-CL:4105); permission wait time excluded from a subagent's elapsed (CC-CL:8577); "waiting" instead of a ticking clock when blocked on a nested agent (CC-CL:5808); completion notification includes duration, e.g. "Agent completed · 3h 2m 5s" (CC-CL:6403). **[V]**
- Collapsed groups: "Read N files", "Ran N shell commands", "Queried {server}", "Called slack 3 times"; Ctrl+O or click expands (CC-CL:7829, 7603, 4680; CC-docs/interactive-mode). **[V]**

### 1.10 Renderer: flicker, performance, input latency

Timeline from the changelog **[V]**:

| Version | Change | CC-CL line |
|---|---|---|
| 1.0.123 | Fixed rendering degradation during token streaming | 9561 |
| 2.0.72 | "Reduced terminal flickering" (changelog text is [V]; that this was the renderer rewrite is [U], per steipete.me/posts/2025/signature-flicker) | 9103 |
| 2.1.0 | Faster rendering for emoji/ANSI/Unicode text under native installer/Bun | 9035 |
| 2.1.15, 2.1.69 | React Compiler adopted for UI rendering | 8803, 8261 |
| 2.1.23 | "optimized screen data layout" | 8712 |
| 2.1.53 | Fixed input briefly disappearing after submit before the message rendered | 8359 |
| 2.1.69 | Spinner's 50 ms loop isolated; fixed frame stalls during typing from sync debug-log flushes; fixed flicker from animated elements at the scrollback boundary | 8260, 8247, 8245 |
| 2.1.84 | Reduced flicker when animated tool progress scrolls above the viewport | 7723 |
| 2.1.89 | `CLAUDE_CODE_NO_FLICKER=1`: alt-screen rendering with virtualised scrollback | 7568 |
| 2.1.97 | Focus view (prompt, one-line tool summary with diffstats, final response) | 7424 |
| 2.1.110 | `/tui fullscreen` to switch renderers in-conversation | 7204 |
| 2.1.129, 2.1.166, 2.1.200 | Synchronized output (DEC 2026) forced/enabled for more terminals (Emacs eat, JetBrains, tmux) | 6771, 6006, 5540 |
| 2.1.144 | Garbled output after a missed resize self-heals on next frame | 6408 |
| 2.1.161 | Layout engine JIT profile stabilised | 6100 |
| 2.1.169 | Lower CPU while streaming and during spinner animations | 5976 |
| 2.1.196 | Skip no-op subtree walks per frame during streaming | 5636 |
| 2.1.208 | Task-list updates no longer re-render the entire UI | 5357 |
| 2.1.239 | Fullscreen becomes the default for new installs | 4498 |
| 2.1.257 | Streaming no longer slows as the reply grows; background-agent updates no longer re-render the whole screen | 4149 |
| 2.1.259 | Text measurements reused for resize and first render | 4062 |
| 2.1.267 | Keystrokes no longer wait a frame behind spinner/streaming repaints | 3811 |
| 2.1.269 | Transcript updates no longer reprocess the whole conversation to build collapsed summaries | 3630 |
| 2.1.271 | Large diffs and long transcripts render with fewer slow frames | 3522 |
| 2.1.292, 2.1.296 | Long lists stream faster; syntax-highlighted code blocks cached | 1815, 1514 |
| 2.1.301 | Subagent progress trail updates incrementally | 1165 |

Fullscreen renderer design (CC-docs/fullscreen) **[V]**: draws on the alternate screen, keeps only visible messages in the render tree (flat memory), sends only changed cells per frame, input box pinned to the bottom, in-app mouse selection/click-to-expand/wheel scroll, auto-follow that pauses when the user scrolls up with a floating "Jump to bottom" button showing "N new messages", and a sticky dim header showing the last prompt scrolled out of view. The view stays put when a response finishes while auto-follow is paused. Costs: native Cmd+F and terminal selection stop working, replaced by Ctrl+O transcript mode with `/` search and `[` to dump to scrollback.

History and trade-off commentary **[U]** (steipete.me/posts/2025/signature-flicker): Claude Code started on Ink, rewrote the renderer while keeping React as the component model, and for a long time resisted alt-screen because rebuilt selection/search "would not feel like" the native terminal; Gemini CLI shipped an alt-screen TUI and rolled it back within about a week; Amp moved to its own alt-screen renderer in September 2025.

### 1.11 Prompt suggestions

- First open: a grayed example command derived from git history. After a response: a predicted next prompt as ghost text; Tab or Right arrow places it in the input, typing dismisses it. Generated by a background request on the same model reusing the prompt cache. Skipped when the cache is cold, after an error, in plan mode, near usage limits; shown less often after 20 consecutive unused suggestions. **[V]** CC-docs/interactive-mode `#prompt-suggestions`; CC-CL:2677.
- Enter on an empty input must not auto-submit a suggestion (fixed 2.1.136, CC-CL:6703; an earlier 2.0.70 behaviour did accept-and-submit on Enter, CC-CL:9125). **[V]**
- SDK: `promptSuggestions: true` emits a `prompt_suggestion` message after a turn. **[V]** CC-docs/agent-sdk/typescript line 546, 5963.

### 1.12 What a GUI can do through stream-json / Agent SDK streaming input

All **[V]** from CC-docs/agent-sdk/typescript, /streaming-vs-single-mode, /streaming-output, /user-input unless noted.

| Capability | Mechanism | Limits |
|---|---|---|
| Queue vs steer vs interrupt-and-send | `SDKUserMessage.priority`: `"next"` (default: read at the next tool boundary in the same turn, or starts the next turn), `"later"` (held to turn end, new turn), `"now"` (with `origin:{kind:"human"}`: background movable work and read in the same turn, else interrupt; without that origin: always interrupt). | `"now"` backgrounding needs CLI >= 2.1.286. A backgrounded WebFetch/WebSearch yields `tool_use_result: {detachedToolCall:true}` and no second result, so the tool row must be marked "moved to background". |
| Interrupt | `query.interrupt()`; resolves with `{still_queued: uuid[]}` when capability `interrupt_receipt_v1` is advertised (>= 2.1.205). | Listed messages will still run after the interrupt and may be merged into one turn; resending duplicates them. Only messages sent with a UUID are listed. |
| Interrupt and drop queue | Raw control request `interrupt` with `cancel_queued: true` (capability `interrupt_cancel_queued_v1`, >= 2.1.219); receipt lists `cancelled`. | Not exposed by the SDK `interrupt()` method. I found no documented control request that cancels or edits one specific queued message; cancellation is all-or-nothing at interrupt time. |
| Queue depth | `queued_turn_count` on the result message (human-origin messages still waiting). | SDK >= 0.3.242. |
| Permission mode | `setPermissionMode(mode)`; `applyFlagSettings` for other settings. | Streaming input mode only. |
| Model / effort mid-session | `setModel()` applies on the next model round-trip inside the running turn (CC-CL:5238 [2.1.212]); `applyFlagSettings({effortLevel})`. | Model switch mid-turn can cause a prompt-cache miss (CC-CL:2993). |
| Background running work | `backgroundTasks(toolUseId?)` (same as Ctrl+B), `stopTask(taskId)`. | |
| Partial messages | `includePartialMessages` yields raw API stream events (`message_start`, `content_block_delta` with `text_delta` / `input_json_delta`, …) alongside complete messages. | Main session only: subagent token deltas are not forwarded and `parent_tool_use_id` is always null on stream events. `ping` events every 30 s are liveness during non-streaming fallback (CC-CL:2448). A cut stream must still get `message_stop` (bugs fixed in 2.1.287/2.1.290). |
| Approvals and questions | `canUseTool` callback for tool permission, `AskUserQuestion`, `ExitPlanMode`; `reinitialize()` re-delivers pending permission requests after a transport gap. | |
| Rewind | `rewindFiles(userMessageId, {dryRun})` with `enableFileCheckpointing`. | File restore only; conversation truncation is `resumeSessionAt`. |
| Inject context without a turn | `shouldQuery: false` appends and merges into the next triggering user message. | |
| Echo of queued prompts | `replayUserMessages` replays `queued_command` attachments as `SDKUserMessageReplay` (CC-CL:8779). | |
| Prompt suggestions, context usage | `promptSuggestions`, `getContextUsage()`. | |

Single-message (`-p` one-shot) mode cannot attach images, queue dynamically, or interrupt. **[V]** CC-docs/agent-sdk/streaming-vs-single-mode.

### 1.13 Completion and attention

- Desktop notification (Ghostty, Kitty, iTerm2) or bell when Claude finishes or waits on a prompt and the user appears away; `permission_prompt` fires after about 6 s without a keystroke, `idle_prompt` about 60 s after the response and only if no background agent is still running. **[V]** CC-docs/terminal-config, CC-docs/hooks lines 2524-2550.
- OSC 9;4 progress stays visible across the whole turn (CC-CL:6828); OSC 7501 program status (working / waiting / done) for terminals that implement it. **[V]** CC-docs/terminal-config.
- Session recap: a one-line recap generated in the background after >= 3 min unfocused, capped at 400 chars. **[V]** CC-docs/interactive-mode `#session-recap`.

---

## 2. pi (badlogic/pi-mono)

### 2.1 Differential renderer (`packages/tui`)

- Self-description: "Minimal terminal UI framework with differential rendering and synchronized output". **[V]** pi:packages/tui/README.md:3.
- Main-screen strategy has three paths: first render prints everything without clearing scrollback; a width change or a change above the viewport clears and fully re-renders; a normal update moves the cursor to the first changed line, clears to end, and rewrites from there. Alt-screen mode owns a terminal-height viewport and rewrites only changed rows, follows streaming at the bottom, and keeps a manual scroll position while content grows. Both wrap each frame in `CSI ?2026h … ?2026l`. **[V]** pi:packages/tui/README.md:745-755; implementation pi:packages/tui/src/tui-main-screen.ts:254-377 (`fullRender`, `firstChanged`/`lastChanged` scan, 2026 at :285/:307).
- Frame pacing: `MIN_RENDER_INTERVAL_MS = 16`; `requestRender()` coalesces to one render per tick and throttles to ~60 fps. **[V]** pi:packages/tui/src/tui.ts:524, 1033-1075.
- Input preempts the throttle: after dispatching a key to the focused component it calls `requestImmediateRender()`, cancelling any pending throttled frame, with the comment that keyboard input is latency-sensitive and a `setTimeout(0)` can cost a full 16 ms tick on Windows. **[V]** pi:packages/tui/src/tui.ts:1044-1060, 1159-1161. This is the same lesson as Claude Code 2.1.267.
- Components render to an array of lines for a given width (pi:packages/tui/src/tui.ts:119-120 **[V]**); an `invalidate()` hook exists for cached state (e.g. loader.ts:64-67), so per-component caching is by convention rather than enforced by the framework **[U]**.

### 2.2 Two-queue design: steering vs follow-up

- User-facing: Enter while working = adjust the current task (delivered after the current response and its tool calls, guides the next response); Alt+Enter = add work after the current task; Alt+Up = return queued messages to the editor; Escape = stop. Aborting returns queued messages to the editor. **[V]** pi:packages/coding-agent/docs/usage.md:33-40; keybindings pi:packages/coding-agent/docs/keybindings.md:124,166-167.
- Loop placement: steering is polled at loop start and after every completed assistant turn (after tool execution, before the next LLM call); follow-ups are polled only when the agent would otherwise stop, and then re-enter the inner loop. **[V]** pi:packages/agent/src/agent-loop.ts:184-192, 304, 310-316.
- Delivery granularity is a setting per queue: `steeringMode` and `followUpMode`, each `"all" | "one-at-a-time"`, default one-at-a-time. **[V]** pi:packages/coding-agent/docs/settings.md:31-32.
- Display: dim single-line rows `Steering: …` and `Follow-up: …` in a pending container above the editor, then a hint row "↳ <key> to edit all queued messages". **[V]** pi:packages/coding-agent/src/modes/interactive/interactive-mode.ts:4907-4928.
- Dequeue: clears both queues, joins all entries with blank lines, prepends them to the current editor text; the same routine is used on abort. **[V]** same file :4930-4948.
- Messages typed during compaction are queued separately and announced with a status line. **[V]** same file :4950-4956.

### 2.3 Working indicator and streaming

- Braille spinner `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏` at 80 ms per frame. **[V]** pi:packages/tui/src/components/loader.ts:11-12.
- Label "Working (<interrupt key> to interrupt)", spinner in accent colour, text muted; can be embedded in the editor border; retry state shows a countdown ("Retrying (n/m) in Ns…"); compaction and branch summary have their own labels with a cancel hint. Extensions can replace the message, frames and interval. **[V]** pi:packages/coding-agent/src/modes/interactive/components/status-indicator.ts:38-112; interactive-mode.ts:505, 2619.
- No shimmer on the working label. A shimmer exists only for partner-provider names in selectors (section 6).
- Ctrl+O expands tool output, Ctrl+T toggles thinking blocks; footer shows token totals, cost, cache hit rate and a colour-coded context percentage. **[V]** pi:packages/coding-agent/docs/usage.md:22-24; components/footer.ts:141-199.
- Deliberate omissions: no sub-agents, no plan mode, no per-tool permission prompts. **[V]** pi:packages/coding-agent/README.md:19; docs/usage.md:26.
- Double Escape on an empty editor opens the session tree (`doubleEscapeAction: "tree" | "fork" | "none"`). **[V]** pi:packages/coding-agent/docs/settings.md:34.

### 2.4 RPC mode for GUIs

JSON lines over stdin/stdout. **[V]** pi:packages/coding-agent/docs/rpc.md, rpc-commands.md, json.md, rpc-extension-ui.md.

- `prompt` while streaming must carry `streamingBehavior: "steer" | "followUp"` or it is rejected; dedicated `steer` and `follow_up` commands exist. Responses report `disposition: "started" | "queued" | "handled"`. (rpc-commands.md:7-44, 46-92)
- `clear_queue` removes both queues and returns their text; the doc tells clients to implement terminal Esc as `clear_queue` then `abort`, restoring the returned text into the editor, because `abort` alone lets queued messages continue. (rpc-commands.md:107-128)
- `queue_update` events carry the complete current `steering` and `followUp` arrays, so the client renders server truth rather than guessing. (json.md:121)
- Lifecycle: `agent_end` closes one low-level run but retries, compaction, steering or follow-ups can still follow; `agent_settled` means nothing more will happen automatically. (rpc.md:58-72)
- Streaming: `message_update` is delta-only (`text_delta`, `thinking_delta`, `toolcall_delta` keyed by `contentIndex`); `*_end` events and `message_end` carry authoritative content and must replace the reconstruction. (json.md:62-100)
- Other controls: `set_model`, `set_thinking_level`, `set_steering_mode`, `set_follow_up_mode`, `compact`, `abort_retry`, `fork`, `get_tree`, `get_state`.
- Extension UI sub-protocol: blocking `select` / `confirm` / `input` / `editor` requests with ids and optional agent-side timeout; fire-and-forget `notify`, `setStatus`, `setWidget`, `setTitle`, `set_editor_text`. Working-indicator customisation calls are no-ops in RPC mode. (rpc-extension-ui.md:1-25)

---

## 3. Crush (charmbracelet/crush)

### 3.1 Rendering

- Bubble Tea v2.1.0, Lip Gloss v2, Glamour v2, Ultraviolet. **[V]** crush:go.mod:7-26.
- Hybrid pipeline: the top-level model draws into an Ultraviolet cell screen buffer by rectangle; sub-components such as the chat list render strings; chat items cache their output and invalidate on data change; the list is lazy and viewport-tracked. Rule: no IO or expensive work in `Update`. **[V]** crush:internal/ui/AGENTS.md:9, 22-34, 91-92, 205.
- One shared animation clock drives every visible spinner; an `Anim` never schedules its own tick, and the clock stops when nothing is animating. **[V]** crush:internal/ui/anim/anim.go:89-94, 429-431; internal/ui/model/chat.go:590-603.

### 3.2 The gradient "thinking" indicator

Full algorithm in section 6. In short: 15 scrambled glyphs coloured by a moving two-colour HCL gradient, optional label ("Thinking", "Summarizing", "Running") with an animated ellipsis, and an elapsed-time suffix that replaces the ellipsis. **[V]** crush:internal/ui/anim/anim.go; usage crush:internal/ui/chat/pending.go:46-58, assistant.go:802-804, tools.go:226-237, shell.go:86-92.

- The pending placeholder and the assistant message that replaces it share the same animation identity "so the handover between them is invisible". **[V]** crush:internal/ui/chat/pending.go:42-45.
- Tool spinners show overall turn time for quick tools and the tool's own elapsed once that is the more useful signal. **[V]** crush:internal/ui/chat/tools.go:233-236.

### 3.3 Tool calls, queue, cancel, permissions

- Tool rows: status icon (`●` pending, success, error, cancelled styles), one renderer per tool. **[V]** crush:internal/ui/styles/styles.go:51, quickstyle.go:813-816; AGENTS.md:121-133.
- Queue: Enter while busy enqueues; the queue is shown as a pill with up to nine gradient triangles and "N Queued", plus a list of items collapsed to one line and truncated to 60 cells. **[V]** crush:internal/ui/model/pills.go:21-22, 48-61, 123.
- Delivery: at each model step (`PrepareStep`) queued user prompts are folded into the active turn (step-boundary steering); prompts carrying a run id stay queued to run as their own turn. User prompts are ordered ahead of queued subagent reports. **[V]** crush:internal/agent/agent.go:466-515, 1012.
- Esc is two-stage: with queued prompts the first Esc clears the queue (help text "clear queue"); otherwise the first Esc arms a 2 s window ("press again to cancel") and the second cancels. The UI updates its cached queue/busy state optimistically and bumps a generation counter so a stale in-flight probe cannot repopulate the pill. **[V]** crush:internal/ui/model/ui.go:4055-4062, 5742-5803.
- No in-place edit of queued prompts and no rewind/checkpoint feature found (grep for rewind/checkpoint hits only tests and db). **[V]** absence at this SHA.
- Permission dialog: Allow (`a`) / Allow for Session (`s`) / Deny (`d`), left/right/tab to move, `t` toggles diff view, `f` fullscreen, shift+arrows scroll. A request flagged dangerous starts with the cursor on Deny so a reflexive Enter refuses. **[V]** crush:internal/ui/dialog/permissions.go:31-32, 105-183, 195-203.
- Plan handoff: inline prompt at the bottom of the editor with three buttons (start coding, code in YOLO, "Revise plan"). **[V]** crush:internal/ui/dialog/plan_handoff_inline.go:18-31, 269.
- Structured questions: dedicated dialog types for single, multi, yes/no, confirm, free text, form. **[V]** crush:internal/ui/dialog/question_*.go (file list).
- Notifications: native OS notification backend with terminal-bell fallback. **[V]** crush:internal/ui/notification/bell.go, native_beeep.go.

---

## 4. Amp, Gemini CLI, and others

### 4.1 Amp (closed source)

- Enter while the agent works steers by default: the message is delivered when the current step (a command or thinking block) finishes. To queue until the whole turn ends: Cmd/Ctrl+Enter on web and desktop, long-press send on iOS, or the "prompt: queue message" palette command in the CLI. Default is configurable. Enter Enter on a queued message promotes it to steer. Prompts Amp writes for you (Ship, Review) always queue. Esc Esc forcibly stops the agent and sends your message immediately. **[V]** amp-docs/prompting `#queueing-messages`; amp-docs/cli/keybindings.
- Editing: Up/Down move to queued and previous messages to edit them; Tab navigates to prior messages and `e` edits one. Alt+T expands thinking and tool blocks. Esc Esc interrupts the active turn (single Esc does not). Ctrl+O is the command palette. **[V]** amp-docs/cli/keybindings; amp-docs/prompting `#edit`.
- Thread model: threads are the unit (archive, snooze with muted notifications, labels, parent/child threads, handoff to a fresh thread with relevant context, cross-client continuation); mode is fixed after the first message. **[V]** amp-docs/threads, amp-docs/cli/keybindings.
- Rendering: "Look Ma, No Flicker" (2025-09-02) announces a fullscreen TUI on Amp's own framework with smooth scrolling while tool calls stream, mouse interaction, overlays, clickable buttons, and no flicker. It does not describe the diffing strategy. **[V]** ampcode.com/news/look-ma-no-flicker. Trade-offs reported by a third party: find only covers on-screen text, custom scrollbar, selection requires in-app handling. **[U]** steipete.me/posts/2025/signature-flicker.
- Working indicator design, plan approval, permission prompt layout: not found in the docs I fetched. **[U]** unknown.
- Programmatic: `amp --execute --stream-json --stream-json-input` uses Claude-Code-shaped user/assistant JSON lines. **[V]** amp-docs/cli/streaming-json.

### 4.2 Gemini CLI (Ink)

- Static/dynamic split: finished history goes into Ink `<Static>` (printed once to scrollback); only pending items re-render. Long streaming messages are split at the last safe markdown boundary so the completed prefix moves into `<Static>`, with a source comment naming heavy terminal flicker as the reason. **[V]** gemini:packages/cli/src/ui/components/MainContent.tsx:452-467; hooks/useGeminiStream.ts:1252-1272.
- An alternate-buffer mode swaps `<Static>` for a virtualised scrollable list. **[V]** gemini:MainContent.tsx:399-417, 448-450.
- Working indicator: `cli-spinners` "dots" glyph whose colour walks the Google brand gradient (purple, blue, cyan, green, yellow, red, back to purple) over 4000 ms, updated every 30 ms (120 ms under tmux); primary text is the model's thought subject or "Thinking..."; suffix `(esc to cancel, 12s)`; a dim italic witty phrase or tip beside it. Phrases rotate every 5 s, tips every 10 s, with minimum display times so they do not flip on re-render. **[V]** gemini:components/GeminiSpinner.tsx:15-16, 31-56; components/CliSpinner.tsx:17; components/LoadingIndicator.tsx:68-94; hooks/usePhraseCycler.ts:11-12, 49-50.
- No shimmer sweep on text (grep for "shimmer" in the UI package returns nothing). **[V]** absence at this SHA.
- Queue: Enter while busy appends to a client-side queue; when streaming goes idle all queued messages are joined with blank lines and submitted as one prompt. Display: "Queued (press ↑ to edit):" plus up to 3 single-line dim previews and "… (+N more)". Cancel with restore puts last prompt plus queued text back into the input. **[V]** gemini:hooks/useMessageQueue.ts:71-86; components/QueuedMessageDisplay.tsx:9-47; AppContainer.tsx:1577-1610.
- Experimental model steering (`experimental.modelSteering`): Enter while the agent runs becomes a steering hint injected at the next model turn, acknowledged immediately by a one-sentence reply from a small fast model. **[V]** gemini:docs/cli/model-steering.md; AppContainer.tsx:1667-1671, 1401-1425.
- Rewind: Esc Esc (or `/rewind`) clears a non-empty prompt, otherwise lists prior interactions with three actions (rewind conversation and revert code / rewind conversation / revert code). **[V]** gemini:docs/cli/rewind.md; docs/reference/keyboard-shortcuts.md:252-253.
- Tool confirmation options: Allow once / Allow for this session / Allow for all future sessions (or this file) / Modify with external editor / "No, suggest changes (esc)". **[V]** gemini:components/messages/ToolConfirmationMessage.tsx:293-397.
- Plan mode: Shift+Tab cycles Default -> Auto-Edit -> Plan; Ctrl+X opens the plan in the external editor for edits and inline comments; approve into auto-edit or manual, or iterate with feedback. **[V]** gemini:docs/cli/plan-mode.md:38, 70-78, 92-98.
- Structured questions: `AskUserDialog` component exists. **[V]** gemini:components/AskUserDialog.tsx (not read in depth).
- Notifications: `general.enableNotifications` for action-required prompts and session completion. **[V]** gemini:docs/cli/settings.md:32-33.

### 4.3 Brief notes

- **GitHub Copilot CLI**: Enter while working is steering (applied to the current task); Ctrl+Enter or Ctrl+Q queues for the next turn; steering text can accompany a rejected tool permission. **[V]** docs.github.com/en/copilot/how-tos/copilot-cli/use-copilot-cli/steer-agents. The Copilot SDK names the two modes `"immediate"` and `"enqueue"` (default enqueue), says an unprocessed steer falls to the front of the queue, and recommends showing a queued count and letting the user choose the mode while busy. **[V]** docs.github.com/en/copilot/how-tos/copilot-sdk/use-copilot-sdk/steering-and-queueing.
- **Factory Droid**: Shift+Tab toggles Normal/Spec mode; after a spec is proposed the options are proceed with manual approvals, proceed at Low/Medium/High autonomy, or keep iterating; Ctrl+O transcript view; `?` shortcut pane. **[V]** docs.factory.com/autonomy-and-safety/specification-mode.md, /droid-cli/quickstart.md. Queue behaviour not documented in the pages fetched. **[U]**
- **Cursor CLI**: Shift+Tab rotates Agent/Plan/Ask; Ctrl+R reviews changes with `i` for follow-up instructions; command approval is y/n. **[V]** cursor.com/docs/cli/using. Queue behaviour in the CLI is contested on the forum; the IDE setting offers Queue vs Steer for new messages. **[U]** forum.cursor.com threads 155105, 171529.
- **Aider**: no queue; Ctrl+C interrupts and the partial response stays in the conversation; undo is git-based (`/undo`). **[V]** aider.chat/docs/usage/commands.html.

---

## 5. Synthesis

### 5.1 Conventions table

| Convention | Claude Code | pi | Crush | Gemini CLI | Amp |
|---|---|---|---|---|---|
| Working indicator | Ping-pong glyph + whimsical verb with shimmer sweep + elapsed, token counter, thinking phase, "esc to interrupt", tip line; amber/red for stalls; past-tense "Worked for Ns" on finish | Braille dots (80 ms), "Working (esc to interrupt)", optionally in the editor border | 15 scrambled glyphs under a moving HCL gradient + label with ellipsis + elapsed | Braille dots tinted by a 4 s brand-gradient cycle + thought subject + "(esc to cancel, Ns)" + rotating witty phrase/tip | Not documented [U] |
| Enter while busy | Queue; delivered at next tool boundary in the same turn, else at turn end | Steer (after current assistant turn's tools) | Queue; folded into the turn at next model step | Queue until idle (default); steering hint (experimental) | Steer (after current step) |
| Explicit "after the turn" | Slash and `!` commands always wait; SDK `priority:"later"` | Alt+Enter (follow-up queue) | Run-id prompts | Default behaviour | Cmd/Ctrl+Enter or palette "queue" |
| Interrupt and send now | Ctrl+Enter (backgrounds movable work, else interrupts) | None (Esc returns queue to editor) | None | None | Esc Esc with a typed message |
| Queue display | Gray rows in the transcript above the spinner; gray until the model reads them | Dim "Steering:" / "Follow-up:" rows above the editor with an edit hint | "▶▶▶ N Queued" pill + truncated list | "Queued (press ↑ to edit):" + 3 previews + "+N more" | Navigable queued messages |
| Edit / cancel queued | Up pulls all back into the input, one per line; clear to drop | Alt+Up pulls all back; abort also returns them | Esc clears all; no edit | Up pulls all back | Up/Down selects one to edit; Enter Enter promotes to steer |
| Interrupt key | Esc (single); Ctrl+C | Esc | Esc Esc within 2 s (first Esc clears queue) | Esc | Esc Esc |
| What survives interrupt | Work so far and partial text; queue is kept and sent next; draft stays; prompt restored if nothing was produced | Work so far; queued messages go back to the editor (not sent) | Work so far; queue already cleared | Work so far; last prompt + queued text restored to input | Work so far; typed message is sent |
| Rewind / edit previous | Esc Esc on empty input: prompt list -> restore code / conversation / both / summarize; prompt returns to input | Esc Esc: session tree or fork; no file checkpoints | None | Esc Esc: list -> conversation / code / both | Tab then `e` on a prior message; thread handoff |
| Plan approval | Card: Yes + auto mode / Yes + manual approvals / No, keep planning (+feedback); Ctrl+G edits plan | None by design | Inline: start coding / code YOLO / revise plan | Approve to auto-edit or manual, or iterate; Ctrl+X edits plan | Not documented [U] |
| Structured questions | AskUserQuestion: header chip, 2-4 options with descriptions, multi-select, Other, notes, previews; auto-submit single-select; never times out by default | Extension UI select / confirm / input / editor | Dialogs: single, multi, yes/no, free text, form | AskUserDialog | Not documented [U] |
| Permission prompt | Inline: command between dashed lines; Yes / Yes don't ask again for prefix / Yes + auto mode / No; Tab adds a comment; "2 of 5", oldest first | None by design | Modal: Allow / Allow for Session / Deny with a/s/d keys; diff toggle; cursor starts on Deny when dangerous | Allow once / session / always / modify in editor / No, suggest changes | Not documented [U] |
| Subagents / background | Panel below prompt (tree rows, live tokens, elapsed), /tasks dialog, Ctrl+B to background, 30 s linger after finish, notices placed where they arrived | None by design | Subagent rows with spinner | Background task display | Child threads |
| Elapsed / tokens / context | Elapsed + token counter on the spinner; per-tool live elapsed (dim); "Thinking for Ns"; context in status line and /context | Footer: tokens, cost, cache hit, colour-coded context % | Elapsed suffix on every spinner | "(…, Ns)" on the indicator | Cost & usage per thread |
| Completion notification | Desktop notification or bell only when away (6 s for prompts, 60 s idle); OSC 9;4 and OSC 7501; recap after 3 min unfocused | OSC 7501 program status | Native notification with bell fallback | Optional run-event notifications | App/mobile notifications; snooze mutes |

Copilot CLI matches pi and Amp (Enter steers, Ctrl+Enter queues).

### 5.2 De-facto standard, best in class, GUI recommendation

1. **Working indicator.** Standard: a one-line status directly above the composer containing motion, a present-participle label, elapsed seconds and an interrupt hint. Best: Claude Code, because the line carries state (phase, tokens, stall colour, current task) rather than only motion. GUI: one status row pinned above the composer with fixed height so it never shifts the transcript (Claude Code 2.1.31 fixed exactly that jitter). Shimmer the label text; show elapsed only after a short delay (the reimplementation uses 30 s; I would use ~5 s); show a token count only once non-zero; switch the label to the real phase when known (thinking, running `<tool>`, waiting for permission, running hooks, compacting). Use colour for stalls (amber at 10 s without progress) instead of adding a spinner. End with a quiet "Worked for 42s" line. Honour Reduce Motion by rendering a static label.

2. **Enter while busy.** Standard: Enter never interrupts and never blocks; five of the six tools with documented behaviour (Claude Code, pi, Crush, Amp, Copilot CLI) deliver at the next tool/step boundary, and only Gemini's default waits for idle. Best: Claude Code's three-level model (`next` / `later` / `now`), which is also what its SDK exposes. GUI: Enter = send with boundary delivery where the backend supports it (Claude SDK `priority:"next"`, pi `steer`, Copilot `immediate`); Option+Enter = "after this turn" (`later` / `follow_up` / `enqueue`); Cmd+Enter = "send now" (Claude `priority:"now"` with human origin; elsewhere interrupt then send). Show the mode on the send button while busy. Where the backend only has prompt + cancel, all queued items are client-side "after this turn" and the UI should say so rather than pretend to steer (inference; the ACP researcher should confirm what ACP agents expose).

3. **Queue display.** Standard: dim rows adjacent to the composer plus a "press X to edit" hint. Best: Claude Code's "gray until the model has read it", because it tells the user the one thing they need (has the agent seen this yet), and pi's `queue_update` server-truth event. GUI: optimistic insert on Enter with zero delay; render queued items as dimmed bubbles in a tray attached to the composer top; un-dim and animate the bubble into the transcript at the moment the backend confirms delivery (Claude `SDKUserMessageReplay` / first event with `user_message_uuid`; pi `queue_update`). Give every queued message a client UUID so interrupt receipts can be reconciled. Per-item edit, delete, reorder and "send now" are where a GUI beats every TUI (all of which only support pull-everything-back, except Amp). Keep Up-arrow-on-empty-composer to pull queued text back, since Claude Code, Gemini and pi all trained that.

4. **Interrupt.** Standard: Esc. Claude Code, pi and Gemini use a single Esc; Crush and Amp require a double press. Best: Claude Code's context-sensitive Esc (dialog first, then selection, then turn) and its restore-prompt-if-nothing-happened rule. GUI: Esc and Cmd+. stop the turn; a Stop button replaces Send only when the composer is empty, otherwise both are visible. On stop: keep partial text and mark the tool row "Interrupted" in a neutral colour (Claude Code moved from red to grey); if no output had arrived, put the prompt back in the composer; keep the draft untouched. Queue policy on interrupt is the one real divergence: Claude Code sends the queue next, pi and Gemini hand it back to the editor. Because a GUI queue is visible and durable, pause it on interrupt and offer "Resume queue" rather than silently running it; with Claude Code this requires the raw `interrupt` control request with `cancel_queued:true` and client-side re-enqueue, otherwise the CLI runs `still_queued` messages on its own.

5. **Rewind / edit previous.** Standard: Esc Esc on an empty input opens a list of the user's prompts (Claude Code, Gemini; pi opens its tree). Best: Claude Code's separate code vs conversation restore with a per-prompt changed-files count, and returning the prompt to the input. GUI: hover action "Edit and resend" / "Rewind to here" on each user bubble, plus Esc Esc on an empty composer opening the same list for muscle memory. State plainly what is not restored (Bash-made changes, subagent edits, mid-turn messages have no checkpoint).

6. **Plan approval.** Standard: a card ending the plan turn with approve-with-autonomy-choice or keep-iterating-with-feedback, plus "open in editor". Best: Claude Code (autonomy choice folded into the approve action, edited-by-user marker) and Gemini (inline comments in the plan). GUI: render the plan as an editable document in place; primary button approves in the session's current mode with a menu for the other modes; a feedback field sends "keep planning". Never auto-resolve.

7. **Structured questions.** Standard: Claude Code's schema is the reference shape (header chip <= 12 chars, 2-4 options with descriptions, multi-select, Other). GUI: native radio/checkbox cards with number-key shortcuts, an always-present free-text row, auto-submit when the last question is single-select, optional preview pane, and a way to answer with a normal chat message. Do not time out by default.

8. **Permission prompts.** Standard: inline (not window-modal) prompt showing exactly what will run, with once / session-or-rule / deny, and deny-with-feedback. Best: Claude Code (comment on allow or deny, "2 of 5", oldest first, scrolls into view regardless of scroll lock) and Crush (cursor on Deny for dangerous requests, single-letter accelerators). GUI: inline card anchored at the bottom of the transcript and mirrored on the composer; Return = allow once, Cmd+Return = allow for session, Esc = deny; a text field that turns a denial into guidance; show a stack count; name the subagent that is asking; default focus on Deny for flagged commands; a system notification after ~6 s if the window is not key.

9. **Subagents / background.** Standard: a compact live list near the composer plus a detail view. Best: Claude Code (rows with live tokens and elapsed, "waiting" state, 30 s linger, notices inserted where they arrived). GUI: collapsible strip above the composer with one row per running task (label, phase, elapsed), click to open its transcript in a side pane, a "move to background" control on long tool rows, completed rows fading after a delay. Remember stream deltas for subagents are not available from the Claude SDK; render subagent progress from complete messages.

10. **Elapsed / tokens / context.** Standard: elapsed on the working line; context usage in a footer. Best: Claude Code's per-tool ticking elapsed (dim) and pi's colour-coded context percentage. GUI: tabular (monospaced-digit) numerals so counters do not jitter; per-tool elapsed only after ~2 s; a context meter in the composer footer that changes colour near the limit.

11. **Completion notification.** Standard: notify only when the user is away, and distinguish "needs you" from "done". Best: Claude Code's gating (6 s for prompts, 60 s idle, suppressed while background agents run) and the unfocused recap. GUI: UNUserNotification only when the window is not key; Dock badge for sessions needing input; sidebar status per session (working / needs input / done); a one-line recap at the top of a session reopened after several minutes.

### 5.3 Rendering lessons that transfer to a native app

- Keep the keystroke path off the streaming/animation frame budget. Both Claude Code (2.1.267) and pi (immediate render on input) fixed typing latency caused by spinner or stream repaints. In SwiftUI/AppKit: the composer must not live in a view subtree invalidated by streaming text or the shimmer timeline.
- Freeze finished content. Gemini's `<Static>` split, Claude Code's virtualised fullscreen list and Crush's cached items all exist so that only the tail re-lays out. Split a streaming message at safe markdown boundaries and never re-measure completed blocks (Claude Code 2.1.257, 2.1.259, 2.1.269).
- One animation clock for all indicators, stopped when nothing is visible (Crush; Claude Code's isolated 50 ms loop). 20 fps is what the leading terminals ship; a Mac app can run the shimmer at display rate on the GPU but should not drive layout from it.
- Scroll anchoring: pause auto-follow on user scroll, show "Jump to bottom · N new", keep position when the response finishes, but always bring permission prompts into view (Claude Code fullscreen docs).
- Optimistic UI with reconciliation: Crush bumps a generation counter so stale probes cannot overwrite optimistic queue state; Claude Code shipped several "queued message shown twice / flickering / input disappeared after submit" bugs. Give each optimistic item a stable id and reconcile by id.
- Reserve layout for transient rows (indicator, queue tray) so their appearance does not move the transcript.

---

## 6. Shimmer and gradient specifics

| Tool | Element | Algorithm | Constants | Source |
|---|---|---|---|---|
| Claude Code (official facts) | Verb and thinking text | Shimmering spinner; subtle shimmer on thinking status; honours Reduce motion | Animation loop 50 ms (20 fps); amber after 10 s of thinking; red on permission stall | **[V]** CC-CL:9680, 8754, 8260, 6528, 6845 |
| Claude Code (as reimplemented) | Verb text | Linear highlight band sweeping one character per tick across the label; per-character colour = lerp(base, highlight, max(0, 1 - distance/band)); cycle length = label length + 2 x band, then wraps (the band fully exits before re-entering); direction reverses between "requesting" and "responding" | Band half-width 4 chars; tick 50 ms while waiting on the request (20 chars/s), 200 ms while receiving (5 chars/s); base orange rgb(215,119,87), highlight #F59575; linear falloff, no easing | **[U]** pcs:index.ts:53-60, 123-146, 308 |
| Claude Code (as reimplemented) | "thinking" label glow | Whole-label opacity = (sin(2*pi*t/period)+1)/2 | Starts after 3 s; period 2 s | **[U]** pcs:index.ts:69-70, 206 |
| Claude Code (as reimplemented) | Tool-running flash | Whole verb oscillates base <-> highlight with a sine | Half-cycle about 1 s | **[U]** pcs:index.ts:249-250 |
| Claude Code (as reimplemented) | Stall | Verb colour blends to red over 30 ticks after 3 s without new tokens, blends back when tokens resume | 3 s timeout; 30-frame transition | **[U]** pcs:index.ts:63-65, 312-317 |
| Claude Code (as reimplemented) | Glyph | `· ✢ ✳ ✶ ✻ ✽` forward then reversed | 120 ms per frame | **[U]** pcs:index.ts:42-48, 357 |
| Claude Code (as reimplemented) | Token counter | Displayed value eases toward the target on its own 50 ms clock with step size growing with the gap | Steps 3 / max(8, 15 % of gap) / 50 | **[U]** pcs:index.ts:59, 341 |
| Crush | Scrambled glyph strip | Pre-rendered frames. Ramp = HCL blend A->B->A->B over 3 x width cells; frame i uses ramp offset i, so the gradient slides one cell per frame; frame count 2 x width, giving a seamless loop. Each cell shows a random rune from `0-9a-fA-F~!@#$£€%^&*()+=_`, fixed per frame from a seeded PRNG. Each column appears at a random "birth" step, showing `.` until then. Label is static colour followed by an ellipsis cycling `.`, `..`, `...`, empty; an elapsed suffix suppresses the ellipsis | 20 fps (50 ms); strip width 15; birth stagger up to 20 steps (1 s); ellipsis step every 8 frames (400 ms); loop period = 2 x width x 50 ms = 30 frames = 1.5 s for the 15-cell turn strip (frames are built at construction, and `SetLabel("Thinking")` does not rebuild them); gradient travels 20 cells/s with a 30-cell A->B->A wavelength; colours = theme primary -> secondary; blend space HCL; no easing | **[V]** crush:internal/ui/anim/anim.go:20-48, 57-58, 232-296, 329-336, 432-449, 452-506, 510-556; chat/pending.go:46-58; styles/quickstyle.go:244-246 |
| Crush | Queue pill | Static horizontal gradient across up to nine `▶` | error -> secondary | **[V]** crush:internal/ui/model/pills.go:55; styles/quickstyle.go:1297-1298 |
| Gemini CLI | Spinner glyph colour | Single colour for the whole glyph sampled from a closed gradient at progress = (t mod period)/period | Period 4000 ms; tick 30 ms (120 ms in tmux); stops: purple, blue, cyan, green, yellow, red, purple | **[V]** gemini:packages/cli/src/ui/components/GeminiSpinner.tsx:15-16, 31-56 |
| Gemini CLI | Phrases | Random pick without immediate flip | Witty phrase 5 s, tip 10 s, same minimum display times | **[V]** gemini:hooks/usePhraseCycler.ts:11-12, 49-50 |
| pi | Working label | No shimmer; braille spinner | 80 ms per frame | **[V]** pi:packages/tui/src/components/loader.ts:11-12 |
| pi | Partner provider name in selectors (not the working label) | Four colour bands flowing left to right; within a band, smoothstep `t*t*(3-2t)` blend to the next colour in sRGB | 4 chars per colour, 10 chars/s, colours #4d9abf #83ccd2 #f1be57 #f09082 (16-char cycle, 1.6 s) | **[V]** pi:packages/coding-agent/src/modes/interactive/components/provider-brand.ts:20-45 |
| Amp | Unknown | Not documented | n/a | **[U]** |

### Translating to a native shimmer

Terminal shimmers are quantised to character cells and 20 fps; the recognisable properties are a narrow soft band, a constant-velocity sweep, a pause while the band is off-text, and speed that encodes phase. A native equivalent (my recommendation, not sourced):

- Render the label once in the secondary label colour and overlay the same text in the primary/accent colour masked by a moving linear gradient (transparent -> opaque -> transparent). Band width about 30-35 % of the label width (the terminal band is 8 cells on a 9-12 character verb, which is wider than it looks; a narrower band reads better at pixel resolution).
- Linear timing, period about 1.6-2.0 s per sweep including an off-text gap of about 25 % of the cycle, matching "length + 2 x band" wrap behaviour. Do not ease the sweep; easing reads as pulsing.
- Optionally run faster before the first token arrives and slower once output is streaming, mirroring the 50 ms vs 200 ms ticks.
- Drive it with a GPU-side animation (`CAGradientLayer` position animation or a `TimelineView` limited to the label) so it never invalidates transcript layout; pause when the window is occluded or the row is off-screen.
- Reduce Motion: static label, no sweep; keep the stall colour change, since it is information.

---

## Gaps and caveats

- Claude Code glyph cycle and shimmer constants are from a third-party reimplementation; only the 50 ms loop, the existence of the shimmer, and the colour-state rules are official.
- Amp's indicator, plan and permission UI are undocumented in the pages fetched; its renderer internals are not public.
- The Claude Code changelog and docs describe versions up to 2.1.321; behaviour differs by version (send-now needs 2.1.275, background-on-send-now 2.1.281/2.1.286, interrupt receipts 2.1.205, cancel-queued 2.1.219). A GUI must feature-detect via `SDKSystemMessage.capabilities`.
- I did not find a documented way to cancel or edit a single queued message inside the Claude Code process; the recommendation to hold "later" items client-side follows from that absence.
- Crush queue, cancel and plan-handoff findings are from source reading only, not from running the app.
