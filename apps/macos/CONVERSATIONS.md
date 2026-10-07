# Conversations and workspace tools

In-app execution requires the [optional runtime](RUNTIME.md). History browsing,
exact transcript Find, raw evidence, external resume, files, browser tabs, and
terminals remain available in the history-only build. Historical ingestion
support does not imply that a provider can be launched.

## Start and compose

Choose a provider and project on Home. A projectless conversation receives a
retained private folder. Existing projects can use their current folder or a new
Git worktree from the selected local ref; uncommitted source files are not copied.

Choose **Model and permissions…** in the composer's provider menu to open an unsent
conversation and inspect its actual settings. The **+** button attaches files;
the chooser and file/image paste or drop also open an unsent draft. The normal Home
send applies saved provider-specific choices before sending; an unconfirmed setting retains the
draft for inspection. Settings are only exposed when the provider supplies them.
Model and permission controls remain available during active turns and approval
requests. Claude starts and resumes in native Auto mode unless you have explicitly
chosen a different conversation or provider mode. The provider determines when a change takes effect; rejected changes
leave the conversation connected and display the reason beside the composer.

The selected conversation title appears in the main toolbar. The grouped controls
end with **…**, containing branch, fork, rewind, and parent/child navigation actions.
A narrow tick rail beside the transcript previews prompts on hover and jumps to
them on click. Pending messages, activity, approvals, and questions stay above the
workspace changes bar; thinking uses the shared Sidequery shimmer without a bubble.
Models appear directly in their picker, with submenus only for variants and effort.
An empty new conversation
does not warn merely because its provider has not written a transcript yet.

Inside a conversation, the composer supports captured file and image attachments,
file/chat context, provider commands, skills and plugins, prompt recall, and saved
prompt stashes. Terminal output, file contents, diff review, and browser context
can also be added from their workspace panes. Captured context is saved as bytes,
so a later change to the original file does not change a queued prompt.

Select text in the transcript to open **Add to chat** above the selection.
It adds exactly the highlighted text and its source to the existing draft without
sending. This also works in code blocks. Find highlights do not open the popover.
Click a **Selected text** attachment to scroll to and highlight its captured
passage in the transcript. Its × button removes only that attachment. New captures
retain their exact rendered location across relaunch; older captures use an exact
text match within their source message. If the original passage is missing or
ambiguous, Memex reports that instead of selecting unrelated text.
Conversations open in another app must be closed there before context can be
added here; attachment errors explain the actual failure.

The composer’s **+** opens a searchable context picker. **Skills** and **Plugins**
appear directly in the main list under plain headings, with plugins first. Discovery
uses the selected conversation's provider installation and workspace, showing
enabled entries and reporting loading or discovery failures. Codex selections
become retained native skill/plugin references in the unsent draft. Claude skill
selections insert their native slash command; its plugin entries open their
skills. Plugins containing only tools remain available through the provider.
Choosing context never sends a message. Unlike captured files, skill references
use the provider's skill at execution time rather than copying its instructions.
**Prompt actions** at the bottom groups
stash/history, annotations, and active-turn controls. It also offers on-device
dictation, with a retained recording and
retry/export controls if transcription fails. Review the transcript before
inserting it; insertion never sends. Dictation requires microphone and speech
permission and an available on-device recognizer. HEIC, TIFF and oversized still
images have a bounded normalization path; supported small images keep their
original bytes. An attachment's annotation editor associates a passage or region
description and comment with its captured identity without modifying the source.

Automatic file and skill discovery uses the local conversation's workspace and
provider home. A remote or server-owned conversation never treats its path strings
as local files; deliberately choosing a local attachment remains an upload.

## Follow-ups and recovery

While the agent works, prepare a draft, queue it, or steer if supported. Queue
controls can edit, reorder, cancel, and promote a queued prompt. Stop holds the
queue while requesting native cancellation, with acknowledgement errors and a
timeout shown explicitly. A terminal snapshot settles the stopping state.
Providers that can stop but cannot steer offer the separately labeled
**Stop and start new turn** action. It waits for the stop to settle, retains the
draft on failure, and leaves queued work held. A local queued prompt can move to
a reviewed side-chat draft; the source entry is removed only after that draft is
durably saved. Remote queue transfer is not advertised.

Outgoing intent is persisted before dispatch. The provider's acknowledgement of
that exact command confirms delivery even when its transcript omits the echoed
message identity. Reopening checks saved acknowledgements without starting a
provider. A crash or missing acknowledgement does not silently turn an uncertain
command into a new send; inspect native history and the retained request before
explicitly resolving uncertainty. Queued but undispatched work stays held after
recovery until explicitly resumed. Conversation rows keep activity indicators
inline instead of adding a separate status line.

Approvals retain native decision IDs and details. Structured questions preserve
their explanatory choices and support single choice, multiple selection, and
custom answers as advertised by the provider. Stale/disconnected request controls
are disabled. Unsupported actions are not presented as universal capabilities.

Standard MCP form requests use typed controls and validate required values before
submission. Decline and Cancel request use the form protocol's own responses.
Unsupported proprietary forms and URL verification remain explicitly unsupported.
Provider-offered policy amendments retain their exact decision scope, and native
automatic approval reviews retain risk, rationale and raw details.

Use **Back** and **Next** to review pending questions; changing steps preserves each
question's draft and does not send it. **Submit answer** replies only to that
question's original native request. **Attach text files to answer…** captures up to
10 UTF-8 files totaling 1 MB and includes their actual contents in the answer,
including when the files later change. Question drafts and these attachments stay
in the open viewer until submitted; they are not recovered after app restart.
Images, audio, video, PDF and binary answer attachments are rejected because the
current native question transports accept strings, not media blocks. Attach media
to a separate prompt instead. Codex, Claude SDK and the current ACP bridge expose
no independent asynchronous-question dismissal operation, so **Stop conversation**
remains an interruption action and is not presented as dismissing one question.

## Providers and related work

Built-in local execution uses Codex's app server and Claude Code's SDK. **Configure
providers…** adds an ACP stdio executable, arguments, and its original home. ACP
resume/configuration support is negotiated; ACP steering is not advertised.
The original launch identity remains associated with a created conversation even
if the editable provider catalog changes.

**Settings → Provider tools** manages a selected local Codex or Claude installation:
MCP inventory/add/remove, native OAuth login, plugin inventory/install/remove and
supported configuration. Home, executable and workspace are explicit. ACP
providers retain their own administration tools; historical ingestion does not
become a new native execution adapter.

Plans show native steps, status and full document content. The plan editor creates
a local editable copy, checks for external edits on save, and attaches the exact
saved text and provenance for implementation. **Refine** and **Implement** attach the plan to
the current draft; **Implement in new conversation…** prepares a new draft with
the selected plan and captured source context. None of these actions auto-sends.
The live agent roster uses native child status. Verified child history can open
through its connected parent before indexing catches up, with Find and raw
evidence. Reading a disconnected parent's child requires an explicit connection.
Model and reasoning details are shown only when the provider supplies them.

Interactive MCP tool results use a sandboxed native web host, bound to the exact
session, call and originating server. Tool calls require allow-once approval.
Disconnected/restored results retain a compact fallback; stale bindings cannot
reuse a live connection's authority.

The **Conversation** menu offers native fork/rewind when supported and idle, plus
**Branch with context…** for a provider transition or context branch. Branches
retain parent/child navigation. **Add context to parent draft** preserves the
parent's unsent text. Context transfer does not merge Git branches. Interrupted
history mutations retain an inspection marker instead of being blindly retried.

## Organize conversations

Use row menus or multi-selection for rename, pin, archive, restore, and **Remove
from Memex…**. Find removed and archived conversations in the sidebar's ellipsis menu;
these actions do not delete provider logs or workspace data. Manual reordering
preserves hidden rows and does not replace relevance ordering during search.
The same menus create custom sections, move conversations and mark them read or
unread. Removing a section preserves its chats and their other metadata. Settings
offers configurable app shortcuts, text/code sizes, light/dark appearance,
increased contrast and reduced motion while honoring system accessibility settings.

Notification preferences control completion/input alerts and sound. They are
opt-in and require macOS permission. A notification opens the exact conversation.
Quitting warns before ending active app-owned agents or live shells; independently
running execution-host sessions are detached from the viewer.

## Projects, files, and changes

Each conversation's right pane starts with a **Tools** grid for Terminal, Files,
Browser, and Changes. Opening a tool adds its tab; **+** returns to the grid.
Open tools and the selected tab are retained separately for each conversation
while the app is open. Closing the last tab returns to the grid. Returning to the
grid keeps file drafts, browser tabs, and workspace shells intact.

**Add New Project** supports an existing folder, named new repository, or clone.
Project controls can reuse a checkout and explicitly run its setup script. Failed
setup/clone state retains its files for inspection and retry. Managed worktree
archive/reattach and cleanup check ownership, references, and dirty state.

**Files** browses the workspace, creates files/folders, edits UTF-8 text, and
previews supported Markdown, HTML, delimited data, images, PDF, audio, and video.
Edits have durable drafts and an explicit Save. Saving checks the loaded file
version; an external edit opens a comparison instead of silently overwriting it.
HTML previews do not enable script execution. Symlinks and traversal cannot be
used to edit outside the selected workspace.

**Changes** offers current, branch, and checkpoint comparisons with unified/split
display and review context capture. Its **Git** menu commits explicitly staged
changes, pushes the chosen branch, and creates a draft pull request through the
configured GitHub CLI. It does not automatically stage all files or merge PRs.
The native pull-request sheet reads metadata, reviews, inline review threads and
the PR diff, and can capture exact PR/thread context. It does not post reviews or
comments. Large or paginated results disclose their limits.

Checkpoints capture raw working-file bytes and the index, including before/after
observed local turns. Conversation rewind and file restore are separate actions.
File restore requires an owned isolated worktree, no active work, and safe path
checks; it saves a recovery snapshot before modifying files. It does not run Git
clean/smudge filters or replace overlapping ignored files.
Explicit workspace snapshots additionally preserve ignored files and independent
HEAD/index state. Recovery creates another owned checkout and leaves the original
untouched. Clean, unreferenced pruning saves a recoverable snapshot first; dirty
or ignored content still prevents pruning.

## Browser and terminals

Browser tabs retain per-conversation state. Context controls capture selected
text, page information, annotations, or screenshots into the draft. Agent browser
access requires an explicit live grant for app-owned tabs; JavaScript evaluation
has a separate grant. Grants do not survive app restart. See the [execution-host
boundary](../../docs/execution-host.md#control-mcp-and-browser-boundaries) for the
remote bridge.
Granted actions include navigation, history, reload, wait, DOM key events and tab
selection. A separate viewport-recording grant permits short, silent H.264 MP4
clips: 1–5 seconds at 1–5 frames per second, at most 8 MiB. Clips capture only the
app-owned WebKit viewport and remain temporary files on the desktop host. Revoking
access or closing the tab cancels capture and removes partial output. DOM keys do
not claim native keyboard-shortcut behavior.

Each canonical local workspace has a terminal group. Add independent shells,
switch or split them, move the same group between drawer and side pane, and save
scrollback snapshots. Close/restart confirms when a shell is live. Switching chats
or hiding a pane preserves the processes; restarting the app preserves saved
snapshots, not the previous shell process. Remote paths cannot spawn local shells.

## Remote clients and schedules

[Execution hosts](../../docs/execution-host.md) provide separately authenticated,
service-owned sessions, a native viewer, responsive web/mobile controls, interval
schedules, and a distinct opt-in control MCP. Paired hosts can expose files,
conflict-checked text saves, diffs and host-owned terminals within their registered
workspaces. The native pairing flow can open an SSH tunnel to an already configured
host using existing SSH authentication and trusted host keys.

Schedules can target an existing chat or create a fresh conversation, run on an
interval/local-time recurrence or an explicitly named event, and retain run
outcomes in an inbox with read/unread state and per-schedule notification policy.
The host must be running; native notifications additionally require the app to be
running with the existing opt-in notification setting.

The existing retrieval MCP keeps its retrieval authority. Remote capabilities
depend on the actual host/provider and explicit grants; an imported machine name
never authorizes filesystem, terminal or arbitrary application access.
