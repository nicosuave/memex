# Web AI-chat UI building blocks: how they work (source-level)

Date: 2026-11-09. Read-only source study; nothing was built, installed, or run.
All repos are shallow clones under
`/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/web-*`.

Claim labels: **VERIFIED** = read in source at the SHA below (`path:line`). **UNVERIFIED** = from memory / public docs / product observation, not checked in source during this task.

## Sources and SHAs

| Clone dir | Repo | SHA | Commit date |
|---|---|---|---|
| web-vercel-ai | vercel/ai | 88b95757c170b03d7ce6014e6b94d7b27d9b902c | 2026-11-09 |
| web-vercel-ai-elements | vercel/ai-elements | ca8be600bfd01b3131f9bf6134b51602a71f4374 | 2026-10-14 |
| web-vercel-streamdown | vercel/streamdown | 072ce7ead33a5e932c85b6e8f06cf83cc0f64191 | 2026-11-05 |
| web-stackblitz-labs-use-stick-to-bottom | stackblitz-labs/use-stick-to-bottom | 8d6a19a0ca6ab632830588073e6a29312a06a088 | 2026-06-04 |
| web-assistant-ui-assistant-ui | assistant-ui/assistant-ui | 3321a8bbb0b19534cff9d24744fec7dbeb434614 | 2026-11-09 |
| web-inokawa-virtua | inokawa/virtua | 21d7be68ba04079a71a366a50377591b9a182e25 | 2026-11-07 |
| web-TanStack-virtual | TanStack/virtual | 54a3f7d3cc32b373187b3802d81cb4b24ae08b0b | 2026-11-06 |
| web-petyosi-react-virtuoso | petyosi/react-virtuoso | ff76994211f61455dd13cadfc14c45fe1432b615 | 2026-11-09 |
| web-LegendApp-legend-list | LegendApp/legend-list | 26df31ae3202bc9d1e1438ca7ebc59e24fbcb5ff | 2026-11-07 |
| web-ibelick-motion-primitives | ibelick/motion-primitives | 120f64f6ca60348e251f929e9c81f11ccbe45eda | 2026-09-28 |
| web-ibelick-prompt-kit | ibelick/prompt-kit | 5a9496635cc0e1bf7679fe2a9e35d9b36b99c08a | 2026-09-28 |
| web-magicuidesign-magicui | magicuidesign/magicui | 1ac985adca3cdfab9b1739253fd14209687c4e9c | 2026-11-08 |
| web-kingshuaishuai-incremark | kingshuaishuai/incremark | 0e20ef433307ee9f252aaff756ea6127c2a03eae | 2026-01-22 |
| web-llm-ui-kit-llm-ui | llm-ui-kit/llm-ui | e9fcf369b89ac772c17fcca199cf250b2e80d94d | 2026-09-29 |
| web-vercel-ai-chatbot | vercel/ai-chatbot (Chat SDK template) | a8989b522fa3672a2ae52f053105e40993b36ca1 | 2026-11-03 |

Paths below are relative to the clone dir named in each section heading.

---

## 1. Vercel AI SDK UI (`web-vercel-ai`) — VERIFIED

### State model
- A framework-agnostic `AbstractChat` owns `status`, `error`, `messages` through a `ChatState` interface with exactly three mutators: `pushMessage`, `popMessage`, `replaceMessage(index, message)`, plus `snapshot()` (`packages/ai/src/ui/chat.ts:152-163`).
- The React binding (`ReactChatState`) keeps three **separate** subscriber sets (messages / status / error) and always replaces the array immutably: `pushMessage` = `concat`, `replaceMessage` = slice + `snapshot(message)` + slice (`packages/react/src/chat.react.ts:34-36, 69-86`). `snapshot` shallow-clones the message and each part (`chat.react.ts:88-109`), so every untouched message keeps reference identity and only the streaming message gets a new object. That is the property that makes `memo` on message rows work.
- Status enum: `'submitted' | 'streaming' | 'ready' | 'error'` (`chat.ts:134`), documented at `chat.ts:317-324`.
  - `submitted` is set immediately before the transport call (`chat.ts:989`).
  - `streaming` is set by the first `write()` from the stream processor (`chat.ts:1087-1089`).
  - `ready` on normal completion or abort (`chat.ts:1141-1156`); `error` on failure (`chat.ts:1198`).
  - `setStatus` is a no-op when status is unchanged (`chat.ts:336-341`), so per-token writes do not notify status subscribers.

### Optimistic user append
- `sendMessage` pushes the user message into state **synchronously before** the network call: `this.state.pushMessage({...uiMessage, id: uiMessage.id ?? this.generateId(), role: 'user'})` then `await this.makeRequest(...)` (`chat.ts:491-503`). There is no pending/confirmed flag on the user message; a failure leaves the message in place and flips `status` to `error`.
- Edit-and-resend: passing `messageId` truncates everything after that message and replaces it in place (`chat.ts:459-489`). `regenerate` slices the list back to before the assistant message (`chat.ts:530-536`).

### Message ids
- Default generator: 16 random chars from `0-9A-Za-z` (`packages/provider-utils/src/generate-id.ts:15-16, 57`). Overridable via `generateId` (`chat.ts:208-212`); the Chat SDK template passes a UUID generator (`web-vercel-ai-chatbot/hooks/use-active-chat.tsx:109-110`).
- The assistant message gets a **client-generated** id at request time (`chat.ts:1037`), but the server's `start` chunk may overwrite it: `state.message.id = chunk.messageId` (`packages/ai/src/ui/process-ui-message-stream.ts:1026-1028`). Because the assistant message is only pushed into `messages` on the first `write()` (`chat.ts:1091-1115`), the swap happens before the row exists, so no key change is visible.
- Consequence: between submit and first chunk there is **no assistant message in the list**. The template fills the gap with a separate `ThinkingMessage` row when `status === "submitted"` and the last message is not an assistant (`web-vercel-ai-chatbot/components/chat/messages.tsx:106-107`). That is a row swap (placeholder unmounts, real row mounts), which is a reconciliation flash risk. assistant-ui avoids this (section 5).

### Stream processing
- Parts carry their own lifecycle: a text part is created with `state: 'streaming'` on `text-start`, mutated in place on `text-delta`, then `state = 'done'` (`process-ui-message-stream.ts:491-518, 533`). Every delta calls `write()`, which calls `replaceMessage` (`chat.ts:1096-1099`).
- All updates run through a `SerialJobExecutor` so tool callbacks and stream writes cannot interleave (`chat.ts:285, 1075`).

### Throttle (`experimental_throttle` is now a deprecated alias of `throttle`)
- `useChat({ throttle })`; `experimental_throttle` kept as deprecated alias; default `undefined` = no throttling (`packages/react/src/use-chat.ts:48-57, 195`).
- Implementation is `throttleit` wrapped around the **messages** subscriber only (`packages/react/src/throttle.ts:1-8`, `chat.react.ts:111-122`). Status and error callbacks are unthrottled and publish a complete snapshot, so `ready`/`error` can never commit beside stale messages (`use-chat.ts:304-311`).
- Docs example value: `throttle: 50` (ms) (`content/docs/04-ai-sdk-ui/02-chatbot.mdx:328-346`).
- Important design note in source: the hook deliberately uses plain `useState` snapshots, **not** `useSyncExternalStore` (it "would make streaming updates synchronous and can repeatedly restart navigation renders") and **not** `startTransition` (it "can group it with a suspended navigation in the same root, holding back streamed text and local edits") (`use-chat.ts:271-279`).

### `smoothStream` (server-side transform)
- Defaults: `delayInMs = 10`, `chunking = 'word'` (`packages/ai/src/generate-text/smooth-stream.ts:36-38`).
- Chunking regexes: `word: /\S+\s+/m`, `line: /\n+/m` (`smooth-stream.ts:5-8`). Also accepts a custom `RegExp`, a `ChunkDetector` function, or an `Intl.Segmenter` (recommended for CJK) (`smooth-stream.ts:42, 54-67`).
- Algorithm: buffer deltas; while the detector finds a chunk at the head of the buffer, enqueue it and `await delay(delayInMs)` (`smooth-stream.ts:173-190`). Non-text chunks flush the buffer first (`smooth-stream.ts:148-153`). It smooths both `text-delta` and `reasoning-delta` (`smooth-stream.ts:120, 149`).
- The delay is skipped when `document.visibilityState === 'hidden'`, because background-tab timer throttling would stall the stream through backpressure (`smooth-stream.ts:10-17, 189`).
- It never drops the trailing partial word: a word is only emitted once whitespace follows it, so the last word waits for the next delta or a flush.
- Template usage: `experimental_transform: smoothStream({ chunking: "word" })` (`web-vercel-ai-chatbot/artifacts/text/server.ts:12`).

### Resumable streams
- Client: `useChat({ resume: true })` calls `chat.resumeStream()` once on mount when status is not active (`use-chat.ts:141-147, 340-347`) and again on `visibilitychange` → visible if the last response ended in a network disconnect (`use-chat.ts:83-110, 126-133`).
- `resume-stream` first asks the transport to reconnect and only then sets `submitted`, specifically to avoid "a brief flash of 'submitted' status when there is no stream to resume" (`chat.ts:931-960, 989`).
- Default HTTP transport: `GET {api}/{chatId}/stream`; `204` means no active stream (`packages/ai/src/ui/http-chat-transport.ts:232-289`).
- Disconnect detection is by `TypeError` message sniffing (`fetch` / `network` / `load failed`), then the in-flight `StreamingUIMessageState` is retained so the resumed stream continues the same message object (`chat.ts:1176-1192, 1027-1029`).
- Server: `consumeSseStream` + the `resumable-stream` package (Redis pub/sub), `activeStreamId` persisted per chat (`content/docs/04-ai-sdk-ui/03-chatbot-resume-streams.mdx:28-42, 137-191`; template `app/(chat)/api/chat/route.ts:13, 406-415`).

### Why it feels good
Synchronous optimistic append, per-message reference stability, a 4-state status that the composer button maps 1:1 to an icon, and throttling at the store boundary rather than in components.

---

## 2. Vercel AI Elements (`web-vercel-ai-elements`, `packages/elements/src/`) — VERIFIED

### Conversation
- Thin wrapper over `use-stick-to-bottom`: `<StickToBottom initial="smooth" resize="smooth" role="log" className="relative flex-1 overflow-y-hidden">` (`conversation.tsx:13-21`). Content: `flex flex-col gap-8 p-4` (`conversation.tsx:31-34`).
- Scroll-to-bottom button renders only when `!isAtBottom`, absolutely positioned `bottom-4 left-[50%]` so it never affects layout (`conversation.tsx:78-100`).

### Message / Response
- `MessageResponse` = `memo(Streamdown)` with a custom comparator: re-render only when `children` (the markdown string) or `isAnimating` changes (`message.tsx:326-340`). Plugins object is module-level (`message.tsx:324`) so its identity is stable.
- Branching is view-only: `MessageBranch` holds `currentBranch` index + array of branch elements; prev/next wrap around (`message.tsx:146-172`); label "N of M" (`message.tsx:319`).

### Reasoning
- `AUTO_CLOSE_DELAY = 1000` ms (`reasoning.tsx:55`).
- Auto-open: effect sets open when `isStreaming && !isOpen` unless `defaultOpen === false` was explicit (`reasoning.tsx:69-71, 101-105`).
- Auto-collapse: once only (`hasAutoClosed`), and only if it ever streamed, 1000 ms after streaming ends (`reasoning.tsx:108-122`).
- Duration: `Date.now()` at stream start; on end `Math.ceil(elapsed / 1000)` seconds (`reasoning.tsx:88-98`).
- Trigger text: streaming or `duration === 0` → `<Shimmer duration={1}>Thinking...</Shimmer>`; otherwise "Thought for N seconds" / "Thought for a few seconds" (`reasoning.tsx:157-165`). Chevron rotates 180° with `transition-transform` (`reasoning.tsx:188-193`).
- Content animates with `slide-in-from-top-2` / `slide-out-to-top-2` + fade (`reasoning.tsx:214`).

### Shimmer (exact implementation)
`shimmer.tsx:34-75`:
- Props: `duration = 2` (seconds), `spread = 2` (`shimmer.tsx:38-39`).
- `--spread = children.length * spread` px (`shimmer.tsx:45-48, 61`), so the highlight width scales with text length.
- Two stacked backgrounds, both clipped to text (`bg-clip-text text-transparent`):
  1. highlight: `linear-gradient(90deg, #0000 calc(50% - var(--spread)), var(--color-background), #0000 calc(50% + var(--spread)))`, `background-size: 250% 100%`, `no-repeat`;
  2. base: solid `var(--color-muted-foreground)` (`shimmer.tsx:54-55, 62-63`).
- Motion: `initial {backgroundPosition: "100% center"}` → `animate {backgroundPosition: "0% center"}`, `ease: "linear"`, `repeat: Infinity` (`shimmer.tsx:52, 58, 66-70`).
- Motion components are cached per element type at module level to avoid `motion.create` during render (`shimmer.tsx:11-24`); component is `memo`'d (`shimmer.tsx:77`).
- Note: the highlight colour is `--color-background`, i.e. the sweep is a *dimming* band toward the page colour over muted text, not a brightening band.

### Tool
- State → label map: `input-streaming: Pending`, `input-available: Running`, `approval-requested: Awaiting Approval`, `approval-responded: Responded`, `output-available: Completed`, `output-denied: Denied`, `output-error: Error` (`tool.tsx:47-55`).
- Running state is a clock icon with `animate-pulse`, not a spinner (`tool.tsx:60`). Body is a Radix collapsible with the same slide/fade classes (`tool.tsx:110`).

### Task, ChainOfThought, Plan, Queue, Confirmation, Checkpoint
- Task: collapsible, `defaultOpen = true`, items under a `border-l-2 pl-4` rail (`task.tsx:41-47, 82`).
- ChainOfThought: `defaultOpen = false`; step status → colour only (`active: text-foreground`, `complete: text-muted-foreground`, `pending: text-muted-foreground/50`); each step enters with `fade-in-0 slide-in-from-top-2 animate-in` (`chain-of-thought.tsx:45, 111-115, 131`).
- Plan: title and description render inside `<Shimmer>` while `isStreaming` (`plan.tsx:82, 107`).
- Queue: items have `pending | completed`; completed = `line-through` + 50%/40% opacity; list is a ScrollArea capped at `max-h-40` (`queue.tsx:31, 80, 101, 189-190`).
- Confirmation: renders nothing during `input-streaming` / `input-available`; request UI only in `approval-requested`; accepted/rejected variants in `approval-responded | output-denied | output-available` (`confirmation.tsx:65, 93, 111-114, 133-136`).
- Checkpoint: icon + separator row with tooltip trigger (`checkpoint.tsx:23-30, 42, 57-58`).
- Loader: **no `loader.tsx` exists at this SHA** (directory listing of `packages/elements/src`); the only spinner is the shadcn `Spinner` used in the submit button.

### PromptInput
- Textarea auto-size is pure CSS: `field-sizing-content max-h-48 min-h-16` (`prompt-input.tsx:1058`).
- Enter submits unless IME composing (`isComposing || e.nativeEvent.isComposing`) (`prompt-input.tsx:977-978`); Backspace on empty textarea removes the last attachment (`prompt-input.tsx:998-1000`).
- Submit button icon is a pure function of chat status: `submitted → Spinner`, `streaming → Square (stop)`, `error → X`, else Enter-arrow; in generating states the button becomes `type="button"` and calls `onStop` (`prompt-input.tsx:1226-1256`).
- Optimistic clear: the form is reset **immediately after capturing text**, before async attachment conversion, "to avoid race condition where user input during async blob conversion would be lost"; on `onSubmit` rejection the inputs are intentionally *not* cleared so the user can retry (`prompt-input.tsx:856-900`).

### Why it feels good
Everything in-progress is expressed as text shimmer or a pulse, collapsibles animate with one shared slide/fade idiom, and reasoning opens/closes itself with a 1 s grace period so the collapse is never simultaneous with the first answer tokens.

---

## 3. Streamdown (`web-vercel-streamdown`) — VERIFIED

### Pipeline
`children` → `remend()` (heal unterminated markdown) → `parseMarkdownIntoBlocks()` (marked block lexer) → one memoized `Block` per block, each with its own remark/rehype pipeline (`packages/streamdown/index.tsx:776-812, 1225-1248`).

### Incomplete-markdown healing: `remend` (`packages/remend/src/`)
- Only runs in `mode === "streaming"` with `parseIncompleteMarkdown` (default true) (`streamdown/index.tsx:695, 779-782`).
- Pre-step: a single trailing space is stripped (double space kept, since that is a hard break) (`remend/src/index.ts:286-288`).
- Handlers run in fixed priority order (`remend/src/index.ts:101-117`): singleTilde 0, comparisonOperators 5, htmlTags 10, setextHeadings 15, links 20, boldItalic 30, bold 35, `__` 40, `*` 41, `_` 42, inlineCode 50, strikethrough 60, block KaTeX 70, inline KaTeX 75 (opt-in, default off because `$` is ambiguous with currency, `index.ts:69-73, 257-260`).
- Rules:
  - **Bold**: trailing pattern `/(\*\*)([^*]*\*?)$/` (`patterns.ts:23-27`); skip if marker is inside fenced or inline code, or content after marker is empty / only whitespace-or-markers (`/^[\s_~*`]*$/`, `patterns.ts:63`); if the count of `**` pairs is odd, append `**`, or just `*` when the text already ends with one `*` (half-streamed closer) (`emphasis-handlers.ts:394-426`).
  - **Italic** (`*`, `_`, `__`): same shape, plus a skip for list-item markers with multiline content and horizontal rules (`emphasis-handlers.ts:429-456`).
  - **Inline code**: a span opened by N backticks closes only on N; append `N - k` where k is the trailing partial run; do nothing inside an open fence or when the span has no meaningful content (`inline-code-handler.ts:10-45`).
  - **Strikethrough**: `/(~~)([^~]*?)$/`, append `~~` (or `~` for a half closer) when the `~~` count is odd and not in code (`strikethrough-handler.ts:19-66`).
  - **Links**: `[text](partial-url` → `[text](streamdown:incomplete-link)`; `[partial text` → `...](streamdown:incomplete-link)`; alternative `linkMode: 'text-only'` shows just the text. **Images** with incomplete URL/alt become `![alt](streamdown:incomplete-image)` (`link-image-handler.ts:13, 44-53, 97-116`). After the link handler emits a placeholder, healing early-returns (`index.ts:164-166`). The renderer marks such links `data-incomplete` and disables link-safety interception (`streamdown/lib/components.tsx:329-333, 378`). Loop bounded by `MAX_HEAL_PASSES = 32` (`link-image-handler.ts:180`).
  - **HTML tags**: a trailing `<tag...` with no `>` is *stripped* (not closed), skipping code and math (`html-tag-handler.ts:7, 19-50`).
  - **Code fences are not closed by remend**: inside an unterminated fence everything is left alone (`inline-code-handler.ts:13-17`) and the renderer handles it: `hasIncompleteCodeFence(block)` walks lines per CommonMark (`/^[ \t]{0,3}(`{3,}|~{3,})/`, closer must be same char and >= length) (`streamdown/lib/incomplete-code-utils.ts:6, 29-59`), sets `isIncomplete` only for the **last** block while `isAnimating` (`streamdown/index.tsx:1227-1230`) and passes it down through `BlockIncompleteContext` (`index.tsx:587`).
- Healing is designed to be idempotent ("healed output re-heals to itself", `index.ts:316-321`).

### Block-level memoization
- Splitting uses marked's **block** tokenizer only (`lexer.blockTokens`, inline tokenizing skipped as wasted work) (`streamdown/lib/parse-blocks.tsx:105-122`).
- Incremental reuse across ticks: a block is "stable" only if it ends with a blank line **and** at least two blocks follow it (`countStableBlocks`, loop starts at `length - 3`) (`parse-blocks.tsx:124-157`). Stable blocks keep the **same string instances**; only the tail is re-lexed (`parse-blocks.tsx:254-323`). Exact-input hit returns the same array instance (`parse-blocks.tsx:345-349`).
- Bail-outs to a single block: footnotes present (`parse-blocks.tsx:330-339`) or link reference definitions present (`parse-blocks.tsx:355-362`), because those need one mdast tree.
- Merge rules: content inside an unclosed HTML block and blocks following an odd number of `$$` are merged into the previous block (`parse-blocks.tsx:179-198, 225-238`).
- React side: `Block` is `memo` with a comparator on `content` (string identity/equality), `index`, `isIncomplete`, `dir`, components (shallow by key), plugin array references, and animate-plugin presence (`streamdown/index.tsx:552, 598-656`).
- Keys are **index-based** (`${useId()}-${idx}`), explicitly *not* content hashes, because a content-hash key would unmount/remount the growing block on every token (`index.tsx:826-833`).
- `useTransition` was **removed**: "The previous displayBlocks + useTransition path could be starved by sibling urgent updates ... freezing markdown at the first parse. Eager updates are correct here." (`index.tsx:814-817`).

### Code highlighting (shiki)
- The highlighted body is `lazy()` + `Suspense`; the fallback is the **same `CodeBlockBody` rendered with un-highlighted tokens** (same DOM shape, `color: inherit`), so there is no height change when highlighting arrives (`streamdown/lib/code-block/index.tsx:35, 56-71, 98-122`).
- Highlight is async with a stale-result guard (`current` flag) and a synchronous cached return path (`code-block/highlighted-body.tsx:32-60`).
- Incremental tokenizing: completed lines are tokenized once and their shiki `grammarState` is saved; each tick only tokenizes new completed lines plus the partial tail line starting from the saved grammar state (`packages/streamdown-code/index.ts:241-270`). Up to `MAX_INCREMENTAL_STATES = 4` recent states per highlighter (`index.ts:193`).
- Results for incomplete (still streaming) blocks are not put in the LRU; complete ones are, bounded by `MAX_CACHED_COST = 300_000` and `MAX_CACHED_RESULTS = 5000` (`index.ts:112-120, 384-386`).
- Default code block max height 400 px with internal scroll (`streamdown/index.tsx:490-491, 523`); inner scroll is bottom-pinned while active with an 8 px threshold (`streamdown/lib/use-pinned-scroll.ts:3, 31-46`).

### Fade-in animation (`animated` prop; `streamdown/lib/animate.ts`, `styles.css`)
- A rehype plugin wraps **only newly arrived** words (or chars) in `<span data-sd-animate>`; each block's plugin remembers `prevContentLength` so already-visible text is never re-animated (`animate.ts:291, 554, 632-641`, `index.tsx:854, 573`).
- Defaults: `animation: "fadeIn"`, `duration: 150` ms, `easing: "ease"`, `sep: "word"`, `stagger: 40` ms (`animate.ts:615-622`). Presets `fadeIn`, `blurIn` (blur 4px), `slideUp` (translateY 4px) (`styles.css:1-30`).
- CSS: `[data-sd-animate] { animation: var(--sd-animation, sd-fadeIn) var(--sd-duration,150ms) var(--sd-easing,ease) var(--sd-delay,0ms) both; }` (`styles.css:32-35`).
- Backlog control: a shared wall-clock timeline serializes stagger across blocks and ticks. `MAX_ANIMATION_BACKLOG_MS = 320`: if the ideal cascade would end later than now+320 ms, the per-word step is compressed to fit, floored at `MIN_STAGGER_STEP_MS = 4` (`animate.ts:15, 21, 90-123`). `beginPass` clamps accumulated debt to at most 320 ms ahead of now (`animate.ts:75-83`). Render protocol: `beginPass` in render, `take` per block, `commitPass` in a layout effect; `mark`/`rewind` make StrictMode double-invocation produce identical delays (`animate.ts:31-42`, `index.tsx:881, 893-895`).
- Skipped inside `pre`, `svg`, `math`, `annotation` (`animate.ts:172`). List bullets fade via `::marker` colour animation because `::marker` ignores opacity/transform (`styles.css:37-53`).
- Caret: optional `caret="block" | "circle"` (` ▋` / ` ●`) rendered as CSS `::after` on the last block's last child, only while `isAnimating` (`index.tsx:483-486, 1107-1112, 1214-1215`).

### Math / Mermaid deferral
- Math: synchronous `rehype-katex` per block (`packages/streamdown-math/index.ts:3-4`); stability comes from remend auto-closing `$$` (priority 70) and merging blocks with an odd `$$` count. No idle deferral.
- Mermaid: `lazy()` chunk with a `CodeBlockSkeleton` fallback (`streamdown/lib/components.tsx:39, 949`). Rendering is deferred until near viewport: IntersectionObserver `rootMargin "300px"`, 300 ms debounce, then `requestIdleCallback` with 500 ms timeout (`streamdown/hooks/use-deferred-render.ts:6, 12, 17`). While the fence is still streaming only one render runs at a time; intermediate charts are skipped and the latest is rendered when the in-flight one settles; the last valid SVG is kept on screen (`streamdown/lib/mermaid/index.tsx:87, 98-106`).
- Caveat: `CodeBlockSkeleton` is a fixed 46 px header + spinner (`streamdown/lib/code-block/skeleton.tsx:13-15`), so a mermaid/custom-renderer chunk load does change height once. This is the one spinner in the stack.

### Why it feels good
Tokens never flash raw `**`/backticks/half links; finished paragraphs are literally not re-rendered; code blocks keep their geometry before and after highlighting; new words fade in over 150 ms with a self-limiting cascade.

---

## 4. use-stick-to-bottom (`web-stackblitz-labs-use-stick-to-bottom`, `src/useStickToBottom.ts`) — VERIFIED

### Constants
- Spring defaults: `damping: 0.7`, `stiffness: 0.05`, `mass: 1.25` (`useStickToBottom.ts:41-64`).
- `STICK_TO_BOTTOM_OFFSET_PX = 70` (near-bottom / re-engage threshold), `SIXTY_FPS_INTERVAL_MS = 1000/60`, `RETAIN_ANIMATION_DURATION_MS = 350` (`:131-133`).
- Target: `scrollHeight - 1 - clientHeight` (`:223-231`), i.e. it intentionally stops 1 px short.

### Spring algorithm (per rAF)
`:305-353`:
```
tickDelta   = (now - lastTick) / (1000/60)
velocity    = (damping * velocity + stiffness * scrollDifference) / mass
accumulated += velocity * tickDelta
scrollTop   += accumulated
if (scrollTop actually changed) accumulated = 0
```
- It is a velocity-based chase toward a **moving** target (the target is re-read every frame), so content that keeps growing is followed at a continuously varying speed instead of a restarting CSS smooth-scroll.
- `accumulated` carries sub-pixel movement that the browser rounded away, so slow motion does not stall.
- Frame-rate independent via `tickDelta`. Velocity and `lastTick` reset one frame after the animation ends (`:382-388`).
- `"instant"` behaviour assigns the target directly (`:335-338`).
- After reaching the target the animation is held alive for 350 ms on resize-triggered scrolls (`duration: RETAIN_ANIMATION_DURATION_MS`), so back-to-back token growth reuses the same animation and velocity rather than restarting (`:355-359, 556-562`).

### Distinguishing user scroll from content growth
- Every programmatic write records `ignoreScrollToTop = scrollTop` (`:203-221`); the scroll handler ignores an event whose `scrollTop` equals it (`:446`).
- ResizeObserver on the **content** element computes `resizeDifference = height - previousHeight` (`:528-532`). The scroll handler defers its decision by `setTimeout(..., 1)` and bails if `resizeDifference` is non-zero, because scroll events can arrive before the ResizeObserver callback (`:435-448`). `resizeDifference` is cleared after `rAF + setTimeout(1)` (`:584-590`).
- Escape rules (`:456-475`): scrolling **up** → `escapedFromLock = true`, `isAtBottom = false`; scrolling **down** → `escapedFromLock = false`; if not escaped and within 70 px → `isAtBottom = true` (re-engage).
- Wheel listener escapes immediately on `deltaY < 0` when the scroller actually overflows, because "the browser may cancel the scrolling from the mouse wheel if we update it from the animation in meantime" (`:481-509`).
- Text selection: while the mouse is down with a selection intersecting the scroller, the animation loop idles and a scroll counts as an escape (`:135-147, 159-174, 323-325, 450-454`).
- Both listeners are `{ passive: true }` (`:514-515`).

### Resize logic
- Positive resize: `scrollToBottom({ animation: resize ?? initial on first measure, wait: true, preserveScrollPosition: true, duration: 350 })`; `preserveScrollPosition` means it only scrolls if already at bottom (`:544-562`, `:283-285`).
- Negative resize: if now within 70 px, un-escape and mark at-bottom (content shrank into view) (`:563-573`).
- Overscroll clamp: if `scrollTop > targetScrollTop`, set it back (`:534-540`).
- Overrides CSS `scroll-behavior: smooth` around each write so the browser does not intercept (`:205-219`).

### overflow-anchor
- README: "Does not require `overflow-anchor` browser-level CSS support which Safari does not support" and "Correctly handles Scroll Anchoring" (`README.md:15, 19`). There is no reference to `overflow-anchor` in `src/` (grep). The mechanism above (explicit ResizeObserver + own scroll writes) replaces it, which makes behaviour identical across engines.
- Platform note (VERIFIED as a statement in TanStack source, UNVERIFIED as platform fact): TanStack Virtual's iOS shim says "WebKit shipped `overflow-anchor`" in the iOS 27 release and uses `CSS.supports('overflow-anchor: auto')` as its iOS-27 feature check (`web-TanStack-virtual/packages/virtual-core/src/ios-gesture.ts:17-33`). use-stick-to-bottom's README predates that.

### Why it feels good
One continuous spring instead of repeated `scrollIntoView({behavior:"smooth"})` calls; the user can always break away with a single wheel tick; re-engagement is generous (70 px) but only when moving down.

---

## 5. assistant-ui (`web-assistant-ui-assistant-ui`) — VERIFIED

### Runtime / thread model
- Messages live in a **tree**, `MessageRepository`: `Map<id, RepositoryMessage>`, a `head` pointer, each node has `prev`, `children`, `next` (the selected child), `level` (`packages/core/src/runtime/utils/message-repository.ts:203-233`). API: `getMessages(headId)`, `addOrUpdateMessage(parentId, message)`, `getBranches`, `switchToBranch`, `resetHead`, `deleteMessage`, `export`/`import` (`:323, 346, 394, 443, 493, 549, 563, 637`).
- The visible thread is the path root → head. Edit = add a sibling under the same parent; regenerate = add a sibling assistant message; branch picker just calls `switchToBranch`.

### Optimistic updates
- Local runtime `append`: user message is created with status `complete`, added to the repository and subscribers notified **before** any await (`packages/core/src/runtimes/local/local-thread-runtime-core.ts:990-995`). If thread initialization fails the append is rolled back and rejected as `MessageNotSentError` "so the composer restores the draft" (`:997-1013`). History persistence is chained after display (`:1019-1036`).
- `startRun` creates the assistant message immediately with a client id and `status: { type: "running" }`, `content: []` (`:1166-1182`), so a real assistant row exists from t=0.
- External-store runtime: while `isRunning` and no trailing assistant message exists, it inserts a **placeholder assistant message** with `metadata.isOptimistic: true`, status `running`, and a generated id that is **reused while the parent id is unchanged** (`packages/core/src/runtimes/external-store/external-store-thread-runtime-core.ts:861-880`). `export()` never persists these. This is the cleanest fix for the "thinking row swaps for the real row" flash noted in section 1.

### Smooth text streaming: `useSmooth` (`packages/react/src/utils/smooth/useSmooth.ts`)
- Defaults: `drainMs = 250`, `maxCharIntervalMs = 5`, `maxCharsPerFrame = Infinity`, `minCommitMs = 0` (`:19-48`).
- Per rAF (`:82-123`):
  ```
  remaining      = target.length - shown.length
  timePerChar    = min(maxCharIntervalMs, drainMs / remaining)
  while (budget >= timePerChar && added < min(remaining, maxCharsPerFrame)) { added++; budget -= timePerChar }
  shown = target.slice(0, shown.length + added)
  lastUpdateTime = now - leftoverBudget
  ```
  So the reveal rate is at least 200 chars/s (5 ms/char) and accelerates so that any backlog drains in about 250 ms. Leftover time is banked for the next frame, except when the per-frame cap was hit (`:99-103`).
- `minCommitMs` decouples the reveal cursor from React commits: the cursor advances every frame but `setText` (and therefore markdown re-parse) fires at most once per interval; the final frame always commits (`:38-44, 118-122`).
- Correctness guards: render-phase resync when the part identity changes or the new text is not a continuation of the displayed prefix (prevents showing the previous thread's text for one paint) (`:177-192`); if the source settles before any char was revealed, commit the full text immediately so a missed frame cannot leave an empty bubble (`:268-277`).
- Disabled under `prefers-reduced-motion: reduce` even if `smooth={true}` (`:161-164`).
- While the reveal lags the source, the part reports status `running` so the caret/shimmer stays until the text has visually caught up (`:126-128, 291-301`).
- `MarkdownText` defaults `smooth = true` (`packages/react-markdown/src/primitives/MarkdownText.tsx:212, 219`). The markdown renderer is `memo`'d and an optional `useDeferredValue` path is isolated in its own component so that the urgent pass bails out on the memo instead of parsing twice (`MarkdownText.tsx:73-93`).

### Viewport auto-scroll (`packages/store/src/utils/thread-viewport-auto-scroll.ts`, `viewport-scroll.ts`)
- At-bottom test: `|scrollHeight - scrollTop - clientHeight| <= 1` or content does not overflow (`viewport-scroll.ts:12-22`). Content insets (e.g. floating composer height) are subtracted (`:24-30`).
- Growth detection: `ResizeObserver` on the viewport **and each direct child**, plus a `MutationObserver` to (un)observe added/removed children, because content can grow with no DOM mutation (CSS height animation, image/font load) (`viewport-scroll.ts:57-86`).
- On growth: if following and not paused → `scrollToBottom("instant")` (`thread-viewport-auto-scroll.ts:272-284`). Following stops when the user scrolled up, detected by `scrollTop < min(lastScrollTop, lastScrollHeight - clientHeight)` with `scrollHeight` not shrinking (`:222-235`).
- **Input-intent tracking**: wheel `deltaY < 0`, touchmove moving down the screen, and `ArrowUp/PageUp/Home/Shift+Space` set an `upwardInput` flag that lives exactly two animation frames, "so the signal lasts two frames and cannot outlive the scroll it caused" (`:168-179, 350-370`). Nested scrollers that can still scroll up absorb the gesture (`:335-348`).
- **Pause follow on expand**: a capture-phase click on a disclosure (`[aria-expanded="false"]` or closed `<summary>`) inside a message sets `followPaused` so opening a tool/reasoning panel while streaming does not yank the view to the bottom (`:8, 28-39, 316-328, 432`).
- Pending smooth scroll-to-bottom is cancelled by pointerdown / wheel / touchstart / scroll keys (`:302-314, 424-431`).
- Prepend anchoring: on history prepend the first visible message's viewport offset is captured and restored with an instant scroll (`:136-147`, `packages/react/src/primitives/thread/useThreadViewportAutoScroll.ts:84-101`).
- Events: scroll to bottom on run start, on first messages, on thread switch (all default true) (`useThreadViewportAutoScroll.ts:50-55, 107-114`).

### Top-anchor turn mode (the ChatGPT/Claude-style "question jumps to top")
- `turnAnchor: "top"` disables bottom-follow (`useThreadViewportAutoScroll.ts:70-78`) and instead pins the last user message to the top of the viewport by appending a **reserve spacer** after the target whose height is `max(0, targetScrollTop + viewport.clientHeight - (scrollHeight - reserve.offsetHeight))` (`packages/react/src/primitives/thread/topAnchor/computeTopAnchorSlack.ts:73-94`).
- Tall user messages are clamped: if taller than `10em`, only `6em` stays visible (`packages/react/src/context/stores/ThreadViewport.ts:230-231`; math at `computeTopAnchorSlack.ts:59-71`).
- Uses layout offsets (`offsetTop`), not `getBoundingClientRect`, "so entrance transforms/animations on the anchor do not shift the scroll target while they settle", and never reads `scrollHeight` for the target because it is volatile while streaming (`computeTopAnchorSlack.ts:30-58`).
- The same idea exists in Legend List as `anchoredEndSpace` (`web-LegendApp-legend-list/src/types.base.ts:282-285`) and in Virtuoso Message List's AI example (`index: 'LAST', align: 'start'`, `web-petyosi-react-virtuoso/packages/message-list/docs/3.examples/02.ai-chatbot.md:95-96`).

### Collapse without scroll jump
- `useScrollLock(animatedElementRef, animationDuration)`: when a reasoning/tool group collapses, it finds the scrollable ancestor, records `scrollTop`, hides the scrollbar (`scrollbar-width: none`, compensating padding), and re-pins `scrollTop` on every scroll event for the animation duration (`packages/react/src/primitives/reasoning/useScrollLock.ts:27, 57, 69, 111, 132-134`). Reasoning element uses `ANIMATION_DURATION = 200` ms and chevron easing `cubic-bezier(0.32,0.72,0,1)`; reasoning body is capped at `max-h-64 overflow-y-auto` (`packages/ui/src/components/react/assistant-ui/elements/reasoning.tsx:22, 209, 306`).

### tw-shimmer
See section 7; it is assistant-ui's own package (`packages/tw-shimmer/src/index.css`).

---

## 6. Virtualized chat lists

Summary judgement for a bottom-anchored chat whose last row grows while streaming:

| Library | Chat-specific API (VERIFIED) | Streaming-row behaviour | Verdict |
|---|---|---|---|
| TanStack Virtual | `anchorTo: 'end'`, `followOnAppend: boolean \| ScrollBehavior`, `scrollEndThreshold` (default 1; docs use 80), `scrollToEnd()`, `isAtEnd()` (`packages/virtual-core/src/index.ts:393-395, 809-811`; `docs/chat.md:9-19`) | If pinned within threshold before a size change, adjusts by the delta (`index.ts:2080-2084`). Re-measure of an item that merely **spans** the fold is *not* compensated, specifically for "a streaming chat message growing at its bottom" (`index.ts:2090-2103`) | Best-documented open-source option now; normal DOM order, no `column-reverse` |
| virtua | `shift` (anchor from end, only while prepending) and `reverse`; no built-in follow (`src/react/Virtualizer.tsx:119-124`) | Resize of items above the viewport is compensated via a `jump`; chat story hand-rolls `shouldStickToBottom` + `scrollToIndex(last, {align:"end"})` (`stories/react/advanced/Chat.stories.tsx:105-117, 181-190`) | Smallest and very solid measurement; you write the follow logic |
| react-virtuoso | `followOutput`, `alignToBottom`, `atBottomThreshold` (default 4 px) (`packages/react-virtuoso/src/component-interfaces/Virtuoso.ts:158-174, 275-289`; `stateFlagsSystem.ts:57, 91`) | Follow triggers on **count** change; a 100 ms "trap next size increase" window catches growth right after (`followOutputSystem.ts:52-110`) | Fine for append; weaker for one row growing for 30 s |
| Virtuoso Message List (commercial) | `scrollModifier`: `auto-scroll-to-bottom` (callback gets `atBottom`, `scrollInProgress`), `prepend`, `items-change`, plus `purgeItemSizes` on conversation switch (`packages/message-list/docs/20.scroll-modifier.md:20, 35-110`) | `items-change` exists precisely for "a streaming bot response" (`:97-110`). Source is **not in the repo** (docs only); behaviour claims are from docs | Most purpose-built; closed source, licensed |
| Legend List | `maintainScrollAtEnd`, `maintainScrollAtEndThreshold` (default 0.1 of screen), `alignItemsAtEnd`, `initialScrollAtEnd`, `maintainVisibleContentPosition`, `anchoredEndSpace` (`src/types.base.ts:113-117, 182-187, 252-285`) | Web + React Native from one API | Best if RN/web parity matters |

### Known pitfalls
- **iOS momentum vs scroll writes** (VERIFIED in source comments): on iOS WebKit a programmatic `scrollTop` write during momentum scrolling stops the scroll; virtua defers the correction (`pendingJump`) until idle (`web-inokawa-virtua/src/core/store.ts:170-183`). A dragging finger ignores the write entirely and iOS only fires touch events at the start of momentum (`src/core/driver.ts:536-538`). TanStack records that iOS 27 changed this (momentum now overrides the write within a frame) and that rubber-band overscroll discards writes (`web-TanStack-virtual/packages/virtual-core/src/ios-gesture.ts:1-33`; `index.ts:1417-1428`).
- **Measuring during streaming**: every token changes the last row's height → ResizeObserver → scroll correction. Only compensate rows entirely above the viewport (TanStack `index.ts:2086-2103`; virtua `store.ts:160-167`).
- **Index keys break prepend anchoring**; use message ids (`docs/chat.md:46-52`; virtua README:440-449).
- **Do you need virtualization at all?** None of AI Elements, prompt-kit, the Chat SDK template, or assistant-ui's default thread virtualize; they rely on memoized rows + a stick-to-bottom scroller. For long agent transcripts the cheaper first step is `content-visibility: auto` (section 9). TanStack additionally documents pre-computing text heights with Pretext to avoid measurement corrections (`docs/pretext.md:1-20`).

---

## 7. Shimmer / thinking affordances

### Implementations (all VERIFIED)

| Source | Technique | Gradient | background-size | Motion | Duration / easing |
|---|---|---|---|---|---|
| AI Elements `Shimmer` (`web-vercel-ai-elements/packages/elements/src/shimmer.tsx:38-70`) | 2 layers clipped to text, animated `background-position` via Motion | `90deg, #0000 calc(50% - S), var(--color-background), #0000 calc(50% + S)` over solid `--color-muted-foreground`; `S = chars * 2 px` | `250% 100%` | `100% center → 0% center`, infinite | 2 s (1 s in Reasoning), linear |
| motion-primitives `TextShimmer` (`web-ibelick-motion-primitives/components/core/text-shimmer.tsx:18-49`) | Identical structure (AI Elements derives from it) | base `#a1a1aa` (dark `#71717a`), highlight `#000` (dark `#fff`) | `250% 100%` | same | 2 s, linear |
| prompt-kit `TextShimmer` (`web-ibelick-prompt-kit/components/prompt-kit/text-shimmer.tsx:15-33`) | Single gradient, pure CSS keyframes | `to right, muted-fg (50-S)%, foreground 50%, muted-fg (50+S)%`, `S = clamp(spread,5,45)`, default 20 | `200% auto` | `background-position 200% 50% → -200% 50%` (`app/globals.css:129-136`) | 4 s, linear, infinite |
| prompt-kit `TextShimmerLoader` (`components/prompt-kit/loader.tsx:412-417`) | same | `muted-fg 40%, foreground 60%, muted-fg 80%` | `200% auto` | same | 4 s linear |
| magic-ui `AnimatedShinyText` (`web-magicuidesign-magicui/apps/www/registry/magicui/animated-shiny-text.tsx:16-33`; keyframes `apps/www/styles/globals.css:158, 331-341`) | Narrow band over text | `to right, transparent, black/80 50%, transparent` (dark `white/80`); band width `--shiny-width: 100px` | `100px 100%`, no-repeat | position `calc(-100% - W) → calc(100% + W)` at 30-60%, back at 90% | 8 s cycle, mostly idle |
| assistant-ui `tw-shimmer` (`web-assistant-ui-assistant-ui/packages/tw-shimmer/src/index.css`) | Speed-based; composited overlay | 17-stop sine falloff (`:94-123`), angle `15deg` (`:7-11`), spread `calc(4ch + 80px)` (`:63`), highlight = `currentColor` at 20% alpha (dark: lightness +0.4) (`:66-69, 79-85`) | computed from track + gradient width | **speed 200 px/s** (`:62`), repeat delay `20000/speed` = 100 ms (`:38`) | duration derived from width so speed is constant across elements; linear |
| shadcn `Skeleton` (`web-ibelick-prompt-kit/components/ui/skeleton.tsx:7`) | block placeholder | `bg-primary/10` | n/a | Tailwind `animate-pulse` (opacity) | Tailwind default 2 s `cubic-bezier(0.4,0,0.6,1)` (UNVERIFIED: Tailwind default, not read here) |

### Why tw-shimmer is the best of these
1. **Constant speed, not constant duration**: `duration = (track width + gradient width) / speed` (`index.css:31-37`), so a 3-word label and a 60-char tool title sweep at the same px/s. The duration-based ones sweep long text visibly faster.
2. **Compositor-only path**: where `-webkit-mask-clip: text` is supported it does not animate a text-clipped background at all ("A text-clipped background makes Chrome re-record the text mask when the text changes", `:142-146`). It masks the element to the glyphs and translates a `::before` gradient overlay with `will-change: translate` (`:154-203`). Background-position animation of `background-clip: text` repaints every frame; this does not.
3. **Sine falloff with 17 stops** instead of a 3-stop triangle, which removes the visible band edge.
4. Reduced motion pauses it (`:206-210`); unsupported browsers get plain static text rather than invisible text (`:71-76, 134-140`).
5. `--shimmer-x/--shimmer-y` offsets let several lines share one continuous sweep (`:42-45, 57-59`).

### What ChatGPT / Claude.ai / Cursor ship — UNVERIFIED (product observation from memory, not inspected during this task)
- ChatGPT: "Thinking" / "Searching the web" labels with a left-to-right text-gradient sweep on muted text, roughly 1.5-2 s linear loop; collapses to "Thought for Ns" on completion; composer button becomes a stop square. https://chatgpt.com
- Claude.ai: animated spark glyph next to a status phrase with a subtle text shimmer; thinking summary is a collapsible that streams a short rolling excerpt; no spinner in the message column. https://claude.ai
- Cursor: tool/step rows ("Planning next moves", "Reading file…") use a text shimmer on the active row only, completed rows go muted static; "Thought for Ns" disclosure. https://cursor.com
- Common pattern across all three: (a) shimmer only on the **currently active** line, (b) wording changes with the phase, (c) the label is replaced in place by a past-tense static label with a duration, (d) no layout change at the transition.

---

## 8. Other libraries worth knowing

- **prompt-kit** (VERIFIED): `ChatContainer` = `StickToBottom resize="smooth" initial="instant"` (`web-ibelick-prompt-kit/components/prompt-kit/chat-container.tsx:27-30`). Its `Markdown` is the minimal version of the Streamdown idea: `marked.lexer` → blocks → `memo` per block (`components/prompt-kit/markdown.tsx:16-17, 60, 92`). `ResponseStream` offers `typewriter` and `fade` modes with `speed` 1-100 (default 20) (`components/prompt-kit/response-stream.tsx:6, 33-34, 282-288`). Loaders include dots, wave, bars, text-shimmer (`app/globals.css:20-136`).
- **llm-ui** (VERIFIED): a frame-rate-aware *throttle* that smooths to the model's measured token rate rather than a fixed rate: defaults `readAheadChars: 15`, `targetBufferChars: 15`, `adjustPercentage: 0.2`, `frameLookBackMs: 10000`, `windowLookBackMs: 2000` (`web-llm-ui-kit-llm-ui/packages/react/src/throttle/basic.ts:12-16`). It keeps ~15 chars of look-ahead hidden (enough to recognise markdown syntax before showing it) and speeds up/slows down by 20% to hold the buffer at target (`basic.ts:121-135`). Different philosophy from remend: hide ambiguous tail vs. heal it.
- **incremark** (VERIFIED): a true incremental parser. `append(chunk)` finds a "stable boundary" line, parses only `lines[pendingStartLine..stableBoundary]` into `completedBlocks` (each with a stable id), and re-parses only the pending tail (`web-kingshuaishuai-incremark/packages/core/src/parser/IncremarkParser.ts:71-72, 196-273`). Streamdown now gets most of this benefit via `reuseParsedBlocks`.
- **Chat SDK template** (VERIFIED): composes AI SDK + AI Elements + `resumable-stream`; URL is updated with `history.replaceState/pushState` rather than a router navigation so sending the first message does not remount the chat (`web-vercel-ai-chatbot/hooks/use-active-chat.tsx:228`, `components/chat/multimodal-input.tsx:230`).
- **Virtuoso Message List**: see section 6.
- **CopilotKit**: UNVERIFIED, not cloned; ships its own chat components and has been moving to the AG-UI protocol; no distinctive chat-feel mechanism known to me beyond the above. https://github.com/CopilotKit/CopilotKit

---

## 9. Web platform features for jank-free streaming

All UNVERIFIED against specs in this task (general platform knowledge; MDN URLs given), except where a source citation is attached.

- **`content-visibility: auto` + `contain-intrinsic-size: auto <len>`**: skips layout/paint for off-screen messages; `auto` remembers the last rendered size so the scrollbar does not jump after first render. Cheap alternative to virtualization for long transcripts. Do not apply to the streaming message. https://developer.mozilla.org/en-US/docs/Web/CSS/content-visibility
- **`overflow-anchor`**: browser scroll anchoring keeps the viewport stable when content above changes. For stick-to-bottom, the classic trick is `overflow-anchor: none` on messages and `auto` on a 1 px sentinel at the end. Historically unsupported in Safari, which is why use-stick-to-bottom avoids it (VERIFIED: `web-stackblitz-labs-use-stick-to-bottom/README.md:15`); TanStack source states WebKit shipped it in iOS 27 (VERIFIED as source statement: `ios-gesture.ts:17-21`). If you run your own scroll controller, set `overflow-anchor: none` on the scroller so the browser and your code do not both compensate. https://developer.mozilla.org/en-US/docs/Web/CSS/overflow-anchor
- **View Transitions** (`document.startViewTransition`, `view-transition-name`): good for the composer-to-bubble morph on send and for thread switches; bad for per-token updates (each transition snapshots and blocks rendering for a frame). In React, `<ViewTransition>` is still experimental. https://developer.mozilla.org/en-US/docs/Web/API/View_Transition_API
- **`scrollbar-gutter: stable`**: reserves scrollbar space so text does not reflow when the conversation first overflows. assistant-ui's scroll lock has to do this compensation by hand with padding (VERIFIED: `useScrollLock.ts:57-65`). https://developer.mozilla.org/en-US/docs/Web/CSS/scrollbar-gutter
- **`interpolate-size: allow-keywords`** / `calc-size()`: animate `height: auto` for collapsibles without measuring in JS; Chromium-only at the time of writing, so keep the Radix `--radix-collapsible-content-height` path as fallback. https://developer.mozilla.org/en-US/docs/Web/CSS/interpolate-size
- **`field-sizing: content`**: auto-growing textarea with no JS (VERIFIED in use: `prompt-input.tsx:1058`). https://developer.mozilla.org/en-US/docs/Web/CSS/field-sizing
- **`requestAnimationFrame` batching**: coalesce all deltas that arrive within a frame into one state write. assistant-ui's animator and use-stick-to-bottom both run on rAF (VERIFIED: `useSmooth.ts:82-123`; `useStickToBottom.ts:306`). A 30-50 ms throttle (AI SDK) is the coarser version.
- **`startTransition` / `useDeferredValue`**: both Vercel libraries moved **away** from transitions for streamed text (VERIFIED: `use-chat.ts:271-276`; `streamdown/index.tsx:814-817`) because transition updates can be starved or entangled with navigation. Safe use: defer *secondary* consumers (token counters, outline, minimap) or isolate `useDeferredValue` behind a memoized renderer as assistant-ui does (VERIFIED: `MarkdownText.tsx:83-93`).
- **React Compiler**: auto-memoizes components and hook values, which removes most accidental re-renders of settled messages, but it cannot fix a store that hands out new object identities each tick. The contracts still have to be designed: immutable-per-message updates (AI SDK `replaceMessage`), stable plugin/config objects at module scope (AI Elements `streamdownPlugins`), index-stable block keys (Streamdown). Custom `memo` comparators like `MessageResponse`'s remain meaningful because they compare by string value. https://react.dev/learn/react-compiler
- **Passive listeners** for wheel/touch/scroll (VERIFIED in both scroll controllers), `contain: content` on message rows, and animating only `opacity`/`transform` (Streamdown's fade presets, tw-shimmer's translate path) keep work off the main thread.

---

## (a) Reference recipe: jank-free streaming chat in React

1. **Store**: external chat store with immutable-per-message updates and separate channels for messages vs status (AI SDK `Chat`). Subscribe with `throttle: 30-50` ms or one rAF. Do not wrap token updates in `startTransition`.
2. **Send (optimistic)**: on submit, synchronously (a) push the user message with a client id, (b) push an assistant placeholder with a client id and `status: running` (assistant-ui pattern) so the row never swaps, (c) clear the composer immediately, (d) flip status to `submitted`. On failure keep the user message, mark it failed, restore the draft.
3. **Reconciliation**: key rows by client id forever; if the server assigns ids, store them as a field, not as the key. Reuse the placeholder id until a real assistant message with the same parent arrives.
4. **Pre-first-token**: inside the assistant row show a text shimmer ("Thinking…"), not a spinner row. Use the tw-shimmer approach (200 px/s constant speed, composited overlay). Keep it until the *visible* text has started (smooth-status), then cross to content in place.
5. **Text pacing**: either server-side `smoothStream({ chunking: 'word', delayInMs: 10 })` or client-side `useSmooth` (`drainMs 250`, `maxCharIntervalMs 5`, `minCommitMs ≈ 32`). Use one, not both.
6. **Markdown**: Streamdown with remend on while streaming; module-level plugin objects; `memo` the response on `(text, isAnimating)`. Optional `animated={{ animation: 'fadeIn', duration: 150, stagger: 40, sep: 'word' }}`.
7. **Code blocks**: render plain tokens in the final DOM shape first, upgrade to shiki asynchronously with incremental grammar state; cap height (400 px) with inner bottom-pinned scroll.
8. **Scroll**: use-stick-to-bottom (`resize="smooth"`, `initial="instant"` for restored threads) or the assistant-ui controller if you need pause-on-expand and top-anchor turns. Set `overflow-anchor: none` on the scroller and `scrollbar-gutter: stable`. Always scroll to bottom on local send; only follow remote growth when pinned (threshold 70 px to re-engage, any upward wheel/touch to escape).
9. **Reasoning/tool rows**: auto-open while streaming, auto-collapse 1000 ms after, with a 200 ms height animation and scroll lock during collapse; pause follow when the user opens a disclosure. Tool status is a label + pulse/shimmer, never a layout-changing spinner.
10. **Long threads**: `content-visibility: auto; contain-intrinsic-size: auto 120px` on settled messages first; only if that is insufficient, TanStack Virtual with `anchorTo: 'end'`, `followOnAppend`, `scrollEndThreshold: 80`, id keys.
11. **Resume**: on mount and on tab-visible, probe for an active stream before showing any pending state (AI SDK avoids the `submitted` flash this way); continue the same message object.
12. **Motion budget**: 150 ms word fade, 200 ms collapsibles, 320 ms maximum animation backlog, respect `prefers-reduced-motion` everywhere.

## (b) Ideas that port directly to a native Swift client

| Web idea (source) | Native equivalent |
|---|---|
| Immutable-per-message updates, untouched rows keep identity (`chat.react.ts:69-86`) | Value-type message models with stable `id`; diffable data source / `Equatable` row views so only the streaming row re-renders |
| Separate messages vs status channels, throttle only messages (`use-chat.ts:304-311`) | Separate `@Observable` properties; coalesce token deltas on a `CADisplayLink` tick before publishing |
| Optimistic user push + optimistic assistant placeholder with reused id (`external-store-thread-runtime-core.ts:861-880`) | Insert both rows synchronously on send with client UUIDs; map server ids as a field, never as row identity |
| 4-state status drives the send button icon (`prompt-input.tsx:1226-1235`) | One enum → SF Symbol swap with `contentTransition(.symbolEffect(.replace))` |
| Clear composer immediately, restore draft on failure (`prompt-input.tsx:856-900`; `local-thread-runtime-core.ts:997-1013`) | Reset the text view on submit; keep the draft string to restore in the failure path |
| `useSmooth` reveal: `timePerChar = min(5 ms, 250 ms / backlog)` (`useSmooth.ts:87-98`) | Display-link driven reveal cursor over an `AttributedString`, commit to the text view at most every 2 frames |
| `smoothStream` word chunking `/\S+\s+/`, skip when hidden (`smooth-stream.ts:5-17`) | Emit on word boundaries; flush instantly when the window is occluded/app inactive |
| remend healing rules + placeholder URL for half links (`remend/src/*`) | Pre-pass on the markdown tail before parsing: close `**`/`*`/backticks/`~~`, replace partial links with inert placeholder, strip partial HTML tag |
| Stable-prefix block reuse: a block is final when it ends with a blank line and two blocks follow (`parse-blocks.tsx:150-157`) | Cache parsed/laid-out blocks by index; re-parse only the tail; keep `NSTextLayoutFragment`s for settled blocks |
| Index-based block identity, not content hash (`streamdown/index.tsx:826-833`) | Identify block views by position within the message so the growing paragraph is updated, not replaced |
| Plain-then-highlighted code with identical geometry; incremental grammar state per completed line (`code-block/index.tsx:56-122`; `streamdown-code/index.ts:241-270`) | Render monospaced plain text first, apply highlight attributes in place (no relayout); cache tokenizer state at last newline |
| Word fade 150 ms / 40 ms stagger with 320 ms backlog cap (`animate.ts:15-21, 615-622`) | Per-run opacity animation on newly appended ranges (TextKit 2 rendering attributes or `CATextLayer` fragments); compress stagger when behind |
| Spring follow toward a moving target: damping 0.7, stiffness 0.05, mass 1.25 per 60 Hz tick (`useStickToBottom.ts:41-64, 340-345`) | Display-link driven `contentOffset` chase with the same recurrence (not `scrollToBottom(animated:)` per token) |
| Escape on any upward user input; re-engage within 70 px when moving down (`useStickToBottom.ts:131, 456-475, 498-506`) | Track `isTracking/isDragging/isDecelerating` and scroll-wheel deltas; unpin on upward user scroll, re-pin within ~70 pt |
| Ignore own scroll writes and resize-caused scroll events (`useStickToBottom.ts:203-221, 442-448`) | Flag programmatic offset changes and content-size changes so the delegate does not treat them as user scrolls |
| 350 ms animation retention so consecutive growth reuses one animation (`useStickToBottom.ts:133, 556-562`) | Keep the follow animator alive briefly after reaching bottom instead of restarting per layout pass |
| Pause follow when the user opens a disclosure (`thread-viewport-auto-scroll.ts:316-328`) | Suspend auto-follow when the user expands a tool/reasoning row until they scroll again |
| Top-anchor turn with reserve spacer; clamp tall user message to 6em visible (`computeTopAnchorSlack.ts:59-94`; `ThreadViewport.ts:230-231`) | On send, scroll the user bubble to the top and add bottom content inset = viewport height − content below anchor; shrink as the answer grows |
| Prepend anchoring by first-visible message offset (`thread-viewport-auto-scroll.ts:136-147`) | Capture anchor row frame before inserting history, restore offset after layout in the same transaction |
| Scroll lock during collapse animation (`useScrollLock.ts`) | Pin `contentOffset` (or anchor the row above) for the duration of the collapse animation |
| Reasoning auto-open, auto-close after 1000 ms, "Thought for Ns" (`reasoning.tsx:55, 88-122, 157-165`) | Same timers on a `DisclosureGroup`-like row; replace shimmer label in place with the duration label |
| Constant-speed shimmer, 200 px/s, sine falloff, translate-only (`tw-shimmer/src/index.css:31-38, 62-63, 94-123, 154-203`) | `CAGradientLayer` masked by text (or `mask` in SwiftUI), animate layer `position.x` at 200 pt/s with duration = width/speed; stop on Reduce Motion |
| Deferred heavy embeds: 300 px margin, 300 ms debounce, idle (`use-deferred-render.ts:6-17`) | Render diagrams/images when the row is within a prefetch margin and the run loop is idle; keep last valid render while streaming |
| Resume: probe before showing pending state (`chat.ts:931-960`) | On foreground/reconnect, ask the daemon for an active stream before flipping UI to "working" |
| Follow only when pinned; always scroll on local send (Virtuoso docs `20.scroll-modifier.md:75-80`) | Two policies: `scrollToBottomAlways` for own sends, `ifAtBottom` for agent output |
| Only compensate rows fully above the viewport (`TanStack index.ts:2086-2103`) | When a row above resizes, adjust offset by the delta; never adjust for the row spanning the bottom edge |
| Defer scroll corrections during momentum (`virtua store.ts:170-183`) | Do not set `contentOffset` while `isDecelerating`; queue and apply on `scrollViewDidEndDecelerating` |
