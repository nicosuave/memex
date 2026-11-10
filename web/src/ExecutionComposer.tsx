import { useImperativeHandle, useRef, useState, type RefObject } from "react"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { captureExecutionAttachment, emptyDraft, isBlankDraft, type ExecutionClient, type ExecutionDraft, type HostSnapshot } from "@/execution"

export type PromptKind = "send" | "steer" | "queue.add"
export type ComposerHandle = {
  /** Applies `change` to a conversation's draft (on screen or saved) and reports whether it changed anything. */
  edit: (conversationId: string, change: (draft: ExecutionDraft) => ExecutionDraft) => boolean
}
// `attempted`: the user tried to send while blocked, so a quiet reason is shown.
type ComposerState = { conversationId: string; draft: ExecutionDraft; error: string; attempted: boolean }

function loadState(client: ExecutionClient, conversationId: string): ComposerState {
  try { return { conversationId, draft: client.draft(conversationId), error: "", attempted: false } }
  catch (error) { return { conversationId, draft: emptyDraft, error: `Saved draft could not be read: ${String(error)}`, attempted: false } }
}

/**
 * Owns the draft so typing re-renders only the composer. Text input is never disabled; a blocked send
 * says why next to the buttons.
 */
export function ExecutionComposer({ client, conversationId, promptCapabilities, running, canSteer, sendBlockedReason, quietReason, queueBlockedReason, error, onSubmit, handleRef }: {
  client: ExecutionClient
  conversationId: string
  promptCapabilities: NonNullable<HostSnapshot["controls"]>["promptCapabilities"] | undefined
  /** A turn is running or a send is on its way, so Cmd+Enter adds to the host queue instead. */
  running: boolean
  canSteer: boolean
  /** Why a new prompt cannot start now, apart from a running turn. Null allows sending. */
  sendBlockedReason: string | null
  /** Hold the reason back (a load that may finish within 200 ms) unless the user tries to send. */
  quietReason: boolean
  /** Why nothing may be sent at all (an earlier prompt's delivery is unconfirmed). */
  queueBlockedReason: string | null
  error: string
  /** Accepts the prompt for delivery, or returns why it could not be saved for sending. */
  onSubmit: (conversationId: string, kind: PromptKind, draft: ExecutionDraft) => string | null
  handleRef: RefObject<ComposerHandle | null>
}) {
  const [stored, setStored] = useState(() => loadState(client, conversationId))
  let state = stored
  if (stored.conversationId !== conversationId) {
    state = loadState(client, conversationId)
    setStored(state)
  }
  const { draft } = state
  const latest = useRef(state)
  latest.current = state
  const textarea = useRef<HTMLTextAreaElement>(null)

  useImperativeHandle(handleRef, () => ({
    edit(target, change) {
      if (target !== latest.current.conversationId) {
        try { return client.editDraft(target, change) } catch { return false }
      }
      const next = change(latest.current.draft)
      if (next === latest.current.draft) return false
      latest.current = { ...latest.current, draft: next }
      setStored(current => current.conversationId === target ? { ...current, draft: next } : current)
      try { client.saveDraft(target, next) } catch { /* Kept on screen; the next edit saves it again. */ }
      return true
    },
  }), [client])

  function update(next: ExecutionDraft) {
    // Typing is never blocked by local storage; a failed save is reported instead.
    setStored(current => ({ ...current, draft: next }))
    try { client.saveDraft(conversationId, next) }
    catch (failure) { setStored(current => ({ ...current, error: `Draft could not be saved locally: ${String(failure)}` })) }
  }

  const workingHint = canSteer
    ? "The agent is working. ⌘/Ctrl+Enter queues this message; Steer adds it to the current turn."
    : "The agent is working. ⌘/Ctrl+Enter queues this message for the next turn."
  const sendDisabledReason = queueBlockedReason ?? (running ? workingHint : sendBlockedReason)
  const empty = isBlankDraft(draft)

  function submit(kind: PromptKind) {
    const outgoing = latest.current.draft
    if (kind === "steer" ? !outgoing.text.trim() : isBlankDraft(outgoing)) return
    if (kind === "send" ? sendDisabledReason !== null : kind === "steer" ? queueBlockedReason || !canSteer : queueBlockedReason) {
      setStored(current => current.attempted ? current : { ...current, attempted: true })
      return
    }
    const failure = onSubmit(conversationId, kind, outgoing)
    if (failure) {
      setStored(current => ({ ...current, error: failure }))
      return
    }
    setStored(current => ({ ...current, draft: emptyDraft, error: "" }))
    try { client.saveDraft(conversationId, emptyDraft) } catch { /* The outbox already holds the exact request. */ }
    textarea.current?.focus()
  }

  const shownError = error || state.error
  return <>
    <Textarea ref={textarea} aria-label="Message to agent" placeholder="Message this conversation…" value={draft.text}
      onChange={event => update({ ...latest.current.draft, text: event.target.value })}
      onKeyDown={event => {
        if (event.key !== "Enter" || !(event.metaKey || event.ctrlKey) || event.nativeEvent.isComposing) return
        event.preventDefault()
        submit(running ? "queue.add" : "send")
      }} />
    {shownError && <p className="text-sm text-destructive" role="alert">{shownError}</p>}
    <div className="flex flex-wrap items-center gap-2">
      <label className="cursor-pointer rounded border px-2 py-1 text-xs">Attach file<input className="sr-only" type="file" multiple disabled={!promptCapabilities} onChange={async event => {
        const files = Array.from(event.target.files || []); event.target.value = ""
        const target = conversationId
        if (!promptCapabilities || !files.length) return
        try {
          const attachments = await Promise.all(files.map(file => captureExecutionAttachment(file, promptCapabilities)))
          // Capture can outlive an edit or a conversation switch. Append to the
          // current draft of the original conversation only.
          if (latest.current.conversationId === target) update({ ...latest.current.draft, attachments: [...latest.current.draft.attachments, ...attachments] })
          else client.editDraft(target, current => ({ ...current, attachments: [...current.attachments, ...attachments] }))
        } catch (failure) { setStored(current => current.conversationId === target ? { ...current, error: String(failure) } : current) }
      }} /></label>
      {draft.attachments.map(attachment => <Button key={attachment.id} size="sm" variant="outline" onClick={() => update({ ...draft, attachments: draft.attachments.filter(item => item.id !== attachment.id) })}>{attachment.name} ×</Button>)}
      <div className="ml-auto flex gap-2">
        <Button variant="outline" disabled={!!queueBlockedReason || empty} onClick={() => submit("queue.add")}>Queue</Button>
        {canSteer && <Button variant="outline" disabled={!!queueBlockedReason || !draft.text.trim()} onClick={() => submit("steer")}>Steer</Button>}
        <Button disabled={sendDisabledReason !== null || empty} onClick={() => submit("send")}>Send</Button>
      </div>
    </div>
    {/* Reserved line, so a reason appearing or clearing does not move the composer. */}
    <p className="min-h-4 text-xs text-muted-foreground">{quietReason && !state.attempted ? "" : sendDisabledReason}</p>
  </>
}
