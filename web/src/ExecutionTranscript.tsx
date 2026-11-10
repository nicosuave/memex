import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { MessageContent } from "@/MessageContent"
import { useDelayedFlag } from "@/hooks/use-delayed-flag"
import { entityKey, entityText, transcriptEntities, type HostEntity, type HostSnapshot } from "@/execution"
import type { Message } from "@/session"

/** A prompt the user sent that the transcript does not show yet. Keyed by its client command ID. */
export type PromptRow = {
  commandId: string
  kind: "send" | "steer"
  text: string
  attachmentNames: string[]
  state: "sending" | "accepted" | "unconfirmed" | "failed"
  error?: string
}

type EntityRow = { key: string; signature: string; entity: HostEntity; label: string; role: string; text: string }

// Follow re-engages within this distance of the bottom.
const bottomThreshold = 64

function entityRows(snapshot: HostSnapshot): EntityRow[] {
  return transcriptEntities(snapshot).map((entity, index) => {
    const data = entity.body.data
    const role = entity.body.kind === "tool_invocation" ? "tool_use" : entity.body.kind === "tool_result" ? "tool_result" : data.role || entity.body.kind
    return {
      key: entityKey(entity, index),
      // Rows re-render only when their own entity changes.
      signature: JSON.stringify(entity),
      entity,
      role,
      label: `${role}${data.name ? ` · ${data.name}` : ""}${data.status ? ` · ${data.status}` : ""}`,
      text: entity.body.kind === "tool_invocation" ? JSON.stringify({ name: data.name, arguments: data.raw_arguments }) : entityText(entity),
    }
  })
}

function SourceDetails({ entity }: { entity: HostEntity }) {
  const [open, setOpen] = useState(false)
  return <details className="mt-2 text-xs text-muted-foreground" onToggle={event => setOpen(event.currentTarget.open)}>
    <summary>Source details</summary>
    {open && <pre className="overflow-auto whitespace-pre-wrap">{JSON.stringify(entity, null, 2)}</pre>}
  </details>
}

const EntityRowView = memo(function EntityRowView({ row }: { row: EntityRow }) {
  const message = useMemo<Message>(() => ({ record_id: row.key, role: row.role, content: row.text, ts: 0 }), [row.key, row.role, row.text])
  return <article className="min-w-0 rounded-lg border p-3">
    <div className="mb-2 text-xs font-medium text-muted-foreground">{row.label}</div>
    <MessageContent message={message} />
    <SourceDetails entity={row.entity} />
  </article>
}, (previous, next) => previous.row.signature === next.row.signature)

const PromptRowView = memo(function PromptRowView({ prompt, onDismiss }: { prompt: PromptRow; onDismiss: (commandId: string) => void }) {
  const slow = useDelayedFlag(prompt.state === "sending", 2000)
  const content = prompt.attachmentNames.length
    ? `${prompt.text}${prompt.text ? "\n\n" : ""}Attached: ${prompt.attachmentNames.join(", ")}` : prompt.text
  const message = useMemo<Message>(() => ({ record_id: `pending:${prompt.commandId}`, role: "user", content, ts: 0 }), [prompt.commandId, content])
  return <article className="min-w-0 rounded-lg border p-3" data-command-id={prompt.commandId}>
    <div className="mb-2 text-xs font-medium text-muted-foreground">user{prompt.kind === "steer" ? " · steer" : ""}</div>
    <MessageContent message={message} />
    {/* Occupies the same line as a delivered row's source details, so the swap does not move anything. */}
    <div className="mt-2 flex min-h-4 flex-wrap items-center gap-2 text-xs text-muted-foreground">
      {prompt.state === "sending" && slow && <span>Sending…</span>}
      {prompt.state === "unconfirmed" && <span>Delivery unconfirmed. Inspect or retry it under Unconfirmed commands.</span>}
      {prompt.state === "failed" && <>
        <span className="text-destructive">Not sent: {prompt.error}</span>
        <Button size="xs" variant="ghost" onClick={() => onDismiss(prompt.commandId)}>Dismiss</Button>
      </>}
    </div>
  </article>
})

/**
 * Follows new content only while the reader is at the bottom. Any user-originated upward scroll
 * detaches; returning to the bottom re-attaches. The follow is a direct assignment, never animated.
 */
function useStickToBottom(conversationKey: string) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const contentRef = useRef<HTMLDivElement>(null)
  const following = useRef(true)
  const programmaticTop = useRef<number | null>(null)
  const lastTop = useRef(0)
  const [detached, setDetached] = useState(false)

  const setFollowing = useCallback((value: boolean) => {
    following.current = value
    setDetached(!value)
  }, [])
  const toBottom = useCallback(() => {
    const scroller = scrollRef.current
    if (!scroller) return
    scroller.scrollTop = scroller.scrollHeight
    programmaticTop.current = scroller.scrollTop
    lastTop.current = scroller.scrollTop
  }, [])
  const jumpToLatest = useCallback(() => {
    setFollowing(true)
    toBottom()
  }, [setFollowing, toBottom])

  useLayoutEffect(() => jumpToLatest(), [conversationKey, jumpToLatest])

  useEffect(() => {
    const scroller = scrollRef.current
    const content = contentRef.current
    if (!scroller || !content) return
    const observer = new ResizeObserver(() => { if (following.current) toBottom() })
    observer.observe(content)
    observer.observe(scroller)
    const detach = () => { if (following.current && scroller.scrollHeight > scroller.clientHeight) setFollowing(false) }
    let touchY: number | null = null
    const onWheel = (event: WheelEvent) => { if (event.deltaY < 0) detach() }
    const onTouchStart = (event: TouchEvent) => { touchY = event.touches[0]?.clientY ?? null }
    const onTouchMove = (event: TouchEvent) => {
      const y = event.touches[0]?.clientY
      if (y === undefined) return
      // A finger moving down the screen scrolls the content up.
      if (touchY !== null && y > touchY) detach()
      touchY = y
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (["ArrowUp", "PageUp", "Home"].includes(event.key) || (event.key === " " && event.shiftKey)) detach()
    }
    const onScroll = () => {
      const top = scroller.scrollTop
      const previous = lastTop.current
      lastTop.current = top
      if (programmaticTop.current !== null && Math.abs(top - programmaticTop.current) < 1) return
      programmaticTop.current = null
      const distance = scroller.scrollHeight - scroller.clientHeight - top
      // Covers scrollbar drags and other user scrolling that sends no wheel, touch, or key event.
      if (top < previous - 1 && distance > bottomThreshold) { if (following.current) setFollowing(false) }
      else if (top > previous && distance <= bottomThreshold && !following.current) setFollowing(true)
    }
    scroller.addEventListener("wheel", onWheel, { passive: true })
    scroller.addEventListener("touchstart", onTouchStart, { passive: true })
    scroller.addEventListener("touchmove", onTouchMove, { passive: true })
    scroller.addEventListener("keydown", onKeyDown)
    scroller.addEventListener("scroll", onScroll, { passive: true })
    return () => {
      observer.disconnect()
      scroller.removeEventListener("wheel", onWheel)
      scroller.removeEventListener("touchstart", onTouchStart)
      scroller.removeEventListener("touchmove", onTouchMove)
      scroller.removeEventListener("keydown", onKeyDown)
      scroller.removeEventListener("scroll", onScroll)
    }
  }, [setFollowing, toBottom])

  return { scrollRef, contentRef, detached, jumpToLatest }
}

export const ExecutionTranscript = memo(function ExecutionTranscript({ conversationKey, snapshot, prompts, working, stale, onDismiss }: {
  /** The conversation whose content is on screen; changing it starts at the bottom. */
  conversationKey: string
  snapshot: HostSnapshot | null
  prompts: PromptRow[]
  working: boolean
  /** The frame belongs to a conversation that has not loaded yet. */
  stale: boolean
  onDismiss: (commandId: string) => void
}) {
  const rows = useMemo(() => snapshot ? entityRows(snapshot) : [], [snapshot])
  const { scrollRef, contentRef, detached, jumpToLatest } = useStickToBottom(conversationKey)
  const seenPrompts = useRef(new Set<string>())
  useLayoutEffect(() => {
    let sent = false
    for (const prompt of prompts) {
      if (seenPrompts.current.has(prompt.commandId)) continue
      seenPrompts.current.add(prompt.commandId)
      if (prompt.state === "sending") sent = true
    }
    // The user's own send always lands in view.
    if (sent) jumpToLatest()
  }, [prompts, jumpToLatest])

  return <div className="relative flex min-h-40 flex-1 flex-col">
    <div ref={scrollRef} className="execution-scroll min-h-0 flex-1 overflow-auto p-3 md:p-5" tabIndex={0}>
      <div ref={contentRef} className={stale ? "opacity-60" : undefined}>
        {snapshot?.warning && <p className="mb-3 text-sm text-muted-foreground">{snapshot.warning}</p>}
        {snapshot?.deliveries.filter(delivery => delivery.status === "failed").slice(-5).map(delivery =>
          <p key={delivery.commandId} className="mb-3 text-sm text-destructive" role="alert">{delivery.error || `Command ${delivery.commandId} failed on the host.`}</p>)}
        {rows.length || prompts.length ? <div className="space-y-5">
          {rows.map(row => <EntityRowView key={row.key} row={row} />)}
          {prompts.map(prompt => <PromptRowView key={`pending:${prompt.commandId}`} prompt={prompt} onDismiss={onDismiss} />)}
        </div> : snapshot && !stale && <p className="text-sm text-muted-foreground">The host is ready for a prompt. Configure this conversation before sending.</p>}
        {/* Reserved so the working status never changes the transcript height. */}
        <div className="mt-3 h-5 text-sm" role="status">{working && <span className="execution-shimmer">Working…</span>}</div>
      </div>
    </div>
    {detached && <Button className="absolute bottom-3 left-1/2 -translate-x-1/2 shadow-sm" size="sm" variant="outline" onClick={jumpToLatest}>Jump to latest</Button>}
  </div>
})
