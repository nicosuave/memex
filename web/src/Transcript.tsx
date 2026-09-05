import {
  memo,
  useCallback,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react"
import { useVirtualizer, type VirtualItem } from "@tanstack/react-virtual"
import { Button } from "./components/ui/button"
import { MessageContent } from "./MessageContent"
import {
  type Message,
  type SessionResource,
  type SessionTarget,
  isThinkingMessage,
  isToolMessage,
  targetKey,
} from "./session"

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
})
type ScrollPosition = { offset: number; recordId?: string; delta: number }
const scrollPositions = new Map<string, ScrollPosition>()
const previewCharacters = 8_000
const expandedMessages = new Set<string>()
const measurements = new Map<string, VirtualItem[]>()

const MessageRow = memo(function MessageRow({
  message,
  match,
  version,
  loadContent,
  loading,
}: {
  message: Message
  match: boolean
  version: string
  loadContent: (id: string) => Promise<void>
  loading: boolean
}) {
  const preferenceKey = `${version}:${message.record_id}`
  const [expanded, setExpanded] = useState(() =>
    expandedMessages.has(preferenceKey),
  )
  const toggleExpanded = () => {
    if (expanded) expandedMessages.delete(preferenceKey)
    else {
      expandedMessages.add(preferenceKey)
      if (expandedMessages.size > 1000)
        expandedMessages.delete(expandedMessages.values().next().value!)
    }
    setExpanded((value) => !value)
  }
  const fullContent = message.content
  const displayed = useMemo(
    () => ({
      ...message,
      content: expanded ? fullContent : fullContent.slice(0, previewCharacters),
    }),
    [message, expanded, fullContent],
  )
  const loadedBytes = useMemo(
    () => new TextEncoder().encode(fullContent).length,
    [fullContent],
  )
  const truncated =
    message.truncated && loadedBytes < (message.content_bytes || 0)
  return (
    <article
      className="message"
      data-record-id={message.record_id}
      data-match={match}
    >
      <div className="message-meta">
        <span>{message.tool_name || message.role || "event"}</span>
        <time>
          {message.ts ? dateFormatter.format(new Date(message.ts)) : ""}
        </time>
      </div>
      <div className={expanded ? undefined : "message-content-preview"}>
        <MessageContent message={displayed} />
      </div>
      <div className="message-actions">
        {fullContent.length > previewCharacters && (
          <Button size="sm" variant="outline" onClick={toggleExpanded}>
            {expanded ? "Collapse message" : "Expand message"}
          </Button>
        )}
        {truncated && (
          <Button
            size="sm"
            variant="outline"
            disabled={loading}
            onClick={() => void loadContent(message.record_id)}
          >
            {loading ? "Loading…" : "Load more message content"}
          </Button>
        )}
        {truncated && (
          <span>
            {loadedBytes.toLocaleString()} of{" "}
            {message.content_bytes?.toLocaleString()} bytes loaded
          </span>
        )}
      </div>
    </article>
  )
})

const VirtualMessages = memo(function VirtualMessages({
  rows,
  target,
  version,
  positionKey,
  loadContent,
  loading,
}: {
  rows: Message[]
  target: SessionTarget
  version: string
  positionKey: string
  loadContent: (id: string) => Promise<void>
  loading: boolean
}) {
  const parent = useRef<HTMLDivElement>(null)
  const currentRows = useRef(rows)
  currentRows.current = rows
  const getKey = useCallback((index: number) => rows[index].record_id, [rows])
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parent.current,
    estimateSize: () => 220,
    getItemKey: getKey,
    overscan: 3,
    useFlushSync: false,
    initialOffset: scrollPositions.get(positionKey)?.offset || 0,
    initialMeasurementsCache: measurements
      .get(positionKey)
      ?.filter((item, index) => rows[index]?.record_id === item.key),
  })
  const restoreFrame = useRef<number | null>(null)
  const previousRows = useRef<Message[]>([])
  const initialPosition = useRef(scrollPositions.get(positionKey))
  const savePosition = () => {
    const offset = parent.current?.scrollTop || 0
    const first = virtualizer
      .getVirtualItems()
      .find((item) => item.end > offset)
    scrollPositions.set(positionKey, {
      offset,
      recordId: first ? currentRows.current[first.index]?.record_id : undefined,
      delta: first ? offset - first.start : 0,
    })
    while (scrollPositions.size > 64)
      scrollPositions.delete(scrollPositions.keys().next().value!)
  }
  useLayoutEffect(
    () => () => {
      savePosition()
      measurements.set(positionKey, [...virtualizer.measurementsCache])
      while (measurements.size > 64)
        measurements.delete(measurements.keys().next().value!)
    },
    [positionKey, virtualizer],
  )
  useLayoutEffect(() => {
    const old = previousRows.current
    if (!rows.length) return
    const initial = !old.length || old === rows
    const saved = initial
      ? initialPosition.current
      : scrollPositions.get(positionKey)
    const recordId = saved?.recordId || (initial ? target.recordId : undefined)
    const index = recordId
      ? rows.findIndex((row) => row.record_id === recordId)
      : -1
    if (index >= 0) {
      virtualizer.scrollToIndex(index, { align: "start" })
      // The anchor may initially be estimated/offscreen. Apply its saved intra-row
      // offset after mounting and ResizeObserver measurements settle.
      let attempts = 0
      const restore = () => {
        const anchor = virtualizer
          .getVirtualItems()
          .find((item) => item.key === recordId)
        if (anchor)
          virtualizer.scrollToOffset(
            anchor.start +
              Math.min(saved?.delta || 0, Math.max(0, anchor.size - 1)),
            { align: "start" },
          )
        if (++attempts < 4)
          restoreFrame.current = requestAnimationFrame(restore)
        else restoreFrame.current = null
      }
      restoreFrame.current = requestAnimationFrame(restore)
    }
    previousRows.current = rows
    return () => {
      if (restoreFrame.current !== null)
        cancelAnimationFrame(restoreFrame.current)
    }
  }, [rows, virtualizer, positionKey, target.recordId])
  return (
    <div
      ref={parent}
      className="transcript-scroll"
      onScroll={savePosition}
      onWheel={() => {
        if (restoreFrame.current !== null)
          cancelAnimationFrame(restoreFrame.current)
      }}
      onTouchStart={() => {
        if (restoreFrame.current !== null)
          cancelAnimationFrame(restoreFrame.current)
      }}
      onKeyDown={() => {
        if (restoreFrame.current !== null)
          cancelAnimationFrame(restoreFrame.current)
      }}
    >
      <div className="messages">
        <div
          className="virtual-messages"
          style={{ height: virtualizer.getTotalSize() }}
        >
          {virtualizer.getVirtualItems().map((item) => (
            <div
              key={item.key}
              ref={virtualizer.measureElement}
              data-index={item.index}
              className="virtual-message"
              style={{ transform: `translateY(${item.start}px)` }}
            >
              <MessageRow
                message={rows[item.index]}
                match={rows[item.index].record_id === target.recordId}
                version={version}
                loadContent={loadContent}
                loading={loading}
              />
            </div>
          ))}
        </div>
      </div>
    </div>
  )
})

export const Transcript = memo(function Transcript({
  resource,
  target,
  mode,
  showThinking,
  showDetails,
  onReveal,
}: {
  resource: SessionResource
  target: SessionTarget | null
  mode: "history" | "matches"
  showThinking: boolean
  showDetails: boolean
  onReveal: () => void
}) {
  const { session, error, loading, loadPage, loadContent, refresh } = resource
  const { rows, hiddenHit } = useMemo(() => {
    if (!session) return { rows: [], hiddenHit: false }
    const visible = session.messages.filter(
      (message) =>
        (showThinking || !isThinkingMessage(message)) &&
        (showDetails || !isToolMessage(message)),
    )
    const hit = session.messages.find(
      (message) => message.record_id === target?.recordId,
    )
    const hiddenHit = Boolean(hit && !visible.includes(hit))
    if (mode === "history") return { rows: visible, hiddenHit }
    if (!target?.recordId) return { rows: visible.slice(-12), hiddenHit }
    const match = visible.findIndex(
      (message) => message.record_id === target.recordId,
    )
    return {
      rows:
        match >= 0
          ? visible.slice(Math.max(0, match - 1), match + 2)
          : visible.slice(-12),
      hiddenHit,
    }
  }, [session, target?.recordId, mode, showThinking, showDetails])
  const key = `${targetKey(target)}:${session?.version}:${mode}`
  return (
    <section
      className="transcript-surface"
      aria-label="Transcript"
      aria-busy={loading}
    >
      <div className="transcript-status">
        {session && (
          <span>
            {session.project || "Untitled session"} · {session.source} ·{" "}
            {session.offset + 1}–{session.offset + session.messages.length} of{" "}
            {session.total} messages
          </span>
        )}
        {mode === "matches" && target?.recordId && (
          <span>Selected search hit and context</span>
        )}
        {session &&
          !session.messages.some(
            (message) => isThinkingMessage(message) || isToolMessage(message),
          ) && <span>This window has no reasoning or tool messages.</span>}
        {loading && <span>Loading transcript…</span>}
        {error && <span role="alert">{error}</span>}
        {(error || session) && (
          <Button
            size="sm"
            variant="ghost"
            disabled={loading}
            onClick={refresh}
          >
            Reload latest transcript
          </Button>
        )}
        {hiddenHit && (
          <>
            <span>Match in hidden content.</span>
            <Button size="sm" onClick={onReveal}>
              Reveal hidden match
            </Button>
          </>
        )}
        {session && session.offset > 0 && (
          <Button
            size="sm"
            variant="outline"
            disabled={loading}
            onClick={() => void loadPage("earlier")}
          >
            Load earlier messages
          </Button>
        )}
        {session &&
          session.offset + session.messages.length < session.total && (
            <Button
              size="sm"
              variant="outline"
              disabled={loading}
              onClick={() => void loadPage("later")}
            >
              Load later messages
            </Button>
          )}
      </div>
      {!session && !loading && <p className="empty">No session to preview.</p>}
      {session && !rows.length && (
        <p className="empty">
          No visible messages in this window. Load an adjacent page or enable
          tools and reasoning.
        </p>
      )}
      {session && target && (
        <VirtualMessages
          key={key}
          rows={rows}
          target={target}
          version={session.version}
          positionKey={key}
          loadContent={loadContent}
          loading={loading}
        />
      )}
    </section>
  )
})
