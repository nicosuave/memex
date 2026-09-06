import {
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
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
const measurements = new Map<string, VirtualItem[]>()

const MessageRow = memo(function MessageRow({
  message,
  match,
  loadContent,
  busy,
  loading,
  error,
}: {
  message: Message
  match: boolean
  loadContent: (id: string) => Promise<void>
  busy: boolean
  loading: boolean
  error: string
}) {
  const continuation = useRef<HTMLDivElement>(null)
  const loadedBytes = useMemo(
    () => new TextEncoder().encode(message.content).length,
    [message.content],
  )
  const truncated =
    message.truncated && loadedBytes < (message.content_bytes || 0)
  useEffect(() => {
    const element = continuation.current
    if (!element || !truncated || busy || error) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) void loadContent(message.record_id)
      },
      {
        root: element.closest(".transcript-scroll"),
        rootMargin: "0px 0px 320px 0px",
      },
    )
    observer.observe(element)
    return () => observer.disconnect()
  }, [truncated, busy, error, loadedBytes, loadContent, message.record_id])
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
      <MessageContent message={message} />
      {truncated && (
        <div ref={continuation} className="message-actions">
          {error ? (
            <Button
              size="sm"
              variant="outline"
              disabled={loading}
              onClick={() => void loadContent(message.record_id)}
            >
              Retry loading message
            </Button>
          ) : (
            <span role="status">Loading message…</span>
          )}
        </div>
      )}
    </article>
  )
})

const VirtualMessages = memo(function VirtualMessages({
  rows,
  target,
  positionKey,
  loadContent,
  loadPage,
  loading,
  paginationFailed,
  error,
  contentErrorRecordId,
  contentLoadingRecordId,
  canLoadEarlier,
  canLoadLater,
  initialAtTail,
  autoPage,
}: {
  rows: Message[]
  target: SessionTarget
  positionKey: string
  loadContent: (id: string) => Promise<void>
  loadPage: (direction: "earlier" | "later") => Promise<void>
  loading: boolean
  paginationFailed: boolean
  error: string
  contentErrorRecordId: string | null
  contentLoadingRecordId: string | null
  canLoadEarlier: boolean
  canLoadLater: boolean
  initialAtTail: boolean
  autoPage: boolean
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
    overscan: 5,
    anchorTo: "end",
    directDomUpdates: true,
    useFlushSync: false,
    initialOffset: scrollPositions.get(positionKey)?.offset || 0,
    initialMeasurementsCache: measurements
      .get(positionKey)
      ?.filter((item, index) => rows[index]?.record_id === item.key),
  })
  const restoreFrame = useRef<number | null>(null)
  const boundaryFrame = useRef<number | null>(null)
  const pagination = useRef({
    autoPage,
    loading,
    paginationFailed,
    canLoadEarlier,
    canLoadLater,
    loadPage,
  })
  pagination.current = {
    autoPage,
    loading,
    paginationFailed,
    canLoadEarlier,
    canLoadLater,
    loadPage,
  }
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
  const loadAtBoundary = useCallback(() => {
    const element = parent.current
    const state = pagination.current
    if (
      !element ||
      !state.autoPage ||
      state.loading ||
      state.paginationFailed ||
      restoreFrame.current !== null
    )
      return
    const threshold = Math.min(640, Math.max(160, element.clientHeight / 2))
    if (state.canLoadEarlier && element.scrollTop <= threshold) {
      savePosition()
      void state.loadPage("earlier")
      return
    }
    const distanceFromEnd =
      element.scrollHeight - element.clientHeight - element.scrollTop
    if (state.canLoadLater && distanceFromEnd <= threshold) {
      savePosition()
      void state.loadPage("later")
    }
  }, [])
  const scheduleBoundaryCheck = useCallback(() => {
    if (boundaryFrame.current !== null)
      cancelAnimationFrame(boundaryFrame.current)
    boundaryFrame.current = requestAnimationFrame(() => {
      boundaryFrame.current = null
      loadAtBoundary()
    })
  }, [loadAtBoundary])
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
    previousRows.current = rows
    // Changes to an existing list are anchored by the virtualizer itself.
    // Replaying an imperative restoration after every prepend, filter change,
    // or content continuation fights its measurement corrections and causes
    // visible back-and-forth movement while the user is scrolling.
    if (!initial) return
    const saved = initial
      ? initialPosition.current
      : scrollPositions.get(positionKey)
    const recordId = saved?.recordId || (initial ? target.recordId : undefined)
    const index = recordId
      ? rows.findIndex((row) => row.record_id === recordId)
      : -1
    if (index >= 0) {
      virtualizer.scrollToIndex(index, { align: "start" })
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
        restoreFrame.current = null
        scheduleBoundaryCheck()
      }
      restoreFrame.current = requestAnimationFrame(restore)
    } else if (initial && initialAtTail && !saved) {
      const restoreTail = () => {
        virtualizer.scrollToOffset(virtualizer.getTotalSize(), {
          align: "end",
        })
        restoreFrame.current = null
        scheduleBoundaryCheck()
      }
      restoreFrame.current = requestAnimationFrame(restoreTail)
    }
    return () => {
      if (restoreFrame.current !== null) {
        cancelAnimationFrame(restoreFrame.current)
        restoreFrame.current = null
      }
    }
  }, [rows, virtualizer, positionKey, target.recordId, initialAtTail])
  useLayoutEffect(() => {
    if (!autoPage) return
    // Let anchor restoration and virtual row measurement settle before deciding
    // whether the viewport is still at an unloaded boundary.
    scheduleBoundaryCheck()
    return () => {
      if (boundaryFrame.current !== null)
        cancelAnimationFrame(boundaryFrame.current)
    }
  }, [
    rows.length,
    autoPage,
    loading,
    paginationFailed,
    canLoadEarlier,
    canLoadLater,
    scheduleBoundaryCheck,
  ])
  return (
    <div
      ref={parent}
      className="transcript-scroll"
      tabIndex={0}
      onScroll={() => {
        savePosition()
        loadAtBoundary()
      }}
      onWheel={() => {
        if (restoreFrame.current !== null) {
          cancelAnimationFrame(restoreFrame.current)
          restoreFrame.current = null
        }
        scheduleBoundaryCheck()
      }}
      onTouchStart={() => {
        if (restoreFrame.current !== null) {
          cancelAnimationFrame(restoreFrame.current)
          restoreFrame.current = null
        }
        scheduleBoundaryCheck()
      }}
      onKeyDown={() => {
        if (restoreFrame.current !== null) {
          cancelAnimationFrame(restoreFrame.current)
          restoreFrame.current = null
        }
        scheduleBoundaryCheck()
      }}
    >
      <div className="messages">
        <div
          ref={virtualizer.containerRef}
          className="virtual-messages"
        >
          {virtualizer.getVirtualItems().map((item) => (
            <div
              key={item.key}
              ref={virtualizer.measureElement}
              data-index={item.index}
              className="virtual-message"
            >
              <MessageRow
                message={rows[item.index]}
                match={rows[item.index].record_id === target.recordId}
                loadContent={loadContent}
                busy={loading || Boolean(error)}
                loading={
                  rows[item.index].record_id === contentLoadingRecordId
                }
                error={
                  rows[item.index].record_id === contentErrorRecordId
                    ? error
                    : ""
                }
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
}: {
  resource: SessionResource
  target: SessionTarget | null
  mode: "history" | "matches"
  showThinking: boolean
  showDetails: boolean
}) {
  const {
    session,
    error,
    notice,
    loading,
    pageErrorDirection,
    contentErrorRecordId,
    contentLoadingRecordId,
    refreshRequired,
    loadPage,
    loadContent,
    refresh,
  } = resource
  const rows = useMemo(() => {
    if (!session) return []
    const visible = session.messages.filter(
      (message) =>
        message.record_id === target?.recordId ||
        ((showThinking || !isThinkingMessage(message)) &&
          (showDetails || !isToolMessage(message))),
    )
    if (mode === "history") return visible
    if (!target?.recordId) return visible.slice(-12)
    const match = visible.findIndex(
      (message) => message.record_id === target.recordId,
    )
    return match >= 0
      ? visible.slice(Math.max(0, match - 1), match + 2)
      : visible.slice(-12)
  }, [session, target?.recordId, mode, showThinking, showDetails])
  const key = `${targetKey(target)}:${session?.version}:${mode}`
  const canLoadEarlier = Boolean(session && session.offset > 0)
  const canLoadLater = Boolean(
    session && session.offset + session.messages.length < session.total,
  )
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
        {loading && !session && <span>Loading transcript…</span>}
        {notice && <span>{notice}</span>}
        {error && <span role="alert">{error}</span>}
        {error && pageErrorDirection && !refreshRequired && (
          <Button
            size="sm"
            variant="outline"
            disabled={loading}
            onClick={() => void loadPage(pageErrorDirection)}
          >
            Retry loading {pageErrorDirection} messages
          </Button>
        )}
        {error && refreshRequired && (
          <Button
            size="sm"
            variant="ghost"
            disabled={loading}
            onClick={refresh}
          >
            Refresh transcript
          </Button>
        )}
      </div>
      {!session && !loading && <p className="empty">No session to preview.</p>}
      {session && !rows.length && (
        <p className="empty">
          {mode === "history" &&
          (canLoadEarlier || canLoadLater) &&
          !pageErrorDirection
            ? "Loading messages…"
            : "No messages match these filters."}
        </p>
      )}
      {session && target && (
        <VirtualMessages
          key={key}
          rows={rows}
          target={target}
          positionKey={key}
          loadContent={loadContent}
          loadPage={loadPage}
          loading={loading}
          error={error}
          contentErrorRecordId={contentErrorRecordId}
          contentLoadingRecordId={contentLoadingRecordId}
          paginationFailed={Boolean(error)}
          canLoadEarlier={canLoadEarlier}
          canLoadLater={canLoadLater}
          initialAtTail={
            mode === "history" &&
            !canLoadLater &&
            !rows.some((row) => row.record_id === target.recordId)
          }
          autoPage={mode === "history"}
        />
      )}
    </section>
  )
})
