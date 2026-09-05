import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { api, ApiError } from "./api"

export type Message = {
  record_id: string
  role: string
  content: string
  ts: number
  tool_name?: string | null
  truncated?: boolean
  content_bytes?: number
}
export const isThinkingMessage = (message: Message) =>
  ["reasoning", "thinking"].includes(message.role)
export const isToolMessage = (message: Message) =>
  ["tool_use", "tool_result", "system"].includes(message.role)
export type SessionPayload = {
  session_id: string
  source_path?: string
  project: string
  source: string
  version: string
  started_at: number
  ended_at: number
  offset: number
  total: number
  messages: Message[]
}
export type SessionTarget = {
  id: string
  sourcePath?: string
  sessionSource?: string
  recordId?: string
}
export const targetKey = (target: SessionTarget | null) =>
  target
    ? JSON.stringify([
        target.id,
        target.sessionSource || "",
        target.sourcePath || "",
        target.recordId || "",
      ])
    : ""
const sessionKey = (target: SessionTarget | null) =>
  target
    ? JSON.stringify([
        target.sessionSource || "",
        target.id,
        target.sourcePath || "",
      ])
    : ""
type CachedSession = { windows: SessionPayload[]; at: number; bytes: number }

// Coalesce adjacent windows, retaining expanded content within the same snapshot.
function retainWindows(
  windows: SessionPayload[],
  incoming: SessionPayload,
): SessionPayload[] {
  const sorted = [
    ...windows.filter((window) => window.version === incoming.version),
    incoming,
  ].sort((a, b) => a.offset - b.offset)
  const merged: SessionPayload[] = []
  for (const window of sorted) {
    const previous = merged.at(-1)
    if (
      !previous ||
      previous.offset + previous.messages.length < window.offset
    ) {
      merged.push(window)
      continue
    }
    const messages = [...previous.messages]
    window.messages.forEach((message, index) => {
      const at = window.offset - previous.offset + index
      if (
        !messages[at] ||
        message.content.length >= messages[at].content.length
      )
        messages[at] = message
    })
    merged[merged.length - 1] = { ...previous, messages }
  }
  return merged
}

const maxCacheBytes = 16 * 1024 * 1024
const cacheLifetime = 30_000

function paramsFor(target: SessionTarget) {
  const params = new URLSearchParams({ id: target.id, limit: "40" })
  if (target.sourcePath) params.set("source_path", target.sourcePath)
  if (target.sessionSource) params.set("session_source", target.sessionSource)
  return params
}

export function useSessionResource(target: SessionTarget | null) {
  const [session, setSession] = useState<SessionPayload | null>(null)
  const [error, setError] = useState("")
  const [pageErrorDirection, setPageErrorDirection] = useState<
    "earlier" | "later" | null
  >(null)
  const [loading, setLoading] = useState(false)
  const [revision, setRevision] = useState(0)
  const cache = useRef(new Map<string, CachedSession>())
  const key = targetKey(target)
  const cacheKey = sessionKey(target)
  const [displayKey, setDisplayKey] = useState(key)
  const activeKey = useRef(key)
  activeKey.current = key
  const controller = useRef<AbortController | null>(null)
  const busy = useRef(false)
  const generation = useRef(0)
  const current = useRef(session)
  current.current = session

  const publish = useCallback((key: string, data: SessionPayload) => {
    const windows = retainWindows(cache.current.get(key)?.windows || [], data)
    const selected =
      windows.find(
        (window) =>
          window.offset <= data.offset &&
          window.offset + window.messages.length >=
            data.offset + data.messages.length,
      ) || data
    const bytes = windows.reduce(
      (sum, window) =>
        sum +
        window.messages.reduce(
          (bytes, message) => bytes + message.content.length * 2 + 256,
          1024,
        ),
      0,
    )
    cache.current.delete(key)
    cache.current.set(key, { windows, at: Date.now(), bytes })
    let total = Array.from(cache.current.values()).reduce(
      (sum, entry) => sum + entry.bytes,
      0,
    )
    for (const [oldKey, entry] of cache.current) {
      if (total <= maxCacheBytes) break
      cache.current.delete(oldKey)
      total -= entry.bytes
    }
    setDisplayKey(activeKey.current)
    current.current = selected
    setSession(selected)
  }, [])

  useEffect(() => {
    const request = new AbortController()
    controller.current = request
    const requestGeneration = ++generation.current
    setDisplayKey(key)
    busy.current = false
    setError("")
    setPageErrorDirection(null)
    setLoading(false)
    if (!target) {
      current.current = null
      setSession(null)
      return () => request.abort()
    }
    const cachedEntry = cache.current.get(cacheKey)
    const cached = cachedEntry?.windows.find((window) =>
      target.recordId
        ? window.messages.some(
            (message) => message.record_id === target.recordId,
          )
        : window.offset + window.messages.length === window.total,
    )
    current.current = cached || null
    setSession(cached || null)
    if (cached && cachedEntry && Date.now() - cachedEntry.at < cacheLifetime) {
      cache.current.delete(cacheKey)
      cache.current.set(cacheKey, cachedEntry)
      return () => {
        request.abort()
        ++generation.current
      }
    }
    const params = paramsFor(target)
    if (target.recordId) params.set("around", target.recordId)
    else params.set("tail", "true")
    busy.current = true
    setLoading(true)
    const fetchInitial = async () => {
      try {
        return await api<SessionPayload>(
          `/api/session?${params}`,
          request.signal,
        )
      } catch (err) {
        if (
          !(err instanceof ApiError) ||
          err.status !== 404 ||
          !target.recordId ||
          request.signal.aborted
        )
          throw err
        params.delete("around")
        params.set("tail", "true")
        const latest = await api<SessionPayload>(
          `/api/session?${params}`,
          request.signal,
        )
        if (!request.signal.aborted && activeKey.current === key)
          setError(
            "Selected hit is no longer available. Showing latest messages.",
          )
        return latest
      }
    }
    void fetchInitial()
      .then((data) => {
        if (
          request.signal.aborted ||
          activeKey.current !== key ||
          requestGeneration !== generation.current
        )
          return
        // Revalidation replaces the snapshot; it never appends a different generation.
        if (cached?.version === data.version) publish(cacheKey, cached)
        else publish(cacheKey, data)
      })
      .catch((err) => {
        if (!request.signal.aborted && activeKey.current === key)
          setError(
            err instanceof Error ? err.message : "Could not load transcript",
          )
      })
      .finally(() => {
        if (!request.signal.aborted && activeKey.current === key) {
          busy.current = false
          setLoading(false)
        }
      })
    return () => {
      request.abort()
      ++generation.current
    }
  }, [key, cacheKey, revision, publish]) // The canonical target key owns the request, not object identity.

  const refresh = useCallback(() => {
    const cached = cache.current.get(cacheKey)
    if (cached) cache.current.set(cacheKey, { ...cached, at: 0 })
    setPageErrorDirection(null)
    setRevision((value) => value + 1)
  }, [cacheKey])
  const loadPage = useCallback(
    async (direction: "earlier" | "later") => {
      const before = current.current
      if (!target || !before || busy.current) return
      const end = before.offset + before.messages.length
      if (direction === "earlier" ? before.offset === 0 : end >= before.total)
        return
      const requestGeneration = generation.current
      busy.current = true
      setLoading(true)
      setError("")
      setPageErrorDirection(null)
      const params = paramsFor(target)
      params.set(
        direction === "earlier" ? "before" : "offset",
        String(direction === "earlier" ? before.offset : end),
      )
      params.set(
        "limit",
        String(direction === "earlier" ? Math.min(40, before.offset) : 40),
      )
      params.set("version", before.version)
      try {
        const page = await api<SessionPayload>(
          `/api/session?${params}`,
          controller.current?.signal,
        )
        if (
          activeKey.current !== key ||
          generation.current !== requestGeneration
        )
          return
        if (page.version !== before.version)
          throw new ApiError("session changed", 409)
        if (!page.messages.length)
          throw new Error(
            "No further messages returned. Reload latest transcript to reconcile.",
          )
        const adjacent =
          direction === "earlier"
            ? page.offset + page.messages.length === before.offset
            : page.offset === end
        if (!adjacent)
          throw new Error(
            "Transcript page was not contiguous. Reload latest transcript to reconcile.",
          )
        const messages =
          direction === "earlier"
            ? [...page.messages, ...before.messages]
            : [...before.messages, ...page.messages]
        publish(cacheKey, {
          ...before,
          offset: Math.min(page.offset, before.offset),
          messages,
        })
      } catch (err) {
        if (
          activeKey.current !== key ||
          generation.current !== requestGeneration ||
          controller.current?.signal.aborted
        )
          return
        setError(
          err instanceof ApiError && err.status === 409
            ? "Transcript changed on disk."
            : err instanceof Error
              ? err.message
              : "Could not load transcript page",
        )
        setPageErrorDirection(direction)
      } finally {
        if (
          activeKey.current === key &&
          generation.current === requestGeneration
        ) {
          busy.current = false
          setLoading(false)
        }
      }
    },
    [key, cacheKey, target, publish],
  )
  const loadContent = useCallback(
    async (recordId: string) => {
      const before = current.current
      if (!target || !before || busy.current) return
      const message = before.messages.find(
        (item) => item.record_id === recordId,
      )
      if (!message) return
      const requestGeneration = generation.current
      const offset = new TextEncoder().encode(message.content).length
      const params = paramsFor(target)
      params.set("record_id", recordId)
      params.set("version", before.version)
      params.set("offset", String(offset))
      params.set("limit", "65536")
      busy.current = true
      setLoading(true)
      setError("")
      setPageErrorDirection(null)
      try {
        const data = await api<{
          version: string
          record_id: string
          offset: number
          content: string
          truncated: boolean
          content_bytes: number
          next_offset?: number
        }>(`/api/session/content?${params}`, controller.current?.signal)
        if (
          activeKey.current !== key ||
          generation.current !== requestGeneration
        )
          return
        if (data.version !== before.version)
          throw new ApiError("session changed", 409)
        if (
          data.record_id !== recordId ||
          data.offset !== offset ||
          !data.content.length
        )
          throw new Error("No further message content returned.")
        const messages = before.messages.map((item) =>
          item.record_id === recordId
            ? {
                ...item,
                content: item.content + data.content,
                truncated: data.truncated,
                content_bytes: data.content_bytes,
              }
            : item,
        )
        publish(cacheKey, { ...before, messages })
      } catch (err) {
        if (
          activeKey.current === key &&
          generation.current === requestGeneration &&
          !controller.current?.signal.aborted
        )
          setError(
            err instanceof ApiError && err.status === 409
              ? "Transcript changed on disk."
              : err instanceof Error
                ? err.message
                : "Could not load message content",
          )
      } finally {
        if (
          activeKey.current === key &&
          generation.current === requestGeneration
        ) {
          busy.current = false
          setLoading(false)
        }
      }
    },
    [key, cacheKey, target, publish],
  )
  return useMemo(
    () => ({
      session: displayKey === key ? session : null,
      error: displayKey === key ? error : "",
      loading,
      pageErrorDirection,
      loadPage,
      loadContent,
      refresh,
    }),
    [
      key,
      displayKey,
      session,
      error,
      loading,
      pageErrorDirection,
      loadPage,
      loadContent,
      refresh,
    ],
  )
}
export type SessionResource = ReturnType<typeof useSessionResource>
