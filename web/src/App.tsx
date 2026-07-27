import {
  startTransition,
  type CSSProperties,
  type KeyboardEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react"
import { flushSync } from "react-dom"
import {
  Brain,
  Filter,
  Moon,
  Search,
  Sun,
  TerminalSquare,
} from "lucide-react"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "@/components/ui/input-group"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
} from "@/components/ui/sidebar"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  ToggleGroup,
  ToggleGroupItem,
} from "@/components/ui/toggle-group"
import { cn } from "@/lib/utils"

type SearchResult = {
  session_key: string
  session_id: string
  project: string
  source: string
  role: string
  ts: number
  score?: number | null
  snippet: string
}

type SearchPayload = {
  query: string
  offset: number
  has_more: boolean
  results: SearchResult[]
}

type Message = {
  record_key: string
  role: string
  content: string
  ts: number
  tool_name?: string | null
  interaction_id?: string | null
  event_id?: string | null
  parent_event_id?: string | null
  parent_tool_use_id?: string | null
  source_tool_use_id?: string | null
  status?: string | null
  source_status?: string | null
  provisional?: boolean
}

type SessionPayload = {
  session_key: string
  session_id: string
  project: string
  source: string
  started_at: number
  ended_at: number
  offset: number
  total: number
  messages: Message[]
}

type PreviewMode = "matches" | "history"
type ShellView = "home" | "transcript"
type PreviewRow = { message: Message; index: number; context: boolean }

const SESSION_HISTORY_PAGE_SIZE = 200
const SESSION_HISTORY_CONCURRENCY = 6

const paramsAtLoad = new URLSearchParams(window.location.search)
const requestedMode = paramsAtLoad.get("mode")
const initialMode: PreviewMode =
  requestedMode === "history" || requestedMode === "matches"
    ? requestedMode
    : localStorage.getItem("memex-preview-mode") === "history"
      ? "history"
      : "matches"
const initialShellView: ShellView = paramsAtLoad.has("session")
  ? "transcript"
  : "home"

const formatDate = (timestamp: number) =>
  timestamp
    ? new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(new Date(timestamp))
    : ""

async function api<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
  })
  const data = (await response
    .json()
    .catch(() => ({ error: `HTTP ${response.status}` }))) as T & {
    error?: string
  }
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`)
  return data
}

function getPreferredTheme() {
  const stored = localStorage.getItem("memex-theme")
  if (stored === "dark" || stored === "light") return stored
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"
}

type XmlField = { label: string; value: string; path: string }
type ToolPayload = Record<string, unknown>

const rustDebugString = /\bString\(("(?:\\.|[^"\\])*")\)/g
const rustDebugStaticBoolean = /\bStatic\(Bool\((true|false)\)\)/g
const rustDebugStaticNull = /\bStatic\(Null\)/g
const rustDebugStaticNumber =
  /\bStatic\((?:I64|U64|F64)\((-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)\)\)/g
const rustDebugBoolean = /\bBool\((true|false)\)/g
const rustDebugNumber =
  /\bNumber\((-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)\)/g

function parseToolPayload(content: string): ToolPayload | null {
  const source = content.trim()
  if (!source.startsWith("{") || !source.endsWith("}")) return null

  for (const candidate of [
    source,
    source
      .replace(rustDebugString, "$1")
      .replace(rustDebugStaticBoolean, "$1")
      .replace(rustDebugStaticNull, "null")
      .replace(rustDebugStaticNumber, "$1")
      .replace(rustDebugBoolean, "$1")
      .replace(rustDebugNumber, "$1"),
  ]) {
    try {
      const value = JSON.parse(candidate) as unknown
      if (value && typeof value === "object" && !Array.isArray(value)) {
        return value as ToolPayload
      }
    } catch {
      // Try the normalized representation before falling back to raw text.
    }
  }

  return null
}

function formatToolLabel(value: string) {
  return value.replace(/[-_]+/g, " ")
}

function ToolValue({ name, value }: { name: string; value: unknown }) {
  const serialized = JSON.stringify(value, null, 2)
  const text =
    typeof value === "string"
      ? value
      : typeof value === "undefined"
        ? "undefined"
        : serialized ?? String(value)
  const blockValue =
    typeof value === "object" ||
    text.includes("\n") ||
    /^(command|code|content|patch|prompt|query|script|sql)$/i.test(name)

  return blockValue ? (
    <pre className="tool-value">{text}</pre>
  ) : (
    <code className="tool-value-inline">{text}</code>
  )
}

function ToolCallContent({ content }: { content: string }) {
  const payload = useMemo(() => parseToolPayload(content), [content])
  if (!payload) return <pre className="tool-content">{content}</pre>

  const description =
    typeof payload.description === "string" ? payload.description : null
  const fields = Object.entries(payload).filter(
    ([name]) => name !== "description",
  )

  return (
    <div className="tool-call">
      {description && <p className="tool-call-description">{description}</p>}
      {fields.length > 0 && (
        <dl className="tool-fields">
          {fields.map(([name, value]) => (
            <div className="tool-field" key={name}>
              <dt>{formatToolLabel(name)}</dt>
              <dd>
                <ToolValue name={name} value={value} />
              </dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  )
}

function parseXml(content: string): { title: string; fields: XmlField[] } | null {
  const source = content.trim()
  if (!/^<[A-Za-z_][\w:.-]*(?:\s[^>]*)?>[\s\S]*>$/.test(source)) return null

  const parser = new DOMParser()
  let documentNode = parser.parseFromString(source, "application/xml")
  let root = documentNode.documentElement
  let fragment = documentNode.querySelector("parsererror") !== null
  if (fragment) {
    documentNode = parser.parseFromString(
      `<memex-fragment>${source}</memex-fragment>`,
      "application/xml",
    )
    if (documentNode.querySelector("parsererror")) return null
    root = documentNode.documentElement
    if (!root.children.length) return null
  }

  const fields: XmlField[] = []
  const walk = (node: Element, parentPath = "") => {
    const path = parentPath ? `${parentPath}/${node.tagName}` : node.tagName
    if (!node.children.length) {
      fields.push({
        label: node.tagName.replace(/[-_]+/g, " "),
        value: node.textContent?.trim() || "",
        path,
      })
      return
    }
    Array.from(node.children).forEach((child) => walk(child, path))
  }

  if (fragment) Array.from(root.children).forEach((child) => walk(child))
  else walk(root)

  return {
    title: fragment
      ? "structured message"
      : root.tagName.replace(/[-_]+/g, " "),
    fields,
  }
}

function XmlMessage({ content }: { content: string }) {
  const parsed = useMemo(() => parseXml(content), [content])
  if (!parsed) return null

  return (
    <div className="xml-card">
      <div className="xml-title">{parsed.title}</div>
      <dl>
        {parsed.fields.map((field, index) => (
          <div className="xml-row" key={`${field.path}-${index}`} title={field.path}>
            <dt>{field.label}</dt>
            <dd>{field.value}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}

function MessageContent({ message }: { message: Message }) {
  if (!message.content.trim() && message.status) {
    return (
      <div className="tool-content">
        {message.source_status || message.status}
      </div>
    )
  }
  if (message.role === "tool_use")
    return <ToolCallContent content={message.content} />

  if (message.role === "tool_result")
    return <pre className="tool-content">{message.content}</pre>

  if (parseXml(message.content)) return <XmlMessage content={message.content} />

  return (
    <div className="markdown">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{message.content}</ReactMarkdown>
    </div>
  )
}

type ActivityMetric = "sessions" | "tokens"

type ActivityPayload = {
  metric: ActivityMetric
  days: number
  token_usage_enabled: boolean
  partial: boolean
  points: Array<{
    date: string
    source: string
    value: number
  }>
}

const activityColors = [
  "oklch(0.55 0.14 255)",
  "oklch(0.62 0.14 155)",
  "oklch(0.66 0.15 65)",
  "oklch(0.58 0.16 320)",
  "oklch(0.62 0.15 20)",
  "oklch(0.58 0.1 205)",
  "oklch(0.5 0.02 260)",
]

const sourceActivityColors: Record<string, string> = {
  claude: "rgb(214 138 88)",
  codex: "rgb(160 180 200)",
  opencode: "rgb(150 180 150)",
  cursor: "rgb(170 150 200)",
  pi: "rgb(120 190 190)",
  openclaw: "rgb(235 160 110)",
  copilot: "rgb(140 160 220)",
}

const compactNumber = new Intl.NumberFormat(undefined, {
  notation: "compact",
  maximumFractionDigits: 1,
})

const brailleLevels = [" ", "⣀", "⣤", "⣶", "⣿"] as const
const homeChartHeight = 6

type ActivityGroup = {
  color: string
  label: string
  total: number
}

type BrailleChartData = {
  grid: Array<Array<{ color: string; glyph: string }>>
  groups: ActivityGroup[]
  total: number
}

function activityDateKeys(days: number, points: ActivityPayload["points"]) {
  const latestPoint = points
    .map((point) => point.date)
    .sort()
    .at(-1)
  const end = latestPoint
    ? new Date(`${latestPoint}T00:00:00Z`)
    : new Date()
  return Array.from({ length: days }, (_, index) => {
    const date = new Date(end)
    date.setUTCDate(end.getUTCDate() - (days - index - 1))
    return date.toISOString().slice(0, 10)
  })
}

function buildBrailleChart(payload: ActivityPayload | null): BrailleChartData {
  const points = payload?.points || []
  const dates = activityDateKeys(payload?.days || 30, points)
  const totalsBySource = new Map<string, number>()
  const valuesByDate = new Map<string, Map<string, number>>()
  let total = 0

  points.forEach((point) => {
    total += point.value
    totalsBySource.set(
      point.source,
      (totalsBySource.get(point.source) || 0) + point.value,
    )
    const row = valuesByDate.get(point.date) || new Map<string, number>()
    row.set(point.source, (row.get(point.source) || 0) + point.value)
    valuesByDate.set(point.date, row)
  })

  const groups = Array.from(totalsBySource.entries())
    .sort(
      ([leftName, leftTotal], [rightName, rightTotal]) =>
        rightTotal - leftTotal || leftName.localeCompare(rightName),
    )
    .map(([label, groupTotal], index) => ({
      color:
        sourceActivityColors[label.toLocaleLowerCase()] ||
        activityColors[index % activityColors.length],
      label,
      total: groupTotal,
    }))
  const columnTotals = dates.map((date) =>
    Array.from(valuesByDate.get(date)?.values() || []).reduce(
      (sum, value) => sum + value,
      0,
    ),
  )
  const maximum = Math.max(0, ...columnTotals)
  const emptyColor = "var(--muted-foreground)"
  const grid = Array.from({ length: homeChartHeight }, () =>
    dates.map(() => ({ color: emptyColor, glyph: " " })),
  )

  dates.forEach((date, column) => {
    const columnTotal = columnTotals[column]
    if (!columnTotal || !maximum) return
    const level = Math.ceil(
      (columnTotal * homeChartHeight * 4) / maximum,
    )
    const values = valuesByDate.get(date)
    const dotColors: string[] = []
    let cumulative = 0

    groups.forEach((group) => {
      cumulative += values?.get(group.label) || 0
      const boundary = Math.floor((cumulative * level) / columnTotal)
      while (dotColors.length < boundary) dotColors.push(group.color)
    })

    for (let row = 0; row < homeChartHeight; row += 1) {
      const base = (homeChartHeight - row - 1) * 4
      const fill = Math.min(4, Math.max(0, level - base))
      if (!fill) continue
      grid[row][column] = {
        color: dotColors[base + Math.floor((fill - 1) / 2)] || groups[0].color,
        glyph: brailleLevels[fill],
      }
    }
  })

  return { grid, groups, total }
}

function HomeActivityChart({
  active,
  project,
  source,
}: {
  active: boolean
  project: string
  source: string
}) {
  const [metric, setMetric] = useState<ActivityMetric>("sessions")
  const [payload, setPayload] = useState<ActivityPayload | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState("")
  const requestGeneration = useRef(0)

  useEffect(() => {
    if (!active) return
    const generation = ++requestGeneration.current
    const params = new URLSearchParams({ days: "30", metric })
    if (source !== "all") params.set("source", source)
    if (project.trim()) params.set("project", project.trim())
    setLoading(true)
    setError("")
    void api<ActivityPayload>(`/api/activity?${params}`)
      .then((data) => {
        if (generation === requestGeneration.current) setPayload(data)
      })
      .catch((requestError) => {
        if (generation !== requestGeneration.current) return
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Could not load activity",
        )
      })
      .finally(() => {
        if (generation === requestGeneration.current) setLoading(false)
      })
  }, [active, metric, project, source])

  const chart = useMemo(() => buildBrailleChart(payload), [payload])
  const chartLabel = `${compactNumber.format(chart.total)} ${metric} over the last 30 days`

  return (
    <section
      aria-busy={loading}
      aria-label="Recent activity"
      className="home-activity"
    >
      <div
        aria-label={chartLabel}
        className={cn("braille-chart", loading && "is-loading")}
        role="img"
      >
        {chart.grid.map((row, rowIndex) => (
          <div className="braille-row" key={rowIndex}>
            {row.map((cell, columnIndex) => (
              <span
                aria-hidden="true"
                key={`${rowIndex}-${columnIndex}`}
                style={{ color: cell.color }}
              >
                {cell.glyph}
              </span>
            ))}
          </div>
        ))}
      </div>

      <div className="home-activity-caption">
        <div className="activity-summary">
          <span>
            {error
              ? "Activity unavailable"
              : loading && !payload
                ? "Loading activity…"
                : `${compactNumber.format(chart.total)} ${metric}${payload?.partial ? " · partial" : ""}`}
          </span>
          {chart.groups.length > 0 && (
            <span className="activity-legend" aria-hidden="true">
              {chart.groups.map((group) => (
                <span key={group.label}>
                  <i style={{ background: group.color }} />
                  {group.label}
                </span>
              ))}
            </span>
          )}
        </div>
        <Select
          onValueChange={(value) => setMetric(value as ActivityMetric)}
          value={metric}
        >
          <SelectTrigger
            aria-label="Activity metric"
            className="home-chart-select"
            size="sm"
            variant="ghost"
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value="sessions">Sessions</SelectItem>
              <SelectItem value="tokens">Tokens</SelectItem>
            </SelectGroup>
          </SelectContent>
        </Select>
      </div>

      {metric === "tokens" && payload && !payload.token_usage_enabled && (
        <p className="home-activity-note">
          Token usage is disabled. Set <code>token_usage = true</code> in the
          memex config to enable it.
        </p>
      )}
    </section>
  )
}

function App() {
  const [query, setQuery] = useState(paramsAtLoad.get("q") || "")
  const [source, setSource] = useState(paramsAtLoad.get("source") || "all")
  const [project, setProject] = useState(paramsAtLoad.get("project") || "")
  const [shellView, setShellView] = useState<ShellView>(initialShellView)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [mode, setMode] = useState<PreviewMode>(initialMode)
  const [showThinking, setShowThinking] = useState(false)
  const [showDetails, setShowDetails] = useState(false)
  const [results, setResults] = useState<SearchResult[]>([])
  const [knownProjects, setKnownProjects] = useState<string[]>([])
  const [homeSelectedIndex, setHomeSelectedIndex] = useState(0)
  const [hasMoreResults, setHasMoreResults] = useState(false)
  const [loadingMoreResults, setLoadingMoreResults] = useState(false)
  const [selectedId, setSelectedId] = useState(paramsAtLoad.get("session"))
  const [session, setSession] = useState<SessionPayload | null>(null)
  const [status, setStatus] = useState("Loading recent sessions…")
  const [error, setError] = useState("")
  const [documentCount, setDocumentCount] = useState<number | null>(null)
  const [historyLimit, setHistoryLimit] = useState(150)
  const [theme, setTheme] = useState(getPreferredTheme)
  const searchGeneration = useRef(0)
  const sessionGeneration = useRef(0)
  const sessionCache = useRef(new Map<string, Promise<SessionPayload>>())

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark")
    localStorage.setItem("memex-theme", theme)
  }, [theme])

  useEffect(() => {
    localStorage.setItem("memex-preview-mode", mode)
  }, [mode])

  const updateLocation = useCallback(
    (nextSelectedKey: string | null) => {
      const next = new URLSearchParams()
      if (query.trim()) next.set("q", query.trim())
      if (source !== "all") next.set("source", source)
      if (project.trim()) next.set("project", project.trim())
      if (nextSelectedKey) next.set("session", nextSelectedKey)
      if (nextSelectedKey && mode !== "matches") next.set("mode", mode)
      history.replaceState({}, "", next.size ? `?${next}` : location.pathname)
    },
    [mode, project, query, source],
  )

  const fetchFirstPage = useCallback((key: string) => {
    const cached = sessionCache.current.get(key)
    if (cached) return cached
    const request = api<SessionPayload>(
      `/api/session?key=${encodeURIComponent(key)}&limit=40`,
    ).finally(() => {
      // Keep only in-flight prefetches. A later selection revalidates the first page so
      // active sessions cannot remain permanently stale.
      sessionCache.current.delete(key)
    })
    sessionCache.current.set(key, request)
    while (sessionCache.current.size > 8) {
      const oldest = sessionCache.current.keys().next().value
      if (oldest) sessionCache.current.delete(oldest)
      else break
    }
    return request
  }, [])

  const searchParamsFor = useCallback(
    (offset: number) => {
      const searchParams = new URLSearchParams({
        limit: "50",
        offset: String(offset),
      })
      if (query.trim()) searchParams.set("q", query.trim())
      if (source !== "all") searchParams.set("source", source)
      if (project.trim()) searchParams.set("project", project.trim())
      return searchParams
    },
    [project, query, source],
  )

  const searchStatus = useCallback(
    (count: number, hasMore: boolean) =>
      count
        ? `${count}${hasMore ? "+" : ""} ${query.trim() ? "matching" : "recent"} session${count === 1 ? "" : "s"}`
        : "No sessions found",
    [query],
  )

  const selectSession = useCallback(
    async (
      key: string,
      summary?: SearchResult,
      shouldUpdateLocation = true,
    ) => {
      setSelectedId(key)
      setHistoryLimit(150)
      setError("")
      if (shouldUpdateLocation) updateLocation(key)

      const generation = ++sessionGeneration.current
      if (summary) {
        setSession({
          session_key: key,
          session_id: summary.session_id,
          project: summary.project,
          source: summary.source,
          started_at: summary.ts,
          ended_at: summary.ts,
          offset: 0,
          total: 1,
          messages: [
            {
              record_key: `provisional:${key}`,
              role: summary.role,
              content: summary.snippet || "Loading transcript…",
              ts: summary.ts,
              provisional: true,
            },
          ],
        })
      }

      try {
        const firstPage = await fetchFirstPage(key)
        if (generation !== sessionGeneration.current) return
        setSession(firstPage)
        const messages = [...firstPage.messages]
        const remainingOffsets: number[] = []
        for (
          let offset = messages.length;
          offset < firstPage.total;
          offset += SESSION_HISTORY_PAGE_SIZE
        ) {
          remainingOffsets.push(offset)
        }
        for (
          let start = 0;
          start < remainingOffsets.length;
          start += SESSION_HISTORY_CONCURRENCY
        ) {
          const offsets = remainingOffsets.slice(
            start,
            start + SESSION_HISTORY_CONCURRENCY,
          )
          const pages = await Promise.all(
            offsets.map((offset) =>
              api<SessionPayload>(
                `/api/session?key=${encodeURIComponent(key)}&offset=${offset}&limit=${SESSION_HISTORY_PAGE_SIZE}`,
              ),
            ),
          )
          if (generation !== sessionGeneration.current) return
          for (const page of pages) messages.push(...page.messages)
          const loadedMessages = [...messages]
          startTransition(() =>
            setSession({ ...firstPage, messages: loadedMessages }),
          )
        }
      } catch (requestError) {
        if (generation !== sessionGeneration.current) return
        setError(
          requestError instanceof Error
            ? requestError.message
            : "Could not load transcript",
        )
      }
    },
    [fetchFirstPage, updateLocation],
  )

  useEffect(() => {
    const timer = window.setTimeout(async () => {
      const generation = ++searchGeneration.current
      const searchParams = searchParamsFor(0)
      setStatus(query.trim() ? "Searching…" : "Loading recent sessions…")
      setError("")
      setHasMoreResults(false)
      setLoadingMoreResults(false)

      try {
        const data = await api<SearchPayload>(`/api/search?${searchParams}`)
        if (generation !== searchGeneration.current) return
        setResults(data.results)
        setHasMoreResults(data.has_more)
        setStatus(searchStatus(data.results.length, data.has_more))

        if (shellView === "transcript") {
          const currentId = selectedId
          const next =
            data.results.find((item) => item.session_key === currentId) ||
            data.results[0]
          if (next) void selectSession(next.session_key, next, false)
          else {
            setSelectedId(null)
            setSession(null)
          }
        }
      } catch (requestError) {
        if (generation !== searchGeneration.current) return
        const message =
          requestError instanceof Error ? requestError.message : "Search failed"
        setStatus(message)
        setError(message)
      }
    }, 180)
    return () => window.clearTimeout(timer)
  }, [
    query,
    searchParamsFor,
    searchStatus,
    selectSession,
    shellView,
  ])

  const loadMoreResults = useCallback(async () => {
    if (!hasMoreResults || loadingMoreResults) return
    const generation = searchGeneration.current
    const offset = results.length
    setLoadingMoreResults(true)
    try {
      const data = await api<SearchPayload>(
        `/api/search?${searchParamsFor(offset)}`,
      )
      if (generation !== searchGeneration.current) return
      const known = new Set(results.map((result) => result.session_key))
      const additions = data.results.filter(
        (result) => !known.has(result.session_key),
      )
      const nextCount = results.length + additions.length
      setResults((current) => [...current, ...additions])
      setHasMoreResults(data.has_more)
      setStatus(searchStatus(nextCount, data.has_more))
    } catch (requestError) {
      if (generation !== searchGeneration.current) return
      setError(
        requestError instanceof Error
          ? requestError.message
          : "Could not load more results",
      )
    } finally {
      if (generation === searchGeneration.current) setLoadingMoreResults(false)
    }
  }, [
    hasMoreResults,
    loadingMoreResults,
    results,
    searchParamsFor,
    searchStatus,
  ])

  useEffect(() => {
    void api<{ documents: number }>("/api/stats")
      .then((data) => setDocumentCount(data.documents))
      .catch(() => {})
  }, [])

  useEffect(
    () => updateLocation(selectedId),
    [mode, selectedId, updateLocation],
  )

  const preview = useMemo(() => {
    if (!session) return { rows: [] as PreviewRow[], noMatches: false, remaining: 0 }
    const visible = session.messages
      .map((message, index) => ({ message, index, context: false }))
      .filter(({ message }) => {
        if (
          !message.provisional &&
          !message.content.trim() &&
          !message.status
        )
          return false
        const tool = ["tool_use", "tool_result", "system"].includes(message.role)
        const thinking = ["reasoning", "thinking"].includes(message.role)
        return (
          (message.provisional || showDetails || !tool) &&
          (showThinking || !thinking)
        )
      })

    if (mode === "history") {
      return {
        rows: visible.slice(0, historyLimit),
        noMatches: false,
        remaining: Math.max(0, visible.length - historyLimit),
      }
    }

    const terms = Array.from(
      new Set(
        query
          .toLocaleLowerCase()
          .split(/\s+/)
          .map((value) =>
            value.replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, ""),
          )
          .filter((value) => value.length >= 2),
      ),
    )
    if (!terms.length)
      return { rows: visible.slice(-12), noMatches: false, remaining: 0 }

    const matches = new Set<number>()
    visible.forEach(({ message, index }) => {
      const text = message.content.toLocaleLowerCase()
      if (terms.some((term) => text.includes(term))) matches.add(index)
    })
    if (!matches.size) return { rows: [], noMatches: true, remaining: 0 }

    const included = new Set<number>()
    matches.forEach((index) => {
      included.add(index - 1)
      included.add(index)
      included.add(index + 1)
    })
    return {
      rows: visible
        .filter(({ index }) => included.has(index))
        .map((row) => ({ ...row, context: !matches.has(row.index) })),
      noMatches: false,
      remaining: 0,
    }
  }, [historyLimit, mode, query, session, showDetails, showThinking])

  const homeResults = useMemo(() => {
    const unique = new Map<string, SearchResult>()
    results.forEach((result) => {
      if (!unique.has(result.session_key)) unique.set(result.session_key, result)
    })
    return Array.from(unique.values()).slice(0, 12)
  }, [results])

  useEffect(() => {
    setHomeSelectedIndex(0)
  }, [query, source, project])

  useEffect(() => {
    const discovered = results
      .map((result) => result.project.trim())
      .filter(Boolean)
    if (!discovered.length) return
    setKnownProjects((current) => {
      const next = Array.from(new Set([...current, ...discovered])).sort((a, b) =>
        a.localeCompare(b),
      )
      return next.length === current.length &&
        next.every((value, index) => value === current[index])
        ? current
        : next
    })
  }, [results])

  const openTranscript = useCallback(
    (result: SearchResult) => {
      const update = () => {
        setShellView("transcript")
        setSidebarOpen(true)
        void selectSession(result.session_key, result)
      }
      const startViewTransition = (
        document as Document & {
          startViewTransition?: (callback: () => void) => unknown
        }
      ).startViewTransition

      if (shellView === "home" && startViewTransition) {
        startViewTransition.call(document, () => flushSync(update))
      } else {
        update()
      }
    },
    [selectSession, shellView],
  )

  const returnHome = useCallback(() => {
    const update = () => {
      setShellView("home")
      setSidebarOpen(false)
      setSelectedId(null)
      setSession(null)
    }
    const startViewTransition = (
      document as Document & {
        startViewTransition?: (callback: () => void) => unknown
      }
    ).startViewTransition

    if (startViewTransition) {
      startViewTransition.call(document, () => flushSync(update))
    } else {
      update()
    }
  }, [])

  const handleHomeSearchKeyDown = useCallback(
    (event: KeyboardEvent<HTMLInputElement>) => {
      if (event.key === "ArrowDown") {
        event.preventDefault()
        setHomeSelectedIndex((index) =>
          Math.min(index + 1, Math.max(0, homeResults.length - 1)),
        )
        return
      }
      if (event.key === "ArrowUp") {
        event.preventDefault()
        setHomeSelectedIndex((index) => Math.max(0, index - 1))
        return
      }
      if (event.key === "Enter") {
        const result = homeResults[homeSelectedIndex] || homeResults[0]
        if (!result) return
        event.preventDefault()
        openTranscript(result)
      }
    },
    [homeResults, homeSelectedIndex, openTranscript],
  )

  const handleSidebarKeyDown = useCallback(
    (event: KeyboardEvent<HTMLElement>) => {
      if (event.key === "Enter" || event.key === " ") {
        const button = (event.target as Element).closest<HTMLButtonElement>(
          ".session-button",
        )
        const result = results.find(
          (item) => item.session_key === button?.dataset.sessionKey,
        )
        if (!result) return
        event.preventDefault()
        openTranscript(result)
        return
      }

      const direction =
        event.key === "ArrowDown" || event.key === "j"
          ? 1
          : event.key === "ArrowUp" || event.key === "k"
            ? -1
            : 0
      const edge =
        event.key === "Home" ? 0 : event.key === "End" ? results.length - 1 : -1
      if (
        (!direction && edge < 0) ||
        event.altKey ||
        event.ctrlKey ||
        event.metaKey
      )
        return

      const buttons = Array.from(
        event.currentTarget.querySelectorAll<HTMLButtonElement>(
          ".session-button:not(:disabled)",
        ),
      )
      if (!buttons.length) return

      event.preventDefault()
      const target = event.target as Element
      const focusedIndex = buttons.findIndex(
        (button) => button === target || button.contains(target),
      )
      const selectedIndex = buttons.findIndex(
        (button) => button.dataset.sessionKey === selectedId,
      )
      const currentIndex =
        focusedIndex >= 0 ? focusedIndex : Math.max(0, selectedIndex)
      const nextIndex =
        edge >= 0
          ? Math.min(edge, buttons.length - 1)
          : Math.min(
              buttons.length - 1,
              Math.max(0, currentIndex + direction),
            )
      buttons[nextIndex].focus({ preventScroll: true })
      buttons[nextIndex].scrollIntoView({ block: "nearest" })
    },
    [openTranscript, results, selectedId],
  )

  const filterCount = Number(source !== "all") + Number(Boolean(project.trim()))
  const homeSurface = (
    <main className="home-surface">
      <div className="home-column">
        <HomeActivityChart
          active={shellView === "home"}
          project={project}
          source={source}
        />

        <InputGroup className="home-search search-morph shadow-none">
          <InputGroupAddon>
            <Search />
          </InputGroupAddon>
          <InputGroupInput
            aria-activedescendant={
              homeResults[homeSelectedIndex]
                ? `home-result-${homeSelectedIndex}`
                : undefined
            }
            aria-controls="home-results"
            aria-autocomplete="list"
            aria-expanded={homeResults.length > 0}
            aria-label="Search conversations"
            autoFocus
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={handleHomeSearchKeyDown}
            placeholder="Search your sessions…"
            role="combobox"
            value={query}
          />
        </InputGroup>

        <div className="home-results-heading">
          <div>
            <strong>{query.trim() ? "matches" : "recent"}</strong>
            <span>
              {results.length}
              {hasMoreResults ? "+" : ""}
            </span>
          </div>
          <div className="home-result-filters">
            <Select onValueChange={setSource} value={source}>
              <SelectTrigger
                aria-label="Source"
                className="home-filter-select"
                size="sm"
                variant="ghost"
              >
                <SelectValue placeholder="all sources" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  <SelectItem value="all">all sources</SelectItem>
                  <SelectItem value="claude">Claude</SelectItem>
                  <SelectItem value="codex">Codex</SelectItem>
                  <SelectItem value="opencode">OpenCode</SelectItem>
                  <SelectItem value="cursor">Cursor</SelectItem>
                  <SelectItem value="pi">Pi</SelectItem>
                  <SelectItem value="openclaw">OpenClaw</SelectItem>
                  <SelectItem value="copilot">Copilot</SelectItem>
                </SelectGroup>
              </SelectContent>
            </Select>
            <Select
              onValueChange={(value) => setProject(value === "all" ? "" : value)}
              value={project || "all"}
            >
              <SelectTrigger
                aria-label="Project"
                className="home-filter-select home-project-select"
                size="sm"
                variant="ghost"
              >
                <SelectValue placeholder="All projects" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  <SelectItem value="all">All projects</SelectItem>
                  {knownProjects.map((option) => (
                    <SelectItem key={option} value={option}>
                      {option}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>
        </div>

        <div
          aria-label={query.trim() ? "Matching sessions" : "Recent sessions"}
          className="home-results"
          id="home-results"
          role="listbox"
        >
          {error ? (
            <div className="home-results-empty text-destructive">{error}</div>
          ) : homeResults.length === 0 ? (
            <div className="home-results-empty">
              {status === "Searching…" || status === "Loading recent sessions…"
                ? status
                : query.trim()
                  ? "No matching sessions"
                  : "No recent sessions"}
            </div>
          ) : (
            homeResults.map((result, index) => (
              <button
                aria-selected={homeSelectedIndex === index}
                className={cn(
                  "home-result",
                  homeSelectedIndex === index && "is-selected",
                )}
                id={`home-result-${index}`}
                key={result.session_key}
                onClick={() => openTranscript(result)}
                onMouseEnter={() => setHomeSelectedIndex(index)}
                role="option"
                type="button"
              >
                <span className="home-result-title">
                  {result.project || "Untitled session"}
                </span>
                <span className="home-result-meta">
                  {result.source} · {result.role}
                </span>
                <time>{formatDate(result.ts)}</time>
                <span className="home-result-snippet">
                  {result.snippet || "No text preview"}
                </span>
              </button>
            ))
          )}
        </div>
      </div>
    </main>
  )

  const transcriptSurface = (
    <div className="transcript-surface">
      <div className="transcript-scroll">
        <div className="messages">
          {error ? (
            <div className="empty text-destructive">{error}</div>
          ) : !session ? (
            <div className="empty">No session to preview.</div>
          ) : preview.noMatches ? (
            <div className="empty">
              This session matched the index, but no literal query terms appear
              in its stored messages.
            </div>
          ) : preview.rows.length === 0 ? (
            <div className="empty">No visible messages in this preview.</div>
          ) : (
            <>
              {preview.rows.map(({ message, index, context }, rowIndex) => {
                const previous = preview.rows[rowIndex - 1]?.message
                const startsInteraction =
                  message.interaction_id &&
                  message.interaction_id !== previous?.interaction_id

                return (
                  <div className="message-row" key={message.record_key || `${message.ts}-${index}`}>
                    {startsInteraction && (
                      <div
                        className="interaction-divider"
                        title={message.interaction_id ?? undefined}
                      >
                        <span>Interaction</span>
                      </div>
                    )}
                    <article
                      className={cn("message", context && "context")}
                      data-interaction-id={message.interaction_id || undefined}
                    >
                      <div className="message-meta">
                        <span>{message.tool_name || message.role || "event"}</span>
                        {message.status && (
                          <Badge variant="outline">
                            {message.source_status || message.status}
                          </Badge>
                        )}
                        <time>{formatDate(message.ts)}</time>
                      </div>
                      <MessageContent message={message} />
                    </article>
                  </div>
                )
              })}
              {preview.remaining > 0 && (
                <Button
                  className="load-more shadow-none"
                  onClick={() => setHistoryLimit((limit) => limit + 150)}
                  variant="outline"
                >
                  Show {Math.min(150, preview.remaining)} more
                </Button>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )

  return (
    <SidebarProvider
      className="memex-shell"
      defaultOpen={false}
      onOpenChange={setSidebarOpen}
      open={sidebarOpen}
      style={{ "--sidebar-width": "19rem" } as CSSProperties}
    >
      <Sidebar collapsible="offcanvas">
        <SidebarHeader className="memex-sidebar-header">
          <div className="brand-row">
            <button className="brand-name" onClick={returnHome} type="button">
              memex
            </button>
          </div>
          <div className="sidebar-summary">
            <span className={cn(error && "text-destructive")}>{status}</span>
            <span>
              {documentCount === null
                ? "— records"
                : `${documentCount.toLocaleString()} records`}
            </span>
          </div>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup className="pt-0 pr-0">
            <SidebarGroupContent>
              <SidebarMenu onKeyDown={handleSidebarKeyDown}>
                {results.map((result) => (
                  <SidebarMenuItem key={result.session_key}>
                    <SidebarMenuButton
                      className="session-button"
                      data-session-key={result.session_key}
                      isActive={selectedId === result.session_key}
                      onClick={() => openTranscript(result)}
                      onPointerEnter={() =>
                        void fetchFirstPage(result.session_key).catch(() => {})
                      }
                      size="lg"
                      tooltip={result.project || "Untitled session"}
                    >
                      <div className="session-copy">
                        <div className="session-title-row">
                          <strong>
                            {result.project || "Untitled session"}
                          </strong>
                          <time>{formatDate(result.ts)}</time>
                        </div>
                        <div className="session-meta">
                          {result.source} · {result.role}
                          {result.score == null
                            ? ""
                            : ` · ${result.score.toFixed(2)}`}
                        </div>
                        <div className="session-snippet">
                          {result.snippet || "No text preview"}
                        </div>
                      </div>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
                {hasMoreResults && (
                  <SidebarMenuItem>
                    <Button
                      className="load-more-results"
                      disabled={loadingMoreResults}
                      onClick={() => void loadMoreResults()}
                      variant="ghost"
                    >
                      {loadingMoreResults ? "Loading…" : "Load more results"}
                    </Button>
                  </SidebarMenuItem>
                )}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
      </Sidebar>

      <SidebarInset className="min-h-0 min-w-0 gap-2 overflow-hidden bg-transparent p-2 shadow-none">
        {shellView === "home" ? (
          homeSurface
        ) : (
          <Tabs
            className="transcript-tabs"
            onValueChange={(value) => setMode(value as PreviewMode)}
            value={mode}
          >
            <header className="command-bar">
              <SidebarTrigger />
              <InputGroup className="search-group search-morph shadow-none">
                <InputGroupAddon>
                  <Search />
                </InputGroupAddon>
                <InputGroupInput
                  aria-label="Search conversations"
                  autoFocus
                  onChange={(event) => setQuery(event.target.value)}
                  onKeyDown={(event) => {
                    if (
                      event.key !== "Escape" &&
                      !(event.key === "Backspace" && query.length === 0)
                    )
                      return
                    event.preventDefault()
                    returnHome()
                  }}
                  placeholder="Search conversations…"
                  value={query}
                />
              </InputGroup>

              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    aria-label="Filters"
                    className="filter-trigger shadow-none"
                    size="icon"
                    variant="outline"
                  >
                    <Filter />
                    {filterCount > 0 && (
                      <Badge className="filter-count">{filterCount}</Badge>
                    )}
                  </Button>
                </PopoverTrigger>
                <PopoverContent align="end" className="filter-popover">
                  <div className="filter-field">
                    <label>Source</label>
                    <Select onValueChange={setSource} value={source}>
                      <SelectTrigger
                        aria-label="Source"
                        className="w-full shadow-none"
                      >
                        <SelectValue placeholder="All sources" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectGroup>
                          <SelectItem value="all">All sources</SelectItem>
                          <SelectItem value="claude">Claude</SelectItem>
                          <SelectItem value="codex">Codex</SelectItem>
                          <SelectItem value="opencode">OpenCode</SelectItem>
                          <SelectItem value="cursor">Cursor</SelectItem>
                          <SelectItem value="pi">Pi</SelectItem>
                          <SelectItem value="openclaw">OpenClaw</SelectItem>
                          <SelectItem value="copilot">Copilot</SelectItem>
                        </SelectGroup>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="filter-field">
                    <label htmlFor="project-filter">Project</label>
                    <Input
                      className="shadow-none"
                      id="project-filter"
                      onChange={(event) => setProject(event.target.value)}
                      placeholder="Any project"
                      value={project}
                    />
                  </div>
                </PopoverContent>
              </Popover>

              <TabsList>
                <TabsTrigger value="matches">Matches</TabsTrigger>
                <TabsTrigger value="history">History</TabsTrigger>
              </TabsList>

              <ToggleGroup
                aria-label="Transcript visibility"
                className="view-toggles"
                multiple
                onValueChange={(values) => {
                  setShowThinking(values.includes("reasoning"))
                  setShowDetails(values.includes("tools"))
                }}
                value={[
                  ...(showThinking ? ["reasoning"] : []),
                  ...(showDetails ? ["tools"] : []),
                ]}
                variant="outline"
              >
                <ToggleGroupItem
                  aria-label="Show reasoning"
                  title="Reasoning"
                  value="reasoning"
                >
                  <Brain />
                </ToggleGroupItem>
                <ToggleGroupItem
                  aria-label="Show tool calls"
                  title="Tool calls"
                  value="tools"
                >
                  <TerminalSquare />
                </ToggleGroupItem>
              </ToggleGroup>

              <Button
                aria-label={`Use ${theme === "dark" ? "light" : "dark"} theme`}
                onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
                size="icon-sm"
                variant="ghost"
              >
                {theme === "dark" ? <Sun /> : <Moon />}
              </Button>
            </header>

            <TabsContent className="transcript-tab" value="matches">
              {mode === "matches" && transcriptSurface}
            </TabsContent>
            <TabsContent className="transcript-tab" value="history">
              {mode === "history" && transcriptSurface}
            </TabsContent>
          </Tabs>
        )}
      </SidebarInset>
    </SidebarProvider>
  )
}

export default App
