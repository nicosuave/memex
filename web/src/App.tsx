import {
  type CSSProperties,
  type KeyboardEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react"
import { Brain, Filter, Moon, Search, Sun, TerminalSquare } from "lucide-react"

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
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import { cn } from "@/lib/utils"
import { api, authenticationRequiredMessage } from "@/api"
import {
  isThinkingMessage,
  isToolMessage,
  useSessionResource,
  type SessionTarget,
} from "@/session"
import { Transcript } from "@/Transcript"
import { useSidebar } from "@/components/ui/sidebar"

type SearchResult = {
  session_id: string
  record_id?: string
  source_path?: string
  project: string
  source: string
  role: string
  ts: number
  score?: number | null
  snippet: string
  snippet_matches?: Array<{ start: number; end: number }>
}

type SearchPayload = {
  query: string
  offset: number
  has_more: boolean
  results: SearchResult[]
}

type PreviewMode = "matches" | "history"
type ShellView = "home" | "transcript"
type TimeRange = "24h" | "7d" | "30d" | "all"
type SearchSort = "relevance" | "newest" | "oldest"

function parseSearchSort(value: string | null): SearchSort | null {
  return value === "relevance" || value === "newest" || value === "oldest" ? value : null
}

function SortControl({ value, hasQuery, onChange }: {
  value: SearchSort; hasQuery: boolean; onChange: (value: string) => void
}) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger aria-label="Sort results" className="home-filter-select" size="sm" variant="ghost">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          <SelectItem value="relevance" disabled={!hasQuery}>Relevance</SelectItem>
          <SelectItem value="newest">Newest</SelectItem>
          <SelectItem value="oldest">Oldest</SelectItem>
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

const timeRanges: TimeRange[] = ["24h", "7d", "30d", "all"]
const defaultTimeRange: TimeRange = "30d"

function ResultSnippet({ result }: { result: SearchResult }) {
  if (!result.snippet) return <>No text preview</>
  const characters = Array.from(result.snippet)
  const matches = result.snippet_matches || []
  if (!matches.length) return <>{result.snippet}</>

  const parts = []
  let cursor = 0
  for (const [index, match] of matches.entries()) {
    const start = Math.max(cursor, Math.min(characters.length, match.start))
    const end = Math.max(start, Math.min(characters.length, match.end))
    if (start > cursor) parts.push(characters.slice(cursor, start).join(""))
    if (end > start) {
      parts.push(
        <mark
          className="rounded-sm bg-primary/20 text-inherit"
          key={`match-${index}`}
        >
          {characters.slice(start, end).join("")}
        </mark>,
      )
    }
    cursor = end
  }
  if (cursor < characters.length)
    parts.push(characters.slice(cursor).join(""))
  return <>{parts}</>
}

function parseTimeRange(value: string | null): TimeRange {
  return timeRanges.includes(value as TimeRange)
    ? (value as TimeRange)
    : defaultTimeRange
}

function describeTimeRange(range: TimeRange) {
  switch (range) {
    case "24h":
      return "the last 24 hours"
    case "7d":
      return "the last 7 days"
    case "30d":
      return "the last 30 days"
    case "all":
      return "all time"
  }
}

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

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
})
const formatDate = (timestamp: number) =>
  timestamp ? dateFormatter.format(new Date(timestamp)) : ""

function getPreferredTheme() {
  const stored = localStorage.getItem("memex-theme")
  if (stored === "dark" || stored === "light") return stored
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light"
}

type ActivityMetric = "sessions" | "tokens"

type ActivityPayload = {
  metric: ActivityMetric
  range?: TimeRange
  bucket_keys?: string[]
  days?: number
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

function activityDateKeys(days: number) {
  const end = new Date()
  return Array.from({ length: days }, (_, index) => {
    const date = new Date(end)
    date.setUTCDate(end.getUTCDate() - (days - index - 1))
    return date.toISOString().slice(0, 10)
  })
}

function buildBrailleChart(payload: ActivityPayload | null): BrailleChartData {
  const points = payload?.points || []
  const dates =
    payload?.bucket_keys?.length
      ? payload.bucket_keys
      : activityDateKeys(payload?.days || 30)
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
    const level = Math.ceil((columnTotal * homeChartHeight * 4) / maximum)
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
  query,
  onRangeChange,
  project,
  range,
  source,
  origin,
}: {
  active: boolean
  query: string
  onRangeChange: (value: string) => void
  project: string
  range: TimeRange
  source: string
  origin: string
}) {
  const [metric, setMetric] = useState<ActivityMetric>("sessions")
  const [payload, setPayload] = useState<{
    data: ActivityPayload
    intent: string
  } | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState("")
  const requestGeneration = useRef(0)
  const searchQuery = query.trim()
  const intent = JSON.stringify([
    searchQuery, metric, project, range, source, origin,
  ])

  useEffect(() => {
    if (!active) return
    const controller = new AbortController()
    const generation = ++requestGeneration.current
    const params = new URLSearchParams({ range, metric })
    if (searchQuery) params.set("q", searchQuery)
    if (source !== "all") params.set("source", source)
    if (project.trim()) params.set("project", project.trim())
    if (origin !== "interactive") params.set("origin", origin)
    setLoading(true)
    setPayload(null)
    setError("")
    const timer = window.setTimeout(() => {
      void api<ActivityPayload>(`/api/activity?${params}`, controller.signal)
        .then((data) => {
          if (generation === requestGeneration.current)
            setPayload({ data, intent })
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
    }, 180)
    return () => {
      window.clearTimeout(timer)
      ++requestGeneration.current
      controller.abort()
    }
  }, [active, metric, project, range, source, origin, searchQuery, intent])

  const currentPayload =
    payload?.intent === intent ? payload.data : null

  const chart = useMemo(
    () => buildBrailleChart(currentPayload),
    [currentPayload],
  )
  const chartLabel = `${compactNumber.format(chart.total)} ${metric} over ${describeTimeRange(range)}`

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
          <div
            className="braille-row"
            key={rowIndex}
            style={{
              gridTemplateColumns: `repeat(${Math.max(1, row.length)}, minmax(0, 1fr))`,
            }}
          >
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
              : loading || !currentPayload
                ? "Loading activity…"
                : `${compactNumber.format(chart.total)} ${metric}${currentPayload.partial ? " · partial" : ""}`}
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
        <div className="home-chart-controls">
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
          <Select onValueChange={onRangeChange} value={range}>
            <SelectTrigger
              aria-label="Time range"
              className="home-chart-select"
              size="sm"
              variant="ghost"
            >
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value="24h">24h</SelectItem>
                <SelectItem value="7d">7d</SelectItem>
                <SelectItem value="30d">30d</SelectItem>
                <SelectItem value="all">All</SelectItem>
              </SelectGroup>
            </SelectContent>
          </Select>
        </div>
      </div>

      {metric === "tokens" &&
        currentPayload &&
        !currentPayload.token_usage_enabled && (
          <p className="home-activity-note">
            Token usage is disabled. Set <code>token_usage = true</code> in the
            memex config to enable it.
          </p>
        )}
    </section>
  )
}

function CloseMobileOnNavigation({ target }: { target: SessionTarget | null }) {
  const { setOpenMobile } = useSidebar()
  useEffect(() => {
    setOpenMobile(false)
  }, [target, setOpenMobile])
  return null
}

function ResultContinuation({
  hasMore,
  loading,
  error,
  loadMore,
  intent,
  sidebar = false,
}: {
  hasMore: boolean
  loading: boolean
  error: string
  loadMore: () => Promise<void>
  intent: string
  sidebar?: boolean
}) {
  const boundary = useRef<HTMLDivElement>(null)
  const { isMobile, open, openMobile } = useSidebar()
  const visible = !sidebar || (isMobile ? openMobile : open)
  useEffect(() => {
    boundary.current?.parentElement?.scrollTo({ top: 0 })
  }, [intent])
  useEffect(() => {
    const element = boundary.current
    if (!element || !visible || !hasMore || loading || error) return
    const scrollContainer = element.parentElement
    if (!scrollContainer) return
    // Both continuations live directly inside their list's scroll container.
    // Start the next request one viewport ahead so network and search time overlap
    // with the user's remaining scroll distance, including taller search matches.
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) void loadMore()
      },
      {
        root: scrollContainer,
        rootMargin: `0px 0px ${Math.max(240, scrollContainer.clientHeight)}px 0px`,
      },
    )
    observer.observe(element)
    return () => observer.disconnect()
  }, [visible, hasMore, loading, error, loadMore])
  return (
    <div ref={boundary} className="result-continuation">
      {loading && <span role="status">Loading more sessions…</span>}
      {error && (
        <div role="alert">
          {error}
          <Button variant="ghost" onClick={() => void loadMore()}>
            Retry loading sessions
          </Button>
        </div>
      )}
    </div>
  )
}

function App() {
  const [query, setQuery] = useState(paramsAtLoad.get("q") || "")
  const [source, setSource] = useState(paramsAtLoad.get("source") || "all")
  const [project, setProject] = useState(paramsAtLoad.get("project") || "")
  const [origin, setOrigin] = useState(
    paramsAtLoad.get("origin") || "interactive",
  )
  const [timeRange, setTimeRange] = useState(() =>
    parseTimeRange(paramsAtLoad.get("range")),
  )
  const [selectedSort, setSelectedSort] = useState(() => parseSearchSort(paramsAtLoad.get("sort")))
  const sort: SearchSort = !query.trim() && selectedSort === "relevance"
    ? "newest" : selectedSort ?? (query.trim() ? "relevance" : "newest")
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
  const [target, setTarget] = useState<SessionTarget | null>(() =>
    paramsAtLoad.has("session")
      ? {
          id: paramsAtLoad.get("session")!,
          sourcePath: paramsAtLoad.get("path") || undefined,
          sessionSource: paramsAtLoad.get("session_source") || undefined,
          recordId: paramsAtLoad.get("record") || undefined,
        }
      : null,
  )
  const selectedId = target?.id || null
  const resource = useSessionResource(target)
  const visibilityCounts = useMemo(
    () => ({
      reasoning:
        resource.session?.messages.filter(isThinkingMessage).length || 0,
      tools: resource.session?.messages.filter(isToolMessage).length || 0,
    }),
    [resource.session],
  )
  const [status, setStatus] = useState("Loading sessions…")
  const [error, setError] = useState("")
  const [pageError, setPageError] = useState("")
  const [searchRevision, setSearchRevision] = useState(0)
  const [theme, setTheme] = useState(getPreferredTheme)
  const searchGeneration = useRef(0)
  const searchController = useRef<AbortController | null>(null)
  const resultOffset = useRef(0)
  const resultIntent = useRef("")
  const loadingMore = useRef(false)
  const intent = JSON.stringify([query, source, project, origin, timeRange, sort])
  const currentIntent = useRef(intent)
  currentIntent.current = intent

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark")
    localStorage.setItem("memex-theme", theme)
  }, [theme])

  useEffect(() => {
    localStorage.setItem("memex-preview-mode", mode)
  }, [mode])

  const locationForTarget = useCallback(
    (
      nextTarget: SessionTarget | null,
      nextTimeRange: TimeRange = timeRange,
      nextSort: SearchSort | null = selectedSort,
    ) => {
      const next = new URLSearchParams()
      if (query.trim()) next.set("q", query.trim())
      if (source !== "all") next.set("source", source)
      if (project.trim()) next.set("project", project.trim())
      if (origin !== "interactive") next.set("origin", origin)
      if (nextTimeRange !== defaultTimeRange) next.set("range", nextTimeRange)
      if (nextSort) next.set("sort", nextSort)
      if (nextTarget) {
        next.set("session", nextTarget.id)
        if (nextTarget.sourcePath) next.set("path", nextTarget.sourcePath)
        if (nextTarget.sessionSource)
          next.set("session_source", nextTarget.sessionSource)
        if (nextTarget.recordId) next.set("record", nextTarget.recordId)
        if (mode !== "matches") next.set("mode", mode)
      }
      const url = next.size ? `?${next}` : location.pathname
      return url
    },
    [mode, origin, project, query, source, timeRange, selectedSort],
  )

  const updateLocation = useCallback(
    (nextTarget: SessionTarget | null, push = false) => {
      const url = locationForTarget(nextTarget)
      if (push) history.pushState({}, "", url)
      else history.replaceState({}, "", url)
    },
    [locationForTarget],
  )
  const resultHref = (result: SearchResult) =>
    locationForTarget({
      id: result.session_id,
      sourcePath: result.source_path,
      sessionSource: result.source,
      recordId: query.trim() ? result.record_id : undefined,
    })

  const changeTimeRange = useCallback(
    (value: string) => {
      const nextRange = parseTimeRange(value)
      if (nextRange === timeRange) return
      history.pushState({}, "", locationForTarget(target, nextRange))
      setTimeRange(nextRange)
    },
    [locationForTarget, target, timeRange],
  )

  const changeSort = useCallback((value: string) => {
    const nextSort = parseSearchSort(value)
    if (!nextSort || nextSort === sort) return
    history.pushState({}, "", locationForTarget(target, timeRange, nextSort))
    setSelectedSort(nextSort)
  }, [locationForTarget, sort, target, timeRange])

  useEffect(() => {
    const restore = () => {
      const params = new URLSearchParams(location.search)
      setQuery(params.get("q") || "")
      setSource(params.get("source") || "all")
      setProject(params.get("project") || "")
      setOrigin(params.get("origin") || "interactive")
      setTimeRange(parseTimeRange(params.get("range")))
      setSelectedSort(parseSearchSort(params.get("sort")))
      setMode(params.get("mode") === "history" ? "history" : "matches")
      setTarget(
        params.has("session")
          ? {
              id: params.get("session")!,
              sourcePath: params.get("path") || undefined,
              sessionSource: params.get("session_source") || undefined,
              recordId: params.get("record") || undefined,
            }
          : null,
      )
      setShellView(params.has("session") ? "transcript" : "home")
    }
    window.addEventListener("popstate", restore)
    return () => window.removeEventListener("popstate", restore)
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
      if (origin !== "interactive") searchParams.set("origin", origin)
      searchParams.set("range", timeRange)
      searchParams.set("sort", sort)
      return searchParams
    },
    [origin, project, query, source, timeRange, sort],
  )

  const searchStatus = useCallback(
    (count: number) =>
      count
        ? query.trim() ? "Search results" : "Sessions"
        : "No sessions found",
    [query],
  )

  useEffect(() => {
    const generation = ++searchGeneration.current
    const controller = new AbortController()
    searchController.current = controller
    loadingMore.current = false
    setHasMoreResults(false)
    const timer = window.setTimeout(async () => {
      const searchParams = searchParamsFor(0)
      setStatus(query.trim() ? "Searching…" : "Loading sessions…")
      setError("")
      setPageError("")
      setHasMoreResults(false)
      setLoadingMoreResults(false)

      try {
        const data = await api<SearchPayload>(
          `/api/search?${searchParams}`,
          controller.signal,
        )
        if (
          generation !== searchGeneration.current ||
          currentIntent.current !== intent
        )
          return
        resultOffset.current = data.offset + data.results.length
        resultIntent.current = intent
        setResults(data.results)
        setHasMoreResults(data.has_more && data.results.length > 0)
        setStatus(searchStatus(data.results.length))
      } catch (requestError) {
        if (
          generation !== searchGeneration.current ||
          currentIntent.current !== intent
        )
          return
        const message =
          requestError instanceof Error ? requestError.message : "Search failed"
        setStatus(message)
        setError(message)
      }
    }, 180)
    return () => {
      window.clearTimeout(timer)
      controller.abort()
      ++searchGeneration.current
    }
  }, [intent, query, searchParamsFor, searchStatus, searchRevision])

  const loadMoreResults = useCallback(async () => {
    if (
      !hasMoreResults ||
      loadingMore.current ||
      resultIntent.current !== intent ||
      currentIntent.current !== intent
    )
      return
    loadingMore.current = true
    const generation = searchGeneration.current
    const offset = resultOffset.current
    setLoadingMoreResults(true)
    setPageError("")
    try {
      const data = await api<SearchPayload>(
        `/api/search?${searchParamsFor(offset)}`,
        searchController.current?.signal,
      )
      if (
        generation !== searchGeneration.current ||
        currentIntent.current !== intent
      )
        return
      resultOffset.current = data.offset + data.results.length
      const known = new Set(
        results.map(
          (result) =>
            `${result.source}:${result.source_path}:${result.session_id}`,
        ),
      )
      const additions = data.results.filter(
        (result) =>
          !known.has(
            `${result.source}:${result.source_path}:${result.session_id}`,
          ),
      )
      const nextCount = results.length + additions.length
      setResults((current) => [...current, ...additions])
      setHasMoreResults(data.has_more && data.results.length > 0)
      setStatus(searchStatus(nextCount))
    } catch (requestError) {
      if (
        generation !== searchGeneration.current ||
        currentIntent.current !== intent
      )
        return
      setPageError(
        requestError instanceof Error
          ? requestError.message
          : "Could not load more results",
      )
    } finally {
      if (generation === searchGeneration.current) {
        loadingMore.current = false
        setLoadingMoreResults(false)
      }
    }
  }, [hasMoreResults, intent, results, searchParamsFor, searchStatus])

  useEffect(() => updateLocation(target), [target, updateLocation])

  const homeResults = useMemo(() => {
    const unique = new Map<string, SearchResult>()
    results.forEach((result) => {
      const key = `${result.source}:${result.source_path}:${result.session_id}`
      if (!unique.has(key)) unique.set(key, result)
    })
    return Array.from(unique.values())
  }, [results])

  useEffect(() => {
    setHomeSelectedIndex(0)
  }, [origin, project, query, source, timeRange, sort])

  useEffect(() => {
    const discovered = results
      .map((result) => result.project.trim())
      .filter(Boolean)
    if (!discovered.length) return
    setKnownProjects((current) => {
      const next = Array.from(new Set([...current, ...discovered])).sort(
        (a, b) => a.localeCompare(b),
      )
      return next.length === current.length &&
        next.every((value, index) => value === current[index])
        ? current
        : next
    })
  }, [results])

  const openTranscript = useCallback(
    (result: SearchResult) => {
      const next = {
        id: result.session_id,
        sourcePath: result.source_path,
        sessionSource: result.source,
        recordId: query.trim() ? result.record_id : undefined,
      }
      updateLocation(next, true)
      setTarget(next)
      setShellView("transcript")
      setSidebarOpen(true)
    },
    [query, updateLocation],
  )

  const returnHome = useCallback(() => {
    updateLocation(null, true)
    setShellView("home")
    setSidebarOpen(false)
    setTarget(null)
  }, [updateLocation])

  useEffect(() => {
    document
      .getElementById(`home-result-${homeSelectedIndex}`)
      ?.scrollIntoView({ block: "nearest" })
  }, [homeSelectedIndex])

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
      if (event.altKey || event.ctrlKey || event.metaKey) return
      if (event.key === "Enter" || event.key === " ") {
        const button = (event.target as Element).closest<HTMLElement>(
          ".session-button",
        )
        const result = results.find(
          (item) =>
            item.session_id === button?.dataset.sessionId &&
            item.source_path === button?.dataset.sourcePath &&
            item.source === button?.dataset.sessionSource,
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
        event.currentTarget.querySelectorAll<HTMLElement>(
          ".session-button:not(:disabled)",
        ),
      )
      if (!buttons.length) return

      event.preventDefault()
      const focusedElement = event.target as Element
      const focusedIndex = buttons.findIndex(
        (button) =>
          button === focusedElement || button.contains(focusedElement),
      )
      const selectedIndex = buttons.findIndex(
        (button) =>
          button.dataset.sessionId === selectedId &&
          button.dataset.sourcePath === target?.sourcePath &&
          button.dataset.sessionSource === target?.sessionSource,
      )
      const currentIndex =
        focusedIndex >= 0 ? focusedIndex : Math.max(0, selectedIndex)
      const nextIndex =
        edge >= 0
          ? Math.min(edge, buttons.length - 1)
          : Math.min(buttons.length - 1, Math.max(0, currentIndex + direction))
      buttons[nextIndex].focus({ preventScroll: true })
      buttons[nextIndex].scrollIntoView({ block: "nearest" })
    },
    [openTranscript, results, selectedId, target],
  )

  const filterCount =
    Number(source !== "all") +
    Number(Boolean(project.trim())) +
    Number(origin !== "interactive")
  const homeSurface = (
    <main className="home-surface">
      <div className="home-column">
        <HomeActivityChart
          query={query}
          active={shellView === "home"}
          onRangeChange={changeTimeRange}
          project={project}
          range={timeRange}
          source={source}
          origin={origin}
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
            <strong>{query.trim() ? "matches" : "sessions"}</strong>
            <span>
              {results.length}
              {hasMoreResults ? "+" : ""}
            </span>
          </div>
          <div className="home-result-filters">
            <SortControl value={sort} hasQuery={Boolean(query.trim())} onChange={changeSort} />
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
            <Select onValueChange={setOrigin} value={origin}>
              <SelectTrigger
                aria-label="Origin"
                className="home-filter-select"
                size="sm"
                variant="ghost"
              >
                <SelectValue placeholder="interactive" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  <SelectItem value="interactive">interactive</SelectItem>
                  <SelectItem value="subagent">subagent</SelectItem>
                  <SelectItem value="regular">regular (no permission reviews)</SelectItem>
                  <SelectItem value="all">all (includes permission reviews)</SelectItem>
                </SelectGroup>
              </SelectContent>
            </Select>
            <Select
              onValueChange={(value) =>
                setProject(value === "all" ? "" : value)
              }
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
          aria-label={query.trim() ? "Matching sessions" : "Sessions"}
          className="home-results"
          id="home-results"
          role="listbox"
        >
          {error && (
            <div role="alert" className="home-results-empty text-destructive">
              {error}
              <Button
                variant="ghost"
                onClick={() => setSearchRevision((value) => value + 1)}
              >
                Retry search
              </Button>
            </div>
          )}
          {homeResults.length === 0 ? (
            <div className="home-results-empty">
              {status === "Searching…" || status === "Loading sessions…"
                ? status
                : query.trim()
                  ? "No matching sessions"
                  : "No recent sessions"}
            </div>
          ) : (
            homeResults.map((result, index) => (
              <a
                aria-selected={homeSelectedIndex === index}
                className={cn(
                  "home-result",
                  homeSelectedIndex === index && "is-selected",
                )}
                id={`home-result-${index}`}
                key={`${result.source}:${result.source_path}:${result.session_id}`}
                onClick={(event) => {
                  if (
                    event.button !== 0 ||
                    event.metaKey ||
                    event.ctrlKey ||
                    event.shiftKey ||
                    event.altKey
                  )
                    return
                  event.preventDefault()
                  openTranscript(result)
                }}
                onMouseEnter={() => setHomeSelectedIndex(index)}
                role="option"
                href={resultHref(result)}
              >
                <span className="home-result-title">
                  {result.project || "Untitled session"}
                </span>
                <span className="home-result-meta">
                  {result.source} · {result.role}
                </span>
                <time>{formatDate(result.ts)}</time>
                <span
                  className="home-result-snippet"
                  style={result.snippet_matches?.length ? { whiteSpace: "normal", overflowWrap: "anywhere" } : undefined}
                >
                  <ResultSnippet result={result} />
                </span>
              </a>
            ))
          )}
          <ResultContinuation
            hasMore={hasMoreResults}
            loading={loadingMoreResults}
            error={pageError}
            loadMore={loadMoreResults}
            intent={intent}
          />
        </div>
      </div>
    </main>
  )

  const transcriptSurface = (
    <Transcript
      resource={resource}
      target={target}
      mode={mode}
      showThinking={showThinking}
      showDetails={showDetails}
    />
  )

  if (
    [error, pageError, resource.error].includes(authenticationRequiredMessage)
  ) {
    return (
      <main className="transcript-surface">
        <div className="empty">
          <strong>Open Memex from your terminal</strong>
          <p>
            Run <code>memex web open</code> to create a new browser session.
          </p>
        </div>
      </main>
    )
  }

  return (
    <SidebarProvider
      className="memex-shell"
      defaultOpen={false}
      onOpenChange={setSidebarOpen}
      open={sidebarOpen}
      style={{ "--sidebar-width": "19rem" } as CSSProperties}
    >
      <CloseMobileOnNavigation target={target} />
      <Sidebar collapsible="offcanvas">
        <SidebarHeader className="memex-sidebar-header">
          <div className="brand-row">
            <button className="brand-name" onClick={returnHome} type="button">
              memex
            </button>
          </div>
          <div className="sidebar-summary">
            <span className={cn(error && "text-destructive")}>{status}</span>
            <SortControl value={sort} hasQuery={Boolean(query.trim())} onChange={changeSort} />
          </div>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup className="pt-0 pr-0">
            <SidebarGroupContent>
              <SidebarMenu onKeyDown={handleSidebarKeyDown}>
                {results.map((result) => (
                  <SidebarMenuItem
                    key={`${result.source}:${result.source_path}:${result.session_id}`}
                  >
                    <SidebarMenuButton
                      className="session-button"
                      asChild
                      data-session-id={result.session_id}
                      data-source-path={result.source_path}
                      data-session-source={result.source}
                      isActive={
                        selectedId === result.session_id &&
                        (!target?.sourcePath ||
                          target.sourcePath === result.source_path) &&
                        (!target?.sessionSource ||
                          target.sessionSource === result.source)
                      }
                      onClick={(event) => {
                        if (
                          event.button !== 0 ||
                          event.metaKey ||
                          event.ctrlKey ||
                          event.shiftKey ||
                          event.altKey
                        )
                          return
                        event.preventDefault()
                        openTranscript(result)
                      }}
                      size="lg"
                      tooltip={result.project || "Untitled session"}
                    >
                      <a href={resultHref(result)}>
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
                          <div
                            className="session-snippet"
                            style={
                              result.snippet_matches?.length
                                ? { WebkitLineClamp: "unset" }
                                : undefined
                            }
                          >
                            <ResultSnippet result={result} />
                          </div>
                        </div>
                      </a>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
          <ResultContinuation
            hasMore={hasMoreResults}
            loading={loadingMoreResults}
            error={pageError}
            loadMore={loadMoreResults}
            intent={intent}
            sidebar
          />
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
              <Button
                onClick={returnHome}
                aria-label="Back to sessions"
                size="sm"
                variant="ghost"
              >
                Back
              </Button>
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
                    <label>Origin</label>
                    <Select onValueChange={setOrigin} value={origin}>
                      <SelectTrigger
                        aria-label="Origin"
                        className="w-full shadow-none"
                      >
                        <SelectValue placeholder="Interactive" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectGroup>
                          <SelectItem value="interactive">
                            Interactive
                          </SelectItem>
                          <SelectItem value="subagent">Subagent</SelectItem>
                          <SelectItem value="regular">Regular (no permission reviews)</SelectItem>
                          <SelectItem value="all">All (includes permission reviews)</SelectItem>
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

              <Select onValueChange={changeTimeRange} value={timeRange}>
                <SelectTrigger
                  aria-label="Time range"
                  className="transcript-range-select"
                  size="sm"
                  variant="ghost"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value="24h">24h</SelectItem>
                    <SelectItem value="7d">7d</SelectItem>
                    <SelectItem value="30d">30d</SelectItem>
                    <SelectItem value="all">All</SelectItem>
                  </SelectGroup>
                </SelectContent>
              </Select>

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
                  disabled={visibilityCounts.reasoning === 0}
                  title={
                    visibilityCounts.reasoning
                      ? `${showThinking ? "Hide" : "Show"} reasoning (${visibilityCounts.reasoning} in loaded messages)`
                      : "No reasoning in loaded messages"
                  }
                  value="reasoning"
                >
                  <Brain />
                  <span aria-hidden="true">{visibilityCounts.reasoning}</span>
                </ToggleGroupItem>
                <ToggleGroupItem
                  aria-label="Show tool calls"
                  disabled={visibilityCounts.tools === 0}
                  title={
                    visibilityCounts.tools
                      ? `${showDetails ? "Hide" : "Show"} tool calls (${visibilityCounts.tools} in loaded messages)`
                      : "No tool calls in loaded messages"
                  }
                  value="tools"
                >
                  <TerminalSquare />
                  <span aria-hidden="true">{visibilityCounts.tools}</span>
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

            {transcriptSurface}
          </Tabs>
        )}
      </SidebarInset>
    </SidebarProvider>
  )
}

export default App
