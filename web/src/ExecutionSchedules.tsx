import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import type { ExecutionClient, HostInfo, HostSchedule, HostScheduleRun, HostConversation, HostWorkspace } from "@/execution"

const weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
const selectStyle = "mt-1 w-full rounded border bg-background p-2"
type Props = {
  client: ExecutionClient; info: HostInfo; schedules: HostSchedule[]; conversations: HostConversation[]; workspaces: HostWorkspace[];
  selected: string; busy: boolean; operate: <T>(method: string, params?: Record<string, unknown>) => Promise<T | undefined>;
  openConversation: (id: string) => void;
}

export function ExecutionSchedules({ client, info, schedules, conversations, workspaces, selected, busy, operate, openConversation }: Props) {
  const [id, setId] = useState(() => crypto.randomUUID() as string)
  const [text, setText] = useState("")
  const [conversation, setConversation] = useState("")
  const [fresh, setFresh] = useState(false)
  const [workspace, setWorkspace] = useState("")
  const [provider, setProvider] = useState("")
  const [title, setTitle] = useState("Scheduled conversation")
  const [kind, setKind] = useState("interval")
  const [interval, setInterval] = useState(3600)
  const [localTime, setLocalTime] = useState("09:00")
  const [days, setDays] = useState([1, 2, 3, 4, 5])
  const [timeZone, setTimeZone] = useState(() => Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC")
  const [eventName, setEventName] = useState("")
  const [notificationPolicy, setNotificationPolicy] = useState("attention")
  const [runs, setRuns] = useState<HostScheduleRun[]>([])
  const [runError, setRunError] = useState("")
  const supportsRuns = info.capabilities.includes("schedules.runs")
  const supportsFresh = info.capabilities.includes("schedules.new_conversation")
  const supportsEvents = info.capabilities.includes("schedules.events")
  const supportsClock = info.capabilities.includes("schedules.wall_clock")

  useEffect(() => {
    if (!supportsRuns) return
    let active = true
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout>
    async function poll() {
      try {
        const result = await client.call<HostScheduleRun[]>("schedule.runs", {}, false, controller.signal)
        if (active) { setRuns(result); setRunError("") }
      } catch (error) { if (active) setRunError(String(error)) }
      if (active) timer = setTimeout(poll, 5000)
    }
    void poll()
    return () => { active = false; controller.abort(); clearTimeout(timer) }
  }, [client, supportsRuns, schedules])

  function edit(schedule: HostSchedule) {
    setId(schedule.id); setText(schedule.prompt); setConversation(schedule.conversationID)
    setFresh(!!schedule.newConversation); setWorkspace(schedule.newConversation?.workspaceID || "")
    setProvider(schedule.newConversation?.provider || ""); setTitle(schedule.newConversation?.title || "Scheduled conversation")
    setKind(schedule.eventName ? "event" : schedule.wallClock ? "wallClock" : "interval")
    setInterval(schedule.intervalSeconds || 3600); setEventName(schedule.eventName || ""); setNotificationPolicy(schedule.notificationPolicy || "attention")
    if (schedule.wallClock) { setLocalTime(schedule.wallClock.localTime); setDays(schedule.wallClock.weekdays); setTimeZone(schedule.wallClock.timeZone) }
  }
  const validTarget = fresh ? supportsFresh && workspaces.some(item => item.id === workspace) && info.providers.includes(provider) : conversations.some(item => item.id === (conversation || selected))
  const validTrigger = kind === "event" ? supportsEvents && !!eventName.trim() : kind === "wallClock" ? supportsClock && !!localTime && !!timeZone.trim() && !!days.length : Number.isFinite(interval) && interval >= 60
  async function save() {
    const target = fresh ? { newConversation: { workspaceId: workspace, provider, title } } : { conversationId: conversation || selected }
    const trigger = kind === "event" ? { eventName } : kind === "wallClock" ? { wallClock: { localTime, weekdays: days, timeZone } } : { intervalSeconds: interval }
    if (await operate("schedule.upsert", { scheduleId: id, text, ...target, ...trigger, ...(supportsRuns ? { notificationPolicy } : {}) })) {
      setId(crypto.randomUUID()); setText("")
    }
  }
  return <>
    <details className="rounded-md border p-3">
      <summary className="text-sm font-medium">Schedules</summary>
      <div className="mt-3 space-y-3">
        {schedules.map(schedule => <div key={schedule.id} className="space-y-1 border-b pb-2 text-xs">
          <p className="line-clamp-3">{schedule.prompt}</p>
          <p className="text-muted-foreground">{schedule.paused ? "Paused" : schedule.eventName ? `Event: ${schedule.eventName}` : `Next: ${new Date(schedule.nextRunAt).toLocaleString()}`} · {schedule.newConversation ? "New conversation each run" : "Existing conversation"}</p>
          {schedule.lastError && <p className="text-destructive">{schedule.lastError}</p>}
          <div className="flex flex-wrap gap-1">
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => edit(schedule)}>Edit</Button>
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => void operate("schedule.pause", { scheduleId: schedule.id, paused: !schedule.paused })}>{schedule.paused ? "Resume" : "Pause"}</Button>
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => void operate("schedule.run", { scheduleId: schedule.id })}>Run now</Button>
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => void operate("schedule.delete", { scheduleId: schedule.id })}>Delete</Button>
          </div>
        </div>)}
        {supportsFresh && <label className="block text-xs">Run in<select aria-label="Schedule target" className={selectStyle} value={fresh ? "new" : "existing"} onChange={event => setFresh(event.target.value === "new")}>
          <option value="existing">Existing conversation</option><option value="new">New conversation each run</option>
        </select></label>}
        {fresh ? <>
          <label className="block text-xs">Workspace<select aria-label="Schedule workspace" className={selectStyle} value={workspace} onChange={event => setWorkspace(event.target.value)}>
            <option value="">Choose a registered folder</option>{workspaces.map(item => <option key={item.id} value={item.id}>{item.path}</option>)}
          </select></label>
          <label className="block text-xs">Provider<select aria-label="Schedule provider" className={selectStyle} value={provider} onChange={event => setProvider(event.target.value)}>
            <option value="">Choose provider</option>{info.providers.map(item => <option key={item} value={item}>{item}</option>)}
          </select></label>
          <Input aria-label="Run conversation title" value={title} onChange={event => setTitle(event.target.value)} />
        </> : <label className="block text-xs">Conversation<select aria-label="Schedule conversation" className={selectStyle} value={conversation || selected} onChange={event => setConversation(event.target.value)}>
          <option value="">Choose a conversation</option>{conversations.map(item => <option key={item.id} value={item.id}>{item.title}</option>)}
        </select></label>}
        <Textarea aria-label="Scheduled prompt" placeholder="Scheduled prompt" value={text} onChange={event => setText(event.target.value)} />
        <label className="block text-xs">Trigger<select aria-label="Schedule recurrence" className={selectStyle} value={kind} onChange={event => setKind(event.target.value)}>
          <option value="interval">Fixed interval</option>{supportsClock && <option value="wallClock">Local time and weekdays</option>}{supportsEvents && <option value="event">Named event</option>}
        </select></label>
        {kind === "event" ? <>
          <Input aria-label="Schedule event name" placeholder="Exact event name" value={eventName} onChange={event => setEventName(event.target.value)} />
          <p className="text-xs text-muted-foreground">Runs when an authenticated integration sends this exact event name to schedule.event.</p>
        </> : kind === "interval" ? <label className="block text-xs">Repeat every (seconds)<Input aria-label="Schedule interval in seconds" type="number" min={60} value={interval} onChange={event => setInterval(Number(event.target.value))} /></label> : <>
          <label className="block text-xs">Local time<Input aria-label="Schedule local time" type="time" value={localTime} onChange={event => setLocalTime(event.target.value)} /></label>
          <label className="block text-xs">Time zone<Input aria-label="Schedule time zone" value={timeZone} onChange={event => setTimeZone(event.target.value)} /></label>
          <fieldset className="flex flex-wrap gap-2"><legend className="mb-1 text-xs">Weekdays</legend>{weekdays.map((day, index) => <label key={day} className="flex items-center gap-1 text-xs">
            <input type="checkbox" aria-label={day} checked={days.includes(index + 1)} onChange={() => setDays(current => current.includes(index + 1) ? current.filter(value => value !== index + 1) : [...current, index + 1].sort())} />{day.slice(0, 3)}
          </label>)}</fieldset>
          <p className="text-xs text-muted-foreground">Daylight saving gaps and occurrences missed by more than a minute are skipped. Repeated times run once.</p>
        </>}
        {supportsRuns && <label className="block text-xs">Run notifications<select aria-label="Schedule notification policy" className={selectStyle} value={notificationPolicy} onChange={event => setNotificationPolicy(event.target.value)}>
          <option value="all">Completions and attention</option><option value="attention">Only when attention is needed</option><option value="never">Never</option>
        </select><span className="text-muted-foreground">Controls attention flags in the run inbox. System alerts depend on device notification settings.</span></label>}
        <Button size="sm" disabled={busy || !text.trim() || !validTarget || !validTrigger} onClick={() => void save()}>Save schedule</Button>
        <Button size="sm" variant="ghost" disabled={busy} onClick={() => { setId(crypto.randomUUID()); setText("") }}>New schedule</Button>
      </div>
    </details>
    {supportsRuns && <details className="rounded-md border p-3">
      <summary className="text-sm font-medium">Schedule run inbox ({runs.filter(run => !run.read).length} unread)</summary>
      <div className="mt-3 space-y-3">
        {runError && <p className="text-xs text-destructive">{runError}</p>}
        {!runs.length && <p className="text-xs text-muted-foreground">No runs yet.</p>}
        {runs.map(run => <article key={run.id} className="space-y-1 border-b pb-2 text-xs" aria-label={`Schedule run ${run.id}`}>
          <p className={run.read ? "" : "font-semibold"}>{run.status} · {new Date(run.createdAt).toLocaleString()}</p>
          {run.needsAttention && <p>Needs attention</p>}{run.error && <p className="text-destructive">{run.error}</p>}
          <div className="flex flex-wrap gap-1">
            {run.conversationID && <Button size="sm" variant="ghost" onClick={() => openConversation(run.conversationID!)}>Open run conversation</Button>}
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => void operate("schedule.run.read", { runId: run.id, read: !run.read })}>{run.read ? "Mark unread" : "Mark read"}</Button>
          </div>
        </article>)}
      </div>
    </details>}
  </>
}
