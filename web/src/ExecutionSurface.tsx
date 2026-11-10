import { useCallback, useEffect, useMemo, useRef, useState, type RefObject } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { ExecutionComposer, type ComposerHandle, type PromptKind } from "@/ExecutionComposer"
import { ExecutionSchedules } from "@/ExecutionSchedules"
import { ExecutionQuestions } from "@/ExecutionQuestions"
import { ExecutionTranscript, type PromptRow } from "@/ExecutionTranscript"
import { useDelayedFlag } from "@/hooks/use-delayed-flag"
import { ExecutionClient, ExecutionError, actionKey, draftPromptContent, emptyDraft, hasPromptEcho, isBlankDraft, promptMethods, transcriptEntities, userEntityKeys,
  type ExecutionDraft, type ExecutionRequest, type HostConversation, type HostEntity, type HostInfo, type HostSnapshot, type HostWorkspace, type HostSchedule,
  type HostWorktree } from "@/execution"

const pairingKey = "memex-execution-pairing-v1"
function rememberedClient(): ExecutionClient | null {
  try {
    const pairing = JSON.parse(sessionStorage.getItem(pairingKey) || "null") as { token: string; hostId: string } | null
    return pairing ? new ExecutionClient(pairing.token, pairing.hostId) : null
  } catch { return null }
}

// conversation.read has no push, delta, or "unchanged" answer: every read rebuilds the full snapshot
// (transcript included) under the host lock. While a turn runs in a visible tab the poll runs at up to
// 4 Hz, backing off to twice the measured round trip (host read, transfer, parse) for long transcripts.
const idlePollMs = 1000
const activePollMs = 250
const snapshotCacheLimit = 16

type LoadedSnapshot = { conversationId: string; snapshot: HostSnapshot; issuedAt: number }
type Snapshots = { latest: LoadedSnapshot | null; cache: ReadonlyMap<string, HostSnapshot> }
type InFlight = { key: string; retry: boolean }
type OutgoingPrompt = {
  request: ExecutionRequest
  conversationId: string
  kind: PromptKind
  draft: ExecutionDraft
  state: "sending" | "accepted" | "failed"
  error?: string
  /** User rows present at dispatch, which cannot be its echo; null when nothing was loaded. */
  knownUserKeys: ReadonlySet<string> | null
  acceptedAt?: number
  /** A snapshot requested after acceptance was applied, so the host's running state covers this prompt. */
  observed: boolean
  sawRunning: boolean
}

function observedBy(prompt: OutgoingPrompt, loaded: LoadedSnapshot | null): boolean {
  return prompt.observed || (!!loaded && loaded.conversationId === prompt.conversationId && prompt.acceptedAt !== undefined && loaded.issuedAt > prompt.acceptedAt)
}

/** What a loaded snapshot says about an accepted prompt: shown by the host now, failed, or still pending. */
function settle(prompt: OutgoingPrompt, loaded: LoadedSnapshot, entities: HostEntity[]) {
  const { snapshot } = loaded
  const observed = observedBy(prompt, loaded)
  const sawRunning = prompt.sawRunning || (observed && snapshot.running)
  if (prompt.kind === "queue.add") return { done: snapshot.queue.some(entry => entry.command.id === prompt.request.id), observed, sawRunning }
  const delivery = snapshot.deliveries.find(item => item.commandId === prompt.request.id)
  if (delivery?.status === "failed") return { done: false, failure: delivery.error || "The provider did not accept this prompt.", observed, sawRunning }
  const echoed = hasPromptEcho(prompt.request.id, prompt.draft.text, prompt.knownUserKeys, snapshot, entities)
  // A turn that finished without echoing the prompt also retires it.
  const finished = observed && !snapshot.running && (sawRunning || delivery?.status === "completed")
  return { done: echoed || finished, observed, sawRunning }
}

function promptRow(prompt: OutgoingPrompt, state: PromptRow["state"], error?: string): PromptRow {
  return { commandId: prompt.request.id, kind: prompt.kind === "steer" ? "steer" : "send", text: prompt.draft.text,
    attachmentNames: prompt.draft.attachments.map(item => item.name), state, error }
}

function unconfirmedRow(request: ExecutionRequest): PromptRow {
  const attachments = Array.isArray(request.params.promptContent) ? request.params.promptContent.length - 1 : 0
  return { commandId: request.id, kind: request.method === "conversation.steer" ? "steer" : "send", text: String(request.params.text ?? ""),
    attachmentNames: attachments > 0 ? [`${attachments} file${attachments === 1 ? "" : "s"}`] : [], state: "unconfirmed" }
}

/**
 * Polls one conversation. Responses are applied in request order, unchanged answers skip state updates,
 * the cadence follows `active`, and a hidden tab does not poll.
 */
function useConversationSnapshot(client: ExecutionClient | null, conversationId: string, active: RefObject<boolean>) {
  const [snapshots, setSnapshots] = useState<Snapshots>({ latest: null, cache: new Map() })
  const [pollError, setPollError] = useState<{ conversationId: string; message: string } | null>(null)
  const loop = useRef({ kick: () => {}, retime: () => {} })

  useEffect(() => {
    if (!client || !conversationId) return
    const control = client
    let live = true
    let issued = 0
    let applied = 0
    let raw: string | undefined
    let cost = 0
    let timer: ReturnType<typeof setTimeout> | undefined
    const controller = new AbortController()
    const delay = () => active.current ? Math.min(idlePollMs, Math.max(activePollMs, 2 * cost)) : idlePollMs
    const schedule = () => {
      clearTimeout(timer)
      timer = live && document.visibilityState !== "hidden" ? setTimeout(poll, delay()) : undefined
    }
    async function poll() {
      clearTimeout(timer)
      timer = undefined
      if (!live || document.visibilityState === "hidden") return
      const sequence = ++issued
      const issuedAt = performance.now()
      try {
        const response = await control.read<HostSnapshot>("conversation.read", { conversationId }, raw, controller.signal)
        // A slower, older response must never replace a newer one.
        if (!live || sequence < applied) return
        applied = sequence
        cost = performance.now() - issuedAt
        setPollError(null)
        if (response.changed) {
          raw = response.raw
          const snapshot = response.result
          setSnapshots(current => {
            const cache = new Map(current.cache)
            cache.delete(conversationId)
            cache.set(conversationId, snapshot)
            while (cache.size > snapshotCacheLimit) cache.delete(cache.keys().next().value!)
            return { latest: { conversationId, snapshot, issuedAt }, cache }
          })
        } else if (active.current) {
          // Same snapshot, newer request: it confirms the host state after an accepted send.
          setSnapshots(current => current.latest?.conversationId === conversationId
            ? { ...current, latest: { ...current.latest, issuedAt } } : current)
        }
      } catch (failure) {
        if (live && sequence >= applied) setPollError({ conversationId, message: String(failure) })
      } finally {
        if (live && sequence === issued) schedule()
      }
    }
    const onVisibility = () => {
      if (document.visibilityState !== "hidden") void poll()
      else { clearTimeout(timer); timer = undefined }
    }
    document.addEventListener("visibilitychange", onVisibility)
    loop.current = { kick: () => void poll(), retime: () => { if (timer !== undefined) schedule() } }
    void poll()
    return () => {
      live = false
      controller.abort()
      clearTimeout(timer)
      document.removeEventListener("visibilitychange", onVisibility)
      loop.current = { kick: () => {}, retime: () => {} }
    }
  }, [client, conversationId, active])

  const kick = useCallback(() => loop.current.kick(), [])
  const retime = useCallback(() => loop.current.retime(), [])
  return { snapshots, pollError, kick, retime }
}

export function ExecutionSurface({ onBack }: { onBack: () => void }) {
  const [client, setClient] = useState(rememberedClient)
  const [token, setToken] = useState("")
  const [pairing, setPairing] = useState(false)
  const [info, setInfo] = useState<HostInfo | null>(null)
  const [conversations, setConversations] = useState<HostConversation[]>([])
  const [workspaces, setWorkspaces] = useState<HostWorkspace[]>([])
  const [worktrees, setWorktrees] = useState<HostWorktree[]>([])
  const [repository, setRepository] = useState("")
  const [baseRef, setBaseRef] = useState("")
  const [selected, setSelected] = useState("")
  const [workspace, setWorkspace] = useState("")
  const [provider, setProvider] = useState("")
  const [title, setTitle] = useState("")
  const [outbox, setOutbox] = useState<ExecutionRequest[]>([])
  const [schedules, setSchedules] = useState<HostSchedule[]>([])
  const [error, setError] = useState("")
  const [receipt, setReceipt] = useState("")
  // Requests on their way, by command ID. Each control waits only for its own action.
  const [inFlight, setInFlight] = useState<ReadonlyMap<string, InFlight>>(new Map())
  const inFlightRef = useRef(inFlight)
  const [outgoing, setOutgoing] = useState<OutgoingPrompt[]>([])
  const [promptErrors, setPromptErrors] = useState<Readonly<Record<string, string>>>({})
  const composer = useRef<ComposerHandle | null>(null)
  const activePoll = useRef(false)
  const { snapshots, pollError, kick, retime } = useConversationSnapshot(client, selected, activePoll)

  const refresh = useCallback(async (control: ExecutionClient) => {
    const [host, chats, folders, recurring] = await Promise.all([control.call<HostInfo>("host.info"), control.call<HostConversation[]>("conversation.list"),
      control.call<HostWorkspace[]>("workspace.list"), control.call<HostSchedule[]>("schedule.list")])
    if (host.hostId !== control.hostId) throw new Error("The endpoint's execution identity changed. Pair it explicitly again.")
    const trees = host.capabilities.includes("worktree.lifecycle") ? await control.call<HostWorktree[]>("worktree.list") : []
    setWorktrees(trees)
    setInfo(host); setConversations(chats); setWorkspaces(folders); setSchedules(recurring); setOutbox(control.pending())
    setWorkspace(current => folders.some(folder => folder.id === current) ? current : folders[0]?.id || "")
    setProvider(current => host.providers.includes(current) ? current : host.providers[0] || "")
    setRepository(current => folders.some(folder => folder.id === current && !folder.worktreeID) ? current : folders.find(folder => !folder.worktreeID)?.id || "")
  }, [])

  useEffect(() => {
    if (!client) return
    let active = true
    void refresh(client).catch(error => { if (active) setError(String(error)) })
    return () => { active = false }
  }, [client, refresh])

  const track = useCallback((id: string, entry: InFlight | null) => {
    const next = new Map(inFlightRef.current)
    if (entry) next.set(id, entry)
    else next.delete(id)
    inFlightRef.current = next
    setInFlight(next)
  }, [])
  const pendingKeys = useMemo(() => new Set([...inFlight.values()].map(item => item.key)), [inFlight])
  const pending = (method: string, params: Record<string, unknown> = {}) => pendingKeys.has(actionKey(method, params))

  async function operate<T>(method: string, params: Record<string, unknown> = {}, retry = false): Promise<T | undefined> {
    if (!client) return
    const key = actionKey(method, params)
    if ([...inFlightRef.current.values()].some(item => item.key === key)) return
    let request: ExecutionRequest
    try { request = client.prepare(method, params, true) }
    catch (error) { setError(String(error)); return undefined }
    track(request.id, { key, retry })
    setError("")
    try {
      const result = await client.dispatch<T>(request, true)
      // Conversation-scoped results arrive with the next snapshot; only the rest change the lists.
      if (method.startsWith("conversation.") && method !== "conversation.fork" && typeof params.conversationId === "string") kick()
      else await refresh(client)
      return result
    } catch (error) { setError(String(error)); return undefined }
    finally { track(request.id, null); setOutbox(client.pending()) }
  }

  function editDraft(conversationId: string, change: (draft: ExecutionDraft) => ExecutionDraft): boolean {
    if (composer.current) return composer.current.edit(conversationId, change)
    try { return client?.editDraft(conversationId, change) ?? false } catch { return false }
  }

  const loaded = snapshots.latest?.conversationId === selected ? snapshots.latest : null
  const loadedEntities = useMemo(() => loaded ? transcriptEntities(loaded.snapshot) : [], [loaded?.snapshot])

  async function dispatchPrompt(request: ExecutionRequest, kind: PromptKind, draft: ExecutionDraft, retry: boolean) {
    if (!client) return
    const control = client
    const id = request.id
    const conversationId = String(request.params.conversationId)
    track(id, { key: actionKey(request.method, request.params), retry })
    setOutgoing(current => [...current.filter(prompt => prompt.request.id !== id), { request, conversationId, kind, draft, state: "sending",
      knownUserKeys: loaded?.conversationId === conversationId ? userEntityKeys(loadedEntities) : null, observed: false, sawRunning: false }])
    try {
      await control.dispatch(request, true)
      const acceptedAt = performance.now()
      setOutgoing(current => current.map(prompt => prompt.request.id === id ? { ...prompt, state: "accepted", acceptedAt } : prompt))
      if (retry) {
        // The retried text leaves the composer only if it is still exactly what was sent.
        editDraft(conversationId, current => current.text === request.params.text
          && JSON.stringify(draftPromptContent(current)) === JSON.stringify(request.params.promptContent) ? emptyDraft : current)
      }
      kick()
    } catch (failure) {
      const restored = editDraft(conversationId, current => isBlankDraft(current) ? draft : current)
      if (failure instanceof ExecutionError && failure.rejected) {
        // The host did not execute it: the text goes back and the next send is free to go.
        setOutgoing(current => restored ? current.filter(prompt => prompt.request.id !== id)
          : current.map(prompt => prompt.request.id === id ? { ...prompt, state: "failed", error: failure.message } : prompt))
        setPromptErrors(current => ({ ...current, [conversationId]: `Not sent: ${failure.message}` }))
      } else {
        // Unknown outcome: the outbox keeps the exact request, shown as unconfirmed until it is resolved.
        setOutgoing(current => current.filter(prompt => prompt.request.id !== id))
        setPromptErrors(current => ({ ...current, [conversationId]: String(failure) }))
      }
    } finally {
      track(id, null)
      setOutbox(control.pending())
    }
  }

  function submitPrompt(conversationId: string, kind: PromptKind, draft: ExecutionDraft): string | null {
    if (!client) return "Pair this execution host first."
    let request: ExecutionRequest
    try {
      const promptContent = draftPromptContent(draft)
      request = client.prepare(`conversation.${kind}`, { conversationId, text: draft.text, ...(promptContent ? { promptContent } : {}) }, true)
    } catch (failure) { return `Not sent: ${String(failure)}` }
    setPromptErrors(current => {
      const { [conversationId]: _cleared, ...rest } = current
      return rest
    })
    void dispatchPrompt(request, kind, draft, false)
    return null
  }

  function retryCommand(request: ExecutionRequest) {
    if (!client) return
    if (!promptMethods.has(request.method) || typeof request.params.conversationId !== "string") {
      void operate(request.method, request.params, true)
      return
    }
    let prepared: ExecutionRequest
    try { prepared = client.prepare(request.method, request.params, true) }
    catch (failure) { setError(String(failure)); return }
    void dispatchPrompt(prepared, request.method.slice("conversation.".length) as PromptKind,
      { text: String(request.params.text ?? ""), attachments: [] }, true)
  }

  // Snapshots settle accepted prompts: retire echoed or finished ones and surface failed deliveries.
  useEffect(() => {
    if (!loaded) return
    const changes = new Map<string, Partial<OutgoingPrompt> | null>()
    const failures: { prompt: OutgoingPrompt; message: string }[] = []
    for (const prompt of outgoing) {
      if (prompt.conversationId !== loaded.conversationId || prompt.state !== "accepted") continue
      const result = settle(prompt, loaded, loadedEntities)
      if (result.done) changes.set(prompt.request.id, null)
      else if (result.failure !== undefined) failures.push({ prompt, message: result.failure })
      else if (result.observed !== prompt.observed || result.sawRunning !== prompt.sawRunning) {
        changes.set(prompt.request.id, { observed: result.observed, sawRunning: result.sawRunning })
      }
    }
    for (const { prompt, message } of failures) {
      changes.set(prompt.request.id, editDraft(prompt.conversationId, current => isBlankDraft(current) ? prompt.draft : current)
        ? null : { state: "failed", error: message })
    }
    if (failures.length) {
      setPromptErrors(current => ({ ...current, ...Object.fromEntries(failures.map(({ prompt, message }) => [prompt.conversationId, `Not delivered: ${message}`])) }))
    }
    if (!changes.size) return
    setOutgoing(current => current.flatMap(prompt => {
      const change = changes.get(prompt.request.id)
      if (change === undefined || prompt.state !== "accepted") return [prompt]
      return change === null ? [] : [{ ...prompt, ...change }]
    }))
  }, [loaded, loadedEntities, outgoing])

  // Retries of saved commands stay listed; fresh requests are not unconfirmed while they are on their way.
  const unconfirmed = useMemo(() => outbox.filter(request => {
    const flight = inFlight.get(request.id)
    return !flight || flight.retry
  }), [outbox, inFlight])

  const cached = snapshots.cache.get(selected)
  const shownFor = loaded || cached ? selected : snapshots.latest?.conversationId ?? ""
  const shownSnapshot = loaded?.snapshot ?? cached ?? snapshots.latest?.snapshot ?? null
  const loadingVisible = useDelayedFlag(!!selected && !loaded, 200)

  const transcriptPrompts = useMemo<PromptRow[]>(() => {
    if (!shownFor) return []
    const rows: PromptRow[] = []
    const listed = new Set<string>()
    for (const prompt of outgoing) {
      if (prompt.conversationId !== shownFor || prompt.kind === "queue.add") continue
      listed.add(prompt.request.id)
      if (prompt.state === "accepted" && loaded) {
        // Settled in the same render that shows the host's copy, so there is no duplicate and no gap.
        const result = settle(prompt, loaded, loadedEntities)
        if (result.done) continue
        if (result.failure !== undefined) { rows.push(promptRow(prompt, "failed", result.failure)); continue }
      }
      rows.push(promptRow(prompt, prompt.state, prompt.error))
    }
    for (const request of unconfirmed) {
      if (listed.has(request.id) || request.params.conversationId !== shownFor || !["conversation.send", "conversation.steer"].includes(request.method)) continue
      rows.push(unconfirmedRow(request))
    }
    return rows
  }, [outgoing, unconfirmed, shownFor, loaded, loadedEntities])

  const dismissPrompt = useCallback((commandId: string) => setOutgoing(current => current.filter(prompt => prompt.request.id !== commandId)), [])

  const snapshot = loaded?.snapshot ?? null
  const pendingIDs = new Set(snapshot?.presentation?.conversation?.state?.pending_interactions || [])
  const requests = snapshot?.thread?.pendingRequests.filter(request => pendingIDs.has(request.requestId)) || []
  const queued = snapshot?.queue.filter(item => ["queued", "held"].includes(item.status)) || []
  const queuedPrompts = outgoing.filter(prompt => prompt.conversationId === selected && prompt.kind === "queue.add"
    && !snapshot?.queue.some(entry => entry.command.id === prompt.request.id))
  const unconfirmedQueue = unconfirmed.filter(request => request.method === "conversation.queue.add" && request.params.conversationId === selected
    && !outgoing.some(prompt => prompt.request.id === request.id))
  // A send on its way counts as a running turn until a later snapshot covers it.
  const localTurn = outgoing.some(prompt => prompt.conversationId === selected && prompt.kind === "send"
    && (prompt.state === "sending" || (prompt.state === "accepted" && !observedBy(prompt, loaded))))
  const running = !!snapshot?.running || localTurn
  const stopping = pending("conversation.interrupt", { conversationId: selected })
  const settingsPending = !!snapshot?.controls?.pendingControlCommandIds.length
    || pending("conversation.model", { conversationId: selected }) || pending("conversation.configuration", { conversationId: selected })
  const controlsBlocked = !snapshot?.ready || running || settingsPending || requests.length > 0
  const unconfirmedPrompts = unconfirmed.filter(request => request.params.conversationId === selected && promptMethods.has(request.method))
  const queueBlockedReason = unconfirmedPrompts.length
    ? "An earlier message's delivery is unconfirmed. Inspect, retry, or mark it inspected under Unconfirmed commands." : null
  const sendBlockedReason = !snapshot ? "Loading conversation…"
    : !snapshot.connected ? "Resume this conversation on the host to send."
    : !snapshot.ready ? "The host is not ready for a prompt yet."
    : requests.length ? "Answer the pending request first."
    : settingsPending ? "Applying conversation settings…" : null
  const listed = conversations.find(chat => chat.id === selected)
  const status = !snapshot ? (loadingVisible ? "Loading…" : "") : stopping ? "Stopping…" : running ? "Working" : snapshot.ready ? "Ready" : "Disconnected"

  useEffect(() => {
    activePoll.current = running
    retime()
  }, [running, retime])

  return <main className="flex min-h-0 flex-1 flex-col overflow-auto rounded-xl border bg-background" aria-label="Execution conversations">
    <header className="flex flex-wrap items-center gap-3 border-b p-3">
      <Button variant="ghost" size="sm" onClick={onBack}>History</Button>
      <strong className="text-sm">Active conversations</strong>
      {info && <span className="min-w-0 truncate text-xs text-muted-foreground" title={info.hostId}>Host {info.hostId}</span>}
      {client && <Button className="ml-auto" variant="ghost" size="sm" onClick={() => { sessionStorage.removeItem(pairingKey); setClient(null); setInfo(null); setSelected(""); setError(""); setOutgoing([]); setPromptErrors({}) }}>Unpair this tab</Button>}
    </header>
    {error && <div className="m-3 rounded-md border border-destructive p-3 text-sm text-destructive" role="alert">{error}</div>}
    {!client ? <form className="mx-auto w-full max-w-lg space-y-4 p-5" onSubmit={event => {
      event.preventDefault(); setPairing(true); setError("")
      void new ExecutionClient(token.trim()).call<HostInfo>("host.info").then(host => {
        sessionStorage.setItem(pairingKey, JSON.stringify({ token: token.trim(), hostId: host.hostId }))
        setClient(new ExecutionClient(token.trim(), host.hostId)); setToken("")
      }).catch(error => setError(String(error))).finally(() => setPairing(false))
    }}>
      <h1 className="text-lg font-medium">Pair this execution host</h1>
      <p className="text-sm text-muted-foreground">Use the execution pairing token from this host. Agents continue running when you close the browser. The token is retained only for this browser tab; outgoing commands and drafts are saved locally.</p>
      <label className="block text-sm">Execution pairing token<Input aria-label="Execution pairing token" className="mt-1" type="password" autoComplete="off" value={token} onChange={event => setToken(event.target.value)} /></label>
      <Button type="submit" disabled={pairing || !token.trim()}>Pair host</Button>
    </form> : <div className="grid min-h-0 flex-1 md:grid-cols-[17rem_minmax(0,1fr)]">
      <aside className="space-y-4 border-b p-3 md:overflow-auto md:border-r md:border-b-0">
        <label className="block text-sm">Conversation<select className="mt-1 w-full rounded-md border bg-background p-2" aria-label="Active conversation" value={selected} onChange={event => setSelected(event.target.value)}>
          <option value="">Choose a conversation</option>{conversations.map(chat => <option key={chat.id} value={chat.id}>{chat.title || chat.nativeSessionID} · {chat.provider}</option>)}
        </select></label>
        <details className="rounded-md border p-3" open={!selected}>
          <summary className="text-sm font-medium">New conversation</summary>
          <div className="mt-3 space-y-3">
            <label className="block text-sm">Workspace<select aria-label="Execution workspace" className="mt-1 w-full rounded-md border bg-background p-2" value={workspace} onChange={event => setWorkspace(event.target.value)}>{workspaces.map(item => <option key={item.id} value={item.id}>{item.path}</option>)}</select></label>
            <label className="block text-sm">Provider<select aria-label="Execution provider" className="mt-1 w-full rounded-md border bg-background p-2" value={provider} onChange={event => setProvider(event.target.value)}>{info?.providers.map(item => <option key={item}>{item}</option>)}</select></label>
            <Input aria-label="Conversation title" placeholder="Conversation title" value={title} onChange={event => setTitle(event.target.value)} />
            <Button disabled={!workspace || !provider || pending("conversation.create", { workspaceId: workspace })} onClick={async () => {
              const result = await operate<{ conversation: HostConversation }>("conversation.create", { workspaceId: workspace, provider, title: title || "New conversation" })
              if (result) setSelected(result.conversation.id)
            }}>Create conversation</Button>
            {info && !workspaces.length && <p className="text-xs text-muted-foreground">Start the execution host with a registered workspace.</p>}
          </div>
        </details>
        {info?.capabilities.includes("worktree.lifecycle") && <details className="rounded-md border p-3">
          <summary className="text-sm font-medium">Managed worktrees</summary>
          <div className="mt-3 space-y-3">
            <p className="text-xs text-muted-foreground">Create an isolated branch from a registered repository. Archive retains all files; cleanup requires no retained chat references and no dirty or ignored files.</p>
            <label className="block text-xs">Repository<select aria-label="Worktree repository" className="mt-1 w-full rounded border bg-background p-2" value={repository} onChange={event => setRepository(event.target.value)}>
              {workspaces.filter(folder => !folder.worktreeID).map(folder => <option key={folder.id} value={folder.id}>{folder.path}</option>)}
            </select></label>
            <Input aria-label="Worktree base ref" placeholder="Base ref (blank uses recorded default)" value={baseRef} onChange={event => setBaseRef(event.target.value)} />
            <Button size="sm" disabled={!repository || pending("worktree.create", { workspaceId: repository })} onClick={async () => {
              const created = await operate<HostWorktree>("worktree.create", { workspaceId: repository, ...(baseRef.trim() ? { baseRef: baseRef.trim() } : {}) })
              if (created) setWorkspace(created.workspaceId)
            }}>Create worktree</Button>
            {worktrees.map(tree => <div key={tree.id} className="space-y-2 border-t pt-2 text-xs">
              <p className="break-all">{tree.branch || tree.id}</p><p className="break-all text-muted-foreground">{tree.path}</p>
              <p>{tree.removed ? "Checkout removed" : tree.archived ? "Archived · files retained" : tree.state}{tree.referencedBy.length ? ` · ${tree.referencedBy.length} chat references` : ""}</p>
              {tree.failure && <p className="text-destructive">{tree.failure}</p>}
              <div className="flex flex-wrap gap-1">
                <Button size="sm" variant="ghost" disabled={tree.state !== "ready" || pending(tree.removed ? "worktree.reattach" : "worktree.archive", { worktreeId: tree.id })} onClick={() => void operate(tree.removed ? "worktree.reattach" : "worktree.archive", tree.removed ? { worktreeId: tree.id } : { worktreeId: tree.id, archived: !tree.archived })}>{tree.removed ? "Reattach" : tree.archived ? "Unarchive" : "Archive"}</Button>
                {!tree.removed && <Button size="sm" variant="ghost" disabled={tree.state !== "ready" || !!tree.referencedBy.length || pending("worktree.cleanup", { worktreeId: tree.id })} onClick={() => {
                  if (window.confirm("Remove this unused, clean checkout? Dirty and ignored files block removal. Its branch and commits remain for reattachment.")) void operate("worktree.cleanup", { worktreeId: tree.id })
                }}>Remove clean checkout</Button>}
              </div>
            </div>)}
          </div>
        </details>}
        {info && <ExecutionSchedules key={info.hostId} client={client} info={info} schedules={schedules} conversations={conversations} workspaces={workspaces} selected={selected} isPending={pending} operate={operate} openConversation={setSelected} />}
        {!!unconfirmed.length && <details className="rounded-md border p-3" open><summary className="text-sm font-medium">Unconfirmed commands ({unconfirmed.length})</summary>
          {unconfirmed.map(request => <div key={request.id} className="mt-3 space-y-2 text-xs">
            <p>{request.method}</p><code className="break-all">{request.id}</code>
            <Button size="sm" variant="outline" onClick={() => void client.call<{ status: string; error?: string }>("command.read", { commandId: request.id }).then(result => setReceipt(`${result.status}: ${result.error || "Recorded by host"}`)).catch(error => setReceipt(String(error)))}>Inspect receipt</Button>
            <Button size="sm" variant="outline" disabled={inFlight.has(request.id)} onClick={() => retryCommand(request)}>Retry exact command</Button>
            <Button size="sm" variant="ghost" disabled={inFlight.has(request.id)} onClick={() => {
              if (window.confirm("Have you checked the native conversation and resolved whether this command executed? This clears only the local pending marker and keeps the saved command for inspection.")) {
                client.resolve(request); setOutbox(client.pending())
              }
            }}>Mark inspected</Button>
          </div>)}{receipt && <p className="mt-3 break-words text-xs">{receipt}</p>}
        </details>}
      </aside>
      <section className="flex min-h-0 min-w-0 flex-col">
        {!selected ? <p className="p-6 text-sm text-muted-foreground">Create or choose a conversation. Model, permissions, and attachments are available before its first send.</p> : <>
          <div className="flex flex-wrap items-center gap-2 border-b p-3">
            <strong className="mr-auto text-sm">{snapshot?.conversation.title || listed?.title || listed?.nativeSessionID || ""}</strong>
            <span className="text-xs text-muted-foreground">{status}</span>
            {snapshot && !snapshot.connected && <Button size="sm" disabled={pending("conversation.resume", { conversationId: selected })} onClick={() => void operate("conversation.resume", { conversationId: selected })}>Resume on host</Button>}
            <Button size="sm" variant="outline" disabled={stopping || !snapshot?.actions.includes("cancel") || !snapshot?.running} onClick={() => void operate("conversation.interrupt", { conversationId: selected })}>{stopping ? "Stopping…" : "Stop"}</Button>
            <Button size="sm" variant="ghost" disabled={!snapshot?.ready || pending("conversation.fork", { conversationId: selected })} onClick={async () => {
              const result = await operate<{ conversation: HostConversation }>("conversation.fork", { conversationId: selected, title: `Fork: ${snapshot?.conversation.title || "Conversation"}` })
              if (result) setSelected(result.conversation.id)
            }}>Fork with context</Button>
            {pollError?.conversationId === selected && <p className="w-full text-xs text-destructive" role="alert">Could not refresh this conversation: {pollError.message}</p>}
          </div>
          <ExecutionTranscript conversationKey={shownFor} snapshot={shownSnapshot} prompts={transcriptPrompts}
            working={!!snapshot && running && !stopping} stale={shownFor !== selected && loadingVisible} onDismiss={dismissPrompt} />
          <div className="space-y-3 border-t p-3 md:p-4">
            {requests.filter(request => request.kind === "approval").map(request => <div key={request.requestId} className="rounded-lg border p-3">
              <p className="text-sm font-medium">{request.payload.title || "Approval required"}</p>
              {request.payload.rawInputJSON && <pre className="my-2 max-h-40 overflow-auto whitespace-pre-wrap text-xs">{request.payload.rawInputJSON}</pre>}
              <div className="flex flex-wrap gap-2">{request.payload.options?.map(option => <Button key={option.id} size="sm" variant="outline" disabled={!snapshot?.connected || pending("conversation.approval", { conversationId: selected, requestId: request.requestId }) || unconfirmed.some(item => item.params.conversationId === selected && item.method === "conversation.approval" && item.params.requestId === request.requestId)} onClick={() => void operate("conversation.approval", { conversationId: selected, requestId: request.requestId, text: option.id })}>{option.name}</Button>)}</div>
            </div>)}
            <ExecutionQuestions key={selected} requests={requests.filter(request => request.kind !== "approval")}
              disabled={!snapshot?.connected}
              blockedRequestIDs={new Set(unconfirmed.filter(request => request.params.conversationId === selected && request.method === "conversation.userInput").map(request => String(request.params.requestId)))}
              onAnswer={async (requestId, text) => (await operate("conversation.userInput", { conversationId: selected, requestId, text })) !== undefined} />
            {snapshot && (!!snapshot.queue.length || !!queuedPrompts.length || !!unconfirmedQueue.length) && <details className="rounded-md border p-2" open><summary className="text-sm">Queue{snapshot.queueHeld ? " · held" : ""}</summary>
              {snapshot.queue.filter(entry => entry.status !== "cancelled").map(entry => <div key={entry.command.id} className="mt-2 flex flex-wrap items-center gap-1 text-xs">
                <span className="mr-auto max-w-full break-words">{entry.command.text} · {entry.status}{entry.error ? ` · ${entry.error}` : ""}</span>
                {["queued", "held"].includes(entry.status) && <>
                  <Button size="sm" variant="ghost" disabled={pending("conversation.queue.edit", { conversationId: selected, queuedCommandId: entry.command.id })} onClick={() => { const text = window.prompt("Edit queued prompt", entry.command.text); if (text !== null) void operate("conversation.queue.edit", { conversationId: selected, queuedCommandId: entry.command.id, text }) }}>Edit</Button>
                  <Button size="sm" variant="ghost" disabled={queued[0]?.command.id === entry.command.id || pending("conversation.queue.reorder", { conversationId: selected })} onClick={() => void operate("conversation.queue.reorder", { conversationId: selected, commandIds: [entry.command.id, ...queued.filter(item => item.command.id !== entry.command.id).map(item => item.command.id)] })}>Move first</Button>
                  <Button size="sm" variant="ghost" disabled={!snapshot.running || !snapshot.actions.includes("steer") || pending("conversation.queue.promote", { conversationId: selected, queuedCommandId: entry.command.id })} onClick={() => void operate("conversation.queue.promote", { conversationId: selected, queuedCommandId: entry.command.id })}>Steer</Button>
                  <Button size="sm" variant="ghost" disabled={pending("conversation.queue.cancel", { conversationId: selected, queuedCommandId: entry.command.id })} onClick={() => void operate("conversation.queue.cancel", { conversationId: selected, queuedCommandId: entry.command.id })}>Cancel</Button>
                </>}
              </div>)}
              {queuedPrompts.map(prompt => <div key={prompt.request.id} className="mt-2 flex flex-wrap items-center gap-1 text-xs">
                <span className="mr-auto max-w-full break-words">{prompt.draft.text} · {prompt.state === "failed" ? `not queued · ${prompt.error}` : prompt.state === "sending" ? "adding" : "queued"}</span>
                {prompt.state === "failed" && <Button size="sm" variant="ghost" onClick={() => dismissPrompt(prompt.request.id)}>Dismiss</Button>}
              </div>)}
              {unconfirmedQueue.map(request => <div key={request.id} className="mt-2 text-xs">
                <span className="max-w-full break-words">{String(request.params.text ?? "")} · delivery unconfirmed</span>
              </div>)}
              {snapshot.queueHeld && <Button className="mt-2" size="sm" disabled={pending("conversation.queue.resume", { conversationId: selected })} onClick={() => void operate("conversation.queue.resume", { conversationId: selected })}>Resume undispatched queue</Button>}
            </details>}
            <div className="flex flex-wrap items-end gap-2">
              {!!snapshot?.controls?.models.length && <label className="text-xs">Model<select aria-label="Execution model" className="ml-2 rounded border bg-background p-1" disabled={controlsBlocked} value={snapshot.controls.selectedModelId || ""} onChange={event => void operate("conversation.model", { conversationId: selected, text: event.target.value })}>
                <option value="" disabled>Provider default</option>{snapshot.controls.models.map(model => <option key={model.id} value={model.id}>{model.name}</option>)}
              </select></label>}
              {snapshot?.controls?.configOptions.map(option => <label key={option.id} className="text-xs">{option.name}<select aria-label={option.name} className="ml-2 rounded border bg-background p-1" disabled={controlsBlocked} value={option.currentValue || ""} onChange={event => void operate("conversation.configuration", { conversationId: selected, optionId: option.id, text: event.target.value })}>
                <option value="" disabled>Provider default</option>{option.choices.map(choice => <option key={choice.value} value={choice.value}>{choice.name}</option>)}
              </select></label>)}
            </div>
            <ExecutionComposer client={client} conversationId={selected} handleRef={composer} promptCapabilities={snapshot?.controls?.promptCapabilities}
              running={running} canSteer={!!snapshot?.running && snapshot.actions.includes("steer")}
              sendBlockedReason={sendBlockedReason} quietReason={!snapshot && !loadingVisible} queueBlockedReason={queueBlockedReason} error={promptErrors[selected] || ""} onSubmit={submitPrompt} />
          </div>
        </>}
      </section>
    </div>}
  </main>
}
