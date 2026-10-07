import { useCallback, useEffect, useRef, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { MessageContent } from "@/MessageContent"
import { ExecutionSchedules } from "@/ExecutionSchedules"
import { ExecutionQuestions } from "@/ExecutionQuestions"
import { ExecutionClient, captureExecutionAttachment, type ExecutionDraft, type ExecutionRequest, type HostConversation,
  type HostInfo, type HostSnapshot, type HostWorkspace, type HostSchedule, type HostEntity, type HostWorktree } from "@/execution"

const pairingKey = "memex-execution-pairing-v1"
function rememberedClient(): ExecutionClient | null {
  try {
    const pairing = JSON.parse(sessionStorage.getItem(pairingKey) || "null") as { token: string; hostId: string } | null
    return pairing ? new ExecutionClient(pairing.token, pairing.hostId) : null
  } catch { return null }
}

function entityText(entity: HostEntity): string {
  return (entity.body.data.parts || []).map(part => {
    if (typeof part.data === "string") return part.data
    return JSON.stringify(part.data, null, 2)
  }).join("\n")
}

function LiveMessages({ snapshot }: { snapshot: HostSnapshot }) {
  const conversation = snapshot.presentation?.conversation
  const saved = conversation?.presentation || []
  const live = [...(conversation?.ephemeral || [])].sort((a, b) => (a.source_order || 0) - (b.source_order || 0))
  // An exact native message identity supersedes its saved projection. Similar text
  // is never a reason to remove another message.
  const liveNativeIDs = new Set(live.map(item => item.body.data.native_message_id).filter(Boolean))
  const entities = [...saved.filter(item => !item.body.data.native_message_id || !liveNativeIDs.has(item.body.data.native_message_id)), ...live]
  const visible = entities.filter(item => ["message", "tool_invocation", "tool_result", "context_boundary"].includes(item.body.kind))
  if (!visible.length) return <p className="text-sm text-muted-foreground">The host is ready for a prompt. Configure this conversation before sending.</p>
  return <div className="space-y-5">{visible.map((entity, index) => {
    const data = entity.body.data
    const role = entity.body.kind === "tool_invocation" ? "tool_use" : entity.body.kind === "tool_result" ? "tool_result" : data.role || entity.body.kind
    const text = entity.body.kind === "tool_invocation" ? JSON.stringify({ name: data.name, arguments: data.raw_arguments }) : entityText(entity)
    return <article className="min-w-0 rounded-lg border p-3" key={entity.item_id || entity.entity_id || `unknown-${index}`}>
      <div className="mb-2 text-xs font-medium text-muted-foreground">{role}{data.name ? ` · ${data.name}` : ""}{data.status ? ` · ${data.status}` : ""}</div>
      <MessageContent message={{ record_id: entity.item_id || entity.entity_id || String(index), role, content: text, ts: 0 }} />
      <details className="mt-2 text-xs text-muted-foreground"><summary>Source details</summary><pre className="overflow-auto whitespace-pre-wrap">{JSON.stringify(entity, null, 2)}</pre></details>
    </article>
  })}</div>
}

export function ExecutionSurface({ onBack }: { onBack: () => void }) {
  const [client, setClient] = useState(rememberedClient)
  const [token, setToken] = useState("")
  const [info, setInfo] = useState<HostInfo | null>(null)
  const [conversations, setConversations] = useState<HostConversation[]>([])
  const [workspaces, setWorkspaces] = useState<HostWorkspace[]>([])
  const [worktrees, setWorktrees] = useState<HostWorktree[]>([])
  const [repository, setRepository] = useState("")
  const [baseRef, setBaseRef] = useState("")
  const [selected, setSelected] = useState("")
  const [snapshot, setSnapshot] = useState<HostSnapshot | null>(null)
  const [workspace, setWorkspace] = useState("")
  const [provider, setProvider] = useState("")
  const [title, setTitle] = useState("")
  const [draft, setDraft] = useState<ExecutionDraft>({ text: "", attachments: [] })
  const [outbox, setOutbox] = useState<ExecutionRequest[]>([])
  const [schedules, setSchedules] = useState<HostSchedule[]>([])
  const [error, setError] = useState("")
  const [receipt, setReceipt] = useState("")
  const [busy, setBusy] = useState(false)
  const operationActive = useRef(false)
  const selectedRef = useRef(selected)
  selectedRef.current = selected

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

  useEffect(() => {
    setSnapshot(null)
    if (!client || !selected) return
    try { setDraft(client.draft(selected)) } catch (error) { setError(`Saved draft could not be read: ${String(error)}`) }
    let active = true
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout>
    const poll = async () => {
      try {
        const next = await client.call<HostSnapshot>("conversation.read", { conversationId: selected }, false, controller.signal)
        if (active) setSnapshot(next)
      } catch (error) { if (active) setError(String(error)) }
      if (active) timer = setTimeout(poll, 1000)
    }
    void poll()
    return () => { active = false; controller.abort(); clearTimeout(timer) }
  }, [client, selected])

  async function operate<T>(method: string, params: Record<string, unknown> = {}): Promise<T | undefined> {
    if (!client || operationActive.current) return
    operationActive.current = true; setBusy(true); setError("")
    try {
      const result = await client.call<T>(method, params, true)
      await refresh(client)
      return result
    } catch (error) { setError(String(error)); return undefined }
    finally { setOutbox(client.pending()); operationActive.current = false; setBusy(false) }
  }

  function saveDraft(next: ExecutionDraft) {
    if (!client || !selected) return
    try { client.saveDraft(selected, next); setDraft(next) }
    catch (error) { setError(`Draft could not be saved; nothing was sent. ${String(error)}`) }
  }

  async function send(kind: "send" | "steer" | "queue.add") {
    if (!selected || (!draft.text.trim() && !draft.attachments.length)) return
    const conversationId = selected
    const outgoing = draft
    const result = await operate(`conversation.${kind}`, { conversationId, text: outgoing.text,
      ...(outgoing.attachments.length ? { promptContent: [{ type: "text", text: outgoing.text }, ...outgoing.attachments.map(item => item.block)] } : {}) })
    if (result !== undefined) {
      if (client && JSON.stringify(client.draft(conversationId)) === JSON.stringify(outgoing)) {
        client.saveDraft(conversationId, { text: "", attachments: [] })
        if (selectedRef.current === conversationId) setDraft(current => JSON.stringify(current) === JSON.stringify(outgoing) ? { text: "", attachments: [] } : current)
      }
    }
  }

  const pendingForChat = outbox.filter(request => request.params.conversationId === selected && ["conversation.send", "conversation.steer", "conversation.queue.add"].includes(request.method))
  const pendingIDs = new Set(snapshot?.presentation?.conversation?.state?.pending_interactions || [])
  const requests = snapshot?.thread?.pendingRequests.filter(request => pendingIDs.has(request.requestId)) || []
  const queued = snapshot?.queue.filter(item => ["queued", "held"].includes(item.status)) || []
  const controlsBlocked = busy || !snapshot?.ready || !!snapshot?.running || !!snapshot?.controls?.pendingControlCommandIds.length || requests.length > 0

  return <main className="flex min-h-0 flex-1 flex-col overflow-auto rounded-xl border bg-background" aria-label="Execution conversations">
    <header className="flex flex-wrap items-center gap-3 border-b p-3">
      <Button variant="ghost" size="sm" onClick={onBack}>History</Button>
      <strong className="text-sm">Active conversations</strong>
      {info && <span className="min-w-0 truncate text-xs text-muted-foreground" title={info.hostId}>Host {info.hostId}</span>}
      {client && <Button className="ml-auto" variant="ghost" size="sm" onClick={() => { sessionStorage.removeItem(pairingKey); setClient(null); setInfo(null); setSelected(""); setError("") }}>Unpair this tab</Button>}
    </header>
    {error && <div className="m-3 rounded-md border border-destructive p-3 text-sm text-destructive" role="alert">{error}</div>}
    {!client ? <form className="mx-auto w-full max-w-lg space-y-4 p-5" onSubmit={event => {
      event.preventDefault(); setBusy(true); setError("")
      void new ExecutionClient(token.trim()).call<HostInfo>("host.info").then(host => {
        sessionStorage.setItem(pairingKey, JSON.stringify({ token: token.trim(), hostId: host.hostId }))
        setClient(new ExecutionClient(token.trim(), host.hostId)); setToken("")
      }).catch(error => setError(String(error))).finally(() => setBusy(false))
    }}>
      <h1 className="text-lg font-medium">Pair this execution host</h1>
      <p className="text-sm text-muted-foreground">Use the execution pairing token from this host. Agents continue running when you close the browser. The token is retained only for this browser tab; outgoing commands and drafts are saved locally.</p>
      <label className="block text-sm">Execution pairing token<Input aria-label="Execution pairing token" className="mt-1" type="password" autoComplete="off" value={token} onChange={event => setToken(event.target.value)} /></label>
      <Button type="submit" disabled={busy || !token.trim()}>Pair host</Button>
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
            <Button disabled={busy || !workspace || !provider} onClick={async () => {
              const result = await operate<{ conversation: HostConversation }>("conversation.create", { workspaceId: workspace, provider, title: title || "New conversation" })
              if (result) setSelected(result.conversation.id)
            }}>Create conversation</Button>
            {!workspaces.length && <p className="text-xs text-muted-foreground">Start the execution host with a registered workspace.</p>}
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
            <Button size="sm" disabled={busy || !repository} onClick={async () => {
              const created = await operate<HostWorktree>("worktree.create", { workspaceId: repository, ...(baseRef.trim() ? { baseRef: baseRef.trim() } : {}) })
              if (created) setWorkspace(created.workspaceId)
            }}>Create worktree</Button>
            {worktrees.map(tree => <div key={tree.id} className="space-y-2 border-t pt-2 text-xs">
              <p className="break-all">{tree.branch || tree.id}</p><p className="break-all text-muted-foreground">{tree.path}</p>
              <p>{tree.removed ? "Checkout removed" : tree.archived ? "Archived · files retained" : tree.state}{tree.referencedBy.length ? ` · ${tree.referencedBy.length} chat references` : ""}</p>
              {tree.failure && <p className="text-destructive">{tree.failure}</p>}
              <div className="flex flex-wrap gap-1">
                <Button size="sm" variant="ghost" disabled={busy || tree.state !== "ready"} onClick={() => void operate(tree.removed ? "worktree.reattach" : "worktree.archive", tree.removed ? { worktreeId: tree.id } : { worktreeId: tree.id, archived: !tree.archived })}>{tree.removed ? "Reattach" : tree.archived ? "Unarchive" : "Archive"}</Button>
                {!tree.removed && <Button size="sm" variant="ghost" disabled={busy || tree.state !== "ready" || !!tree.referencedBy.length} onClick={() => {
                  if (window.confirm("Remove this unused, clean checkout? Dirty and ignored files block removal. Its branch and commits remain for reattachment.")) void operate("worktree.cleanup", { worktreeId: tree.id })
                }}>Remove clean checkout</Button>}
              </div>
            </div>)}
          </div>
        </details>}
        {info && <ExecutionSchedules key={info.hostId} client={client} info={info} schedules={schedules} conversations={conversations} workspaces={workspaces} selected={selected} busy={busy} operate={operate} openConversation={setSelected} />}
        {!!outbox.length && <details className="rounded-md border p-3" open><summary className="text-sm font-medium">Unconfirmed commands ({outbox.length})</summary>
          {outbox.map(request => <div key={request.id} className="mt-3 space-y-2 text-xs">
            <p>{request.method}</p><code className="break-all">{request.id}</code>
            <Button size="sm" variant="outline" disabled={busy} onClick={() => void client.call<{ status: string; error?: string }>("command.read", { commandId: request.id }).then(result => setReceipt(`${result.status}: ${result.error || "Recorded by host"}`)).catch(error => setReceipt(String(error)))}>Inspect receipt</Button>
            <Button size="sm" variant="outline" disabled={busy} onClick={async () => {
              const result = await operate(request.method, request.params)
              if (result !== undefined && ["conversation.send", "conversation.steer", "conversation.queue.add"].includes(request.method)
                && typeof request.params.conversationId === "string") {
                const conversationId = request.params.conversationId
                const current = client.draft(conversationId)
                const content = current.attachments.length ? [{ type: "text", text: current.text }, ...current.attachments.map(item => item.block)] : undefined
                if (request.params.text === current.text && JSON.stringify(request.params.promptContent) === JSON.stringify(content)) {
                  client.saveDraft(conversationId, { text: "", attachments: [] })
                  if (selectedRef.current === conversationId) setDraft({ text: "", attachments: [] })
                }
              }
            }}>Retry exact command</Button>
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => {
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
            <strong className="mr-auto text-sm">{snapshot?.conversation.title || "Loading conversation…"}</strong>
            <span className="text-xs text-muted-foreground">{snapshot?.running ? "Working" : snapshot?.ready ? "Ready" : "Disconnected"}</span>
            {!snapshot?.connected && <Button size="sm" disabled={busy} onClick={() => void operate("conversation.resume", { conversationId: selected })}>Resume on host</Button>}
            <Button size="sm" variant="outline" disabled={busy || !snapshot?.actions.includes("cancel") || !snapshot?.running} onClick={() => void operate("conversation.interrupt", { conversationId: selected })}>Stop</Button>
            <Button size="sm" variant="ghost" disabled={busy || !snapshot?.ready} onClick={async () => {
              const result = await operate<{ conversation: HostConversation }>("conversation.fork", { conversationId: selected, title: `Fork: ${snapshot?.conversation.title || "Conversation"}` })
              if (result) setSelected(result.conversation.id)
            }}>Fork with context</Button>
          </div>
          <div className="min-h-40 flex-1 overflow-auto p-3 md:p-5">
            {snapshot?.warning && <p className="mb-3 text-sm text-muted-foreground">{snapshot.warning}</p>}
            {snapshot?.deliveries.filter(delivery => delivery.status === "failed").slice(-5).map(delivery =>
              <p key={delivery.commandId} className="mb-3 text-sm text-destructive" role="alert">{delivery.error || `Command ${delivery.commandId} failed on the host.`}</p>)}
            {snapshot && <LiveMessages snapshot={snapshot} />}
          </div>
          <div className="space-y-3 border-t p-3 md:p-4">
            {requests.filter(request => request.kind === "approval").map(request => <div key={request.requestId} className="rounded-lg border p-3">
              <p className="text-sm font-medium">{request.payload.title || "Approval required"}</p>
              {request.payload.rawInputJSON && <pre className="my-2 max-h-40 overflow-auto whitespace-pre-wrap text-xs">{request.payload.rawInputJSON}</pre>}
              <div className="flex flex-wrap gap-2">{request.payload.options?.map(option => <Button key={option.id} size="sm" variant="outline" disabled={busy || !snapshot?.connected || outbox.some(item => item.params.conversationId === selected && item.method === "conversation.approval" && item.params.requestId === request.requestId)} onClick={() => void operate("conversation.approval", { conversationId: selected, requestId: request.requestId, text: option.id })}>{option.name}</Button>)}</div>
            </div>)}
            <ExecutionQuestions key={selected} requests={requests.filter(request => request.kind !== "approval")}
              disabled={busy || !snapshot?.connected}
              blockedRequestIDs={new Set(outbox.filter(request => request.params.conversationId === selected && request.method === "conversation.userInput").map(request => String(request.params.requestId)))}
              onAnswer={async (requestId, text) => (await operate("conversation.userInput", { conversationId: selected, requestId, text })) !== undefined} />
            {!!snapshot?.queue.length && <details className="rounded-md border p-2" open><summary className="text-sm">Queue{snapshot.queueHeld ? " · held" : ""}</summary>
              {snapshot.queue.filter(entry => entry.status !== "cancelled").map(entry => <div key={entry.command.id} className="mt-2 flex flex-wrap items-center gap-1 text-xs">
                <span className="mr-auto max-w-full break-words">{entry.command.text} · {entry.status}{entry.error ? ` · ${entry.error}` : ""}</span>
                {["queued", "held"].includes(entry.status) && <>
                  <Button size="sm" variant="ghost" disabled={busy} onClick={() => { const text = window.prompt("Edit queued prompt", entry.command.text); if (text !== null) void operate("conversation.queue.edit", { conversationId: selected, queuedCommandId: entry.command.id, text }) }}>Edit</Button>
                  <Button size="sm" variant="ghost" disabled={busy || queued[0]?.command.id === entry.command.id} onClick={() => void operate("conversation.queue.reorder", { conversationId: selected, commandIds: [entry.command.id, ...queued.filter(item => item.command.id !== entry.command.id).map(item => item.command.id)] })}>Move first</Button>
                  <Button size="sm" variant="ghost" disabled={busy || !snapshot.running || !snapshot.actions.includes("steer")} onClick={() => void operate("conversation.queue.promote", { conversationId: selected, queuedCommandId: entry.command.id })}>Steer</Button>
                  <Button size="sm" variant="ghost" disabled={busy} onClick={() => void operate("conversation.queue.cancel", { conversationId: selected, queuedCommandId: entry.command.id })}>Cancel</Button>
                </>}
              </div>)}
              {snapshot.queueHeld && <Button className="mt-2" size="sm" disabled={busy} onClick={() => void operate("conversation.queue.resume", { conversationId: selected })}>Resume undispatched queue</Button>}
            </details>}
            <div className="flex flex-wrap items-end gap-2">
              {!!snapshot?.controls?.models.length && <label className="text-xs">Model<select aria-label="Execution model" className="ml-2 rounded border bg-background p-1" disabled={controlsBlocked} value={snapshot.controls.selectedModelId || ""} onChange={event => void operate("conversation.model", { conversationId: selected, text: event.target.value })}>
                <option value="" disabled>Provider default</option>{snapshot.controls.models.map(model => <option key={model.id} value={model.id}>{model.name}</option>)}
              </select></label>}
              {snapshot?.controls?.configOptions.map(option => <label key={option.id} className="text-xs">{option.name}<select aria-label={option.name} className="ml-2 rounded border bg-background p-1" disabled={controlsBlocked} value={option.currentValue || ""} onChange={event => void operate("conversation.configuration", { conversationId: selected, optionId: option.id, text: event.target.value })}>
                <option value="" disabled>Provider default</option>{option.choices.map(choice => <option key={choice.value} value={choice.value}>{choice.name}</option>)}
              </select></label>)}
            </div>
            <Textarea aria-label="Message to agent" placeholder="Message this conversation…" value={draft.text} onChange={event => saveDraft({ ...draft, text: event.target.value })}
              onKeyDown={event => { if (event.key === "Enter" && (event.metaKey || event.ctrlKey) && !controlsBlocked && !pendingForChat.length) { event.preventDefault(); void send("send") } }} />
            <div className="flex flex-wrap items-center gap-2">
              <label className="cursor-pointer rounded border px-2 py-1 text-xs">Attach file<input className="sr-only" type="file" multiple disabled={!snapshot?.controls || busy} onChange={async event => {
                const files = Array.from(event.target.files || []); event.target.value = ""
                const conversationId = selected
                try {
                  if (!snapshot?.controls) return
                  const attachments = await Promise.all(files.map(file => captureExecutionAttachment(file, snapshot.controls!.promptCapabilities)))
                  // Capture can outlive an edit or a conversation switch. Append to
                  // the current saved draft for the original conversation only.
                  const current = client.draft(conversationId)
                  const next = { ...current, attachments: [...current.attachments, ...attachments] }
                  client.saveDraft(conversationId, next)
                  if (selectedRef.current === conversationId) setDraft(next)
                } catch (error) { setError(String(error)) }
              }} /></label>
              {draft.attachments.map(attachment => <Button key={attachment.id} size="sm" variant="outline" onClick={() => saveDraft({ ...draft, attachments: draft.attachments.filter(item => item.id !== attachment.id) })}>{attachment.name} ×</Button>)}
              <div className="ml-auto flex gap-2">
                <Button variant="outline" disabled={busy || !!pendingForChat.length || (!draft.text.trim() && !draft.attachments.length)} onClick={() => void send("queue.add")}>Queue</Button>
                {snapshot?.running && snapshot.actions.includes("steer") && <Button variant="outline" disabled={busy || !!pendingForChat.length || !draft.text.trim()} onClick={() => void send("steer")}>Steer</Button>}
                <Button disabled={controlsBlocked || !!pendingForChat.length || (!draft.text.trim() && !draft.attachments.length)} onClick={() => void send("send")}>Send</Button>
              </div>
            </div>
          </div>
        </>}
      </section>
    </div>}
  </main>
}
