export type HostInfo = { hostId: string; providers: string[]; capabilities: string[] }
export type HostConversation = { id: string; nativeSessionID: string; provider: string; providerInstanceID: string; workspaceID: string; cwd: string; transcriptPath?: string; title: string; connected: boolean; parentID?: string }
export type HostWorkspace = { id: string; path: string; worktreeID?: string; repositoryWorkspaceID?: string }
export type HostWorktree = { id: string; workspaceId: string; repositoryWorkspaceId: string; path: string; branch?: string; baseRef?: string; state: string; failure?: string; archived: boolean; removed: boolean; referencedBy: string[] }
export type HostWallClock = { localTime: string; weekdays: number[]; timeZone: string }
export type HostScheduleRun = { id: string; scheduleID: string; conversationID?: string; createdAt: string; updatedAt: string; status: string; error?: string; read: boolean; needsAttention: boolean; notificationPolicy: string }
export type HostSchedule = { id: string; conversationID: string; newConversation?: { workspaceID: string; provider: string; title: string }; eventName?: string; notificationPolicy?: string; lastError?: string; prompt: string; intervalSeconds?: number; wallClock?: HostWallClock; nextRunAt: string; paused: boolean; lastCommandID?: string; lastSkippedAt?: string }
export type HostQueueEntry = { command: { id: string; text: string; issuedAt: string; conversationID: string }; status: string; error?: string }
export type HostPendingRequest = { requestId: string; kind: string; status: string; payload: { title?: string; prompt?: string; rawInputJSON?: string; multiSelect?: boolean; isSecret?: boolean; options?: { id: string; name: string; kind?: string }[]; choices?: { id: string; title: string; value: string; description?: string }[] } }
export type HostEntity = { entity_id?: string; item_id?: string; native_turn_id?: string; source_order?: number; body: { kind: string; data: { role?: string; native_message_id?: string; parts?: { type: string; data: unknown }[]; name?: string; raw_arguments?: string; status?: string } } }
export type HostSnapshot = {
  conversation: HostConversation; ready: boolean; connected: boolean; running: boolean; queueHeld: boolean; actions: string[]; warning?: string;
  queue: HostQueueEntry[]; deliveries: { commandId: string; status: string; error?: string; nativeTurnId?: string; nativeMessageId?: string }[];
  thread: { snapshotSequence: number; messages: { messageId: string; role: string; content: string }[]; pendingRequests: HostPendingRequest[] } | null;
  presentation: { conversation: { presentation: HostEntity[]; ephemeral: HostEntity[]; state?: { pending_interactions?: string[] } } };
  controls?: { models: { id: string; name: string }[]; selectedModelId?: string; pendingControlCommandIds: string[]; promptCapabilities: { image: boolean; audio: boolean; embeddedContext: boolean }; configOptions: { id: string; name: string; currentValue?: string; choices: { value: string; name: string; description?: string }[] }[] };
}
export type ExecutionRequest = { id: string; method: string; params: Record<string, unknown> }
export type CapturedAttachment = { id: string; name: string; block: Record<string, unknown> }
export type ExecutionDraft = { text: string; attachments: CapturedAttachment[] }

export class ExecutionError extends Error {
  /** `rejected` means the command definitively did not execute, so it can be discarded and resent. */
  constructor(readonly code: string, message: string, readonly rejected = false) { super(message) }
}

// Host error codes for which the execution host records the command's receipt as
// "rejected" (`rejectedCodes` in ExecutionHost.mutate), replays such a receipt
// ("rejected"), or fails before any receipt exists (wrong_host, method_not_found).
// Every other host error is recorded by the host as uncertain and stays unconfirmed here.
const rejectedHostCodes = new Set(["invalid_params", "workspace_denied", "unsupported_provider", "not_found", "queue_state",
  "rejected", "wrong_host", "method_not_found"])
// The gateway answers these before relaying the envelope to the host (src/web.rs, /api/control).
const unrelayedStatuses = new Set([400, 401, 403, 405])
type Envelope<T> = { id?: unknown; result?: T; error?: string | { code: string; message: string } }
export type ReadResult<T> = { raw: string; changed: false } | { raw: string; changed: true; result: T }

/** One user action and its most specific target, so only that control waits for its request. */
export function actionKey(method: string, params: Record<string, unknown> = {}): string {
  const target = ["queuedCommandId", "requestId", "worktreeId", "scheduleId", "runId", "conversationId", "workspaceId"]
    .map(name => params[name]).find(value => typeof value === "string")
  return target === undefined ? method : `${method}:${target}`
}

const outboxPrefix = "memex-execution-outbox-v1:"
export class ExecutionClient {
  constructor(readonly token: string, readonly hostId?: string) {}

  async call<T>(method: string, params: Record<string, unknown> = {}, mutation = false, signal?: AbortSignal): Promise<T> {
    return this.dispatch<T>(this.prepare(method, params, mutation), mutation, signal)
  }

  /** Builds the request and, for a mutation, saves it to the outbox before returning. Throws synchronously. */
  prepare(method: string, params: Record<string, unknown> = {}, mutation = false, requestId?: string): ExecutionRequest {
    const commandId = typeof params.commandId === "string" ? params.commandId : crypto.randomUUID()
    const request: ExecutionRequest = { id: mutation ? commandId : requestId ?? commandId, method,
      params: { ...params, ...(this.hostId ? { hostId: this.hostId } : {}) } }
    if (mutation) {
      request.params.commandId = commandId
      request.params.issuedAt ??= new Date().toISOString()
      // Local storage acceptance is synchronous and precedes fetch. A quota or
      // permissions error blocks dispatch rather than losing the outgoing intent.
      const key = this.outboxKey(commandId)
      const previous = localStorage.getItem(key)
      if (previous) {
        const saved = JSON.parse(previous) as ExecutionRequest
        if (saved.method !== request.method || JSON.stringify(saved.params) !== JSON.stringify(request.params)) {
          throw new ExecutionError("id_conflict", "This command ID already has a different saved outgoing request")
        }
      } else { localStorage.setItem(key, JSON.stringify(request)) }
    }
    return request
  }

  async dispatch<T>(request: ExecutionRequest, mutation = false, signal?: AbortSignal): Promise<T> {
    const response = await this.exchange<T>(request, mutation, signal)
    // Without a previous answer to compare against, every answer is a changed one.
    if (!response.changed) throw new ExecutionError("response", "The execution host returned no result")
    return response.result
  }

  /** Reads with a fixed envelope ID, so an unchanged answer is byte-identical to `previous` and is not parsed again. */
  read<T>(method: string, params: Record<string, unknown>, previous: string | undefined, signal?: AbortSignal): Promise<ReadResult<T>> {
    return this.exchange<T>(this.prepare(method, params, false, method), false, signal, previous)
  }

  private async exchange<T>(request: ExecutionRequest, mutation: boolean, signal?: AbortSignal, previous?: string): Promise<ReadResult<T>> {
    let response: Response
    let raw: string
    try {
      response = await fetch("/api/control", { method: "POST", redirect: "error", credentials: "omit",
        headers: { Authorization: `Bearer ${this.token}`, "Content-Type": "application/json" }, body: JSON.stringify(request),
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(65000)]) : AbortSignal.timeout(65000) })
      raw = await response.text()
    } catch (error) {
      throw new ExecutionError("delivery_unknown", mutation
        ? `The exact command ${request.id} is saved. Check its receipt before resending: ${String(error)}` : String(error))
    }
    if (response.ok && previous !== undefined && raw === previous) return { raw, changed: false }
    let envelope: Envelope<T> | null = null
    try { envelope = JSON.parse(raw) as Envelope<T> } catch { envelope = null }
    if (!response.ok) {
      const message = typeof envelope?.error === "string" ? envelope.error : !envelope && raw.trim() ? raw.trim() : `Execution host HTTP ${response.status}`
      throw this.failed(request, mutation, new ExecutionError("http", message, unrelayedStatuses.has(response.status)))
    }
    if (!envelope || typeof envelope !== "object") throw new ExecutionError("response", "The execution host returned an unreadable response")
    if (envelope.error) {
      throw this.failed(request, mutation, typeof envelope.error === "string" ? new ExecutionError("host", envelope.error)
        : new ExecutionError(envelope.error.code, envelope.error.message, envelope.id === request.id && rejectedHostCodes.has(envelope.error.code)))
    }
    if (!("result" in envelope)) throw new ExecutionError("response", "The execution host returned no result")
    if (mutation) localStorage.removeItem(this.outboxKey(request.id))
    return { raw, changed: true, result: envelope.result as T }
  }

  private failed(request: ExecutionRequest, mutation: boolean, error: ExecutionError): ExecutionError {
    // A definitive rejection never executed, so its saved outgoing intent cannot cause a duplicate.
    if (mutation && error.rejected) localStorage.removeItem(this.outboxKey(request.id))
    return error
  }

  pending(): ExecutionRequest[] {
    const prefix = this.outboxKey("")
    return Object.keys(localStorage).filter(key => key.startsWith(prefix)).map(key => JSON.parse(localStorage.getItem(key)!) as ExecutionRequest)
  }
  resolve(request: ExecutionRequest): void {
    // Explicit user reconciliation preserves the original operation for later inspection.
    localStorage.setItem(`memex-execution-resolved-v1:${this.hostId}:${request.id}`, JSON.stringify({ request, resolvedAt: new Date().toISOString() }))
    localStorage.removeItem(this.outboxKey(request.id))
  }
  draft(conversationId: string): ExecutionDraft {
    const value = localStorage.getItem(`memex-execution-draft-v1:${this.hostId}:${conversationId}`)
    return value ? JSON.parse(value) as ExecutionDraft : { text: "", attachments: [] }
  }
  saveDraft(conversationId: string, draft: ExecutionDraft): void {
    localStorage.setItem(`memex-execution-draft-v1:${this.hostId}:${conversationId}`, JSON.stringify(draft))
  }
  /** Applies `change` to a saved draft and reports whether it changed anything. */
  editDraft(conversationId: string, change: (draft: ExecutionDraft) => ExecutionDraft): boolean {
    const saved = this.draft(conversationId)
    const next = change(saved)
    if (next === saved) return false
    this.saveDraft(conversationId, next)
    return true
  }
  private outboxKey(id: string): string { return `${outboxPrefix}${this.hostId ?? "unpaired"}:${id}` }
}

export const emptyDraft: ExecutionDraft = { text: "", attachments: [] }
export const isBlankDraft = (draft: ExecutionDraft) => !draft.text.trim() && !draft.attachments.length

/** The prompt content a draft sends, or undefined for text alone. */
export function draftPromptContent(draft: ExecutionDraft): Record<string, unknown>[] | undefined {
  return draft.attachments.length ? [{ type: "text", text: draft.text }, ...draft.attachments.map(item => item.block)] : undefined
}

export const promptMethods = new Set(["conversation.send", "conversation.steer", "conversation.queue.add"])
const transcriptKinds = new Set(["message", "tool_invocation", "tool_result", "context_boundary"])

export function transcriptEntities(snapshot: HostSnapshot): HostEntity[] {
  const conversation = snapshot.presentation?.conversation
  const saved = conversation?.presentation || []
  const live = [...(conversation?.ephemeral || [])].sort((a, b) => (a.source_order || 0) - (b.source_order || 0))
  // An exact native message identity supersedes its saved projection. Similar text
  // is never a reason to remove another message.
  const liveNativeIDs = new Set(live.map(item => item.body.data.native_message_id).filter(Boolean))
  return [...saved.filter(item => !item.body.data.native_message_id || !liveNativeIDs.has(item.body.data.native_message_id)), ...live]
    .filter(item => transcriptKinds.has(item.body.kind))
}

export function entityKey(entity: HostEntity, index: number): string {
  return entity.item_id || entity.entity_id || `unknown-${index}`
}

export function entityText(entity: HostEntity): string {
  return (entity.body.data.parts || []).map(part => {
    if (typeof part.data === "string") return part.data
    return JSON.stringify(part.data, null, 2)
  }).join("\n")
}

function userEntities(entities: HostEntity[]) {
  return entities.map((entity, index) => ({ entity, key: entityKey(entity, index) }))
    .filter(({ entity }) => entity.body.kind === "message" && entity.body.data.role === "user")
}

/** User messages that already exist when a prompt is dispatched cannot be its echo. */
export function userEntityKeys(entities: HostEntity[]): Set<string> {
  return new Set(userEntities(entities).map(({ key }) => key))
}

/**
 * Whether the transcript holds the provider's own copy of a sent prompt: the exact native message or
 * turn its delivery reports, otherwise (as the macOS client does) a user message that appeared after
 * dispatch and contains the sent text. Without the dispatch-time user keys only exact identity counts.
 */
export function hasPromptEcho(commandId: string, text: string, knownUserKeys: ReadonlySet<string> | null, snapshot: HostSnapshot, entities: HostEntity[]): boolean {
  const delivery = snapshot.deliveries.find(item => item.commandId === commandId)
  const users = userEntities(entities)
  if (delivery?.nativeMessageId && users.some(({ entity }) => entity.body.data.native_message_id === delivery.nativeMessageId)) return true
  if (delivery?.nativeTurnId && users.some(({ entity }) => entity.native_turn_id === delivery.nativeTurnId)) return true
  if (!knownUserKeys) return false
  const wanted = text.trim()
  return users.some(({ entity, key }) => !knownUserKeys.has(key) && (!wanted || entityText(entity).includes(wanted)))
}

export async function captureExecutionAttachment(file: File, capabilities: NonNullable<HostSnapshot["controls"]>["promptCapabilities"]): Promise<CapturedAttachment> {
  if (file.size > 2 * 1024 * 1024) throw new Error("Each attachment must be at most 2 MiB")
  let block: Record<string, unknown>
  if (file.type.startsWith("image/") && capabilities.image) {
    const bytes = new Uint8Array(await file.arrayBuffer())
    let binary = ""
    for (const byte of bytes) binary += String.fromCharCode(byte)
    block = { type: "image", data: btoa(binary), mimeType: file.type }
  } else if (capabilities.embeddedContext && (file.type.startsWith("text/") || /\.(md|txt|json|csv|ts|tsx|js|rs|swift|py|toml|yaml|yml|sql)$/i.test(file.name))) {
    block = { type: "resource", resource: { uri: `memex-attachment:${crypto.randomUUID()}/${encodeURIComponent(file.name)}`, mimeType: file.type || "text/plain", text: await file.text() } }
  } else throw new Error("This provider does not support that attachment type")
  return { id: crypto.randomUUID(), name: file.name, block }
}
