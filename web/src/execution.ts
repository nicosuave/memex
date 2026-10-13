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
  constructor(readonly code: string, message: string) { super(message) }
}

const outboxPrefix = "memex-execution-outbox-v1:"
export class ExecutionClient {
  constructor(readonly token: string, readonly hostId?: string) {}

  async call<T>(method: string, params: Record<string, unknown> = {}, mutation = false, signal?: AbortSignal): Promise<T> {
    const commandId = typeof params.commandId === "string" ? params.commandId : crypto.randomUUID()
    const request: ExecutionRequest = { id: commandId, method, params: { ...params, ...(this.hostId ? { hostId: this.hostId } : {}) } }
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
    let response: Response
    try {
      response = await fetch("/api/control", { method: "POST", redirect: "error", credentials: "omit",
        headers: { Authorization: `Bearer ${this.token}`, "Content-Type": "application/json" }, body: JSON.stringify(request),
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(65000)]) : AbortSignal.timeout(65000) })
    } catch (error) {
      throw new ExecutionError("delivery_unknown", mutation
        ? `The exact command ${commandId} is saved. Check its receipt before resending: ${String(error)}` : String(error))
    }
    const envelope = await response.json() as { result?: T; error?: string | { code: string; message: string } }
    if (!response.ok) throw new ExecutionError("http", typeof envelope.error === "string" ? envelope.error : `Execution host HTTP ${response.status}`)
    if (envelope.error) throw typeof envelope.error === "string" ? new ExecutionError("host", envelope.error) : new ExecutionError(envelope.error.code, envelope.error.message)
    if (!("result" in envelope)) throw new ExecutionError("response", "The execution host returned no result")
    if (mutation) localStorage.removeItem(this.outboxKey(commandId))
    return envelope.result as T
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
  private outboxKey(id: string): string { return `${outboxPrefix}${this.hostId ?? "unpaired"}:${id}` }
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
