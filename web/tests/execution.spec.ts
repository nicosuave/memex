import { expect, test, type Page } from "@playwright/test"
import type { HostSchedule, HostScheduleRun, HostWorktree } from "../src/execution"

async function hostFixture(page: Page, failFirstSend = false, modernSchedules = false) {
  const calls: { method: string; params: Record<string, unknown> }[] = []
  let created = false
  let interrupted = false
  let sendAttempts = 0
  let model = "model-one"
  const worktrees: HostWorktree[] = []
  const schedules: HostSchedule[] = []
  const runs: HostScheduleRun[] = modernSchedules ? [{ id: "run-exact", scheduleID: "old-schedule", conversationID: "run-conversation", createdAt: "2026-10-05T12:00:00Z", updatedAt: "2026-10-05T12:00:00Z", status: "failed", error: "Provider rejected request", read: false, needsAttention: true, notificationPolicy: "attention" }] : []
  const conversation = { id: "host-conversation", nativeSessionID: "native-conversation", provider: "codex", providerInstanceID: "codex:/home/user/.codex",
    workspaceID: "/work/project", cwd: "/work/project", transcriptPath: "/home/user/.codex/sessions/session.jsonl", title: "Test conversation", connected: true }
  await page.route("**/api/**", async route => {
    const path = new URL(route.request().url()).pathname
    if (path !== "/api/control") {
      await route.fulfill({ status: 401, contentType: "application/json", body: JSON.stringify({ error: "Authentication required" }) })
      return
    }
    expect(route.request().headers().authorization).toBe(`Bearer ${"a".repeat(64)}`)
    const request = route.request().postDataJSON() as { id: string; method: string; params: Record<string, unknown> }
    calls.push(request)
    let result: unknown
    switch (request.method) {
      case "host.info": result = { hostId: "test-host", providers: ["codex"], capabilities: ["conversation", "queue", "schedules", "schedules.wall_clock", "worktree.lifecycle", ...(modernSchedules ? ["schedules.runs", "schedules.new_conversation", "schedules.events"] : [])] }; break
      case "workspace.list": result = [{ id: "/work/project", path: "/work/project" }, ...worktrees.filter(tree => !tree.archived && !tree.removed).map(tree => ({ id: tree.workspaceId, path: tree.path, worktreeID: tree.id, repositoryWorkspaceID: tree.repositoryWorkspaceId }))]; break
      case "worktree.list": result = worktrees; break
      case "worktree.create": {
        const tree: HostWorktree = { id: "owned-worktree", path: "/private/host/worktrees/owned/checkout", workspaceId: "/private/host/worktrees/owned/checkout",
          repositoryWorkspaceId: String(request.params.workspaceId), branch: "memex-chat-owned", baseRef: String(request.params.baseRef), state: "ready", archived: false, removed: false, referencedBy: [] }
        worktrees.push(tree); result = tree; break
      }
      case "worktree.archive": {
        const tree = worktrees.find(tree => tree.id === request.params.worktreeId)!
        tree.archived = request.params.archived !== false; result = tree; break
      }
      case "worktree.cleanup": {
        const tree = worktrees.find(tree => tree.id === request.params.worktreeId)!
        tree.removed = true; tree.archived = true; result = tree; break
      }
      case "worktree.reattach": {
        const tree = worktrees.find(tree => tree.id === request.params.worktreeId)!
        tree.removed = false; tree.archived = false; result = tree; break
      }
      case "conversation.list": result = created ? [conversation] : []; break
      case "schedule.list": result = schedules; break
      case "schedule.runs": result = runs; break
      case "schedule.run.read": {
        const run = runs.find(item => item.id === request.params.runId)!
        run.read = request.params.read === true; run.needsAttention = !run.read
        result = run; break
      }
      case "schedule.upsert": {
        const schedule: HostSchedule = { id: String(request.params.scheduleId), conversationID: String(request.params.conversationId ?? ""), prompt: String(request.params.text),
          newConversation: request.params.newConversation ? { ...(request.params.newConversation as { provider: string; title: string }), workspaceID: (request.params.newConversation as { workspaceId: string }).workspaceId } : undefined,
          eventName: request.params.eventName as string | undefined, notificationPolicy: request.params.notificationPolicy as string | undefined,
          intervalSeconds: request.params.intervalSeconds as number | undefined, wallClock: request.params.wallClock as HostSchedule["wallClock"], paused: false, nextRunAt: "2026-10-06T16:30:00Z" }
        const prior = schedules.findIndex(item => item.id === schedule.id)
        if (prior < 0) schedules.push(schedule); else schedules[prior] = schedule
        result = schedule; break
      }
      case "conversation.create": created = true; result = { conversation }; break
      case "conversation.model": model = String(request.params.text); result = { receipt: { accepted: true } }; break
      case "conversation.send":
        sendAttempts++
        if (failFirstSend && sendAttempts === 1) { await route.abort(); return }
        result = { receipt: { accepted: true, commandId: request.params.commandId } }; break
      case "conversation.interrupt": interrupted = true; result = { receipt: { accepted: true } }; break
      case "command.read": result = { status: "completed", result: { receipt: { accepted: true } } }; break
      case "conversation.read": result = {
        conversation, ready: true, connected: true, running: sendAttempts > 0 && !interrupted, queueHeld: false,
        actions: ["prompt", "steer", "cancel", "model"], queue: [], deliveries: [],
        controls: { models: [{ id: "model-one", name: "Model one" }, { id: "model-two", name: "Model two" }], selectedModelId: model,
          configOptions: [], pendingControlCommandIds: [], promptCapabilities: { image: false, audio: false, embeddedContext: true } },
        thread: { snapshotSequence: 1, messages: [], pendingRequests: [] },
        presentation: { conversation: { presentation: [], ephemeral: [], state: { pending_interactions: [] } } }
      }; break
      default: result = true
    }
    await route.fulfill({ contentType: "application/json", body: JSON.stringify({ id: request.id, result }) })
  })
  await page.goto("/?view=execution")
  await page.getByLabel("Execution pairing token").fill("a".repeat(64))
  await page.getByRole("button", { name: "Pair host", exact: true }).click()
  await page.getByRole("button", { name: "Create conversation", exact: true }).click()
  await expect(page.getByLabel("Message to agent")).toBeVisible()
  return calls
}

test("execution pairing works independently of history and configures the first send", async ({ page }) => {
  const calls = await hostFixture(page)
  expect(calls.filter(call => call.method === "conversation.send")).toHaveLength(0)
  await page.getByLabel("Execution model").selectOption("model-two")
  await expect(page.getByLabel("Execution model")).toHaveValue("model-two")
  await page.getByLabel("Message to agent").fill("Configured first prompt")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect.poll(() => calls.filter(call => call.method === "conversation.send").length).toBe(1)
  const send = calls.find(call => call.method === "conversation.send")!
  expect(send.params.hostId).toBe("test-host")
  expect(send.params.text).toBe("Configured first prompt")
  expect(typeof send.params.commandId).toBe("string")
  expect(typeof send.params.issuedAt).toBe("string")
  expect(calls.findIndex(call => call.method === "conversation.model")).toBeLessThan(calls.indexOf(send))
})

test("interrupted acknowledgement retains the exact outgoing request and retry identity", async ({ page }) => {
  const calls = await hostFixture(page, true)
  await page.getByLabel("Message to agent").fill("Do this once")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.getByRole("button", { name: "Retry exact command" })).toBeVisible()
  await expect(page.getByLabel("Message to agent")).toHaveValue("Do this once")
  await expect(page.getByRole("button", { name: "Send", exact: true })).toBeDisabled()
  const saved = await page.evaluate(() => Object.keys(localStorage).filter(key => key.startsWith("memex-execution-outbox-v1:")).map(key => JSON.parse(localStorage.getItem(key)!)))
  expect(saved).toHaveLength(1)
  expect(saved[0].params.text).toBe("Do this once")
  await page.getByRole("button", { name: "Retry exact command" }).click()
  await expect.poll(() => calls.filter(call => call.method === "conversation.send").length).toBe(2)
  const attempts = calls.filter(call => call.method === "conversation.send")
  expect(attempts[1].params).toEqual(attempts[0].params)
  await expect(page.getByRole("button", { name: "Retry exact command" })).toHaveCount(0)
})

test("mobile viewport can operate an active conversation", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  const calls = await hostFixture(page)
  await page.getByLabel("Message to agent").fill("Mobile prompt")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.getByRole("button", { name: "Stop", exact: true })).toBeEnabled()
  await page.getByRole("button", { name: "Stop", exact: true }).click()
  await expect.poll(() => calls.some(call => call.method === "conversation.interrupt")).toBe(true)
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
})

test("retrying an uncertain send preserves newly attached draft context", async ({ page }) => {
  const calls = await hostFixture(page, true)
  await page.getByLabel("Message to agent").fill("Do this once")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.getByRole("button", { name: "Retry exact command" })).toBeVisible()
  await page.locator('input[type="file"]').setInputFiles({ name: "context.txt", mimeType: "text/plain", buffer: Buffer.from("New draft context") })
  await expect(page.getByRole("button", { name: "context.txt ×" })).toBeVisible()
  await page.getByRole("button", { name: "Retry exact command" }).click()
  await expect.poll(() => calls.filter(call => call.method === "conversation.send").length).toBe(2)
  await expect(page.getByRole("button", { name: "Retry exact command" })).toHaveCount(0)
  await expect(page.getByLabel("Message to agent")).toHaveValue("Do this once")
  await expect(page.getByRole("button", { name: "context.txt ×" })).toBeVisible()
})

test("weekday schedule sends explicit local time and zone and restores them for editing", async ({ page }) => {
  const calls = await hostFixture(page)
  await page.getByText("Schedules", { exact: true }).click()
  await page.getByLabel("Scheduled prompt").fill("Weekday check")
  await page.getByLabel("Schedule recurrence").selectOption("wallClock")
  await page.getByLabel("Schedule local time").fill("09:30")
  await page.getByLabel("Schedule time zone").fill("America/Los_Angeles")
  await page.getByLabel("Tuesday", { exact: true }).uncheck()
  await page.getByRole("button", { name: "Save schedule", exact: true }).click()
  await expect.poll(() => calls.filter(call => call.method === "schedule.upsert").length).toBe(1)
  const saved = calls.find(call => call.method === "schedule.upsert")!.params
  expect(saved.wallClock).toEqual({ localTime: "09:30", weekdays: [1, 3, 4, 5], timeZone: "America/Los_Angeles" })
  expect(saved.intervalSeconds).toBeUndefined()
  await page.getByRole("button", { name: "Edit", exact: true }).click()
  await expect(page.getByLabel("Schedule local time")).toHaveValue("09:30")
  await expect(page.getByLabel("Schedule time zone")).toHaveValue("America/Los_Angeles")
  await expect(page.getByLabel("Tuesday", { exact: true })).not.toBeChecked()
})

test("worktree lifecycle uses returned identities and makes checkout available to new conversations", async ({ page }) => {
  const calls = await hostFixture(page)
  await page.getByText("Managed worktrees", { exact: true }).click()
  await page.getByLabel("Worktree base ref").fill("main")
  await page.getByRole("button", { name: "Create worktree", exact: true }).click()
  await expect(page.getByText("memex-chat-owned", { exact: true })).toBeVisible()
  const created = calls.find(call => call.method === "worktree.create")!.params
  expect(created.workspaceId).toBe("/work/project")
  expect(created.baseRef).toBe("main")
  expect(created.path).toBeUndefined()
  await page.getByRole("button", { name: "Archive", exact: true }).click()
  await expect(page.getByText("Archived · files retained", { exact: true })).toBeVisible()
  await page.getByRole("button", { name: "Unarchive", exact: true }).click()
  page.once("dialog", dialog => dialog.accept())
  await page.getByRole("button", { name: "Remove clean checkout", exact: true }).click()
  await expect(page.getByText("Checkout removed", { exact: true })).toBeVisible()
  await page.getByRole("button", { name: "Reattach", exact: true }).click()
  await expect(page.getByRole("button", { name: "Archive", exact: true })).toBeVisible()
  expect(calls.filter(call => call.method === "worktree.cleanup" || call.method === "worktree.reattach").map(call => call.params.worktreeId)).toEqual(["owned-worktree", "owned-worktree"])
  await page.getByText("New conversation", { exact: true }).click()
  await expect(page.getByLabel("Execution workspace").locator("option").filter({ hasText: "/private/host/worktrees/owned/checkout" })).toHaveCount(1)
})


test("event schedule creates a fresh-chat target and round-trips notification policy", async ({ page }) => {
  const calls = await hostFixture(page, false, true)
  await page.getByText("Schedules", { exact: true }).click()
  await page.getByLabel("Schedule target").selectOption("new")
  await page.getByLabel("Schedule workspace").selectOption("/work/project")
  await page.getByLabel("Schedule provider").selectOption("codex")
  await page.getByLabel("Run conversation title").fill("Event analysis")
  await page.getByLabel("Scheduled prompt").fill("Inspect completed import")
  await page.getByLabel("Schedule recurrence").selectOption("event")
  await page.getByLabel("Schedule event name").fill("import.completed")
  await page.getByLabel("Schedule notification policy").selectOption("never")
  await page.getByRole("button", { name: "Save schedule", exact: true }).click()
  await expect.poll(() => calls.filter(call => call.method === "schedule.upsert").length).toBe(1)
  const saved = calls.find(call => call.method === "schedule.upsert")!.params
  expect(saved.newConversation).toEqual({ workspaceId: "/work/project", provider: "codex", title: "Event analysis" })
  expect(saved.conversationId).toBeUndefined()
  expect(saved.intervalSeconds).toBeUndefined()
  expect(saved.wallClock).toBeUndefined()
  expect(saved.eventName).toBe("import.completed")
  expect(saved.notificationPolicy).toBe("never")
  await page.getByRole("button", { name: "Edit", exact: true }).click()
  await expect(page.getByLabel("Schedule target")).toHaveValue("new")
  await expect(page.getByLabel("Schedule workspace")).toHaveValue("/work/project")
  await expect(page.getByLabel("Schedule event name")).toHaveValue("import.completed")
  await expect(page.getByLabel("Schedule notification policy")).toHaveValue("never")
  await page.getByLabel("Schedule target").selectOption("existing")
  await page.getByLabel("Schedule recurrence").selectOption("interval")
  await page.getByRole("button", { name: "Save schedule", exact: true }).click()
  await expect.poll(() => calls.filter(call => call.method === "schedule.upsert").length).toBe(2)
  const edited = calls.filter(call => call.method === "schedule.upsert")[1].params
  expect(edited.newConversation).toBeUndefined()
  expect(edited.eventName).toBeUndefined()
  expect(edited.conversationId).toBe("host-conversation")
  expect(edited.intervalSeconds).toBe(3600)
})

test("run inbox opens exact run conversation and persists read state", async ({ page }) => {
  const calls = await hostFixture(page, false, true)
  await page.getByText("Schedule run inbox (1 unread)", { exact: true }).click()
  const run = page.getByRole("article", { name: "Schedule run run-exact" })
  await expect(run.getByText("Provider rejected request", { exact: true })).toBeVisible()
  await run.getByRole("button", { name: "Mark read", exact: true }).click()
  await expect(run.getByRole("button", { name: "Mark unread", exact: true })).toBeVisible()
  expect(calls.find(call => call.method === "schedule.run.read")!.params).toMatchObject({ runId: "run-exact", read: true })
  await run.getByRole("button", { name: "Open run conversation", exact: true }).click()
  await expect.poll(() => calls.some(call => call.method === "conversation.read" && call.params.conversationId === "run-conversation")).toBe(true)
  await run.getByRole("button", { name: "Mark unread", exact: true }).click()
  await expect(run.getByText("Needs attention", { exact: true })).toBeVisible()
})

test("older hosts do not receive schedule extension requests or fields", async ({ page }) => {
  const calls = await hostFixture(page)
  await page.getByText("Schedules", { exact: true }).click()
  await expect(page.getByLabel("Schedule target")).toHaveCount(0)
  await expect(page.getByLabel("Schedule notification policy")).toHaveCount(0)
  await expect(page.getByLabel("Schedule recurrence").locator('option[value="event"]')).toHaveCount(0)
  await page.getByLabel("Scheduled prompt").fill("Legacy recurring prompt")
  await page.getByRole("button", { name: "Save schedule", exact: true }).click()
  await expect.poll(() => calls.some(call => call.method === "schedule.upsert")).toBe(true)
  expect(calls.some(call => call.method === "schedule.runs")).toBe(false)
  expect(calls.find(call => call.method === "schedule.upsert")!.params.notificationPolicy).toBeUndefined()
})

type Gate = { promise: Promise<void>; release: () => void }
function gate(): Gate {
  let release = () => {}
  const promise = new Promise<void>(resolve => { release = resolve })
  return { promise, release }
}

const outboxKeys = (page: Page) => page.evaluate(() => Object.keys(localStorage).filter(key => key.startsWith("memex-execution-outbox-v1:")))
// Two animation frames: a delivered response has had a chance to commit.
const settleFrames = (page: Page) => page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))))

async function liveHost(page: Page, options: { holdSend?: Gate; holdWorktree?: Gate; sendError?: { code: string; message: string } } = {}) {
  const calls: { method: string; params: Record<string, unknown> }[] = []
  const sent: { commandId: string; text: string }[] = []
  const queue: { command: { id: string; text: string; issuedAt: string; conversationID: string }; status: string }[] = []
  const notes: string[] = []
  const host = { calls, notes, heldReads: 0, releasedReads: 0, holdRead: null as { conversationId: string; gate: Gate } | null }
  const conversations = [
    { id: "chat-a", nativeSessionID: "native-a", provider: "codex", providerInstanceID: "codex:home", workspaceID: "/work/project", cwd: "/work/project", title: "First chat", connected: true },
    { id: "chat-b", nativeSessionID: "native-b", provider: "codex", providerInstanceID: "codex:home", workspaceID: "/work/project", cwd: "/work/project", title: "Second chat", connected: false },
  ]
  const message = (id: string, role: string, text: string, extra: Record<string, unknown> = {}) =>
    ({ item_id: id, body: { kind: "message", data: { role, parts: [{ type: "text", data: text }], ...extra } } })
  const snapshot = (conversationId: string) => {
    const conversation = conversations.find(item => item.id === conversationId)!
    const history = conversationId === "chat-a" ? sent : []
    return {
      conversation, ready: conversation.connected, connected: conversation.connected, running: false, queueHeld: false,
      actions: ["prompt", "steer", "cancel"], queue: queue.filter(entry => entry.command.conversationID === conversationId),
      deliveries: history.map((item, index) => ({ commandId: item.commandId, status: "completed", nativeTurnId: `turn-${index}`, nativeMessageId: `native-user-${index}` })),
      controls: { models: [], pendingControlCommandIds: [], promptCapabilities: { image: false, audio: false, embeddedContext: true }, configOptions: [] },
      thread: { snapshotSequence: history.length, messages: [], pendingRequests: [] },
      presentation: { conversation: { ephemeral: [], state: { pending_interactions: [] }, presentation: [
        message(`seed-${conversationId}`, "assistant", conversationId === "chat-a" ? "First chat history" : "Second chat history"),
        ...(conversationId === "chat-a" ? notes.map((text, index) => message(`note-${index}`, "assistant", text)) : []),
        ...history.flatMap((item, index) => [
          { ...message(`user-${index}`, "user", item.text, { native_message_id: `native-user-${index}` }), native_turn_id: `turn-${index}` },
          message(`assistant-${index}`, "assistant", `Reply ${index + 1}`),
        ]),
      ] } },
    }
  }
  let sendAttempts = 0
  await page.route("**/api/**", async route => {
    if (new URL(route.request().url()).pathname !== "/api/control") {
      await route.fulfill({ status: 401, contentType: "application/json", body: '{"error":"Authentication required"}' }); return
    }
    const request = route.request().postDataJSON() as { id: string; method: string; params: Record<string, unknown> }
    calls.push(request)
    const reply = (body: Record<string, unknown>) => route.fulfill({ contentType: "application/json", body: JSON.stringify({ id: request.id, ...body }) })
    switch (request.method) {
      case "host.info": return reply({ result: { hostId: "live-host", providers: ["codex"], capabilities: ["conversation", "queue", "worktree.lifecycle"] } })
      case "conversation.list": return reply({ result: conversations })
      case "workspace.list": return reply({ result: [{ id: "/work/project", path: "/work/project" }] })
      case "schedule.list": case "worktree.list": return reply({ result: [] })
      case "worktree.create":
        await options.holdWorktree?.promise
        return reply({ result: { id: "tree", path: "/work/tree", workspaceId: "/work/tree", repositoryWorkspaceId: "/work/project", state: "ready", archived: false, removed: false, referencedBy: [] } })
      case "conversation.send":
        sendAttempts++
        await options.holdSend?.promise
        if (options.sendError && sendAttempts === 1) return reply({ error: options.sendError })
        sent.push({ commandId: String(request.params.commandId), text: String(request.params.text) })
        return reply({ result: { receipt: { accepted: true } } })
      case "conversation.queue.add": {
        const entry = { command: { id: String(request.params.commandId), text: String(request.params.text), issuedAt: String(request.params.issuedAt), conversationID: String(request.params.conversationId) }, status: "queued" }
        queue.push(entry)
        return reply({ result: entry })
      }
      case "conversation.read": {
        const conversationId = String(request.params.conversationId)
        // The answer reflects the host when the request arrived, however late it is delivered.
        const result = snapshot(conversationId)
        const held = host.holdRead?.conversationId === conversationId ? host.holdRead : null
        if (held) { host.holdRead = null; host.heldReads++; await held.gate.promise }
        await reply({ result })
        if (held) host.releasedReads++
        return
      }
      default: return reply({ result: true })
    }
  })
  await page.goto("/?view=execution")
  await page.getByLabel("Execution pairing token").fill("a".repeat(64))
  await page.getByRole("button", { name: "Pair host", exact: true }).click()
  await page.getByLabel("Active conversation").selectOption("chat-a")
  await expect(page.getByText("Ready", { exact: true })).toBeVisible()
  return host
}

test("a sent prompt shows at once and reconciles with the host's copy without a duplicate", async ({ page }) => {
  const send = gate()
  const host = await liveHost(page, { holdSend: send })
  const input = page.getByLabel("Message to agent")
  await input.fill("Optimistic hello")
  await input.press("ControlOrMeta+Enter")
  await expect.poll(() => host.calls.filter(call => call.method === "conversation.send").length).toBe(1)
  // The host has not answered yet.
  await expect(page.locator("article", { hasText: "Optimistic hello" })).toHaveCount(1)
  await expect(input).toHaveValue("")
  await expect(input).toBeFocused()
  await expect(page.getByText("Working…", { exact: true })).toBeVisible()
  await expect(page.getByText("Working…", { exact: true })).toHaveCSS("animation-name", "execution-shimmer")
  await page.emulateMedia({ reducedMotion: "reduce" })
  await expect(page.getByText("Working…", { exact: true })).toHaveCSS("animation-name", "none")
  send.release()
  await expect(page.getByText("Reply 1", { exact: true })).toBeVisible()
  await expect(page.locator("article", { hasText: "Optimistic hello" })).toHaveCount(1)
  await expect(page.locator("article[data-command-id]")).toHaveCount(0)
  await expect(page.getByText("Working…", { exact: true })).toHaveCount(0)
})

test("a definitive host rejection restores the draft inline and does not block the next send", async ({ page }) => {
  const host = await liveHost(page, { sendError: { code: "invalid_params", message: "Missing or invalid text" } })
  const input = page.getByLabel("Message to agent")
  await input.fill("Rejected once")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.getByRole("alert")).toContainText("Not sent: Missing or invalid text")
  await expect(input).toHaveValue("Rejected once")
  await expect(page.locator("article[data-command-id]")).toHaveCount(0)
  await expect(page.getByRole("button", { name: "Retry exact command" })).toHaveCount(0)
  expect(await outboxKeys(page)).toHaveLength(0)
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.getByText("Reply 1", { exact: true })).toBeVisible()
  const sends = host.calls.filter(call => call.method === "conversation.send")
  expect(sends).toHaveLength(2)
  expect(sends[1].params.commandId).not.toBe(sends[0].params.commandId)
  await expect(page.getByRole("alert")).toHaveCount(0)
})

test("an uncertain host error keeps the exact prompt unconfirmed and blocks a fresh send", async ({ page }) => {
  await liveHost(page, { sendError: { code: "host_error", message: "Provider connection dropped" } })
  await page.getByLabel("Message to agent").fill("Maybe delivered")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.getByRole("button", { name: "Retry exact command" })).toBeVisible()
  await expect(page.locator("article[data-command-id]")).toContainText("Delivery unconfirmed")
  await expect(page.getByRole("button", { name: "Send", exact: true })).toBeDisabled()
  expect(await outboxKeys(page)).toHaveLength(1)
})

test("typing and unrelated controls stay live during slow requests, and Cmd+Enter queues behind a send", async ({ page }) => {
  const worktree = gate()
  const send = gate()
  const host = await liveHost(page, { holdWorktree: worktree, holdSend: send })
  await page.getByText("Managed worktrees", { exact: true }).click()
  await page.getByRole("button", { name: "Create worktree", exact: true }).click()
  await expect.poll(() => host.calls.filter(call => call.method === "worktree.create").length).toBe(1)
  await expect(page.getByRole("button", { name: "Create worktree", exact: true })).toBeDisabled()
  const input = page.getByLabel("Message to agent")
  await input.pressSequentially("Typed during a slow request")
  await expect(input).toHaveValue("Typed during a slow request")
  await expect(page.getByRole("button", { name: "Send", exact: true })).toBeEnabled()
  await input.press("ControlOrMeta+Enter")
  await expect.poll(() => host.calls.filter(call => call.method === "conversation.send").length).toBe(1)
  await input.pressSequentially("Queued follow-up")
  await expect(input).toHaveValue("Queued follow-up")
  await expect(page.getByRole("button", { name: "Send", exact: true })).toBeDisabled()
  await expect(page.getByText("queues this message", { exact: false })).toBeVisible()
  await input.press("ControlOrMeta+Enter")
  await expect.poll(() => host.calls.filter(call => call.method === "conversation.queue.add").map(call => call.params.text)).toEqual(["Queued follow-up"])
  await expect(page.getByText("Queued follow-up · queued", { exact: true })).toBeVisible()
  await expect(input).toHaveValue("")
  send.release()
  worktree.release()
  await expect(page.getByText("Reply 1", { exact: true })).toBeVisible()
  await expect(page.getByRole("button", { name: "Create worktree", exact: true })).toBeEnabled()
})

test("an older snapshot delivered late never replaces a newer one", async ({ page }) => {
  const host = await liveHost(page)
  const stale = gate()
  // The held read sees a newer host state than the screen, so only request order can tell it is stale.
  host.holdRead = { conversationId: "chat-a", gate: stale }
  host.notes.push("Interim note")
  await expect.poll(() => host.heldReads).toBe(1)
  await page.getByLabel("Message to agent").fill("Newer state")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.getByText("Reply 1", { exact: true })).toBeVisible()
  stale.release()
  await expect.poll(() => host.releasedReads).toBe(1)
  // Checked without retrying: the late answer must not replace the newer screen even briefly.
  for (let frame = 0; frame < 10; frame++) {
    await settleFrames(page)
    expect(await page.getByText("Reply 1", { exact: true }).isVisible()).toBe(true)
  }
  await expect(page.locator("article", { hasText: "Newer state" })).toHaveCount(1)
})

test("switching conversations keeps the previous frame and never shows an unloaded chat as disconnected", async ({ page }) => {
  const host = await liveHost(page)
  await expect(page.getByText("First chat history", { exact: true })).toBeVisible()
  const slow = gate()
  host.holdRead = { conversationId: "chat-b", gate: slow }
  await page.getByLabel("Active conversation").selectOption("chat-b")
  await expect(page.locator("section strong")).toHaveText("Second chat")
  await expect(page.getByText("Loading…", { exact: true })).toBeVisible()
  await expect(page.getByText("First chat history", { exact: true })).toBeVisible()
  await expect(page.getByText("Disconnected", { exact: true })).toHaveCount(0)
  await expect(page.getByRole("button", { name: "Resume on host" })).toHaveCount(0)
  slow.release()
  await expect(page.getByText("Second chat history", { exact: true })).toBeVisible()
  await expect(page.getByText("First chat history", { exact: true })).toHaveCount(0)
  await expect(page.getByText("Disconnected", { exact: true })).toBeVisible()
  await expect(page.getByRole("button", { name: "Resume on host" })).toBeVisible()
})

test("the transcript follows new output only while the reader is at the bottom", async ({ page }) => {
  const host = await liveHost(page)
  for (let index = 0; index < 30; index++) host.notes.push(`Earlier output ${index}\n\n${"A long line of agent output. ".repeat(12)}`)
  const scroller = page.locator(".execution-scroll")
  const distance = () => scroller.evaluate(element => element.scrollHeight - element.clientHeight - element.scrollTop)
  await expect(page.getByText("Earlier output 29", { exact: false })).toBeInViewport()
  expect(await distance()).toBeLessThan(2)
  await scroller.hover()
  await page.mouse.wheel(0, -600)
  await expect(page.getByRole("button", { name: "Jump to latest" })).toBeVisible()
  await expect.poll(distance).toBeGreaterThan(100)
  const top = await scroller.evaluate(element => element.scrollTop)
  host.notes.push("Arrived while reading")
  await expect(page.getByText("Arrived while reading", { exact: true })).toBeAttached()
  await settleFrames(page)
  expect(await scroller.evaluate(element => element.scrollTop)).toBe(top)
  await page.getByRole("button", { name: "Jump to latest" }).click()
  await expect(page.getByText("Arrived while reading", { exact: true })).toBeInViewport()
  await expect(page.getByRole("button", { name: "Jump to latest" })).toHaveCount(0)
  // The user's own send lands in view and re-attaches.
  await page.mouse.wheel(0, -600)
  await expect(page.getByRole("button", { name: "Jump to latest" })).toBeVisible()
  await page.getByLabel("Message to agent").fill("Bring me back down")
  await page.getByRole("button", { name: "Send", exact: true }).click()
  await expect(page.locator("article", { hasText: "Bring me back down" })).toBeInViewport()
  await expect(page.getByRole("button", { name: "Jump to latest" })).toHaveCount(0)
})
