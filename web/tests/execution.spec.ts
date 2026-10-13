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
