import { expect, test, type Page } from "@playwright/test"

type RequestCall = { method: string; params: Record<string, unknown> }

async function questionHost(page: Page, options: { uncertain?: boolean; disconnected?: boolean } = {}) {
  const calls: RequestCall[] = []
  const pending = [
    { requestId: "native-question-a", kind: "userInput", status: "pending", payload: { title: "Select platforms", prompt: "Choose platforms and include your constraints.", multiSelect: true,
      choices: [{ id: "mac", title: "macOS", value: "mac", description: "Native desktop" }, { id: "web", title: "Web", value: "web" }] } },
    { requestId: "native-question-b", kind: "userInput", status: "pending", payload: { title: "Name the release", prompt: "What should it be called?" } },
  ]
  const conversation = { id: "question-chat", nativeSessionID: "native-chat", provider: "codex", providerInstanceID: "codex:home",
    workspaceID: "/work/project", cwd: "/work/project", title: "Questions", connected: !options.disconnected }
  await page.route("**/api/**", async route => {
    if (new URL(route.request().url()).pathname !== "/api/control") {
      await route.fulfill({ status: 401, contentType: "application/json", body: '{"error":"Authentication required"}' }); return
    }
    const request = route.request().postDataJSON() as RequestCall & { id: string }
    calls.push(request)
    let result: unknown = true
    switch (request.method) {
      case "host.info": result = { hostId: "question-host", providers: ["codex"], capabilities: ["conversation"] }; break
      case "workspace.list": result = [{ id: "/work/project", path: "/work/project" }]; break
      case "conversation.list": result = [conversation]; break
      case "schedule.list": result = []; break
      case "conversation.userInput":
        if (options.uncertain) { await route.abort(); return }
        pending.splice(pending.findIndex(item => item.requestId === request.params.requestId), 1)
        result = { receipt: { accepted: true } }; break
      case "conversation.read": result = {
        conversation, ready: true, connected: !options.disconnected, running: true, queueHeld: false,
        actions: ["cancel"], queue: [], deliveries: [],
        thread: { snapshotSequence: 1, messages: [], pendingRequests: pending },
        presentation: { conversation: { presentation: [], ephemeral: [], state: { pending_interactions: pending.map(item => item.requestId) } } },
      }; break
    }
    await route.fulfill({ contentType: "application/json", body: JSON.stringify({ id: request.id, result }) })
  })
  await page.goto("/?view=execution")
  await page.getByLabel("Execution pairing token").fill("a".repeat(64))
  await page.getByRole("button", { name: "Pair host", exact: true }).click()
  await page.getByLabel("Active conversation").selectOption(conversation.id)
  await expect(page.getByText("Question 1 of 2", { exact: true })).toBeVisible()
  return calls
}

function decodeAnswer(text: unknown) {
  expect(typeof text).toBe("string")
  const prefix = "sqacp-user-input-v1:"
  expect(String(text).startsWith(prefix)).toBe(true)
  return JSON.parse(Buffer.from(String(text).slice(prefix.length), "base64").toString("utf-8")) as { selectedValues: string[]; text: string }
}

test("question navigation retains native IDs and captured context until explicit answers", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  const calls = await questionHost(page)
  await page.getByRole("checkbox", { name: "macOS Native desktop" }).check()
  await page.getByRole("checkbox", { name: "Web", exact: true }).check()
  await page.getByLabel("Custom answer").fill("Ship café first")
  await page.getByLabel("Attach answer context").setInputFiles({ name: "constraints.txt", mimeType: "text/plain", buffer: Buffer.from("Keep captured bytes: 雪") })
  await expect(page.getByRole("button", { name: "constraints.txt ×" })).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
  await page.getByRole("button", { name: "Next question" }).click()
  await page.getByLabel("Custom answer").fill("Autumn")
  await page.getByRole("button", { name: "Previous question" }).click()
  await expect(page.getByLabel("Custom answer")).toHaveValue("Ship café first")
  await expect(page.getByRole("checkbox", { name: "Web", exact: true })).toBeChecked()
  await expect(page.getByRole("button", { name: "constraints.txt ×" })).toBeVisible()
  expect(calls.filter(call => call.method === "conversation.userInput")).toHaveLength(0)
  await page.getByRole("button", { name: "Answer", exact: true }).click()
  await expect.poll(() => calls.filter(call => call.method === "conversation.userInput").length).toBe(1)
  const first = calls.find(call => call.method === "conversation.userInput")!
  expect(first.params.requestId).toBe("native-question-a")
  expect(decodeAnswer(first.params.text)).toEqual({ selectedValues: ["mac", "web"], text: "Ship café first\n\nAttached answer context: constraints.txt\nKeep captured bytes: 雪" })
  await expect(page.getByText("Question 1 of 1", { exact: true })).toBeVisible()
  await expect(page.getByLabel("Custom answer")).toHaveValue("Autumn")
  await page.getByRole("button", { name: "Answer", exact: true }).click()
  await expect.poll(() => calls.filter(call => call.method === "conversation.userInput").length).toBe(2)
  const second = calls.filter(call => call.method === "conversation.userInput")[1]
  expect(second.params.requestId).toBe("native-question-b")
  expect(second.params.text).toBe("Autumn")
})

test("question-only text context can be answered but media and invalid bytes cannot", async ({ page }) => {
  const calls = await questionHost(page)
  const upload = page.getByLabel("Attach answer context")
  await upload.setInputFiles({ name: "image.png", mimeType: "image/png", buffer: Buffer.from("not an answer string") })
  await expect(page.getByRole("alert")).toContainText("UTF-8 text files only")
  await expect(page.getByRole("button", { name: "Answer", exact: true })).toBeDisabled()
  await upload.setInputFiles({ name: "binary.txt", mimeType: "text/plain", buffer: Buffer.from([0xff, 0xfe, 0x01]) })
  await expect(page.getByRole("alert")).toContainText("not a UTF-8 text file")
  await upload.setInputFiles({ name: "answer.txt", mimeType: "text/plain", buffer: Buffer.from("Only captured context") })
  await expect(page.getByRole("button", { name: "Answer", exact: true })).toBeEnabled()
  await page.getByRole("button", { name: "Answer", exact: true }).click()
  await expect.poll(() => calls.filter(call => call.method === "conversation.userInput").length).toBe(1)
  expect(calls.find(call => call.method === "conversation.userInput")!.params.text).toBe("Attached answer context: answer.txt\nOnly captured context")
})

test("uncertain question delivery disables fresh replies and preserves exact retry", async ({ page }) => {
  const calls = await questionHost(page, { uncertain: true })
  await page.getByRole("checkbox", { name: "Web", exact: true }).check()
  await page.getByRole("button", { name: "Answer", exact: true }).click()
  await expect(page.getByRole("button", { name: "Retry exact command" })).toBeVisible()
  await expect(page.getByRole("button", { name: "Answer", exact: true })).toBeDisabled()
  await expect(page.getByText("Answer delivery is unconfirmed.", { exact: false })).toBeVisible()
  await page.reload()
  await page.getByLabel("Active conversation").selectOption("question-chat")
  await expect(page.getByRole("button", { name: "Answer", exact: true })).toBeDisabled()
  await page.getByRole("button", { name: "Retry exact command" }).click()
  await expect.poll(() => calls.filter(call => call.method === "conversation.userInput").length).toBe(2)
  const replies = calls.filter(call => call.method === "conversation.userInput")
  expect(replies[1].params).toEqual(replies[0].params)
})

test("disconnected native questions are visible without accepting stale answers", async ({ page }) => {
  await questionHost(page, { disconnected: true })
  await expect(page.getByLabel("Custom answer")).toBeDisabled()
  await expect(page.getByRole("checkbox", { name: "Web", exact: true })).toBeDisabled()
  await expect(page.getByRole("button", { name: "Answer", exact: true })).toBeDisabled()
})
