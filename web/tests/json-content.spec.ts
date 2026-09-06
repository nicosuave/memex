import { expect, test, type Page } from "@playwright/test"

async function showMessage(page: Page, content: string, role = "developer") {
  await page.route("**/api/**", async (route) => {
    const pathname = new URL(route.request().url()).pathname
    await route.fulfill({ json: pathname === "/api/session" ? {
      session_id: "json", source_path: "/json.jsonl", source: "codex",
      project: "JSON rendering", version: "v1", offset: 0, total: 1,
      messages: [{ record_id: "json-r1", role, content, ts: 1_725_000_000_000 }],
    } : pathname === "/api/search" ? { results: [], has_more: false, offset: 0 }
      : pathname === "/api/activity" ? { metric: "sessions", range: "30d", points: [], bucket_keys: [] }
        : { documents: 1 } })
  })
  await page.goto("/?session=json&path=%2Fjson.jsonl&record=json-r1&mode=history")
  const message = page.locator('[data-record-id="json-r1"]')
  await expect(message).toBeVisible()
  return message
}

test("pasted tool transcript formats embedded JSON without linking its URLs", async ({ page }) => {
  const content = [
    "[120] tool exec result: Script completed Wall time 5.4 seconds Output:",
    "",
    '{"chunk_id":"cb7a05","wall_time_seconds":0.365,"exit_code":0,"original_token_count":393,"output":"{\\"baseRefName\\":\\"main\\",\\"statusCheckRollup\\":[{\\"detailsUrl\\":\\"https://github.com/example/repo/actions/runs/123\\"}]}\\ncommitsha\\n"}',
    "",
    "[121] tool exec call: functions.exec_command",
  ].join("\n")
  const message = await showMessage(page, content)
  const json = message.locator("pre code.language-json")
  await expect(json).toHaveCount(1)
  await expect(json).toHaveText([
    "{",
    '  "chunk_id": "cb7a05",',
    '  "wall_time_seconds": 0.365,',
    '  "exit_code": 0,',
    '  "original_token_count": 393,',
    '  "output": "{\\"baseRefName\\":\\"main\\",\\"statusCheckRollup\\":[{\\"detailsUrl\\":\\"https://github.com/example/repo/actions/runs/123\\"}]}\\ncommitsha\\n"',
    "}",
  ].join("\n"))
  await expect(message).toContainText("[120] tool exec result")
  await expect(message).toContainText("[121] tool exec call")
  await expect(message.getByRole("link")).toHaveCount(0)
})

test("standalone objects, arrays, and JSONL are indented while exact literals survive", async ({ page }) => {
  const integer = "900719925474099312345678901234567890"
  const content = [
    `{"id":${integer},"nested":{"ok":true}}`,
    '[1,{"url":"https://example.test/path"}]',
    '{"jsonl":1}',
    '{"jsonl":2}',
  ].join("\n")
  const message = await showMessage(page, content)
  const blocks = message.locator("pre code.language-json")
  await expect(blocks).toHaveCount(4)
  await expect(blocks.nth(0)).toContainText(`"id": ${integer}`)
  await expect(blocks.nth(0)).toContainText('"nested": {\n    "ok": true\n  }')
  await expect(blocks.nth(1)).toContainText('"url": "https://example.test/path"')
  await expect(message.getByRole("link")).toHaveCount(0)
  await expect(blocks.nth(2)).toHaveText('{\n  "jsonl": 1\n}')
  await expect(blocks.nth(3)).toHaveText('{\n  "jsonl": 2\n}')
})

test("tool results and JSON-valued tool fields use the same readable formatting", async ({ page }) => {
  const tool = await showMessage(page, '{"ok":true,"items":[1,2]}', "tool_result")
  await expect(tool.locator(".tool-content")).toHaveText([
    "{",
    '  "ok": true,',
    '  "items": [',
    "    1,",
    "    2",
    "  ]",
    "}",
  ].join("\n"))

  await page.unroute("**/api/**")
  const call = await showMessage(page, '{"data":"{\\"filters\\":[1,2]}"}', "tool_use")
  await expect(call.locator(".tool-value")).toHaveText([
    "{",
    '  "filters": [',
    "    1,",
    "    2",
    "  ]",
    "}",
  ].join("\n"))
})

test("surrounding prose is preserved and invalid, partial, and fenced JSON stays verbatim", async ({ page }) => {
  const content = [
    "Before the payload.",
    '{"valid":true}',
    "After the payload.",
    '{"invalid": nope}',
    '{"partial":[1,2',
    "```json",
    '{"already":"compact"}',
    "```",
  ].join("\n")
  const message = await showMessage(page, content)
  await expect(message).toContainText("Before the payload.")
  await expect(message).toContainText("After the payload.")
  await expect(message).toContainText('{"invalid": nope}')
  await expect(message).toContainText('{"partial":[1,2')
  const blocks = message.locator("pre code.language-json")
  await expect(blocks).toHaveCount(2)
  await expect(blocks.nth(0)).toHaveText('{\n  "valid": true\n}')
  await expect(blocks.nth(1)).toHaveText('{"already":"compact"}')
})

test("an escaped backtick does not swallow later context wrappers", async ({ page }) => {
  const content = [
    "<app-context>",
    "# First context",
    "An escaped " + "\\" + "` opener stays literal.",
    "</app-context>",
    "<environment_context>",
    "# Second context",
    "Visible instructions.",
    "</environment_context>",
  ].join("\n")
  const message = await showMessage(page, content)
  await expect(message.locator(".context-section")).toHaveCount(2)
  await expect(message.locator(".context-title")).toHaveText(["app context", "environment context"])
  await expect(message.getByRole("heading", { name: "First context" })).toBeVisible()
  await expect(message.getByRole("heading", { name: "Second context" })).toBeVisible()
})

test("an unmatched backtick run does not swallow later context wrappers", async ({ page }) => {
  const content = [
    "<app-context>",
    "# First context",
    "An unmatched ` delimiter",
    "</app-context>",
    "<environment_context>",
    "# Second context",
    "Use `value`.",
    "</environment_context>",
  ].join("\n")
  const message = await showMessage(page, content)
  await expect(message.locator(".context-section")).toHaveCount(2)
  await expect(message.locator(".context-title")).toHaveText(["app context", "environment context"])
  await expect(message.getByRole("heading", { name: "First context" })).toBeVisible()
  await expect(message.getByRole("heading", { name: "Second context" })).toBeVisible()
  await expect(message.locator("code")).toHaveText("value")
})

test("a matched multiline code span protects context-like lines", async ({ page }) => {
  const content = [
    "<app-context>",
    "# Outside code",
    "`open span",
    "<environment_context>",
    "Literal body",
    "</environment_context>",
    "close span`",
    "</app-context>",
    "<permissions instructions>",
    "# Following context",
    "</permissions instructions>",
  ].join("\n")
  const message = await showMessage(page, content)
  await expect(message.locator(".context-section")).toHaveCount(2)
  await expect(message.locator(".context-title")).toHaveText(["app context", "permissions instructions"])
  await expect(message).toContainText("<environment_context>")
  await expect(message.getByRole("heading", { name: "Following context" })).toBeVisible()
})
