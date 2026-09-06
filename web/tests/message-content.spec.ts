import { expect, test, type Page } from "@playwright/test"

async function showMessage(page: Page, content: string, role = "developer") {
  await page.route("**/api/**", async (route) => {
    const pathname = new URL(route.request().url()).pathname
    await route.fulfill({ json: pathname === "/api/session" ? {
      session_id: "context", source_path: "/context.jsonl", source: "codex",
      project: "Context rendering", version: "v1", offset: 0, total: 1,
      messages: [{ record_id: "context-r1", role, content, ts: 1_725_000_000_000 }],
    } : pathname === "/api/search" ? { results: [], has_more: false, offset: 0 }
      : pathname === "/api/activity" ? { metric: "sessions", range: "30d", points: [], bucket_keys: [] }
        : { documents: 1 } })
  })
  await page.goto("/?session=context&path=%2Fcontext.jsonl&record=context-r1&mode=history")
  const message = page.locator('[data-record-id="context-r1"]')
  await expect(message).toBeVisible()
  return message
}

test("context wrappers render Markdown headings, lists, nested sections, and raw ampersands", async ({ page }) => {
  const message = await showMessage(page, `<app-context>
# Codex desktop context
- First instruction
- Tools & files

<environment_context>
## Workspace
Nested instructions.
</environment_context>

Text after the nested context.
</app-context>

Outside paragraph.
<permissions instructions>
## Access
Permission details.
</permissions>`)
  await expect(message.getByRole("heading", { name: "Codex desktop context" })).toBeVisible()
  await expect(message.getByRole("listitem")).toHaveText(["First instruction", "Tools & files"])
  await expect(message.locator(".context-section .context-section").getByRole("heading", { name: "Workspace" })).toBeVisible()
  await expect(message).toContainText("Text after the nested context.")
  await expect(message).toContainText("Outside paragraph.")
  await expect(message.getByRole("heading", { name: "Access" })).toBeVisible()
  await expect(message.locator(".context-title")).toHaveText(["app context", "environment context", "permissions instructions"])
  await expect(message).not.toContainText("<app-context>")
})

test("XML and image examples stay literal inside fenced and inline code", async ({ page }) => {
  const message = await showMessage(page, [
    "<app-context>", "# Examples", "", "```xml", "<environment_context>",
    "# Literal heading", "</environment_context>", "```", "",
    "`![alt](url)` and `<app-context>`.", "", "~~~markdown",
    "![audio](/absolute/path.mp3)", "~~~", "</app-context>",
  ].join("\n"))
  await expect(message.locator(".context-section")).toHaveCount(1)
  await expect(message.locator("pre").first()).toContainText("<environment_context>\n# Literal heading\n</environment_context>")
  await expect(message.getByRole("heading", { name: "Literal heading" })).toHaveCount(0)
  await expect(message.locator("code").filter({ hasText: "![alt](url)" })).toHaveText("![alt](url)")
  await expect(message.locator("img")).toHaveCount(0)
})

test("unclosed context stays readable and mismatched closing tags are preserved", async ({ page }) => {
  const message = await showMessage(page, "<app-context>\n# Partial context\nVisible loaded content.\n</other-context>\n")
  await expect(message.getByRole("heading", { name: "Partial context" })).toBeVisible()
  await expect(message).toContainText("Visible loaded content.")
  await expect(message).toContainText("</other-context>")
})

test("failed image references become literal text while valid images still render", async ({ page }) => {
  await page.route("**/missing-example.png", (route) => route.fulfill({ status: 404, body: "" }))
  await page.route("**/valid-example.png", (route) => route.fulfill({
    contentType: "image/png",
    body: Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jhKkAAAAASUVORK5CYII=", "base64"),
  }))
  const message = await showMessage(page, "<app-context>\n# Media examples\n\nExample: ![alt](/missing-example.png)\n\n![real](/valid-example.png)\n</app-context>")
  await expect(message.locator("code")).toHaveText("![alt](/missing-example.png)")
  await expect(message.getByRole("img", { name: "alt", exact: true })).toHaveCount(0)
  await expect(message.getByRole("img", { name: "real" })).toBeVisible()
  await expect.poll(() => message.getByRole("img", { name: "real" }).evaluate((img: HTMLImageElement) => img.naturalWidth)).toBe(1)
})

test("ordinary structured XML retains fields and mixed content loses no text", async ({ page }) => {
  const message = await showMessage(page, "<search-result>\n<status>done</status><count>2</count>\n</search-result>")
  await expect(message.locator(".xml-row dt")).toHaveText(["status", "count"])
  await expect(message.locator(".xml-row dd")).toHaveText(["done", "2"])
  await page.unroute("**/api/**")
  const mixed = await showMessage(page, "<result>Before <value>inside</value> after.</result>")
  await expect(mixed).toContainText("Before")
  await expect(mixed).toContainText("inside")
  await expect(mixed).toContainText("after.")
  await page.unroute("**/api/**")
  const cdata = await showMessage(page, "<result><![CDATA[Before ]]><value>inside</value><![CDATA[ after.]]></result>")
  await expect(cdata).toContainText("Before")
  await expect(cdata).toContainText("inside")
  await expect(cdata).toContainText("after.")
})

test("context-like HTML remains inert and tool output remains verbatim", async ({ page }) => {
  const content = '<app-context>\n# Heading\n\n<script>window.contextPwned = true</script>\n</app-context>'
  const message = await showMessage(page, content)
  await expect(message.locator("script")).toHaveCount(0)
  expect(await page.evaluate(() => (window as any).contextPwned)).toBeUndefined()
  await page.unroute("**/api/**")
  const tool = await showMessage(page, content, "tool_result")
  await expect(tool.locator(".tool-content")).toHaveText(content)
  await expect(tool.locator(".context-section")).toHaveCount(0)
})
