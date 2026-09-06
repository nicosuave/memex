import { expect, test } from "@playwright/test"

for (const mobile of [false, true]) {
  test(`Home highlights matched text on ${mobile ? "mobile" : "desktop"} and clears it with search`, async ({ page }) => {
    await page.setViewportSize(mobile ? { width: 390, height: 844 } : { width: 1280, height: 900 })
    const snippet = '…' + '界 preceding context '.repeat(4) + '<img src=x onerror=alert(1)> postgres evidence'
    const start = Array.from(snippet.slice(0, snippet.indexOf("postgres"))).length
    await page.route("**/api/**", async (route) => {
      const url = new URL(route.request().url())
      const searching = Boolean(url.searchParams.get("q"))
      await route.fulfill({ json: url.pathname === "/api/search" ? {
        results: [{ session_id: "result", record_id: "r1", source_path: "/result.jsonl",
          project: "Matched result", source: "codex", role: "tool_result", ts: 1,
          snippet: searching ? snippet : "Recent message prefix",
          snippet_matches: searching ? [{ start, end: start + 8 }] : [],
        }], has_more: false, offset: 0,
      } : url.pathname === "/api/activity"
        ? { metric: "sessions", range: "30d", points: [], bucket_keys: [] }
        : { documents: 1 } })
    })
    await page.goto('/?q=text%3Apostgres')
    const preview = page.locator(".home-result-snippet")
    const match = preview.locator("mark")
    await expect(match).toHaveText("postgres")
    await expect(match).toBeVisible()
    expect(await match.evaluate((element) => {
      const hit = element.getBoundingClientRect()
      const container = element.parentElement!.getBoundingClientRect()
      return hit.left >= container.left && hit.right <= container.right + 1 &&
        hit.top >= container.top && hit.bottom <= container.bottom + 1
    })).toBe(true)
    await expect(preview).toContainText('<img src=x onerror=alert(1)>')
    await expect(preview.locator("img")).toHaveCount(0)
    await page.getByRole("combobox", { name: "Search conversations" }).fill("")
    await expect(preview).toHaveText("Recent message prefix")
    await expect(preview.locator("mark")).toHaveCount(0)
  })
}
