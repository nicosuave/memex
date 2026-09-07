import { expect, test, type Page } from "@playwright/test"

async function mockSortedResults(page: Page) {
  const calls: URL[] = []
  let releasePage: (() => void) | undefined
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url())
    calls.push(url)
    if (url.pathname === "/api/search") {
      const offset = Number(url.searchParams.get("offset"))
      const sort = url.searchParams.get("sort") || "relevance"
      if (offset && sort === "relevance") await new Promise<void>((resolve) => { releasePage = resolve })
      await route.fulfill({ json: {
        results: Array.from({ length: 50 }, (_, i) => ({
          session_id: `${sort}-${offset + i}`, record_id: `${sort}-r${offset + i}`, source: "codex",
          source_path: `/sessions/${sort}-${offset + i}.jsonl`, project: `${sort} ${offset + i}`,
          role: "assistant", ts: 1000, snippet: "needle evidence", snippet_matches: [{ start: 0, end: 6 }],
        })), offset, has_more: !offset,
      } })
      return
    }
    await route.fulfill({ json: url.pathname === "/api/session" ? {
      session_id: "selected", source: "codex", source_path: "/selected.jsonl", project: "Selected",
      version: "v1", offset: 0, total: 1,
      messages: [{ record_id: "selected-r", role: "assistant", content: "Selected transcript", ts: 1 }],
    } : url.pathname === "/api/activity"
      ? { metric: "sessions", range: "30d", points: [], bucket_keys: [] }
      : { documents: 3763842 } })
  })
  return { calls, release: () => releasePage?.() }
}

for (const mobile of [false, true]) {
  test(`result sort resets stale pagination and restores with Back on ${mobile ? "mobile" : "desktop"}`, async ({ page }) => {
    await page.setViewportSize({ width: mobile ? 390 : 1280, height: 900 })
    const api = await mockSortedResults(page)
    await page.goto("/?q=needle&source=codex&origin=all&range=7d")
    const list = page.locator(".home-results")
    await expect(list.getByRole("option").first()).toContainText("relevance 0")
    const control = page.locator(".home-result-filters").getByRole("combobox", { name: "Sort results" })
    await expect(control).toHaveText("Relevance")
    await list.evaluate((element) => { element.scrollTop = element.scrollHeight })
    await expect.poll(() => api.calls.some((url) => url.pathname === "/api/search" && url.searchParams.get("offset") === "50")).toBe(true)
    const activityCalls = api.calls.filter((url) => url.pathname === "/api/activity").length
    await control.click()
    await page.getByRole("option", { name: "Newest", exact: true }).click()
    await expect(list.getByRole("option").first()).toContainText("newest 0")
    await expect(page).toHaveURL(/sort=newest/)
    const newest = api.calls.find((url) => url.pathname === "/api/search" && url.searchParams.get("sort") === "newest")!
    expect(Object.fromEntries(newest.searchParams)).toMatchObject({ q: "needle", source: "codex", origin: "all", range: "7d", offset: "0" })
    api.release()
    await page.waitForTimeout(100)
    await expect(list).not.toContainText("relevance 50")
    expect(await list.evaluate((element) => element.scrollTop)).toBe(0)
    expect(api.calls.filter((url) => url.pathname === "/api/activity").length).toBe(activityCalls)
    await page.goBack()
    await expect(control).toHaveText("Relevance")
    await expect(list.getByRole("option").first()).toContainText("relevance 0")
    await page.getByRole("combobox", { name: "Search conversations" }).fill("")
    await expect(control).toHaveText("Newest")
    await expect(list.getByRole("option").first()).toContainText("newest 0")
  })
}

test("sidebar replaces counts with a shared sort control without reopening the transcript", async ({ page }) => {
  const api = await mockSortedResults(page)
  await page.goto("/?q=needle&session=selected&path=%2Fselected.jsonl&sort=oldest")
  await expect(page.getByText("Selected transcript", { exact: true })).toBeVisible()
  await page.getByRole("button", { name: "Toggle Sidebar" }).click()
  const header = page.locator('[data-slot="sidebar-header"]:visible')
  await expect(header.getByRole("combobox", { name: "Sort results" })).toHaveText("Oldest")
  await expect(header).not.toContainText("records")
  await expect(header).not.toContainText(/\d+\+? (recent|matching) sessions/)
  const count = api.calls.filter((url) => url.pathname === "/api/session").length
  await header.getByRole("combobox", { name: "Sort results" }).click()
  await page.getByRole("option", { name: "Newest", exact: true }).click()
  await expect(page.locator('[data-slot="sidebar-content"]:visible')).toContainText("newest 0")
  await expect(page.getByText("Selected transcript", { exact: true })).toBeVisible()
  expect(api.calls.filter((url) => url.pathname === "/api/session").length).toBe(count)
  expect(api.calls.some((url) => url.pathname === "/api/stats")).toBe(false)
})

for (const mobile of [false, true]) {
  test(`permission reviews require origin opt-in on ${mobile ? "mobile" : "desktop"}`, async ({ page }) => {
    await page.setViewportSize({ width: mobile ? 390 : 1280, height: 900 })
    const api = await mockSortedResults(page)
    await page.goto("/?q=needle")
    const origin = page.locator(".home-result-filters").getByRole("combobox", { name: "Origin", exact: true })
    await expect(origin).toHaveText("interactive")
    await expect.poll(() => api.calls.some((url) => url.pathname === "/api/search")).toBe(true)
    expect(api.calls.filter((url) => url.pathname === "/api/search").every((url) => !url.searchParams.has("origin"))).toBe(true)
    await origin.click()
    await page.getByRole("option", { name: "all (includes permission reviews)", exact: true }).click()
    await expect(page).toHaveURL(/origin=all/)
    await expect.poll(() => api.calls.some((url) => url.pathname === "/api/search" && url.searchParams.get("origin") === "all")).toBe(true)
    await expect.poll(() => api.calls.some((url) => url.pathname === "/api/activity" && url.searchParams.get("origin") === "all")).toBe(true)
    await origin.click()
    await page.getByRole("option", { name: "regular (no permission reviews)", exact: true }).click()
    await expect(page).toHaveURL(/origin=regular/)
    await expect.poll(() => api.calls.some((url) => url.pathname === "/api/search" && url.searchParams.get("origin") === "regular")).toBe(true)
    await page.reload()
    await expect(origin).toHaveText("regular (no permission reviews)")
  })
}
