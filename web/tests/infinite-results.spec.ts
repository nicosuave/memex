import { expect, test, type Page } from "@playwright/test"

function results(offset: number, count: number) {
  return Array.from({ length: count }, (_, index) => ({
    session_id: `session-${offset + index}`,
    record_id: `record-${offset + index}`,
    source_path: `/sessions/${offset + index}.jsonl`,
    project: `Project ${offset + index}`,
    source: "codex",
    role: "assistant",
    ts: 1_725_000_000_000,
    snippet: `Session preview ${offset + index}`,
  }))
}

async function mockSearch(
  page: Page,
  search: (offset: number) => { count: number; more: boolean; fail?: boolean },
) {
  const offsets: number[] = []
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === "/api/search") {
      const offset = Number(url.searchParams.get("offset"))
      offsets.push(offset)
      const response = search(offset)
      await new Promise((resolve) => setTimeout(resolve, 80))
      await route.fulfill({
        status: response.fail ? 503 : 200,
        json: response.fail
          ? { error: "Temporary search failure" }
          : {
              results: results(offset, response.count),
              has_more: response.more,
              offset,
            },
      })
      return
    }
    await route.fulfill({
      json:
        url.pathname === "/api/session"
          ? {
              session_id: "session-0",
              source_path: "/sessions/0.jsonl",
              source: "codex",
              project: "Project 0",
              version: "v1",
              offset: 0,
              total: 1,
              messages: [
                {
                  record_id: "record-0",
                  role: "assistant",
                  content: "Transcript fixture",
                  ts: 1_725_000_000_000,
                },
              ],
            }
          : url.pathname === "/api/activity"
            ? { metric: "sessions", range: "30d", points: [], bucket_keys: [] }
            : { documents: 125 },
    })
  })
  return offsets
}

for (const mobile of [false, true]) {
  test(`${mobile ? "mobile search" : "desktop recent"} results append while scrolling and stop at the end`, async ({ page }) => {
    if (mobile) await page.setViewportSize({ width: 390, height: 844 })
    const offsets = await mockSearch(page, (offset) => ({
      count: offset < 100 ? 50 : 25,
      more: offset < 100,
    }))
    await page.goto(mobile ? "/?q=fixture" : "/")
    const list = page.locator(".home-results")
    await expect(list.getByRole("option")).toHaveCount(50)
    await page.waitForTimeout(150)
    expect(offsets).toEqual([0])
    const scrollBefore = await list.evaluate((element) => {
      element.scrollTop = element.scrollHeight
      return element.scrollTop
    })
    await expect(list.getByRole("option")).toHaveCount(100)
    expect(await list.evaluate((element) => element.scrollTop)).toBe(scrollBefore)
    await list.evaluate((element) => {
      element.scrollTop = element.scrollHeight
    })
    await expect(list.getByRole("option")).toHaveCount(125)
    await list.evaluate((element) => {
      element.scrollTop = element.scrollHeight
    })
    await page.waitForTimeout(250)
    expect(offsets).toEqual([0, 50, 100])
    await expect(
      page.getByRole("button", { name: "Load more results" }),
    ).toHaveCount(0)
  })
}

for (const mobile of [false, true]) {
  test(`${mobile ? "mobile" : "desktop"} sidebar loads only when open and continues on scroll`, async ({ page }) => {
    if (mobile) await page.setViewportSize({ width: 390, height: 844 })
    const offsets = await mockSearch(page, (offset) => ({
      count: offset === 0 ? 1 : 50,
      more: offset < 51,
    }))
    await page.goto("/?session=session-0&mode=history")
    await expect(
      page.getByText("Transcript fixture", { exact: true }),
    ).toBeVisible()
    await expect.poll(() => offsets.length).toBe(1)
    await page.waitForTimeout(250)
    expect(offsets).toEqual([0])
    await page.getByRole("button", { name: "Toggle Sidebar" }).click()
    const list = page.locator('[data-slot="sidebar-content"]:visible')
    await expect(list.locator("[data-session-id]")).toHaveCount(51)
    await list.evaluate((element) => {
      element.scrollTop = element.scrollHeight
    })
    await expect(list.locator("[data-session-id]")).toHaveCount(101)
    await list.evaluate((element) => {
      element.scrollTop = element.scrollHeight
    })
    await page.waitForTimeout(250)
    expect(offsets).toEqual([0, 1, 51])
  })
}

test("short lists fill automatically, pause after failure, and retry without losing rows", async ({ page }) => {
  let failed = false
  const offsets = await mockSearch(page, (offset) => {
    if (offset === 1 && !failed) {
      failed = true
      return { count: 0, more: true, fail: true }
    }
    return { count: 1, more: offset === 0 }
  })
  await page.goto("/")
  const list = page.locator(".home-results")
  await expect(list.getByRole("alert")).toContainText("Temporary search failure")
  await page.waitForTimeout(250)
  expect(offsets).toEqual([0, 1])
  await expect(list.getByRole("option")).toHaveCount(1)
  await list.getByRole("button", { name: "Retry loading sessions" }).click()
  await expect(list.getByRole("option")).toHaveCount(2)
  await expect(list.getByRole("alert")).toHaveCount(0)
  expect(offsets).toEqual([0, 1, 1])
})
