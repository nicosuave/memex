import { expect, test } from "@playwright/test"

function searchResults(offset: number) {
  return Array.from({ length: 50 }, (_, index) => ({
    session_id: `session-${offset + index}`,
    record_id: `record-${offset + index}`,
    source_path: `/sessions/${offset + index}.jsonl`,
    project: `Project ${offset + index}`,
    source: "codex",
    role: "assistant",
    ts: 1_725_000_000_000,
    snippet: `Context before needle ${"and more matching context ".repeat(5)}${offset + index}`,
    snippet_matches: [{ start: 15, end: 21 }],
  }))
}

test("sidebar preloads a delayed search page one viewport before the boundary", async ({
  page,
}) => {
  const offsets: number[] = []
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === "/api/search") {
      const offset = Number(url.searchParams.get("offset"))
      offsets.push(offset)
      if (offset > 0)
        await new Promise((resolve) => setTimeout(resolve, 400))
      await route.fulfill({
        json: {
          results: searchResults(offset),
          has_more: offset === 0,
          offset,
        },
      })
      return
    }
    if (url.pathname === "/api/session") {
      await route.fulfill({
        json: {
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
        },
      })
      return
    }
    await route.fulfill({
      json:
        url.pathname === "/api/activity"
          ? { metric: "sessions", range: "30d", points: [], bucket_keys: [] }
          : { documents: 100 },
    })
  })

  await page.goto("/?q=needle&session=session-0&mode=history")
  await expect(page.getByText("Transcript fixture", { exact: true })).toBeVisible()
  await expect.poll(() => offsets).toEqual([0])
  await page.getByRole("button", { name: "Toggle Sidebar" }).click()

  const sidebar = page.locator('[data-slot="sidebar-content"]:visible')
  await expect(sidebar.locator("[data-session-id]")).toHaveCount(50)
  await page.waitForTimeout(150)
  expect(offsets).toEqual([0])

  const remaining = await sidebar.evaluate((element) => {
    element.scrollTop =
      element.scrollHeight - element.clientHeight - element.clientHeight * 0.75
    return element.scrollHeight - element.clientHeight - element.scrollTop
  })
  expect(remaining).toBeGreaterThan(240)
  await expect.poll(() => offsets).toEqual([0, 50])

  // The delayed request is already in flight while the existing page remains usable.
  await expect(sidebar.locator("[data-session-id]")).toHaveCount(50)
  await expect(sidebar.locator("[data-session-id]")).toHaveCount(100)
})
