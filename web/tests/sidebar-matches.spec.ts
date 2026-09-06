import { expect, test } from "@playwright/test"

test("sidebar safely renders backend match spans and plain fallback snippets", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 })
  const highlighted =
    '…界界 <img src=x onerror="window.sidebarPwned=true"> needlé evidence'
  const start = Array.from(
    highlighted.slice(0, highlighted.indexOf("needlé")),
  ).length
  const searchResults = [
    {
      session_id: "highlighted",
      record_id: "highlighted-r1",
      source_path: "/sessions/highlighted.jsonl",
      project: "Highlighted result",
      source: "codex",
      role: "assistant",
      ts: 1_725_000_000_000,
      score: 0.9,
      snippet: highlighted,
      snippet_matches: [{ start, end: start + Array.from("needlé").length }],
    },
    {
      session_id: "fallback",
      record_id: "fallback-r1",
      source_path: "/sessions/fallback.jsonl",
      project: "Fallback result",
      source: "codex",
      role: "assistant",
      ts: 1_725_000_000_000,
      score: 0.8,
      snippet: "Plain fallback for query syntax [.*",
    },
  ]

  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === "/api/search") {
      await route.fulfill({
        json: {
          results: url.searchParams.has("q")
            ? searchResults
            : [
                {
                  ...searchResults[0],
                  snippet: "Readable message prefix from recent session",
                  snippet_matches: [],
                },
              ],
          has_more: false,
          offset: 0,
        },
      })
      return
    }
    if (url.pathname === "/api/session") {
      await route.fulfill({
        json: {
          session_id: "highlighted",
          source_path: "/sessions/highlighted.jsonl",
          source: "codex",
          project: "Highlighted result",
          version: "v1",
          offset: 0,
          total: 1,
          messages: [
            {
              record_id: "highlighted-r1",
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
          : { documents: 2 },
    })
  })

  await page.goto('/?q=text%3A%22needl%C3%A9%22%20AND%20%5B.*')
  await page.getByRole("option", { name: /Highlighted result/ }).click()
  await page.getByRole("button", { name: "Toggle Sidebar" }).click()

  const sidebar = page.locator('[data-slot="sidebar"]:visible')
  const markedMatch = sidebar.locator(".session-snippet mark")
  await expect(markedMatch).toHaveText("needlé")
  await expect(markedMatch).toBeVisible()
  expect(
    await markedMatch.evaluate((element) => {
      const match = element.getBoundingClientRect()
      const snippet = element.parentElement!.getBoundingClientRect()
      return match.bottom <= snippet.bottom + 1
    }),
  ).toBe(true)
  await expect(sidebar.locator(".session-snippet").first()).toContainText(
    '<img src=x onerror="window.sidebarPwned=true">',
  )
  await expect(sidebar.locator("img")).toHaveCount(0)
  await expect(
    sidebar.locator(".session-snippet").filter({ hasText: "Plain fallback" }),
  ).toHaveText("Plain fallback for query syntax [.*")
  await expect(
    sidebar
      .locator(".session-snippet")
      .filter({ hasText: "Plain fallback" })
      .locator("mark"),
  ).toHaveCount(0)
  expect(await page.evaluate(() => (window as any).sidebarPwned)).toBeUndefined()

  await page.keyboard.press("Escape")
  await page
    .locator('.command-bar input[aria-label="Search conversations"]')
    .fill("")
  await page.getByRole("button", { name: "Toggle Sidebar" }).click()
  const recentSnippet = sidebar.locator(".session-snippet")
  await expect(recentSnippet).toHaveCount(1)
  await expect(recentSnippet).toHaveText(
    "Readable message prefix from recent session",
  )
  await expect(sidebar.locator(".session-snippet mark")).toHaveCount(0)
  expect(
    await recentSnippet.evaluate(
      (element) => getComputedStyle(element).webkitLineClamp,
    ),
  ).toBe("2")
})
