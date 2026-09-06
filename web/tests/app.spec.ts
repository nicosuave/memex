import { expect, test, type Page } from "@playwright/test"

type SearchResult = {
  session_id: string
  record_id: string
  source_path: string
  project: string
  source: string
  role: string
  ts: number
  score: number
  snippet: string
}

type Message = {
  record_id: string
  role: string
  content: string
  content_bytes: number
  truncated: boolean
  ts: number
  tool_name?: string
}

type MockReply = {
  body?: unknown
  delay?: number
  status?: number
}

type ApiMock = {
  activityCalls: () => URL[]
  calls: URL[]
  searchCalls: () => URL[]
  sessionCalls: (id?: string) => URL[]
}

const sourcePath = "/history/codex/session.jsonl"

function result(id: string, project = `Project ${id}`): SearchResult {
  return {
    session_id: id,
    record_id: `${id}-r5000`,
    source_path: sourcePath.replace("session", id),
    project,
    source: "codex",
    role: "assistant",
    ts: 1_725_000_000_000,
    score: 0.9,
    snippet: `Search hit for ${project}`,
  }
}

function messages(
  id: string,
  offset: number,
  count: number,
  role: (index: number) => Message["role"] = () => "assistant",
): Message[] {
  return Array.from({ length: count }, (_, index) => {
    const absolute = offset + index
    const messageRole = role(absolute)
    const content = `needle ${id} message ${absolute}`
    return {
      record_id: `${id}-r${absolute}`,
      role: messageRole,
      content,
      content_bytes: new TextEncoder().encode(content).length,
      truncated: false,
      ts: 1_725_000_000_000 + absolute,
      ...(messageRole === "tool_use" ? { tool_name: "fixture_tool" } : {}),
    }
  })
}

function sessionPage(
  id: string,
  offset: number,
  count: number,
  total = count,
  version = "v1",
  role?: (index: number) => Message["role"],
) {
  return {
    session_id: id,
    source_path: sourcePath.replace("session", id),
    project: `Project ${id}`,
    source: "codex",
    started_at: 1_725_000_000_000,
    ended_at: 1_725_000_100_000,
    version,
    offset,
    total,
    messages: messages(id, offset, count, role),
  }
}

async function mockApi(
  page: Page,
  respond: (url: URL) => MockReply | Promise<MockReply | undefined> | undefined,
): Promise<ApiMock> {
  const calls: URL[] = []
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url())
    calls.push(url)
    const reply =
      (await respond(url)) ??
      (url.pathname === "/api/stats"
        ? { body: { documents: 10_000 } }
        : url.pathname === "/api/activity"
          ? {
              body: {
                metric: url.searchParams.get("metric") || "sessions",
                range: url.searchParams.get("range") || "30d",
                bucket_keys: [],
                days: 30,
                token_usage_enabled: true,
                partial: false,
                points: [],
              },
            }
          : { status: 404, body: { error: `Unhandled ${url.pathname}` } })
    if (reply.delay)
      await new Promise((resolve) => setTimeout(resolve, reply.delay))
    await route
      .fulfill({
        body: JSON.stringify(reply.body ?? {}),
        contentType: "application/json",
        status: reply.status ?? 200,
      })
      .catch(() => {})
  })
  return {
    activityCalls: () =>
      calls.filter((url) => url.pathname === "/api/activity"),
    calls,
    searchCalls: () => calls.filter((url) => url.pathname === "/api/search"),
    sessionCalls: (id?: string) =>
      calls.filter(
        (url) =>
          url.pathname === "/api/session" &&
          (id === undefined || url.searchParams.get("id") === id),
      ),
  }
}

function searchReply(results: SearchResult[], hasMore = false, offset = 0) {
  return { body: { query: "", offset, has_more: hasMore, results } }
}

async function chooseTimeRange(page: Page, value: "24h" | "7d" | "30d" | "all") {
  await page.getByRole("combobox", { name: "Time range" }).click()
  await page
    .getByRole("option", { name: value === "all" ? "All" : value, exact: true })
    .click()
}

async function scrollTranscriptTo(page: Page, edge: "start" | "end") {
  await page
    .getByRole("region", { name: "Transcript" })
    .locator(".transcript-scroll")
    .evaluate((element, targetEdge) => {
      element.dispatchEvent(new WheelEvent("wheel", { bubbles: true }))
      element.scrollTop = targetEdge === "start" ? 0 : element.scrollHeight
      element.dispatchEvent(new Event("scroll"))
    }, edge)
}

test("mode and visibility toggles stay local and responsive for a large transcript", async ({
  page,
}) => {
  const largePage = sessionPage("large", 0, 240, 240, "v1", (index) =>
    index % 3 === 0 ? "tool_use" : index % 3 === 1 ? "reasoning" : "assistant",
  )
  largePage.messages = largePage.messages.map((message, index) => {
    const content =
      message.role === "tool_use"
        ? JSON.stringify({
            description: `Large tool request ${index}`,
            command: "printf fixture ".repeat(700),
            metadata: {
              nested: Array.from({ length: 80 }, (_, item) => ({
                item,
                ok: true,
              })),
            },
          })
        : `# needle large message ${index}\n\n${"Substantial markdown fixture text. ".repeat(360)}\n\nEND large message ${index}`
    return {
      ...message,
      content,
      content_bytes: new TextEncoder().encode(content).length,
    }
  })
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([result("large")])
    if (url.pathname === "/api/session") return { body: largePage }
  })

  await page.goto(
    "/?session=large&path=%2Fhistory%2Fcodex%2Flarge.jsonl&record=large-r2&mode=history",
  )
  await expect(
    page.getByText("needle large message 2", { exact: true }),
  ).toBeVisible()
  const settledSearches = api.searchCalls().length
  const settledSessions = api.sessionCalls().length
  const transcript = page.getByRole("region", { name: "Transcript" })
  await transcript.evaluate((element) =>
    element.setAttribute("data-mount-token", "same"),
  )
  const expandedRow = transcript.locator('[data-record-id="large-r2"]')
  await expect(expandedRow.getByText(/END large message 2/)).toBeVisible()

  for (let index = 0; index < 3; index += 1) {
    await page.getByRole("tab", { name: "Matches" }).click()
    await page.getByRole("tab", { name: "History" }).click()
    await page.getByRole("button", { name: "Show reasoning" }).click()
    await page.getByRole("button", { name: "Show tool calls" }).click()
  }

  await expect(transcript).toHaveCount(1)
  await expect(transcript).toHaveAttribute("data-mount-token", "same")
  await expect(expandedRow.getByText(/END large message 2/)).toBeVisible()
  expect(api.searchCalls()).toHaveLength(settledSearches)
  expect(api.sessionCalls()).toHaveLength(settledSessions)
  expect(await transcript.locator("article").count()).toBeLessThan(100)
  await page.getByRole("button", { name: /Use (light|dark) theme/ }).click()
  await expect(
    page.getByRole("button", { name: /Use (light|dark) theme/ }),
  ).toBeEnabled()
})

test("a newer search result wins even when an older request finishes last", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname !== "/api/search") return
    const query = url.searchParams.get("q") || ""
    if (query === "slow")
      return { ...searchReply([result("slow", "Slow Project")]), delay: 500 }
    if (query === "fast") return searchReply([result("fast", "Fast Project")])
    return searchReply([])
  })
  await page.goto("/")
  const search = page.getByRole("combobox", { name: "Search conversations" })
  await search.fill("slow")
  await expect
    .poll(() =>
      api.searchCalls().some((url) => url.searchParams.get("q") === "slow"),
    )
    .toBe(true)
  await search.fill("fast")

  await expect(page.getByRole("option", { name: /Fast Project/ })).toBeVisible()
  await page.waitForTimeout(550)
  await expect(page.getByRole("option", { name: /Slow Project/ })).toHaveCount(
    0,
  )
})

test("a deep-linked session outside the search page remains selected", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("other", "Other Project")])
    if (
      url.pathname === "/api/session" &&
      url.searchParams.get("id") === "deep"
    ) {
      return { body: sessionPage("deep", 4980, 40, 10_000) }
    }
  })

  await page.goto(
    "/?q=needle&session=deep&path=%2Fhistory%2Fcodex%2Fdeep.jsonl&record=deep-r5000",
  )
  await expect(page.getByText("needle deep message 5000")).toBeVisible()
  await expect.poll(() => api.sessionCalls("deep").length).toBe(1)
  expect(api.sessionCalls("deep")[0].searchParams.get("around")).toBe(
    "deep-r5000",
  )

  await page
    .getByRole("textbox", { name: "Search conversations" })
    .fill("different")
  await expect
    .poll(() =>
      api
        .searchCalls()
        .some((url) => url.searchParams.get("q") === "different"),
    )
    .toBe(true)
  await expect(page.getByText("needle deep message 5000")).toBeVisible()
  expect(api.sessionCalls("other")).toHaveLength(0)
})

test("returning Home cancels a pending transcript without leaking its result", async ({
  page,
}) => {
  await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([result("pending")])
    if (url.pathname === "/api/session") {
      return { body: sessionPage("pending", 9960, 40, 10_000), delay: 500 }
    }
  })
  await page.goto("/?session=pending&path=%2Fhistory%2Fcodex%2Fpending.jsonl")
  await page.getByRole("button", { name: "Back to sessions" }).click()
  await expect(
    page.getByRole("combobox", { name: "Search conversations" }),
  ).toBeVisible()
  await page.waitForTimeout(550)
  await expect(page.getByText("needle pending message 9999")).toHaveCount(0)
  expect(new URL(page.url()).searchParams.has("session")).toBe(false)
})

test("a stale 401 from a canceled session cannot replace current content", async ({
  page,
}) => {
  let releaseOld: (() => void) | undefined
  await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("old"), result("current")])
    if (
      url.pathname === "/api/session" &&
      url.searchParams.get("id") === "old"
    ) {
      return new Promise<MockReply>((resolve) => {
        releaseOld = () =>
          resolve({ status: 401, body: { error: "Authentication required" } })
      })
    }
    if (
      url.pathname === "/api/session" &&
      url.searchParams.get("id") === "current"
    ) {
      return { body: sessionPage("current", 0, 4) }
    }
  })
  await page.goto("/?session=old&path=%2Fhistory%2Fcodex%2Fold.jsonl")
  const currentSession = page.locator('[data-session-id="current"]')
  await expect(currentSession).toBeAttached()
  await currentSession.evaluate((element: HTMLElement) => element.click())
  await expect(page.getByText("needle current message 0")).toBeVisible()
  releaseOld?.()
  await page.waitForTimeout(100)
  await expect(page.getByText("needle current message 0")).toBeVisible()
  await expect(page.getByText("Open Memex from your terminal")).toHaveCount(0)
})

test("a selected reasoning hit stays visible without changing visibility filters", async ({
  page,
}) => {
  const hidden = sessionPage("hidden", 4980, 40, 10_000)
  hidden.messages = hidden.messages.map((message) =>
    message.record_id === "hidden-r5000"
      ? { ...message, role: "reasoning" }
      : message,
  )
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([result("hidden")])
    if (url.pathname === "/api/session") return { body: hidden }
  })
  await page.goto(
    "/?q=needle&session=hidden&path=%2Fhistory%2Fcodex%2Fhidden.jsonl&record=hidden-r5000",
  )
  const settledSessions = api.sessionCalls("hidden").length
  await expect(page.getByText("needle hidden message 5000")).toBeVisible()
  await expect(
    page.getByRole("button", { name: "Reveal hidden match" }),
  ).toHaveCount(0)
  await expect(
    page.getByRole("button", { name: "Show reasoning" }),
  ).toHaveAttribute("aria-pressed", "false")
  expect(api.sessionCalls("hidden")).toHaveLength(settledSessions)
})

test("a 10k-message session opens one bounded tail page and loads earlier near the boundary", async ({
  page,
}) => {
  let releaseEarlier: (() => void) | undefined
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("huge", "Huge Project")])
    if (url.pathname === "/api/session") {
      if (url.searchParams.has("offset") || url.searchParams.has("before")) {
        const limit = Number(url.searchParams.get("limit"))
        const offset = url.searchParams.has("before")
          ? Number(url.searchParams.get("before")) - limit
          : Number(url.searchParams.get("offset"))
        return new Promise<MockReply>((resolve) => {
          releaseEarlier = () => resolve({ body: sessionPage("huge", offset, limit, 10_000) })
        })
      }
      return { body: sessionPage("huge", 9960, 40, 10_000) }
    }
  })
  await page.goto("/")
  await page.getByRole("option", { name: /Huge Project/ }).click()
  await expect(page.getByText("needle huge message 9988")).toBeVisible()

  await page.waitForTimeout(250)
  expect(api.sessionCalls("huge")).toHaveLength(1)
  const initial = api.sessionCalls("huge")[0]
  expect(initial.searchParams.get("tail")).toBe("true")
  expect(initial.searchParams.get("limit")).toBe("40")
  expect(initial.searchParams.get("session_source")).toBe("codex")

  await page.getByRole("tab", { name: "History" }).click()
  await expect(page.getByText("needle huge message 9998")).toBeVisible()
  await expect(
    page.getByRole("button", { name: /Load (earlier|later) messages/ }),
  ).toHaveCount(0)
  await scrollTranscriptTo(page, "start")
  await scrollTranscriptTo(page, "start")
  await scrollTranscriptTo(page, "start")
  await expect.poll(() => api.sessionCalls("huge").length).toBe(2)
  releaseEarlier?.()
  const earlier = api.sessionCalls("huge")[1]
  const earlierBoundary = earlier.searchParams.has("before")
    ? Number(earlier.searchParams.get("before"))
    : Number(earlier.searchParams.get("offset"))
  expect(earlierBoundary).toBeLessThanOrEqual(9960)
  expect(Number(earlier.searchParams.get("limit"))).toBeLessThanOrEqual(100)
  expect(earlier.searchParams.get("version")).toBe("v1")
  await expect(
    page.getByText(/9,?921–10,?000 of 10,?000 messages/),
  ).toBeVisible()
})

test("around pages expand in both directions and survive a cached navigation revisit", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("middle"), result("second")])
    if (url.pathname !== "/api/session") return
    const id = url.searchParams.get("id") || ""
    const limit = Number(url.searchParams.get("limit"))
    const offset = url.searchParams.has("before")
      ? Number(url.searchParams.get("before")) - limit
      : url.searchParams.has("offset")
        ? Number(url.searchParams.get("offset"))
        : 4980
    return { body: sessionPage(id, offset, limit, 10_000) }
  })
  await page.goto(
    "/?session=middle&path=%2Fhistory%2Fcodex%2Fmiddle.jsonl&record=middle-r5000",
  )
  await expect(page.getByText("needle middle message 5000")).toBeVisible()
  await page.getByRole("tab", { name: "History" }).click()
  await scrollTranscriptTo(page, "start")
  await expect.poll(() => api.sessionCalls("middle").length).toBe(2)
  await scrollTranscriptTo(page, "end")
  await expect.poll(() => api.sessionCalls("middle").length).toBe(3)
  await expect(page.getByRole("region", { name: "Transcript" })).toHaveAttribute(
    "aria-busy",
    "false",
  )
  const visibleRecord = await page.locator(".transcript-scroll").evaluate((element) => {
    const bounds = element.getBoundingClientRect()
    return Array.from(element.querySelectorAll("[data-record-id]")).find((row) => {
      const rect = row.getBoundingClientRect()
      return rect.bottom > bounds.top && rect.top < bounds.bottom
    })?.getAttribute("data-record-id")
  })
  expect(visibleRecord).toBeTruthy()

  await page.getByRole("button", { name: "Toggle Sidebar" }).click()
  await page.locator('[data-session-id="second"]').click()
  await expect(page.getByText(/needle second message/).first()).toBeVisible()
  await page.goBack()
  await expect(page.locator(`[data-record-id="${visibleRecord}"]`)).toBeVisible()
  expect(api.sessionCalls("middle")).toHaveLength(3)
})

test("page failures and stale versions preserve loaded transcript content", async ({
  page,
}) => {
  let pageAttempt = 0
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([result("stale")])
    if (url.pathname !== "/api/session") return
    if (!url.searchParams.has("offset") && !url.searchParams.has("before")) {
      return { body: sessionPage("stale", 9960, 40, 10_000) }
    }
    pageAttempt += 1
    if (pageAttempt === 1)
      return { status: 500, body: { error: "Older page failed" } }
    return {
      status: 409,
      body: { error: "Transcript changed on disk.", version: "v2" },
    }
  })
  await page.goto(
    "/?session=stale&path=%2Fhistory%2Fcodex%2Fstale.jsonl&mode=history",
  )
  await expect(page.getByText("needle stale message 9998")).toBeVisible()

  await scrollTranscriptTo(page, "start")
  await expect(page.getByRole("alert")).toContainText("Older page failed")
  await expect(
    page.getByRole("button", { name: "Refresh transcript" }),
  ).toHaveCount(0)
  await page.waitForTimeout(150)
  expect(api.sessionCalls("stale")).toHaveLength(2)
  await expect(page.getByText("needle stale message 9960")).toBeVisible()

  await page
    .getByRole("button", { name: "Retry loading earlier messages" })
    .click()
  await expect(page.getByText("Transcript changed on disk.")).toBeVisible()
  await expect(
    page.getByRole("button", { name: "Refresh transcript" }),
  ).toBeVisible()
  await expect(page.getByText("needle stale message 9960")).toBeVisible()
  expect(api.sessionCalls("stale")).toHaveLength(3)
})

test("different hit anchors reuse one cached window and retain loaded message bytes", async ({
  page,
}) => {
  const first = { ...result("shared"), record_id: "shared-r100" }
  const second = {
    ...result("shared"),
    record_id: "shared-r110",
    snippet: "Second hit in the same session",
  }
  const initialChunk = "Initial truncated content. "
  const continuation = "Loaded continuation survives cache navigation."
  const shared = sessionPage("shared", 90, 40, 200)
  shared.messages = shared.messages.map((message) =>
    message.record_id === "shared-r100"
      ? {
          ...message,
          content: initialChunk,
          content_bytes: new TextEncoder().encode(initialChunk + continuation)
            .length,
          truncated: true,
        }
      : message,
  )
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") {
      return searchReply([
        url.searchParams.get("q") === "second" ? second : first,
      ])
    }
    if (url.pathname === "/api/session") return { body: shared }
    if (url.pathname === "/api/session/content") {
      return {
        body: {
          version: "v1",
          record_id: "shared-r100",
          offset: new TextEncoder().encode(initialChunk).length,
          content: continuation,
          truncated: false,
          content_bytes: new TextEncoder().encode(initialChunk + continuation)
            .length,
          next_offset: new TextEncoder().encode(initialChunk + continuation)
            .length,
        },
      }
    }
  })

  await page.goto(
    "/?q=needle&session=shared&session_source=codex&path=%2Fhistory%2Fcodex%2Fshared.jsonl&record=shared-r100",
  )
  const firstRow = page.locator('[data-record-id="shared-r100"]')
  await expect(firstRow).toBeVisible()
  await expect(
    firstRow.getByText(/Loaded continuation survives cache navigation/),
  ).toBeVisible()
  expect(api.sessionCalls("shared")).toHaveLength(1)

  await page
    .getByRole("textbox", { name: "Search conversations" })
    .fill("second")
  await expect
    .poll(() =>
      api.searchCalls().some((url) => url.searchParams.get("q") === "second"),
    )
    .toBe(true)
  await expect(
    firstRow.getByText(/Loaded continuation survives cache navigation/),
  ).toBeVisible()
  await page.getByRole("button", { name: "Toggle Sidebar" }).click()
  await page
    .locator('[data-session-id="shared"][href*="record=shared-r110"]')
    .click()
  await expect(page.locator('[data-record-id="shared-r110"]')).toBeVisible()
  expect(api.sessionCalls("shared")).toHaveLength(1)

  await page.goBack()
  await expect(
    firstRow.getByText(/Loaded continuation survives cache navigation/),
  ).toBeVisible()
  expect(api.sessionCalls("shared")).toHaveLength(1)
})

test("intra-message scroll position survives prepend, mode changes, and route revisit", async ({
  page,
}) => {
  let releaseEarlier: (() => void) | undefined
  const tallPage = (offset: number, id = "scroll") => {
    const payload = sessionPage(id, offset, 40, id === "scroll" ? 80 : 40)
    payload.messages = payload.messages.map((message, index) => {
      const content = `# ${id} tall message ${offset + index}\n\n${"Tall markdown content wraps within this row. ".repeat(420)}\n\nEND ${id} ${offset + index}`
      return {
        ...message,
        content,
        content_bytes: new TextEncoder().encode(content).length,
      }
    })
    return payload
  }
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("scroll"), result("away")])
    if (url.pathname !== "/api/session") return
    const id = url.searchParams.get("id") || "scroll"
    if (id === "away") return { body: tallPage(0, "away") }
    if (url.searchParams.has("before"))
      return new Promise<MockReply>((resolve) => {
        releaseEarlier = () => resolve({ body: tallPage(0) })
      })
    return { body: tallPage(40) }
  })

  await page.goto(
    "/?session=scroll&session_source=codex&path=%2Fhistory%2Fcodex%2Fscroll.jsonl&record=scroll-r40&mode=history",
  )
  const transcript = page.getByRole("region", { name: "Transcript" })
  const scroller = transcript.locator(".transcript-scroll")
  const anchor = transcript.locator('[data-record-id="scroll-r40"]')
  await expect(anchor.getByText(/END scroll 40/)).toBeVisible()
  const before = await scroller.evaluate((element) => {
    element.scrollTop = 500
    const row = element
      .querySelector('[data-record-id="scroll-r40"]')
      ?.closest(".virtual-message")
    const delta = row
      ? element.getBoundingClientRect().top - row.getBoundingClientRect().top
      : -1
    element.dispatchEvent(new Event("scroll"))
    return delta
  })
  const anchorDelta = async () =>
    scroller.evaluate((element) => {
      const row = element
        .querySelector('[data-record-id="scroll-r40"]')
        ?.closest(".virtual-message")
      if (!row) return -1
      return (
        element.getBoundingClientRect().top - row.getBoundingClientRect().top
      )
    })
  await expect.poll(anchorDelta).toBeGreaterThan(200)
  await expect.poll(() => Boolean(releaseEarlier)).toBe(true)
  releaseEarlier?.()

  await expect(page.getByText(/1–80 of 80 messages/)).toBeVisible()
  await expect
    .poll(async () => Math.abs((await anchorDelta()) - before))
    .toBeLessThan(100)
  await page.getByRole("tab", { name: "Matches" }).click()
  await page.getByRole("tab", { name: "History" }).click()
  await expect(anchor.getByText(/END scroll 40/)).toBeVisible()
  await expect
    .poll(async () => Math.abs((await anchorDelta()) - before))
    .toBeLessThan(100)

  await page.evaluate(() => {
    history.pushState(
      {},
      "",
      "/?session=away&session_source=codex&path=%2Fhistory%2Fcodex%2Faway.jsonl&mode=history",
    )
    dispatchEvent(new PopStateEvent("popstate"))
  })
  await expect(page.getByText(/away tall message/).first()).toBeVisible()
  await page.goBack()
  await expect(anchor.getByText(/END scroll 40/)).toBeVisible()
  await expect
    .poll(async () => Math.abs((await anchorDelta()) - before))
    .toBeLessThan(100)
  expect(api.sessionCalls("scroll")).toHaveLength(2)
})

test("history wheel scrolling reaches both ends and stops fetching at exhaustion", async ({ page }) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([])
    if (url.pathname === "/api/session")
      return { body: sessionPage("ends", url.searchParams.has("before") ? 0 : 40, 40, 80) }
  })
  await page.goto("/?session=ends&mode=history")
  const scroller = page.locator(".transcript-scroll")
  await expect(page.locator('[data-record-id="ends-r79"]')).toBeInViewport()
  await scroller.hover()
  await page.mouse.wheel(0, -100000)
  await expect(page.getByText(/1–80 of 80 messages/)).toBeVisible()
  await expect(async () => {
    await page.mouse.wheel(0, -600)
    await expect(page.locator('[data-record-id="ends-r0"]')).toBeInViewport({
      timeout: 100,
    })
  }).toPass({ timeout: 10000, intervals: [100] })
  await expect(async () => {
    await page.mouse.wheel(0, 600)
    await expect(page.locator('[data-record-id="ends-r79"]')).toBeInViewport({
      timeout: 100,
    })
  }).toPass({ timeout: 10000, intervals: [100] })
  await page.waitForTimeout(200)
  expect(api.sessionCalls("ends")).toHaveLength(2)
})

test("mixed-height transcript scrolling stays filled and moves in one direction", async ({
  page,
}) => {
  const payload = sessionPage("mixed-height", 0, 80)
  payload.messages = payload.messages.map((message, index) => ({
    ...message,
    content:
      index % 5 === 0
        ? `Long message ${index}\n\n${"Variable height content. ".repeat(900)}`
        : index % 3 === 0
          ? `Medium message ${index}\n\n${"A few wrapped lines. ".repeat(45)}`
          : `Short message ${index}`,
  }))
  await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([])
    if (url.pathname === "/api/session") return { body: payload }
  })
  await page.goto(
    "/?session=mixed-height&record=mixed-height-r0&mode=history",
  )
  const scroller = page.locator(".transcript-scroll")
  await expect(page.locator('[data-record-id="mixed-height-r0"]')).toBeInViewport()
  await scroller.hover()

  type ScrollSample = { offset: number; filled: boolean }
  const startSampling = () =>
    scroller.evaluate((element) => {
      const monitored = element as HTMLDivElement & {
        __scrollSamples?: Array<{ offset: number; filled: boolean }>
        __sampleScroll?: boolean
      }
      monitored.__scrollSamples = []
      monitored.__sampleScroll = true
      const sample = () => {
        const viewport = monitored.getBoundingClientRect()
        const filled = Array.from(
          monitored.querySelectorAll<HTMLElement>(".virtual-message"),
        ).some((row) => {
          const bounds = row.getBoundingClientRect()
          return bounds.bottom > viewport.top && bounds.top < viewport.bottom
        })
        monitored.__scrollSamples?.push({
          offset: monitored.scrollTop,
          filled,
        })
        if (monitored.__sampleScroll) requestAnimationFrame(sample)
      }
      requestAnimationFrame(sample)
    })
  const stopSampling = () =>
    scroller.evaluate((element) => {
      const monitored = element as HTMLDivElement & {
        __scrollSamples?: ScrollSample[]
        __sampleScroll?: boolean
      }
      monitored.__sampleScroll = false
      return monitored.__scrollSamples || []
    })

  await startSampling()
  for (let step = 0; step < 30; step += 1) {
    await page.mouse.wheel(0, 450)
    await page.waitForTimeout(20)
  }
  const downward = await stopSampling()
  expect(downward.length).toBeGreaterThan(20)
  expect(downward.every((sample) => sample.filled)).toBe(true)
  for (let index = 1; index < downward.length; index += 1)
    expect(downward[index].offset).toBeGreaterThanOrEqual(
      downward[index - 1].offset - 2,
    )

  await startSampling()
  for (let step = 0; step < 30; step += 1) {
    await page.mouse.wheel(0, -450)
    await page.waitForTimeout(20)
  }
  const upward = await stopSampling()
  expect(upward.length).toBeGreaterThan(20)
  expect(upward.every((sample) => sample.filled)).toBe(true)
  for (let index = 1; index < upward.length; index += 1)
    expect(upward[index].offset).toBeLessThanOrEqual(
      upward[index - 1].offset + 2,
    )
})

test("a missing around anchor falls back to the bounded tail with an explanation", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([])
    if (url.pathname !== "/api/session") return
    if (url.searchParams.has("around")) {
      return { status: 404, body: { error: "record no longer indexed" } }
    }
    if (url.searchParams.has("before"))
      return { body: sessionPage("missing", 920, 40, 1_000) }
    return { body: sessionPage("missing", 960, 40, 1_000) }
  })
  await page.goto(
    "/?q=needle&session=missing&session_source=codex&path=%2Fhistory%2Fcodex%2Fmissing.jsonl&record=missing-r5000",
  )
  await expect(
    page.getByText(
      "Selected hit is no longer available. Showing latest messages.",
    ),
  ).toBeVisible()
  await expect(
    page.getByRole("button", { name: "Refresh transcript" }),
  ).toHaveCount(0)
  await expect(page.getByText("needle missing message 988")).toBeVisible()
  expect(api.sessionCalls("missing")).toHaveLength(2)
  expect(api.sessionCalls("missing")[0].searchParams.get("around")).toBe(
    "missing-r5000",
  )
  expect(api.sessionCalls("missing")[1].searchParams.get("tail")).toBe("true")
  expect(api.sessionCalls("missing")[1].searchParams.get("limit")).toBe("40")
  await page.getByRole("tab", { name: "History" }).click()
  await expect(page.getByText("needle missing message 998")).toBeVisible()
  await scrollTranscriptTo(page, "start")
  await expect.poll(() => api.sessionCalls("missing").length).toBe(3)
  expect(api.sessionCalls("missing")[2].searchParams.get("before")).toBe("960")
  await expect(page.getByText(/921–1000 of 1000 messages/)).toBeVisible()
})

test("transcript recovery appears only after an error", async ({ page }) => {
  let attempts = 0
  await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([result("refresh")])
    if (url.pathname !== "/api/session") return
    attempts += 1
    if (attempts === 1)
      return { status: 500, body: { error: "Transcript unavailable" } }
    return { body: sessionPage("refresh", 0, 4) }
  })
  await page.goto(
    "/?session=refresh&session_source=codex&path=%2Fhistory%2Fcodex%2Frefresh.jsonl&mode=history",
  )
  const content = page.getByText("needle refresh message 0")
  await expect(page.getByRole("alert")).toContainText("Transcript unavailable")
  const refresh = page.getByRole("button", { name: "Refresh transcript" })
  await expect(refresh).toBeVisible()
  await refresh.click()
  await expect(content).toBeVisible()
  await expect(refresh).toHaveCount(0)
})

test("Back and Forward restore the home and transcript views", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("nav", "Navigation Project")])
    if (url.pathname === "/api/session")
      return { body: sessionPage("nav", 0, 4) }
  })
  await page.goto("/")
  await page.getByRole("option", { name: /Navigation Project/ }).click()
  await expect(page.getByRole("region", { name: "Transcript" })).toBeVisible()
  await page.goBack()
  await expect(
    page.getByRole("combobox", { name: "Search conversations" }),
  ).toBeVisible()
  await page.goForward()
  await expect(page.getByRole("region", { name: "Transcript" })).toBeVisible()
  expect(api.sessionCalls("nav")).toHaveLength(1)
})

test("the shared time range drives search and activity for every value", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    const range = url.searchParams.get("range") || "30d"
    if (url.pathname === "/api/search")
      return searchReply([result(range, `Range ${range}`)])
    if (url.pathname === "/api/activity")
      return {
        body: {
          metric: url.searchParams.get("metric") || "sessions",
          range,
          bucket_keys: [range],
          token_usage_enabled: true,
          partial: false,
          points: [{ date: range, source: "codex", value: 1 }],
        },
      }
  })
  await page.goto("/")

  for (const range of ["30d", "24h", "7d", "all"] as const) {
    if (range !== "30d") await chooseTimeRange(page, range)
    await expect(page.getByRole("option", { name: `Range ${range}` })).toBeVisible()
    await expect
      .poll(() =>
        api
          .activityCalls()
          .some((url) => url.searchParams.get("range") === range),
      )
      .toBe(true)
  }

  expect(
    new Set(api.searchCalls().map((url) => url.searchParams.get("range"))),
  ).toEqual(new Set(["24h", "7d", "30d", "all"]))
  expect(
    new Set(api.activityCalls().map((url) => url.searchParams.get("range"))),
  ).toEqual(new Set(["24h", "7d", "30d", "all"]))
  expect(new URL(page.url()).searchParams.get("range")).toBe("all")
})

test("direct range URLs and Back and Forward preserve transcript content", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("range-nav", "Range navigation")])
    if (url.pathname === "/api/session")
      return { body: sessionPage("range-nav", 0, 4) }
  })
  await page.goto(
    "/?range=7d&session=range-nav&session_source=codex&path=%2Fhistory%2Fcodex%2Frange-nav.jsonl",
  )
  const transcript = page.getByRole("region", { name: "Transcript" })
  const range = page.getByRole("combobox", { name: "Time range" })
  await expect(transcript).toBeVisible()
  await expect(range).toContainText("7d")
  await expect.poll(() => api.sessionCalls("range-nav").length).toBe(1)
  await expect(
    page.locator('[data-session-id="range-nav"]'),
  ).toHaveAttribute("href", /range=7d/)

  await chooseTimeRange(page, "all")
  await expect(range).toContainText("All")
  expect(new URL(page.url()).searchParams.get("range")).toBe("all")
  await page.goBack()
  await expect(range).toContainText("7d")
  await expect(transcript).toBeVisible()
  await page.goForward()
  await expect(range).toContainText("All")
  await expect(transcript).toBeVisible()
  expect(api.sessionCalls("range-nav")).toHaveLength(1)
})

test("stale search and activity responses stay hidden after range and metric changes", async ({
  page,
}) => {
  const api = await mockApi(page, (url) => {
    const range = url.searchParams.get("range") || "30d"
    if (url.pathname === "/api/search")
      return range === "30d"
        ? { ...searchReply([result("old-range", "Old range")]), delay: 500 }
        : searchReply([result("current-range", "Current range")])
    if (url.pathname === "/api/activity") {
      const metric = url.searchParams.get("metric") || "sessions"
      const current = range === "7d" && metric === "tokens"
      return {
        body: {
          metric,
          range,
          bucket_keys: [range],
          token_usage_enabled: current,
          partial: false,
          points: [
            { date: range, source: "codex", value: current ? 7 : 999 },
          ],
        },
        delay: current ? 0 : metric === "tokens" ? 400 : 500,
      }
    }
  })
  await page.goto("/")
  await expect.poll(() => api.searchCalls().length).toBeGreaterThan(0)
  await expect.poll(() => api.activityCalls().length).toBeGreaterThan(0)
  await page.getByRole("combobox", { name: "Activity metric" }).click()
  await page.getByRole("option", { name: "Tokens", exact: true }).click()
  await expect
    .poll(() =>
      api
        .activityCalls()
        .some((url) => url.searchParams.get("metric") === "tokens"),
    )
    .toBe(true)
  await chooseTimeRange(page, "7d")

  await expect(page.getByRole("option", { name: "Current range" })).toBeVisible()
  await expect(page.getByText("7 tokens", { exact: true })).toBeVisible()
  await expect(
    page.getByRole("img", { name: "7 tokens over the last 7 days" }),
  ).toBeVisible()
  await page.waitForTimeout(550)
  await expect(page.getByRole("option", { name: "Old range" })).toHaveCount(0)
  await expect(page.getByText(/999 (sessions|tokens)/)).toHaveCount(0)
  await expect(page.getByText(/Token usage is disabled/)).toHaveCount(0)
})

test("changing range cancels stale pagination and restarts its offset", async ({
  page,
}) => {
  let releaseOldPage: (() => void) | undefined
  const api = await mockApi(page, (url) => {
    if (url.pathname !== "/api/search") return
    const range = url.searchParams.get("range") || "30d"
    const offset = Number(url.searchParams.get("offset") || 0)
    if (range === "30d" && offset === 0)
      return searchReply([result("old-first", "Old first")], true)
    if (range === "30d")
      return new Promise<MockReply>((resolve) => {
        releaseOldPage = () =>
          resolve(searchReply([result("old-page", "Old page")], false, offset))
      })
    if (offset === 0)
      return searchReply([result("new-first", "New first")], true)
    return searchReply([result("new-page", "New page")], false, offset)
  })
  await page.goto("/")
  await expect(page.getByRole("option", { name: "Old first" })).toBeVisible()
  await expect
    .poll(() =>
      api
        .searchCalls()
        .some(
          (url) =>
            url.searchParams.get("range") === "30d" &&
            url.searchParams.get("offset") === "1",
        ),
    )
    .toBe(true)

  await chooseTimeRange(page, "7d")
  await expect(page.getByRole("option", { name: "New first" })).toBeVisible()
  releaseOldPage?.()
  await page.waitForTimeout(50)
  await expect(page.getByRole("option", { name: "Old page" })).toHaveCount(0)
  await expect(page.getByRole("option", { name: "New page" })).toBeVisible()
  const currentPage = api
    .searchCalls()
    .find(
      (url) =>
        url.searchParams.get("range") === "7d" &&
        url.searchParams.get("offset") === "1",
    )
  expect(currentPage).toBeTruthy()
})

test("mobile users can reach results beyond twelve and open the session controls", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 })
  const results = Array.from({ length: 15 }, (_, index) =>
    result(`mobile-${index + 1}`, `Mobile Project ${index + 1}`),
  )
  await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply(results)
    if (url.pathname === "/api/session") {
      const id = url.searchParams.get("id") || "mobile-15"
      return { body: sessionPage(id, 0, 4) }
    }
  })
  await page.goto("/")
  const fifteenth = page.getByRole("option", { name: /Mobile Project 15/ })
  await fifteenth.scrollIntoViewIfNeeded()
  await expect(fifteenth).toBeVisible()
  await fifteenth.click()
  await expect(page.getByRole("region", { name: "Transcript" })).toBeVisible()

  const sidebar = page.getByRole("button", { name: "Toggle Sidebar" })
  await expect(sidebar).toBeVisible()
  await sidebar.click()
  await expect(page.locator('[data-session-id="mobile-1"]')).toBeVisible()
  await page.getByRole("button", { name: "memex" }).click()
  await expect(
    page.getByRole("option", { name: /Mobile Project 15/ }),
  ).toBeVisible()
})

test("visibility controls reveal and hide each supported message kind", async ({
  page,
}) => {
  const roles = [
    "assistant",
    "reasoning",
    "thinking",
    "tool_use",
    "tool_result",
    "system",
    "user",
  ]
  const fixture = sessionPage(
    "visibility",
    0,
    roles.length,
    roles.length,
    "v1",
    (index) => roles[index],
  )
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("visibility")])
    if (url.pathname === "/api/session") return { body: fixture }
  })
  await page.goto("/?session=visibility&mode=history")
  const reasoning = page.getByRole("button", { name: "Show reasoning" })
  const tools = page.getByRole("button", { name: "Show tool calls" })
  const row = (index: number) =>
    page.locator(`[data-record-id="visibility-r${index}"]`)
  await expect(row(0)).toBeVisible()
  await expect.poll(() => api.searchCalls().length).toBe(1)
  const searchCalls = api.searchCalls().length
  const sessionCalls = api.sessionCalls().length
  await expect(reasoning).toHaveText("2")
  await expect(tools).toHaveText("3")
  for (const mode of ["History", "Matches"]) {
    await page.getByRole("tab", { name: mode }).click()
    await expect(reasoning).toHaveAttribute("aria-pressed", "false")
    await expect(tools).toHaveAttribute("aria-pressed", "false")
    for (const index of [1, 2, 3, 4, 5]) await expect(row(index)).toHaveCount(0)
    await reasoning.click()
    await expect(reasoning).toHaveAttribute("aria-pressed", "true")
    for (const index of [1, 2]) await expect(row(index)).toBeVisible()
    for (const index of [3, 4, 5]) await expect(row(index)).toHaveCount(0)
    await tools.click()
    await expect(tools).toHaveAttribute("aria-pressed", "true")
    for (const index of [3, 4, 5]) await expect(row(index)).toBeVisible()
    await reasoning.click()
    for (const index of [1, 2]) await expect(row(index)).toHaveCount(0)
    for (const index of [3, 4, 5]) await expect(row(index)).toBeVisible()
    await tools.click()
    for (const index of [3, 4, 5]) await expect(row(index)).toHaveCount(0)
    await expect(row(0)).toBeVisible()
    await expect(row(6)).toBeVisible()
  }
  expect(api.searchCalls()).toHaveLength(searchCalls)
  expect(api.sessionCalls()).toHaveLength(sessionCalls)
})

test("visibility availability updates while automatic paging traverses hidden kinds", async ({
  page,
}) => {
  const roles = [
    "thinking",
    "tool_result",
    "system",
    "user",
    "assistant",
    "developer",
  ]
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("availability")])
    if (url.pathname === "/api/session") {
      const offset = url.searchParams.has("before") ? 0 : 3
      return {
        body: sessionPage(
          "availability",
          offset,
          3,
          6,
          "v1",
          (index) => roles[index],
        ),
      }
    }
  })
  await page.goto("/?session=availability&mode=history")
  const reasoning = page.getByRole("button", { name: "Show reasoning" })
  const tools = page.getByRole("button", { name: "Show tool calls" })
  await expect.poll(() => api.sessionCalls("availability").length).toBe(2)
  await expect(reasoning).toBeEnabled()
  await expect(reasoning).toHaveText("1")
  await expect(tools).toBeEnabled()
  await expect(tools).toHaveText("2")
})

test("automatic history paging crosses an all-hidden page", async ({ page }) => {
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search")
      return searchReply([result("hidden-page")])
    if (url.pathname !== "/api/session") return
    if (!url.searchParams.has("before"))
      return { body: sessionPage("hidden-page", 80, 40, 120) }
    const before = Number(url.searchParams.get("before"))
    const limit = Number(url.searchParams.get("limit"))
    return {
      body: sessionPage(
        "hidden-page",
        before - limit,
        limit,
        120,
        "v1",
        (index) => (index >= 40 ? "system" : "assistant"),
      ),
    }
  })

  await page.goto("/?session=hidden-page&mode=history")
  await expect(page.getByText("needle hidden-page message 118")).toBeVisible()
  await scrollTranscriptTo(page, "start")
  await expect.poll(() => api.sessionCalls("hidden-page").length).toBe(3)
  await expect(page.getByText(/1–120 of 120 messages/)).toBeVisible()
  await scrollTranscriptTo(page, "start")
  await expect(page.getByText("needle hidden-page message 0")).toBeVisible()
  expect(
    api
      .sessionCalls("hidden-page")
      .slice(1)
      .map((url) => url.searchParams.get("before")),
  ).toEqual(["80", "40"])
})

for (const mobile of [false, true]) {
  test(`long messages scroll with the transcript on ${mobile ? "mobile" : "desktop"}`, async ({ page }) => {
    if (mobile) await page.setViewportSize({ width: 390, height: 844 })
    const payload = sessionPage("inline", 0, 1)
    payload.messages[0].content = Array.from({ length: 100 }, (_, index) =>
      `Paragraph ${index}: ${"Long message text should flow within the transcript. ".repeat(8)}`,
    ).join("\n\n") + "\n\nEND INLINE MESSAGE"
    await mockApi(page, (url) => {
      if (url.pathname === "/api/search") return searchReply([])
      if (url.pathname === "/api/session") return { body: payload }
    })
    await page.goto("/?session=inline&record=inline-r0&mode=history")
    const scroller = page.locator(".transcript-scroll")
    const message = page.locator('[data-record-id="inline-r0"]')
    await expect(message.getByText("END INLINE MESSAGE")).toHaveCount(1)
    await expect(page.getByRole("button", { name: /Expand message|Collapse message|Load more message content/ })).toHaveCount(0)
    expect(await message.evaluate((element) => element.clientHeight)).toBeGreaterThan(
      await scroller.evaluate((element) => element.clientHeight),
    )
    const before = await scroller.evaluate((element) => element.scrollTop)
    await message.locator("p").first().hover()
    await page.mouse.wheel(0, 500)
    await expect.poll(() => scroller.evaluate((element) => element.scrollTop)).toBeGreaterThan(before + 100)
    expect(await message.evaluate((element) => Array.from(element.querySelectorAll("*")).filter((child) => {
      const style = getComputedStyle(child)
      return /auto|scroll/.test(style.overflowY) && child.scrollHeight > child.clientHeight + 1
    }).length)).toBe(0)
  })
}

test("message continuation loads at its inline boundary and pauses after failure", async ({ page }) => {
  const payload = sessionPage("continued", 0, 1)
  const initial = "Initial paragraph with unicode café.\n\n".repeat(80)
  const rest = "Remaining message content loaded inline."
  payload.messages[0] = {
    ...payload.messages[0], content: initial, truncated: true,
    content_bytes: new TextEncoder().encode(initial + rest).length,
  }
  let attempts = 0
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([])
    if (url.pathname === "/api/session") return { body: payload }
    if (url.pathname === "/api/session/content") {
      attempts += 1
      if (attempts === 1) return { status: 503, body: { error: "Content temporarily unavailable" } }
      return { body: {
        record_id: "continued-r0", version: "v1",
        offset: new TextEncoder().encode(initial).length,
        content: rest, truncated: false,
        content_bytes: new TextEncoder().encode(initial + rest).length,
      } }
    }
  })
  await page.goto("/?session=continued&record=continued-r0&mode=history")
  const scroller = page.locator(".transcript-scroll")
  await expect(page.locator('[data-record-id="continued-r0"]')).toBeVisible()
  expect(attempts).toBe(0)
  await scroller.evaluate((element) => { element.scrollTop = element.scrollHeight })
  await expect(page.getByRole("alert")).toContainText("Content temporarily unavailable")
  await expect(
    page.getByRole("button", { name: "Refresh transcript" }),
  ).toHaveCount(0)
  await page.waitForTimeout(200)
  expect(attempts).toBe(1)
  await page.getByRole("button", { name: "Retry loading message" }).click()
  await expect(page.getByText(rest)).toHaveCount(1)
  const contentCalls = api.calls.filter((url) => url.pathname === "/api/session/content")
  expect(contentCalls).toHaveLength(2)
  expect(contentCalls[1].searchParams.get("offset")).toBe(String(new TextEncoder().encode(initial).length))
  await expect(page.getByRole("button", { name: "Retry loading message" })).toHaveCount(0)
})

test("search filters the chart and clearing restores the baseline despite stale responses", async ({ page }) => {
  let releaseSlow: (() => void) | undefined
  const api = await mockApi(page, (url) => {
    if (url.pathname === "/api/search") return searchReply([])
    if (url.pathname !== "/api/activity") return
    const query = url.searchParams.get("q") || ""
    const reply = {
      body: {
        metric: "sessions", range: "30d", bucket_keys: ["2026-09-05"],
        token_usage_enabled: true, partial: false,
        points: [{ date: "2026-09-05", source: "codex", value: query ? 3 : 200 }],
      },
    }
    if (query === "slow") return new Promise<MockReply>((resolve) => {
      releaseSlow = () => resolve(reply)
    })
    return reply
  })
  await page.goto("/")
  await expect(page.getByText("200 sessions", { exact: true })).toBeVisible()
  const search = page.getByRole("combobox", { name: "Search conversations" })
  await search.fill("trino")
  await expect(page.getByText("3 sessions", { exact: true })).toBeVisible()
  expect(api.activityCalls().at(-1)?.searchParams.get("q")).toBe("trino")
  await search.fill("slow")
  await expect.poll(() => Boolean(releaseSlow)).toBe(true)
  await search.fill("")
  await expect(page.getByText("200 sessions", { exact: true })).toBeVisible()
  releaseSlow?.()
  await page.waitForTimeout(200)
  await expect(page.getByText("200 sessions", { exact: true })).toBeVisible()
  expect(api.activityCalls().at(-1)?.searchParams.has("q")).toBe(false)
})
