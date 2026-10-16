import { base64Encode } from "@opencode/util/encode"
import { expect, test } from "@playwright/test"
import {
  assistantMessage,
  directory,
  project,
  session,
  sessionID,
  textPart,
  title,
  userID,
  userMessage,
} from "../performance/timeline-stability/fixture"
import { mockOpenCodeServer } from "../utils/mock-server"
import { installSseTransport } from "../utils/sse-transport"
import { expectSessionTitle } from "../utils/waits"
import { seed } from "../utils/app"

test.use({ viewport: { width: 646, height: 1385 } })

test("refreshes only the recent tail when revisiting a stale tab with loaded history", async ({ page }) => {
  const assistants = Array.from({ length: 59 }, (_, index) =>
    assistantMessage([textPart(`prt_refresh_${index}`, `Response ${index}`)], {
      id: `msg_${String(index + 1001).padStart(4, "0")}_refresh_assistant`,
      parentID: userID,
      created: 1700000001000 + index * 1_000,
    }),
  )
  const messages = [userMessage(), ...assistants]
  const awayID = "ses_refresh_away"
  const server = `http://${process.env.PLAYWRIGHT_SERVER_HOST ?? "127.0.0.1"}:${process.env.PLAYWRIGHT_SERVER_PORT ?? "4096"}`
  const href = (id: string) => `/server/${base64Encode(server)}/session/${id}`
  const pages: { limit: number; before?: string; count: number }[] = []
  const roots: string[] = []
  await test.step("Given a cached tab expanded by native message pages", async () => {
    await page.clock.install()
    await page.route(/https?:\/\/(?:127\.0\.0\.1|localhost):4096\//, (route) => route.abort())
    const transport = await installSseTransport(page, { server, retry: 20 })
    await mockOpenCodeServer(page, {
      directory,
      project: project(),
      provider: {
        all: [{ id: "opencode", name: "OpenCode", models: { "claude-opus-4-6": { id: "claude-opus-4-6", name: "Claude Opus 4.6", limit: { context: 200_000 } } } }],
        connected: ["opencode"],
        default: { providerID: "opencode", modelID: "claude-opus-4-6" },
      },
      sessions: [session(), session({ id: awayID, title: "Refresh away" })],
      message: (id, messageID) => id === sessionID ? messages.find((item) => item.id === messageID) : undefined,
      onMessage: (request) => roots.push(request.messageID),
      pageMessages: (id, limit, before) => {
        if (id !== sessionID) return { items: [] }
        const end = before ? messages.findIndex((message) => message.id === before) : messages.length
        const start = Math.max(0, end - limit)
        const items = messages.slice(start, end).map((item) =>
          !before && pages.length > 1 && item.id === "msg_1059_refresh_assistant" && item.type === "assistant"
            ? { ...item, content: [...item.content, { type: "text" as const, text: "Refreshed" }] }
            : item,
        )
        pages.push({ limit, before, count: items.length })
        return { items, cursor: start > 0 ? messages[start]?.id : undefined }
      },
    })
    await seed(page, {
      servers: [server],
      projects: { [server]: [{ worktree: directory, expanded: true }] },
      lastProject: { [server]: directory },
      tabs: [sessionID, awayID].map((session) => ({ session, server })),
    })
    await page.goto(href(sessionID))
    await transport.waitForConnection()
    await expectSessionTitle(page, title)
    await expect(page.locator('[data-timeline-part-id="msg_1059_refresh_assistant:text:0"]')).toBeVisible()
    const scroller = page.locator(".scroll-view__viewport", { has: page.locator("[data-timeline-virtual-content]") })
    const history = page.waitForResponse((response) => {
      const url = new URL(response.url())
      return url.pathname === `/api/session/${sessionID}/message` && url.searchParams.has("cursor")
    })
    await scroller.hover()
    await page.mouse.wheel(0, -100_000)
    await history
    await expect.poll(() => pages.map((page) => page.limit)).toEqual([40, 20])
    expect(pages.map((page) => page.count)).toEqual([40, 20])
    expect(roots).toEqual([])
    await scroller.evaluate((element) => { element.scrollTop = 250 })
    await expect(page.locator('[data-timeline-part-id="msg_1011_refresh_assistant:text:0"]')).toBeInViewport()
    await page.getByRole("button", { name: "Jump to latest" }).click()
    await expect(page.locator('[data-timeline-part-id="msg_1059_refresh_assistant:text:0"]')).toBeInViewport()
    await page.locator(`[data-slot="titlebar-tabs"] a[href="${href(awayID)}"]`).click()
    await expectSessionTitle(page, "Refresh away")
    await page.clock.setSystemTime(await page.evaluate(() => Date.now() + 16_000))
  })

  const refreshed = page.waitForResponse((response) => {
    const url = new URL(response.url())
    return response.request().method() === "GET" && url.pathname === `/api/session/${sessionID}/message` && !url.searchParams.has("cursor")
  })
  await test.step("When the stale cached titlebar tab is clicked", async () => {
    await page.locator(`[data-slot="titlebar-tabs"] a[href="${href(sessionID)}"]`).click()
  })
  await test.step("Then refresh stays bounded and previously loaded history survives", async () => {
    const response = await refreshed
    const body = await response.body()
    console.log("SESSION_TAB_REFRESH_BASELINE", JSON.stringify({
      requestLimit: Number(new URL(response.url()).searchParams.get("limit")),
      responseItemCount: pages.at(-1)?.count,
      serializedBytes: body.byteLength,
      pages,
    }))
    expect(Number(new URL(response.url()).searchParams.get("limit")), "routine stale revisit must request only 20 messages").toBe(20)
    await expectSessionTitle(page, title)
    await expect(page.locator('[data-timeline-part-id="msg_1059_refresh_assistant:text:1"]')).toHaveText("Refreshed")
    const historyRequests = pages.filter((page) => page.before).length
    await page.locator(".scroll-view__viewport", { has: page.locator("[data-timeline-virtual-content]") }).hover()
    await page.mouse.wheel(0, -100_000)
    await expect(page.locator('[data-timeline-part-id="msg_1011_refresh_assistant:text:0"]')).toBeInViewport()
    await expect(page.locator(`[data-timeline-row="UserMessage"][data-message-id="${userID}"]`)).toBeVisible()
    await expect(page.locator('[data-timeline-part-id="msg_1011_refresh_assistant:text:0"]').locator('xpath=ancestor::*[@data-timeline-row="AssistantPart"]')).toHaveAttribute("data-message-id", userID)
    expect(pages.filter((page) => page.before)).toHaveLength(historyRequests)
  })
})
