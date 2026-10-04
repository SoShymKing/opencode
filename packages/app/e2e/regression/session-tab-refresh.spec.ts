import { base64Encode } from "@opencode-ai/core/util/encode"
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

test.use({ viewport: { width: 646, height: 1385 } })

test("refreshes only the recent tail when revisiting a stale tab with loaded history", async ({ page }) => {
  const assistants = Array.from({ length: 219 }, (_, index) =>
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
  await test.step("Given a cached tab expanded by real 20 and 200 message pages", async () => {
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
      message: (id, messageID) => id === sessionID ? messages.find((item) => item.info.id === messageID) : undefined,
      onMessage: (request) => roots.push(request.messageID),
      pageMessages: (id, limit, before) => {
        if (id !== sessionID) return { items: [] }
        const end = before ? messages.findIndex((message) => message.info.id === before) : messages.length
        const start = Math.max(0, end - limit)
        const items = messages.slice(start, end).map((item) =>
          !before && pages.length > 0 && item.info.id === "msg_1219_refresh_assistant"
            ? { ...item, parts: [...item.parts, { ...textPart("prt_refresh_confirmed", "Refreshed"), sessionID, messageID: item.info.id }] }
            : item,
        )
        pages.push({ limit, before, count: items.length })
        return { items, cursor: start > 0 ? messages[start]?.info.id : undefined }
      },
    })
    await page.addInitScript(({ server, directory, sessionID, awayID }) => {
      localStorage.setItem("opencode.global.dat:server", JSON.stringify({
        list: [server],
        projects: { [server]: [{ worktree: directory, expanded: true }] },
        lastProject: { [server]: directory },
      }))
      localStorage.setItem("opencode.window.browser.dat:tabs", JSON.stringify(
        [sessionID, awayID].map((sessionId) => ({ type: "session", server, sessionId })),
      ))
    }, { server, directory, sessionID, awayID })
    await page.goto(href(sessionID))
    await transport.waitForConnection()
    await expectSessionTitle(page, title)
    await expect(page.locator('[data-timeline-part-id="prt_refresh_218"]')).toBeVisible()
    const scroller = page.locator(".scroll-view__viewport", { has: page.locator("[data-timeline-virtual-content]") })
    const history = page.waitForResponse((response) => {
      const url = new URL(response.url())
      return url.pathname === `/session/${sessionID}/message` && url.searchParams.has("before")
    })
    await scroller.hover()
    await page.mouse.wheel(0, -100_000)
    await history
    await expect.poll(() => pages.map((page) => page.limit)).toEqual([20, 200])
    expect(pages.map((page) => page.count)).toEqual([20, 200])
    expect(roots).toEqual([userID])
    await scroller.evaluate((element) => { element.scrollTop = 250 })
    await expect(page.locator('[data-timeline-part-id="prt_refresh_10"]')).toBeInViewport()
    await page.getByRole("button", { name: "Jump to latest" }).click()
    await expect(page.locator('[data-timeline-part-id="prt_refresh_218"]')).toBeInViewport()
    await page.locator(`[data-slot="titlebar-tabs"] a[href="${href(awayID)}"]`).click()
    await expectSessionTitle(page, "Refresh away")
    await page.clock.setSystemTime(await page.evaluate(() => Date.now() + 16_000))
  })

  const refreshed = page.waitForResponse((response) => {
    const url = new URL(response.url())
    return response.request().method() === "GET" && url.pathname === `/session/${sessionID}/message` && !url.searchParams.has("before")
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
    expect.soft(Number(new URL(response.url()).searchParams.get("limit")), "routine stale revisit must request only 20 messages").toBe(20)
    await expectSessionTitle(page, title)
    await expect(page.locator('[data-timeline-part-id="prt_refresh_confirmed"]')).toBeVisible()
    const historyRequests = pages.filter((page) => page.before).length
    await page.locator(".scroll-view__viewport", { has: page.locator("[data-timeline-virtual-content]") }).hover()
    await page.mouse.wheel(0, -100_000)
    await expect(page.locator('[data-timeline-part-id="prt_refresh_10"]')).toBeInViewport()
    await expect(page.locator(`[data-timeline-part-id="prt_${userID}_text"]`)).toBeVisible()
    await expect(page.locator('[data-timeline-part-id="prt_refresh_10"]').locator('xpath=ancestor::*[@data-timeline-row="AssistantPart"]')).toHaveCount(1)
    expect(pages.filter((page) => page.before)).toHaveLength(historyRequests)
  })
})
