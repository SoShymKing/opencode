import { expect, test } from "@playwright/test"
import { timelinePresets } from "@opencode/session-ui/timeline/detail"
import type { SessionStatus } from "@opencode/client/promise"
import {
  assistantMessage,
  event,
  reasoningPart,
  renderedPartID,
  setupTimeline,
  sessionID,
  textPart,
  toolPart,
  userMessage,
  userID,
} from "../utils/timeline"

const detailed = timelinePresets[1].value

test("updates and resets the current response stream count with an empty reasoning summary", async ({ page }) => {
  const timeline = await setupTimeline(page, {
    messages: [
      userMessage(),
      assistantMessage([], { id: "msg_completed_count", created: 1700000000200, streamEventCount: 77 }),
      assistantMessage([reasoningPart("prt_empty_count", "")], { completed: false, streamEventCount: 0 }),
    ],
    settings: { timelineDetail: { ...detailed, thinking: { placement: "separate", details: "collapsed" } } },
    locale: "en",
  })
  const thinking = page.locator(`[data-timeline-row="Thinking"][data-message-id="${userID}"]`)
  const label = thinking.locator('[data-component="text-shimmer"]')
  await expect(label).toHaveAccessibleName("Thinking(0 streams)")
  const node = await thinking.elementHandle()
  if (!node) throw new Error("Thinking row is missing")
  const counted = event("session.status", {
    sessionID,
    status: { type: "busy", activity: { userMessageID: userID, model: "receiving", streamEventCount: 11 } },
  })
  await timeline.transport.burst([counted, counted])
  await expect(label).toHaveAccessibleName("Thinking(11 streams)")
  await page.clock.install()
  await page.clock.fastForward(8_000)
  await expect(label).toHaveAccessibleName("Thinking(11 streams)")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
  expect(await node.evaluate((element) => element.isConnected)).toBe(true)
  await timeline.send(event("session.status", {
    sessionID,
    status: { type: "busy", activity: { userMessageID: userID, model: "waiting", streamEventCount: 0 } },
  }))
  await expect(label).toHaveAccessibleName("Thinking(0 streams)")
  await timeline.send(event("session.status", {
    sessionID,
    status: { type: "busy", activity: { userMessageID: userID, model: "receiving", streamEventCount: 1 } },
  }))
  await expect(label).toHaveAccessibleName("Thinking(1 stream)")
  await timeline.send(event("session.status", {
    sessionID,
    status: { type: "busy", activity: { userMessageID: "msg_pending_other_root", model: "waiting" } },
  }))
  await expect(label).toHaveAccessibleName("Thinking(? streams)")
  expect(await node.evaluate((element) => element.isConnected)).toBe(true)
  await node.dispose()
})

for (const profile of [
  { name: "summaries off no reasoning", text: "", placement: "hidden", reasoning: false, other: false, thinking: false },
  { name: "summaries off reasoning heading", text: "## Inspecting stability", placement: "hidden", reasoning: true, other: false, thinking: false },
  { name: "summaries off with visible tool", text: "## Inspecting stability", placement: "hidden", reasoning: true, other: true, thinking: false },
  { name: "summaries on no content", text: "", placement: "separate", reasoning: false, other: false, thinking: false },
  { name: "summaries on blank reasoning", text: "   ", placement: "separate", reasoning: true, other: false, thinking: true },
  { name: "summaries on visible reasoning", text: "## Inspecting stability", placement: "separate", reasoning: true, other: false, thinking: true },
  { name: "summaries on visible tool no reasoning", text: "", placement: "separate", reasoning: false, other: true, thinking: false },
] as const) {
  test(`projects busy reasoning profile ${profile.name}`, async ({ page }) => {
    const id = "prt_reasoning_profile"
    await setupTimeline(page, {
      messages: [userMessage(), assistantMessage([
        ...(profile.reasoning ? [reasoningPart(id, profile.text)] : []),
        ...(profile.other ? [toolPart("prt_reasoning_profile_tool", "skill", "running", { name: "inspect" })] : []),
      ], { completed: false })],
      settings: { timelineDetail: { ...detailed, tools: { placement: "separate" }, thinking: { placement: profile.placement, details: "collapsed" } } },
    })
    await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(profile.thinking ? 1 : 0)
    if (profile.thinking) {
      await expect(page.locator(`[data-timeline-part-id="${renderedPartID(id)}"]`)).toBeVisible()
    }
    if (profile.placement === "hidden") {
      await expect(page.locator(`[data-timeline-part-id="${renderedPartID(id)}"]`)).toHaveCount(0)
    }
    if (profile.other) await expect(page.locator('[data-timeline-part-id="prt_reasoning_profile_tool"]')).toBeVisible()
  })
}

test("does not infer reasoning visibility from provider identity", async ({ page }) => {
  await setupTimeline(page, {
    messages: [userMessage(), assistantMessage([textPart("prt_provider_text", "No reasoning payload")], { completed: false })],
    settings: { timelineDetail: { ...detailed, thinking: { placement: "separate", details: "collapsed" } } },
  })
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(0)
  await expect(page.getByText("No reasoning payload", { exact: true })).toBeVisible()
})

test("keeps a newer raw count when a stale reconnect snapshot finishes last", async ({ page }) => {
  const arrived = Promise.withResolvers<void>()
  const released = Promise.withResolvers<void>()
  const gate = { armed: false }
  const statuses: Record<string, SessionStatus> = { [sessionID]: {
    type: "busy", activity: { userMessageID: userID, model: "receiving", streamEventCount: 11 },
  } }
  const timeline = await setupTimeline(page, {
    messages: [userMessage(), assistantMessage([reasoningPart("prt_race_count", "")], { completed: false })],
    sessionStatus: statuses,
    settings: { timelineDetail: { ...detailed, thinking: { placement: "separate", details: "collapsed" } } },
    beforeActiveResponse: () => {
      if (!gate.armed) return Promise.resolve()
      arrived.resolve()
      return released.promise
    },
  })
  const label = page.locator(`[data-timeline-row="Thinking"][data-message-id="${userID}"] [data-component="text-shimmer"]`)
  await expect(label).toHaveAccessibleName("Thinking(11 streams)")
  const connected = await timeline.transport.waitForConnection()
  await page.clock.install()
  gate.armed = true
  const reconnected = timeline.transport.waitForConnection({ after: connected.id })
  await timeline.transport.error("Synthetic reconnect")
  await reconnected
  await arrived.promise
  await timeline.send(event("session.status", { sessionID, status: {
    type: "busy", activity: { userMessageID: userID, model: "receiving", streamEventCount: 200 },
  } }))
  await expect(label).toHaveAccessibleName("Thinking(200 streams)")
  const response = page.waitForResponse((response) => new URL(response.url()).pathname === "/api/session/active")
  released.resolve()
  await (await response).finished()
  await page.clock.fastForward(8_000)
  await expect(label).toHaveAccessibleName("Thinking(200 streams)")
})
