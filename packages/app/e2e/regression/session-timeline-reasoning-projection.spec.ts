import { expect, test } from "@playwright/test"
import {
  assistantMessage,
  assistantID,
  currentStepEvent,
  directory,
  reasoningPart,
  setupTimeline,
  sessionID,
  status,
  textPart,
  toolPart,
  userMessage,
  userID,
} from "../performance/timeline-stability/fixture"

test("updates and resets the current response stream count with an empty reasoning summary", async ({ page }) => {
  const timeline = await setupTimeline(page, {
    protocol: "v2",
    messages: [
      userMessage(),
      assistantMessage([], { id: "msg_completed_count", created: 1700000000200, streamEventCount: 77 }),
      assistantMessage([], { completed: false, created: 1700000002000, streamEventCount: 0 }),
    ],
    settings: { showReasoningSummaries: true },
    locale: "en",
  })
  const thinking = page.locator(`[data-timeline-row="Thinking"][data-message-id="${userID}"]`)
  const label = thinking.locator('[data-component="text-shimmer"]')
  await expect(label).toHaveAccessibleName("Thinking(0 streams)")
  const node = await thinking.elementHandle()
  if (!node) throw new Error("Thinking row is missing")
  const counted = currentStepEvent({
    id: "evt_stream_count_11",
    type: "session.next.step.stream.updated",
    location: { directory },
    data: { sessionID, timestamp: 1700000002100, assistantMessageID: assistantID, streamEventCount: 11 },
  })
  await timeline.transport.burst([counted, counted])
  await expect(label).toHaveAccessibleName("Thinking(11 streams)")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
  await expect(page.locator('[data-timeline-part-id*="reasoning"]')).toHaveCount(0)
  expect(await node.evaluate((element) => element.isConnected)).toBe(true)

  await timeline.transport.send(
    currentStepEvent({
      id: "evt_stream_count_end",
      type: "session.next.step.ended",
      location: { directory },
      data: {
        sessionID,
        timestamp: 1700000002200,
        assistantMessageID: assistantID,
        finish: "stop",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      },
    }),
  )
  await expect(label).toHaveAccessibleName("Thinking(0 streams)")
  await timeline.transport.send(
    currentStepEvent({
      id: "evt_stream_count_next",
      type: "session.next.step.started",
      location: { directory },
      data: {
        sessionID,
        timestamp: 1700000002300,
        assistantMessageID: "msg_next_count",
        agent: "build",
        model: { id: "mock-model", providerID: "mock-provider" },
        streamEventCount: 0,
      },
    }),
  )
  await timeline.transport.send(counted)
  await timeline.transport.send(
    currentStepEvent({
      id: "evt_stream_count_one",
      type: "session.next.step.stream.updated",
      location: { directory },
      data: { sessionID, timestamp: 1700000002400, assistantMessageID: "msg_next_count", streamEventCount: 1 },
    }),
  )
  await expect(label).toHaveAccessibleName("Thinking(1 stream)")
  expect(await node.evaluate((element) => element.isConnected)).toBe(true)
  await timeline.transport.send(
    currentStepEvent({
      id: "evt_stream_count_unavailable",
      type: "session.next.step.started",
      location: { directory },
      data: {
        sessionID,
        timestamp: 1700000002500,
        assistantMessageID: "msg_unavailable_count",
        agent: "build",
        model: { id: "mock-model", providerID: "mock-provider" },
      },
    }),
  )
  await expect(label).toHaveAccessibleName("Thinking(? streams)")
  await node.dispose()

  await page.evaluate(() => localStorage.setItem("opencode.global.dat:language", JSON.stringify({ locale: "ko" })))
  await page.reload()
  await timeline.transport.waitForConnection()
  await expect(label).toHaveAccessibleName("생각 중(0 스트림)")
  await page.evaluate(() => {
    document.documentElement.dir = "rtl"
  })
  await timeline.transport.send(counted)
  await expect(label).toHaveAccessibleName("생각 중(11 스트림)")
  await expect(thinking.locator("bdi")).toHaveAttribute("dir", "auto")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
})

const profiles = [
  { name: "summaries off no reasoning", summaries: false, reasoning: "", other: false, thinking: true, body: false },
  {
    name: "summaries off reasoning heading",
    summaries: false,
    reasoning: "## Inspecting stability",
    other: false,
    thinking: true,
    body: false,
  },
  {
    name: "summaries off with visible tool",
    summaries: false,
    reasoning: "## Inspecting stability",
    other: true,
    thinking: true,
    body: false,
  },
  { name: "summaries on no content", summaries: true, reasoning: "", other: false, thinking: true, body: false },
  {
    name: "summaries on blank reasoning",
    summaries: true,
    reasoning: "   ",
    other: false,
    thinking: true,
    body: false,
  },
  {
    name: "summaries on visible reasoning",
    summaries: true,
    reasoning: "## Inspecting stability",
    other: false,
    thinking: false,
    body: true,
  },
  {
    name: "summaries on visible tool no reasoning",
    summaries: true,
    reasoning: "",
    other: true,
    thinking: false,
    body: false,
  },
] as const

for (const profile of profiles) {
  test(`projects busy reasoning profile ${profile.name}`, async ({ page }) => {
    const reasoningID = `prt_reasoning_matrix_${profiles.indexOf(profile)}`
    const parts = [
      ...(profile.reasoning ? [reasoningPart(reasoningID, profile.reasoning)] : []),
      ...(profile.other
        ? [toolPart(`prt_reasoning_tool_${profiles.indexOf(profile)}`, "skill", "running", { name: "inspect" })]
        : []),
    ]
    const timeline = await setupTimeline(page, {
      messages: [userMessage(), assistantMessage(parts, { completed: false })],
      settings: { showReasoningSummaries: profile.summaries },
    })
    await timeline.send(status("busy"), 150)

    await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(profile.thinking ? 1 : 0)
    await expect(page.locator(`[data-timeline-part-id="${reasoningID}"]`)).toHaveCount(profile.body ? 1 : 0)
    if (!profile.summaries && profile.reasoning.trim()) {
      await expect(page.getByText("Inspecting stability", { exact: true })).toBeVisible()
    }
  })
}

test("does not infer reasoning visibility from provider identity", async ({ page }) => {
  const timeline = await setupTimeline(page, {
    messages: [
      userMessage(),
      assistantMessage([textPart("prt_provider_text", "No reasoning payload")], { completed: false }),
    ],
    settings: { showReasoningSummaries: true },
  })
  await timeline.send(status("busy"), 150)

  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(0)
  await expect(page.locator('[data-timeline-part-id*="reasoning"]')).toHaveCount(0)
  await expect(page.locator('[data-timeline-part-id="prt_provider_text"]')).toBeVisible()
})
