import { expect, test } from "@playwright/test"
import { assistantMessage, event, reasoningPart, sessionID, setupTimeline, toolPart, userID, userMessage } from "./fixture"
import type { SessionStatus } from "@opencode/client/promise"
import { timelinePresets } from "@opencode/session-ui/timeline/detail"

test("resyncs the counter and completed tool after SSE overflow without new activity while retaining pending input", async ({ page }) => {
  const toolID = "prt_counter_resync_background"
  const input = { task_id: "bg_counter_resync" }
  const assistant = assistantMessage([
    toolPart(toolID, "background_output", "running", input), reasoningPart("prt_counter_reasoning", ""),
  ], { completed: false })
  const messages = [userMessage(), assistant]
  const statuses: Record<string, SessionStatus> = { [sessionID]: { type: "busy" } }
  const pendingItems = [{ id: "msg_counter_resync_pending", sessionID, time: { created: 1700000010000 }, type: "user" as const,
    payload: { text: "Pending counter resync followup" }, delivery: "steer" as const }]
  const timeline = await setupTimeline(page, {
    locale: "en",
    settings: { timelineDetail: { ...timelinePresets[0].value, thinking: { placement: "separate", details: "collapsed" } } },
    messages,
    pageMessages: () => ({ items: messages }),
    sessionStatus: statuses,
    inbox: () => pendingItems,
  })
  const owner = page.locator(`[data-timeline-row="Thinking"][data-message-id="${userID}"]`)
  const activity = owner.locator('[data-component="text-shimmer"]')
  const tool = page.locator(`[data-timeline-part-id="${toolID}"]`)
  const toolActivity = tool.locator('[data-slot="basic-tool-tool-title"] [data-component="text-shimmer"]')
  await expect(tool).toBeVisible()
  await expect(toolActivity).toHaveAttribute("data-active", "true")
  await timeline.send(event("session.status", {
    sessionID,
    status: { type: "busy", activity: { userMessageID: userID, model: "receiving", streamEventCount: 11 } },
  }))
  await expect(activity).toHaveAccessibleName("Thinking(11 streams)")
  const admitted = await timeline.transport.send(event("session.inbox.enqueued", {
      sessionID,
      inboxID: "msg_counter_resync_pending",
      item: { type: "user", payload: { text: "Pending counter resync followup" }, delivery: "steer" },
  }))
  const pending = page.locator('[data-timeline-row="UserMessage"][data-message-id="msg_counter_resync_pending"]')
  const badge = pending.locator('[data-component="user-message"]')
  await expect(pending).toContainText("Pending counter resync followup")
  await expect(badge).toHaveAttribute("data-pending", "true")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
  await expect(activity).toHaveAccessibleName("Thinking(11 streams)")

  messages[1] = assistantMessage([
    toolPart(toolID, "background_output", "completed", input, { output: "Synthetic background task completed" }),
    reasoningPart("prt_counter_reasoning", ""),
  ], { completed: false })
  const snapshotStatus = {
    type: "busy",
    activity: { userMessageID: userID, model: "receiving", streamEventCount: 200 },
  } as const
  statuses[sessionID] = snapshotStatus
  const acknowledgements = await timeline.transport.acknowledgements()
  const activeResponse = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/session/active" && response.request().method() === "GET",
  )
  const historyResponse = page.waitForResponse((response) =>
    new URL(response.url()).pathname === `/api/session/${sessionID}/message` && response.request().method() === "GET",
  )
  const reconnected = timeline.transport.waitForConnection({ after: admitted.connectionID })
  await timeline.transport.error("SSE overflow")
  const connection = await reconnected
  expect(connection.id).toBeGreaterThan(admitted.connectionID)
  expect(connection.path).toBe("/api/event")
  expect((await activeResponse).status()).toBe(200)
  expect((await historyResponse).status()).toBe(200)
  await expect(activity).toHaveAccessibleName("Thinking(200 streams)")
  await expect(tool).toHaveCount(1)
  await expect(tool).toBeVisible()
  await expect(toolActivity).toHaveAttribute("data-active", "false")
  await expect(pending).toHaveCount(1)
  await expect(pending).toContainText("Pending counter resync followup")
  await expect(badge).toBeVisible()
  await expect(badge).toHaveAttribute("data-pending", "true")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
  await expect(owner).toBeVisible()
  await expect(tool.locator('xpath=ancestor::*[@data-timeline-row="AssistantPart"]')).toHaveAttribute("data-message-id", userID)
  expect(await timeline.transport.acknowledgements()).toEqual(acknowledgements)
  expect(await timeline.transport.connections()).toContainEqual(expect.objectContaining({
    id: admitted.connectionID,
    endedBy: "error",
    error: "SSE overflow",
  }))
})
