import { expect, test } from "@playwright/test"
import { assistantMessage, directory, event, sessionID, setupTimeline, toolPart, userID, userMessage } from "./fixture"

test("resyncs the counter and completed tool after SSE overflow without new activity while retaining pending input", async ({ page }) => {
  const toolID = "prt_counter_resync_background"
  const input = { task_id: "bg_counter_resync" }
  const assistant = assistantMessage([toolPart(toolID, "background_output", "running", input)], { completed: false })
  const timeline = await setupTimeline(page, {
    protocol: "v2",
    locale: "en",
    settings: { showReasoningSummaries: true },
    messages: [userMessage(), assistant],
  })
  const owner = page.locator(`[data-timeline-row="Thinking"][data-message-id="${userID}"]`)
  const activity = owner.getByRole("status")
  const tool = page.locator(`[data-timeline-part-id="${toolID}"]`)
  const toolActivity = tool.locator('[data-slot="basic-tool-tool-title"] [data-component="text-shimmer"]')
  await expect(tool).toBeVisible()
  await expect(toolActivity).toHaveAttribute("data-active", "true")
  await timeline.send(event("session.status", {
    sessionID,
    status: { type: "busy", activity: { userMessageID: userID, model: "receiving", streamEventCount: 11 } },
  }))
  await expect(activity).toContainText("Receiving model response (streams: 11)")
  const admitted = await timeline.transport.send({
    id: "evt_counter_resync_admitted",
    type: "session.next.prompt.admitted",
    location: { directory },
    data: {
      sessionID,
      messageID: "msg_counter_resync_pending",
      timestamp: 1700000010000,
      prompt: { text: "Pending counter resync followup" },
      delivery: "steer",
    },
  })
  const pending = page.locator('[data-timeline-row="UserMessage"][data-message-id="msg_counter_resync_pending"]')
  const badge = pending.locator('[data-slot="session-input-pending"]')
  await expect(pending).toContainText("Pending counter resync followup")
  await expect(badge).toHaveText("Waiting to run")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
  await expect(activity).toContainText("Receiving model response (streams: 11)")

  assistant.parts = assistantMessage([
    toolPart(toolID, "background_output", "completed", input, { output: "Synthetic background task completed" }),
  ], { completed: false }).parts
  const snapshotStatus = {
    type: "busy",
    activity: { userMessageID: userID, model: "receiving", streamEventCount: 200 },
  } as const
  await page.route("**/api/session/active", (route) => route.fulfill({
    json: { data: { [sessionID]: { type: "running", status: snapshotStatus } } },
    headers: { "access-control-allow-origin": "*" },
  }))
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
  await expect(activity).toContainText("Receiving model response (streams: 200)")
  await expect(tool).toHaveCount(1)
  await expect(tool).toBeVisible()
  await expect(toolActivity).toHaveAttribute("data-active", "false")
  await expect(pending).toHaveCount(1)
  await expect(pending).toContainText("Pending counter resync followup")
  await expect(badge).toBeVisible()
  await expect(badge).toHaveText("Waiting to run")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
  await expect(owner).toBeVisible()
  expect(await timeline.transport.acknowledgements()).toEqual(acknowledgements)
  expect(await timeline.transport.connections()).toContainEqual(expect.objectContaining({
    id: admitted.connectionID,
    endedBy: "error",
    error: "SSE overflow",
  }))
})
