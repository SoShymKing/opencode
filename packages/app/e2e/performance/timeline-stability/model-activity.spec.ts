import { expect, test } from "@playwright/test"
import { assistantMessage, directory, event, sessionID, setupTimeline, shell, toolPart, userID, userMessage } from "./fixture"

test("keeps model activity after completed shell and background_output with a pending owner on Korean mobile", async ({ page }) => {
  const timeline = await setupTimeline(page, {
    protocol: "v2",
    locale: "ko",
    viewport: { width: 390, height: 844 },
    settings: { showReasoningSummaries: true },
    messages: [userMessage(), assistantMessage([
      shell("prt_activity_shell", "completed", "done", "pwd"),
      toolPart("prt_activity_background", "background_output", "completed", { task_id: "bg_fixture" }, { output: "done" }),
    ])],
  })
  const owner = page.locator(`[data-timeline-row="Thinking"][data-message-id="${userID}"]`)
  const activity = owner.getByRole("status")
  await expect(page.locator('[data-timeline-part-id="prt_activity_shell"]')).toBeVisible()
  await expect(page.locator('[data-timeline-part-id="prt_activity_background"]')).toBeVisible()
  await timeline.send(event("session.status", { sessionID, status: { type: "busy", activity: { userMessageID: userID, model: "waiting" } } }))
  await expect(activity).toContainText("모델 응답 대기 중(스트림: ?)")
  const admitted = {
    id: "evt_activity_admitted", type: "session.next.prompt.admitted" as const,
    location: { directory },
    data: { sessionID, messageID: "msg_activity_pending", timestamp: 1700000010000, prompt: { text: "Pending followup" }, delivery: "steer" as const },
  }
  await timeline.transport.send(admitted)
  const pending = page.locator('[data-timeline-row="UserMessage"][data-message-id="msg_activity_pending"]')
  await expect(pending).toContainText("Pending followup")
  await expect(pending.locator('[data-slot="session-input-pending"]')).toHaveText("실행 대기 중")
  await expect(page.locator('[data-timeline-row="Thinking"]')).toHaveCount(1)
  await expect(activity).toContainText("모델 응답 대기 중(스트림: ?)")
  await timeline.send(event("session.status", { sessionID, status: { type: "busy", activity: { userMessageID: userID, model: "waiting", streamEventCount: 0 } } }))
  await expect(activity).toContainText("모델 응답 대기 중(스트림: 0)")
  const received = event("session.status", { sessionID, status: { type: "busy", activity: { userMessageID: userID, model: "receiving", streamEventCount: 7, lastStreamEventAt: Date.now() } } })
  await timeline.send(received)
  await timeline.send(received)
  await expect(activity).toContainText("모델 응답 수신 중(스트림: 7)")
  await expect(activity.getByText(/^마지막 수신 \d+초 전$/)).toBeVisible()
  await timeline.send(event("session.status", { sessionID, status: { type: "busy", activity: { userMessageID: userID, model: "settling", streamEventCount: 7 } } }))
  await expect(activity).toContainText("모델 스트림 종료 후 후속 처리 중(스트림: 7)")
  await expect(activity).not.toContainText("도구 실행 중")
  await timeline.send(event("session.status", { sessionID, status: { type: "busy", activity: { userMessageID: userID, model: "none" } } }))
  await expect(activity).toContainText("진행 중인 모델 요청 없음")
  await expect(activity).not.toContainText("스트림:")
  await timeline.send(event("session.status", { sessionID, status: { type: "idle", terminal: { userMessageID: userID, reason: "error", message: "Synthetic failure" } } }))
  await expect(activity).toContainText("응답 실패")
  await expect(activity).not.toContainText("응답 완료")
  await expect(pending.locator('[data-slot="session-input-pending"]')).toHaveText("실행 대기 중")
  await timeline.transport.send({ ...admitted, id: "evt_activity_promoted", type: "session.next.prompted" })
  await expect(pending.locator('[data-slot="session-input-pending"]')).toHaveCount(0)
  await expect(owner).toBeVisible()
})
