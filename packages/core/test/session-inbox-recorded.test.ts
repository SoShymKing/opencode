import { expect, test } from "bun:test"
import { eq } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { ProjectTable } from "@opencode/core/project/sql"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionInbox } from "@opencode/core/session/inbox"
import { hydrateSelection } from "@opencode/core/session/message-storage"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Project } from "@opencode/schema/project"
import { Prompt } from "@opencode/schema/prompt"
import { AbsolutePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(
  LayerNode.group([Database.node, Bus.node, SessionProjector.node]),
  [Bus.node.replace(Bus.configured({ persist: true }))],
))

const recordedFiles = [
  {
    uri: "file:///unavailable/report.pdf",
    mime: "application/pdf",
    name: "report.pdf",
    description: "Imported reference without bytes",
    source: { start: 7, end: 18, text: "@report.pdf" },
  },
  { uri: "https://unavailable.invalid/image.png", mime: "image/png" },
]

test("inbox reuses the recorded file contract without changing ordinary prompts", () => {
  expect(SessionInbox.UserPayload.fields.recordedFiles).toBe(SessionMessage.User.fields.recordedFiles)
  expect(Prompt.fields).not.toHaveProperty("recordedFiles")
  expect(() => Schema.decodeUnknownSync(SessionInbox.UserPayload)({ text: "", recordedFiles: undefined })).toThrow()
})

for (const scenario of [
  { name: "unavailable references", fields: { recordedFiles } },
  { name: "empty references", fields: { recordedFiles: [] } },
  { name: "absent references", fields: {} },
]) {
  it.effect(`inbox delivery preserves ${scenario.name}`, () => Effect.gen(function* () {
    const database = yield* Database.Service
    const db = database.db
    const bus = yield* Bus.Service
    const sessionID = Session.ID.create()
    const inboxID = SessionMessage.ID.create()
    const payload = Schema.decodeUnknownSync(SessionInbox.UserPayload)({
      text: "Review @report.pdf",
      metadata: { imported: true },
      ...scenario.fields,
    })
    expect(Schema.encodeSync(SessionInbox.UserPayload)(payload)).toEqual({
      text: "Review @report.pdf",
      metadata: { imported: true },
      ...(scenario.fields.recordedFiles === undefined ? {} : scenario.fields),
    })
    yield* db.insert(ProjectTable).values({
      id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [],
    }).run()
    yield* db.insert(SessionTable).values({
      id: sessionID, project_id: Project.ID.global, slug: "inbox-recorded",
      directory: "/project", title: "Inbox recorded files", version: "test",
    }).run()
    yield* bus.publish(SessionEvent.InboxEnqueued, {
      sessionID, inboxID, item: { type: "user", delivery: "queue", payload },
    })
    const pending = yield* SessionInbox.find(db, inboxID)
    expect(pending?.payload).toEqual(payload)
    yield* bus.publish(SessionEvent.InboxDelivered, { sessionID, inboxID })
    expect(yield* SessionInbox.find(db, inboxID)).toBeUndefined()
    const rows = yield* hydrateSelection({ db, where: eq(SessionMessageTable.id, inboxID), limit: 1 })
    expect(rows).toHaveLength(1)
    const message = Schema.encodeSync(SessionMessage.Info)(rows[0]!.message)
    expect(message).toEqual({
      id: inboxID, type: "user", text: payload.text, metadata: payload.metadata,
      time: { created: expect.any(Number) },
      ...(scenario.fields.recordedFiles === undefined ? {} : scenario.fields),
    })
    expect(yield* db.select().from(SessionMessagePartTable)
      .where(eq(SessionMessagePartTable.message_id, inboxID)).all()).toEqual([])
  }))
}
