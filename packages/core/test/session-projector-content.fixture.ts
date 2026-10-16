import { asc, eq, sql } from "drizzle-orm"
import { DateTime, Effect } from "effect"
import { Database } from "@opencode/core/database/database"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Bus } from "@opencode/core/bus"
import { EventTable } from "@opencode/core/event/sql"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Project } from "@opencode/schema/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Provider } from "@opencode/schema/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/schema/session"
import { SessionEvent } from "@opencode/core/session/event"
import { hydrateSelection } from "@opencode/core/session/message-storage"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { testEffect } from "./lib/effect"

export const it = testEffect(AppNodeBuilder.build(
  LayerNode.group([Database.node, Bus.node, SessionProjector.node]),
  [Bus.node.replace(Bus.configured({ persist: true }))],
))
export const at = (millis: number) => DateTime.makeUnsafe(millis)
export const text = (value: string): [{ type: "text"; text: string }] => [{ type: "text", text: value }]
export const model = {
  id: Model.ID.make("projector-content"),
  providerID: Provider.ID.make("projector-content"),
}

export function setup(label: string) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* Bus.Service
    const sessionID = Session.ID.make(`ses_projector_content_${label}`)
    const assistantMessageID = SessionMessage.ID.make(`msg_projector_content_${label}`)
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: label,
        directory: "/project",
        title: label,
        version: "test",
      })
      .run()
      .pipe(Effect.orDie)
    yield* events.publish(SessionEvent.Step.Started, {
      sessionID,
      assistantMessageID,
      started: 0,
      agent: Agent.ID.make("build"),
      model,
    })

    const children = db
      .select({
        position: SessionMessagePartTable.position,
        id: SessionMessagePartTable.id,
        type: SessionMessagePartTable.type,
        dataText: sql<string>`${SessionMessagePartTable.data}`,
      })
      .from(SessionMessagePartTable)
      .where(eq(SessionMessagePartTable.message_id, assistantMessageID))
      .orderBy(asc(SessionMessagePartTable.position))
      .all()
      .pipe(Effect.orDie)
    const envelopeText = db
      .select({ dataText: sql<string>`${SessionMessageTable.data}` })
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.id, assistantMessageID))
      .get()
      .pipe(
        Effect.orDie,
        Effect.flatMap((row) => (row ? Effect.succeed(row.dataText) : Effect.die("Missing assistant envelope"))),
      )
    const read = hydrateSelection({ db, where: eq(SessionMessageTable.id, assistantMessageID), limit: 1 }).pipe(
      Effect.flatMap((rows) => {
        const message = rows[0]?.message
        return message?.type === "assistant" ? Effect.succeed(message) : Effect.die("Missing assistant")
      }),
    )
    const storedEvents = db
      .select()
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, sessionID))
      .orderBy(asc(EventTable.seq))
      .all()
      .pipe(Effect.orDie)
    const serializedEvents = storedEvents.pipe(
      Effect.map((rows) =>
        rows.map((row) => ({
          id: row.id,
          aggregateID: row.aggregate_id,
          seq: row.seq,
          type: row.type,
          data: row.data,
        })),
      ),
    )
    const corruptEnvelope = (dataText: string) =>
      db.run(sql`UPDATE session_message SET data = ${dataText} WHERE id = ${assistantMessageID}`).pipe(Effect.orDie)
    const corruptChild = (position: number, dataText: string) =>
      db
        .run(
          sql`UPDATE session_message_part SET data = ${dataText} WHERE message_id = ${assistantMessageID} AND position = ${position}`,
        )
        .pipe(Effect.orDie)

    return {
      db,
      events,
      sessionID,
      assistantMessageID,
      children,
      envelopeText,
      read,
      storedEvents,
      serializedEvents,
      corruptEnvelope,
      corruptChild,
    }
  })
}

export const contentBase = (fixture: {
  readonly sessionID: Session.ID
  readonly assistantMessageID: SessionMessage.ID
}) => ({
  sessionID: fixture.sessionID,
  assistantMessageID: fixture.assistantMessageID,
})

export const tool = (assistant: SessionMessage.Assistant, callID: string) =>
  assistant.content.findLast((item): item is SessionMessage.AssistantTool => item.type === "tool" && item.id === callID)
