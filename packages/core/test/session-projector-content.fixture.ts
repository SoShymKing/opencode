import { asc, eq, sql } from "drizzle-orm"
import { DateTime, Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventTable } from "@opencode-ai/core/event/sql"
import { ModelV2 } from "@opencode-ai/core/model"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { hydrateSelection } from "@opencode-ai/core/session/message-storage"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { testEffect } from "./lib/effect"

export const it = testEffect(LayerNode.compile(LayerNode.group([Database.node, EventV2.node, SessionProjector.node])))
export const at = (millis: number) => DateTime.makeUnsafe(millis)
export const text = (value: string) => [{ type: "text" as const, text: value }]
export const model = {
  id: ModelV2.ID.make("projector-content"),
  providerID: ProviderV2.ID.make("projector-content"),
}

export function setup(label: string) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const sessionID = SessionV2.ID.make(`ses_projector_content_${label}`)
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
      timestamp: at(0),
      agent: "build",
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
  readonly sessionID: SessionV2.ID
  readonly assistantMessageID: SessionMessage.ID
}) => ({
  sessionID: fixture.sessionID,
  assistantMessageID: fixture.assistantMessageID,
})

export const tool = (assistant: SessionMessage.Assistant, callID: string) =>
  assistant.content.findLast((item): item is SessionMessage.AssistantTool => item.type === "tool" && item.id === callID)
