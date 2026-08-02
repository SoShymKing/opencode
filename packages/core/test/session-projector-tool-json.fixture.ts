import { expect } from "bun:test"
import { eq, sql } from "drizzle-orm"
import { DateTime, Effect, Schema } from "effect"
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
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { testEffect } from "./lib/effect"

export const it = testEffect(LayerNode.compile(LayerNode.group([Database.node, EventV2.node, SessionProjector.node])))
const model = { id: ModelV2.ID.make("tool-json"), providerID: ProviderV2.ID.make("tool-json") }
export const at = (millis: number) => DateTime.makeUnsafe(millis)
export const text = (value: string) => [{ type: "text" as const, text: value }]
export const encodeAssistant = Schema.encodeSync(SessionMessage.Assistant)
const decodeAssistant = Schema.decodeUnknownSync(SessionMessage.Assistant)

export function setup(label: string, content: SessionMessage.AssistantContent[] = []) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    const events = yield* EventV2.Service
    const sessionID = SessionV2.ID.make(`ses_tool_json_${label}`)
    const assistantMessageID = SessionMessage.ID.make(`msg_tool_json_${label}`)
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
    const initial = SessionMessage.Assistant.make({
      id: assistantMessageID,
      type: "assistant",
      agent: "build",
      model,
      content,
      time: { created: at(0) },
    })
    const write = (assistant: SessionMessage.Assistant) => {
      const { id: _, type, ...data } = encodeAssistant(assistant)
      return db
        .update(SessionMessageTable)
        .set({ type, data })
        .where(eq(SessionMessageTable.id, assistantMessageID))
        .run()
        .pipe(Effect.orDie)
    }
    const { id: _, type, ...data } = encodeAssistant(initial)
    yield* db
      .insert(SessionMessageTable)
      .values({ id: assistantMessageID, session_id: sessionID, type, seq: -1, time_created: 0, data })
      .run()
      .pipe(Effect.orDie)
    const read = db
      .select()
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.id, assistantMessageID))
      .get()
      .pipe(
        Effect.orDie,
        Effect.flatMap((row) =>
          row
            ? Effect.succeed(decodeAssistant({ ...row.data, id: row.id, type: row.type }))
            : Effect.die("Missing assistant"),
        ),
      )
    const expectRollback = (invalid: string) =>
      Effect.gen(function* () {
        expect(
          (yield* db.get<{ data: string }>(sql`SELECT data FROM session_message WHERE id = ${assistantMessageID}`))
            ?.data,
        ).toBe(invalid)
        expect(
          yield* db.select().from(EventTable).where(eq(EventTable.aggregate_id, sessionID)).all().pipe(Effect.orDie),
        ).toEqual([])
      })
    const corrupt = (invalid: string) =>
      db.run(sql`UPDATE session_message SET data = ${invalid} WHERE id = ${assistantMessageID}`).pipe(Effect.orDie)
    return { db, events, sessionID, assistantMessageID, initial, read, write, expectRollback, corrupt }
  })
}

export const tool = (assistant: SessionMessage.Assistant, callID: string) =>
  assistant.content.findLast((item): item is SessionMessage.AssistantTool => item.type === "tool" && item.id === callID)
