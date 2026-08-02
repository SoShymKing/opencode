import { and, eq, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { Database } from "../database/database"
import { ModelV2 } from "../model"
import { ProviderV2 } from "../provider"
import { SessionEvent } from "./event"
import { SessionMessage } from "./message"
import { SessionMessageUpdater } from "./message-updater"
import { SessionMessageTable } from "./sql"

export type ToolEvent =
  | SessionEvent.Tool.Input.Started
  | SessionEvent.Tool.Input.Ended
  | SessionEvent.Tool.Called
  | SessionEvent.Tool.Progress
  | SessionEvent.Tool.Success
  | SessionEvent.Tool.Failed

type DatabaseService = Database.Interface["db"]
type Replacement = { readonly index: number; readonly encoded: string }

const ToolJson = Schema.fromJsonString(SessionMessage.AssistantTool)
const encodeTool = Schema.encodeSync(ToolJson)
const decodeMessage = Schema.decodeUnknownSync(SessionMessage.Message)
const model = {
  id: ModelV2.ID.make("session-projector-tool"),
  providerID: ProviderV2.ID.make("session-projector-tool"),
}

export function projectToolEvent(db: DatabaseService, event: ToolEvent) {
  return Effect.gen(function* () {
    const row = yield* db
      .select()
      .from(SessionMessageTable)
      .where(
        and(
          eq(SessionMessageTable.id, event.data.assistantMessageID),
          eq(SessionMessageTable.session_id, event.data.sessionID),
          eq(SessionMessageTable.type, "assistant"),
        ),
      )
      .get()
      .pipe(Effect.orDie)
    if (!row) return true
    const message = decodeMessage({ ...row.data, id: row.id, type: row.type })
    if (message.type !== "assistant") return false

    if (event.type === SessionEvent.Tool.Input.Started.type) {
      const next = yield* transition(event)
      if (!next) return false
      return yield* append(db, event, encodeTool(next))
    }

    const index = message.content.findLastIndex((part) => part.type === "tool" && part.id === event.data.callID)
    if (index < 0) return true
    const current = message.content[index]
    if (current?.type !== "tool") return false
    const next = yield* transition(event, current)
    if (!next) return false
    if (next === current) return true
    return yield* replace(db, event, { index, encoded: encodeTool(next) })
  })
}

function transition(event: ToolEvent, current?: SessionMessage.AssistantTool) {
  const state: SessionMessageUpdater.MemoryState = {
    messages: [
      SessionMessage.Assistant.make({
        id: event.data.assistantMessageID,
        type: "assistant",
        agent: "projector",
        model,
        content: current ? [current] : [],
        time: { created: event.data.timestamp },
      }),
    ],
  }
  return SessionMessageUpdater.update(SessionMessageUpdater.memory(state), event).pipe(
    Effect.andThen(
      Effect.sync(() => {
        const assistant = state.messages[0]
        const part = assistant?.type === "assistant" ? assistant.content[0] : undefined
        return part?.type === "tool" ? part : undefined
      }),
    ),
  )
}

const compatible = (event: ToolEvent) =>
  and(
    eq(SessionMessageTable.id, event.data.assistantMessageID),
    eq(SessionMessageTable.session_id, event.data.sessionID),
    eq(SessionMessageTable.type, "assistant"),
  )

function append(db: DatabaseService, event: SessionEvent.Tool.Input.Started, encoded: string) {
  return db
    .update(SessionMessageTable)
    .set({ data: sql`json_insert(${SessionMessageTable.data}, '$.content[#]', json(${encoded}))` })
    .where(compatible(event))
    .returning({ id: SessionMessageTable.id })
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => row !== undefined),
    )
}

function replace(
  db: DatabaseService,
  event: Exclude<ToolEvent, SessionEvent.Tool.Input.Started>,
  replacement: Replacement,
) {
  const path = `$.content[${replacement.index}]`
  return db
    .update(SessionMessageTable)
    .set({ data: sql`json_replace(${SessionMessageTable.data}, ${path}, json(${replacement.encoded}))` })
    .where(
      and(
        compatible(event),
        sql`json_extract(${SessionMessageTable.data}, ${`${path}.type`}) = 'tool'`,
        sql`json_extract(${SessionMessageTable.data}, ${`${path}.id`}) = ${event.data.callID}`,
      ),
    )
    .returning({ id: SessionMessageTable.id })
    .get()
    .pipe(
      Effect.orDie,
      Effect.map((row) => row !== undefined),
    )
}
