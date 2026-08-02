import { and, desc, eq, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { isDeepStrictEqual } from "node:util"
import type { Database } from "../database/database"
import { SessionEvent } from "./event"
import { decodeChild, decodeEnvelope } from "./message-storage"
import { SessionMessage } from "./message"
import { SessionMessageUpdater } from "./message-updater"
import { SessionMessagePartTable, SessionMessageTable } from "./sql"

export type ContentEvent =
  | SessionEvent.Text.Started
  | SessionEvent.Text.Ended
  | SessionEvent.Reasoning.Started
  | SessionEvent.Reasoning.Ended
  | SessionEvent.Tool.Input.Started
  | SessionEvent.Tool.Input.Ended
  | SessionEvent.Tool.Called
  | SessionEvent.Tool.Progress
  | SessionEvent.Tool.Success
  | SessionEvent.Tool.Failed

type StartEvent = SessionEvent.Text.Started | SessionEvent.Reasoning.Started | SessionEvent.Tool.Input.Started
type UpdateEvent = Exclude<ContentEvent, StartEvent>
type DatabaseService = Database.Interface["db"]

const encodeContent = Schema.encodeSync(SessionMessage.AssistantContent)

export function projectContentEvent(db: DatabaseService, event: ContentEvent) {
  return Effect.gen(function* () {
    const assistant = yield* loadAssistant(db, event)
    if (!assistant) return
    if (isStart(event)) return yield* append(db, event, assistant)
    return yield* update(db, event, assistant)
  })
}

function loadAssistant(db: DatabaseService, event: ContentEvent) {
  return Effect.gen(function* () {
    const row = yield* db
      .select({
        sessionID: SessionMessageTable.session_id,
        messageID: SessionMessageTable.id,
        type: SessionMessageTable.type,
        dataText: sql<string>`${SessionMessageTable.data}`,
      })
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
    if (!row) return
    const message = yield* decodeEnvelope(row).pipe(Effect.orDie)
    return message.type === "assistant" ? message : undefined
  })
}

function append(db: DatabaseService, event: StartEvent, assistant: SessionMessage.Assistant) {
  return Effect.gen(function* () {
    const next = yield* transition(event, assistant)
    if (!next) return
    const encoded = encodeContent(next)
    const { id, type, ...data } = encoded
    yield* db
      .insert(SessionMessagePartTable)
      .values({
        message_id: event.data.assistantMessageID,
        position: sql<number>`coalesce((select max(${SessionMessagePartTable.position}) + 1 from ${SessionMessagePartTable} where ${SessionMessagePartTable.message_id} = ${event.data.assistantMessageID}), 0)`,
        id,
        type,
        data,
      })
      .run()
      .pipe(Effect.orDie)
  })
}

function update(db: DatabaseService, event: UpdateEvent, assistant: SessionMessage.Assistant) {
  return Effect.gen(function* () {
    const target = eventTarget(event)
    const row = yield* db
      .select({
        position: SessionMessagePartTable.position,
        id: SessionMessagePartTable.id,
        type: SessionMessagePartTable.type,
        dataText: sql<string>`${SessionMessagePartTable.data}`,
      })
      .from(SessionMessagePartTable)
      .where(
        and(
          eq(SessionMessagePartTable.message_id, event.data.assistantMessageID),
          eq(SessionMessagePartTable.type, target.type),
          eq(SessionMessagePartTable.id, target.id),
        ),
      )
      .orderBy(desc(SessionMessagePartTable.position))
      .limit(1)
      .get()
      .pipe(Effect.orDie)
    if (!row) return
    const current = yield* decodeChild({
      ...row,
      sessionID: event.data.sessionID,
      messageID: event.data.assistantMessageID,
    }).pipe(Effect.orDie)
    const next = yield* transition(event, assistant, current)
    if (!next) return
    const encoded = encodeContent(next)
    if (isDeepStrictEqual(encoded, encodeContent(current))) return
    const { id: _, type: __, ...data } = encoded
    yield* db
      .update(SessionMessagePartTable)
      .set({ data })
      .where(
        and(
          eq(SessionMessagePartTable.message_id, event.data.assistantMessageID),
          eq(SessionMessagePartTable.position, row.position),
        ),
      )
      .run()
      .pipe(Effect.orDie)
  })
}

function transition(
  event: ContentEvent,
  assistant: SessionMessage.Assistant,
  current?: SessionMessage.AssistantContent,
) {
  const state: SessionMessageUpdater.MemoryState = {
    messages: [{ ...assistant, content: current ? [current] : [] }],
  }
  return SessionMessageUpdater.update(SessionMessageUpdater.memory(state), event).pipe(
    Effect.andThen(
      Effect.sync(() => {
        const message = state.messages[0]
        return message?.type === "assistant" ? message.content[0] : undefined
      }),
    ),
  )
}

function isStart(event: ContentEvent): event is StartEvent {
  return (
    event.type === SessionEvent.Text.Started.type ||
    event.type === SessionEvent.Reasoning.Started.type ||
    event.type === SessionEvent.Tool.Input.Started.type
  )
}

function eventTarget(event: UpdateEvent) {
  if (event.type === SessionEvent.Text.Ended.type) return { type: "text" as const, id: event.data.textID }
  if (event.type === SessionEvent.Reasoning.Ended.type)
    return { type: "reasoning" as const, id: event.data.reasoningID }
  return { type: "tool" as const, id: event.data.callID }
}
