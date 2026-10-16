import { and, desc, eq, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { isDeepStrictEqual } from "node:util"
import type { Database } from "../database/database.js"
import { SessionEvent } from "./event.js"
import { decodeChild, decodeEnvelope, encodeStoredMessage, insertMessageChildren } from "./message-storage.js"
import { SessionMessage } from "./message.js"
import { SessionMessageUpdater } from "./message-updater.js"
import { SessionMessagePartTable, SessionMessageTable } from "./sql.js"

export type ContentEvent =
  | SessionEvent.Text.Started
  | SessionEvent.Text.Ended
  | SessionEvent.Reasoning.Started
  | SessionEvent.Reasoning.Ended
  | SessionEvent.Tool.Input.Started
  | SessionEvent.Tool.Input.Ended
  | SessionEvent.Tool.Called
  | SessionEvent.Tool.Success
  | SessionEvent.Tool.Failed

type StartEvent = SessionEvent.Text.Started | SessionEvent.Reasoning.Started | SessionEvent.Tool.Input.Started
type UpdateEvent = Exclude<ContentEvent, StartEvent>
type DatabaseService = Database.Interface["db"]

const encodeContent = Schema.encodeSync(SessionMessage.AssistantContent)

function childData(content: SessionMessage.AssistantContent) {
  const encoded = encodeContent(content)
  if (encoded.type === "tool") {
    const { id: _, type: __, ...data } = encoded
    return data
  }
  const { type: _, ...data } = encoded
  return data
}

export function replaceContent(db: DatabaseService, message: SessionMessage.Assistant) {
  return Effect.gen(function* () {
    const stored = yield* encodeStoredMessage(message).pipe(Effect.orDie)
    yield* db
      .delete(SessionMessagePartTable)
      .where(eq(SessionMessagePartTable.message_id, message.id))
      .run()
      .pipe(Effect.orDie)
    yield* insertMessageChildren(db, stored.parts).pipe(Effect.orDie)
  })
}

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
    const id = event.type === SessionEvent.Tool.Input.Started.type ? event.data.id : String(event.data.ordinal)
    yield* db
      .insert(SessionMessagePartTable)
      .values({
        message_id: event.data.assistantMessageID,
        position: sql<number>`coalesce((select max(${SessionMessagePartTable.position}) + 1 from ${SessionMessagePartTable} where ${SessionMessagePartTable.message_id} = ${event.data.assistantMessageID}), 0)`,
        id,
        type: next.type,
        data: childData(next),
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
    yield* db
      .update(SessionMessagePartTable)
      .set({ data: childData(next) })
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
  return Effect.gen(function* () {
    let result: SessionMessage.AssistantContent | undefined
    yield* SessionMessageUpdater.update(
      {
        getAgent: () => Effect.succeed(undefined),
        getModel: () => Effect.succeed(undefined),
        getLocation: () => Effect.succeed(undefined),
        getCurrentAssistant: () => Effect.succeed(undefined),
        getAssistant: () => Effect.succeed({ ...assistant, content: current ? [current] : [] }),
        getShell: () => Effect.succeed(undefined),
        getCompaction: () => Effect.succeed(undefined),
        updateAssistant: (message) => Effect.sync(() => { result = message.content[0] }),
        updateShell: () => Effect.void,
        updateCompaction: () => Effect.void,
        appendMessage: () => Effect.void,
      },
      event,
    )
    return result
  })
}

function isStart(event: ContentEvent): event is StartEvent {
  return (
    event.type === SessionEvent.Text.Started.type ||
    event.type === SessionEvent.Reasoning.Started.type ||
    event.type === SessionEvent.Tool.Input.Started.type
  )
}

function eventTarget(event: UpdateEvent) {
  if (event.type === SessionEvent.Text.Ended.type) return { type: "text" as const, id: String(event.data.ordinal) }
  if (event.type === SessionEvent.Reasoning.Ended.type)
    return { type: "reasoning" as const, id: String(event.data.ordinal) }
  return { type: "tool" as const, id: event.data.id }
}
