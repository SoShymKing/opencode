import { asc, desc, inArray, sql, type SQL } from "drizzle-orm"
import { Effect, Schema } from "effect"
import type { Database } from "../database/database.js"
import { MessageDecodeError } from "./error.js"
import { SessionMessage } from "./message.js"
import { SessionSchema } from "./schema.js"
import { SessionMessagePartTable, SessionMessageTable } from "./sql.js"

type EnvelopeRow = {
  readonly sessionID: SessionSchema.ID
  readonly messageID: SessionMessage.ID
  readonly type: SessionMessage.Type
  readonly dataText: string
}

type ChildRow = {
  readonly sessionID: SessionSchema.ID
  readonly messageID: SessionMessage.ID
  readonly position: number
  readonly id: string
  readonly type: SessionMessage.AssistantContent["type"]
  readonly dataText: string
}

type HydrateInput = {
  readonly db: Database.Interface["db"]
  readonly where?: SQL
  readonly order?: "asc" | "desc"
  readonly limit?: number
}

const parseJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))
const decodeMessage = Schema.decodeUnknownEffect(SessionMessage.Info)
const decodeContent = Schema.decodeUnknownEffect(SessionMessage.AssistantContent)
const encodeMessage = Schema.encodeEffect(SessionMessage.Info)

export const encodeStoredMessage = Effect.fn("MessageStorage.encodeStoredMessage")(function* (message: SessionMessage.Info) {
  const encoded = yield* encodeMessage(message)
  const parts: (typeof SessionMessagePartTable.$inferInsert)[] = []
  const ordinals = { text: 0, reasoning: 0 }
  if (encoded.type === "assistant") {
    const { id: _, type, content, ...data } = encoded
    content.forEach((part, position) => {
      if (part.type === "tool") {
        const { id: partID, type: partType, ...partData } = part
        parts.push({ message_id: message.id, position, id: partID, type: partType, data: partData })
        return
      }
      const { type: partType, ...partData } = part
      parts.push({ message_id: message.id, position, id: String(ordinals[partType]++), type: partType, data: partData })
    })
    return { envelope: { id: message.id, type, data }, parts }
  }
  const { id: _, type, ...data } = encoded
  return { envelope: { id: message.id, type, data }, parts }
})

export const insertMessageChildren = Effect.fn("MessageStorage.insertMessageChildren")(function* (
  db: Pick<Database.Interface["db"], "insert">,
  parts: readonly (typeof SessionMessagePartTable.$inferInsert)[],
) {
  if (parts.length === 0) return
  yield* db.insert(SessionMessagePartTable).values([...parts]).run()
})

const failure = (row: { readonly sessionID: SessionSchema.ID; readonly messageID: SessionMessage.ID }) =>
  new MessageDecodeError({ sessionID: row.sessionID, messageID: row.messageID })

const isObject = (value: unknown): value is object =>
  typeof value === "object" && value !== null && !Array.isArray(value)

export const decodeEnvelope = Effect.fn("MessageStorage.decodeEnvelope")(function* (row: EnvelopeRow) {
  const data = yield* parseJson(row.dataText).pipe(Effect.mapError(() => failure(row)))
  if (!isObject(data) || "id" in data || "type" in data || (row.type === "assistant" && "content" in data)) {
    return yield* failure(row)
  }
  return yield* decodeMessage({
    ...data,
    id: row.messageID,
    type: row.type,
    ...(row.type === "assistant" ? { content: [] } : {}),
  }).pipe(Effect.mapError(() => failure(row)))
})

export const decodeChild = Effect.fn("MessageStorage.decodeChild")(function* (row: ChildRow) {
  const data = yield* parseJson(row.dataText).pipe(Effect.mapError(() => failure(row)))
  if (!isObject(data) || "id" in data || "type" in data || !Number.isSafeInteger(row.position) || row.position < 0) {
    return yield* failure(row)
  }
  return yield* decodeContent({ ...data, ...(row.type === "tool" ? { id: row.id } : {}), type: row.type }).pipe(
    Effect.mapError(() => failure(row)),
  )
})

export const hydrateSelection = Effect.fn("MessageStorage.hydrateSelection")(function* (input: HydrateInput) {
  const order = input.order === "desc" ? desc(SessionMessageTable.seq) : asc(SessionMessageTable.seq)
  const raw = yield* input.db
    .transaction((tx) =>
      Effect.gen(function* () {
        const parentSelection = tx
          .select({
            sessionID: SessionMessageTable.session_id,
            messageID: SessionMessageTable.id,
            type: SessionMessageTable.type,
            seq: SessionMessageTable.seq,
            dataText: sql<string>`${SessionMessageTable.data}`,
          })
          .from(SessionMessageTable)
          .where(input.where)
          .orderBy(order)
        const parents = yield* input.limit === undefined
          ? parentSelection.all()
          : parentSelection.limit(input.limit).all()
        const idSelection = tx
          .select({ messageID: SessionMessageTable.id })
          .from(SessionMessageTable)
          .where(input.where)
          .orderBy(order)
        const selected = input.limit === undefined ? idSelection : idSelection.limit(input.limit)
        const children = yield* tx
          .select({
            messageID: SessionMessagePartTable.message_id,
            position: SessionMessagePartTable.position,
            id: SessionMessagePartTable.id,
            type: SessionMessagePartTable.type,
            dataText: sql<string>`${SessionMessagePartTable.data}`,
          })
          .from(SessionMessagePartTable)
          .where(inArray(SessionMessagePartTable.message_id, selected))
          .orderBy(asc(SessionMessagePartTable.message_id), asc(SessionMessagePartTable.position))
          .all()
        return { parents, children }
      }),
    )
    .pipe(Effect.orDie)

  const childrenByMessage = Map.groupBy(raw.children, (child) => child.messageID)
  return yield* Effect.forEach(raw.parents, (parent) =>
    Effect.gen(function* () {
      const envelope = yield* decodeEnvelope(parent)
      const children = childrenByMessage.get(parent.messageID) ?? []
      if (envelope.type !== "assistant") {
        if (children.length > 0) return yield* failure(parent)
        return { sessionID: parent.sessionID, seq: parent.seq, message: envelope }
      }
      const content = yield* Effect.forEach(children, (child) => decodeChild({ ...child, sessionID: parent.sessionID }))
      const encoded = yield* encodeMessage({ ...envelope, content }).pipe(Effect.mapError(() => failure(parent)))
      const message = yield* decodeMessage(encoded).pipe(Effect.mapError(() => failure(parent)))
      return { sessionID: parent.sessionID, seq: parent.seq, message }
    }),
  )
})
