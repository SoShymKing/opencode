import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { DatabaseLegacyV01Json } from "./legacy-v01-json"
import { DatabaseMigration } from "./migration"
import sessionMessagePartMigration from "./migration/v02_session_message_part"
import { DatabaseLegacyV01Error } from "./legacy-v01"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]
type MessageRow = { readonly id: string; readonly type: string; readonly data: string }
type PartRow = {
  readonly message_id: string
  readonly position: number
  readonly id: string
  readonly type: string
  readonly data: string
}
type Content = {
  readonly id: string
  readonly type: string
  readonly data: DatabaseLegacyV01Json.ObjectValue
}
type StoredMessage = {
  readonly row: MessageRow
  readonly data: DatabaseLegacyV01Json.ObjectValue
  readonly content?: readonly Content[]
}

const decodeMessage = Schema.decodeUnknownEffect(SessionMessage.Message)
const identity = new Set(["id", "type"])
const content = new Set(["content"])

export function downgrade(db: Database) {
  return Effect.gen(function* () {
    yield* validateDatabase(db, "current")
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const messages = yield* readCurrent(tx)
          yield* Effect.forEach(
            messages.filter((message) => message.row.type === "assistant" && message.content !== undefined),
            (message) =>
              tx.run(
                sql`UPDATE session_message SET data = ${DatabaseLegacyV01Json.add(message.data, [
                  { key: "content", raw: contentJson(message.content ?? []) },
                ])} WHERE id = ${message.row.id}`,
              ),
            { discard: true },
          )
          yield* tx.run("DROP TABLE session_message_part")
          yield* tx.run("DELETE FROM migration")
          yield* Effect.forEach(
            DatabaseMigration.legacyBaselineMigrationIDs,
            (id) => tx.run(sql`INSERT INTO migration (id, time_completed) VALUES (${id}, ${Date.now()})`),
            { discard: true },
          )
        }),
      { behavior: "immediate" },
    )
    return yield* validateDatabase(db, "legacy")
  })
}

export function upgrade(db: Database) {
  return Effect.gen(function* () {
    const legacy = yield* validateDatabase(db, "legacy")
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          yield* sessionMessagePartMigration.up(tx)
          yield* Effect.forEach(
            legacy.filter((message) => message.row.type === "assistant"),
            (message) => splitAssistant(tx, message),
            { discard: true },
          )
          yield* tx.run("DELETE FROM migration")
          yield* tx.run(sql`INSERT INTO migration (id, time_completed) VALUES ('v01_baseline', ${Date.now()})`)
          yield* tx.run(
            sql`INSERT INTO migration (id, time_completed) VALUES ('v02_session_message_part', ${Date.now()})`,
          )
        }),
      { behavior: "immediate" },
    )
    return yield* validateDatabase(db, "current")
  })
}

export function validateCurrent(db: Database) {
  return validateDatabase(db, "current")
}

function validateDatabase(db: Database, format: "current" | "legacy") {
  return Effect.gen(function* () {
    const quick = yield* db.get<{ readonly quick_check: string }>(sql`PRAGMA quick_check`)
    if (quick?.quick_check !== "ok") return yield* failure("SQLite quick_check failed")
    if ((yield* db.all(sql`PRAGMA foreign_key_check`)).length > 0) return yield* failure("SQLite foreign keys failed")
    const tables = new Set(
      (yield* db.all<{ readonly name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'table'`)).map(
        (row) => row.name,
      ),
    )
    if (!tables.has("migration") || !tables.has("session_message"))
      return yield* failure("database schema is incomplete")
    const history = (yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration`)).map((row) => row.id)
    if (format === "legacy") {
      if (tables.has("session_message_part")) return yield* failure("legacy database contains current part storage")
      if (
        history.length !== DatabaseMigration.legacyBaselineMigrationIDs.length ||
        DatabaseMigration.legacyBaselineMigrationIDs.some((id) => !history.includes(id))
      )
        return yield* failure("legacy migration history is invalid")
      return yield* readLegacy(db)
    }
    if (!tables.has("session_message_part")) return yield* failure("current part storage is missing")
    if (history.some((id) => !DatabaseMigration.currentMigrationIDs.has(id)))
      return yield* failure("current migration history contains an unknown migration")
    if (!history.includes("v01_baseline") || !history.includes("v02_session_message_part"))
      return yield* failure("current migration history is invalid")
    return yield* readCurrent(db)
  })
}

function readLegacy(db: Pick<Database, "all">) {
  return Effect.gen(function* () {
    const rows = yield* db.all<MessageRow>(sql`SELECT id, type, data FROM session_message ORDER BY session_id, seq, id`)
    return yield* Effect.forEach(rows, (row) =>
      Effect.gen(function* () {
        const data = yield* parse(row.data, row.id)
        if (DatabaseLegacyV01Json.has(data, "id") || DatabaseLegacyV01Json.has(data, "type"))
          return yield* failure(`message ${row.id} stores identity in data`)
        if (row.type !== "assistant") {
          yield* validateMessage({ ...data.value, id: row.id, type: row.type }, row.id)
          return { row, data } satisfies StoredMessage
        }
        const items = DatabaseLegacyV01Json.objects(data, "content")
        if (!items) return yield* failure(`assistant ${row.id} has no legacy content`)
        const children = yield* Effect.forEach(items, (item) => contentValue(item, row.id))
        yield* validateMessage({ ...data.value, id: row.id, type: row.type }, row.id)
        return { row, data, content: children } satisfies StoredMessage
      }),
    )
  })
}

function readCurrent(db: Pick<Database, "all">) {
  return Effect.gen(function* () {
    const rows = yield* db.all<MessageRow>(sql`SELECT id, type, data FROM session_message ORDER BY session_id, seq, id`)
    const parts = yield* db.all<PartRow>(
      sql`SELECT message_id, position, id, type, data FROM session_message_part ORDER BY message_id, position`,
    )
    return yield* Effect.forEach(rows, (row) =>
      Effect.gen(function* () {
        const data = yield* parse(row.data, row.id)
        if (DatabaseLegacyV01Json.has(data, "id") || DatabaseLegacyV01Json.has(data, "type"))
          return yield* failure(`message ${row.id} stores identity in data`)
        const children = parts.filter((part) => part.message_id === row.id)
        if (row.type !== "assistant") {
          if (children.length > 0) return yield* failure(`non-assistant ${row.id} has child content`)
          yield* validateMessage({ ...data.value, id: row.id, type: row.type }, row.id)
          return { row, data } satisfies StoredMessage
        }
        if (DatabaseLegacyV01Json.has(data, "content")) {
          if (children.length > 0) return yield* failure(`assistant ${row.id} has ambiguous content`)
          yield* validateMessage({ ...data.value, id: row.id, type: row.type }, row.id)
          return { row, data } satisfies StoredMessage
        }
        const values = yield* Effect.forEach(children, (part, position) =>
          Effect.gen(function* () {
            if (part.position !== position)
              return yield* failure(`assistant ${row.id} content positions are not contiguous`)
            const child = yield* parse(part.data, `${row.id}:${part.position}`)
            if (DatabaseLegacyV01Json.has(child, "id") || DatabaseLegacyV01Json.has(child, "type"))
              return yield* failure(`assistant ${row.id} child stores identity in data`)
            return { id: part.id, type: part.type, data: child }
          }),
        )
        yield* validateMessage(
          { ...data.value, id: row.id, type: row.type, content: values.map(contentObject) },
          row.id,
        )
        return { row, data, content: values } satisfies StoredMessage
      }),
    )
  })
}

function splitAssistant(tx: Transaction, message: StoredMessage) {
  return Effect.gen(function* () {
    const items = message.content
    if (!items) return yield* failure(`assistant ${message.row.id} content is malformed`)
    yield* tx.run(
      sql`UPDATE session_message SET data = ${DatabaseLegacyV01Json.remove(message.data, content)} WHERE id = ${message.row.id}`,
    )
    yield* Effect.forEach(
      items,
      (part, position) =>
        tx.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data)
          VALUES (${message.row.id}, ${position}, ${part.id}, ${part.type}, ${DatabaseLegacyV01Json.remove(part.data, identity)})`),
      { discard: true },
    )
  })
}

function contentJson(items: readonly Content[]) {
  return `[${items
    .map((item) =>
      DatabaseLegacyV01Json.add(item.data, [
        { key: "id", raw: JSON.stringify(item.id) },
        { key: "type", raw: JSON.stringify(item.type) },
      ]),
    )
    .join(",")}]`
}

function contentValue(data: DatabaseLegacyV01Json.ObjectValue, id: string) {
  const partID = DatabaseLegacyV01Json.string(data, "id")
  const type = DatabaseLegacyV01Json.string(data, "type")
  if (!partID || !type) return failure(`assistant ${id} content is malformed`)
  return Effect.succeed({ id: partID, type, data })
}

function contentObject(value: Content) {
  return { ...value.data.value, id: value.id, type: value.type }
}

function parse(text: string, id: string) {
  return Effect.try({
    try: () => DatabaseLegacyV01Json.object(text),
    catch: (cause) => new DatabaseLegacyV01Error(`message ${id} contains malformed JSON`, { cause }),
  })
}

function validateMessage(value: unknown, id: string) {
  return decodeMessage(value, { onExcessProperty: "error" }).pipe(
    Effect.asVoid,
    Effect.mapError(() => new DatabaseLegacyV01Error(`message ${id} does not match the repository schema`)),
  )
}

function failure(message: string) {
  return Effect.fail(new DatabaseLegacyV01Error(message))
}
