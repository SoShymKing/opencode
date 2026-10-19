export * as WsNativeProgress from "./ws-native-progress.js"

import { createHash } from "node:crypto"
import { sql, type SQL } from "drizzle-orm"
import { Effect, Schema } from "effect"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { DatabaseMigration } from "./migration.js"
import type { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeSeal } from "./ws-native-seal.js"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Message = Pick<WsNativeBatch.Row, "id" | "session_id" | "seq" | "time_created" | "time_updated">
const identifier = DatabaseWsArchive.identifier
const empty = createHash("sha256").digest("hex")
const State = Schema.Struct({
  singleton: Schema.Literal(1), version: Schema.Literal(1), token: Schema.String, archive_id: Schema.String,
  phase: Schema.Literals(["ws", "legacy", "finish", "complete"]),
  session_id: Schema.NullOr(Schema.String), seq: Schema.NullOr(Schema.Number), message_id: Schema.NullOr(Schema.String),
  archive_sha256: Schema.String, prefix_sha256: Schema.String,
})
export type Checkpoint = typeof State.Type & { readonly prefix: string }
const failure = (message: string) => Effect.fail(new DatabaseWsArchive.ArchiveError(message))

export function initialize(db: Database, token: string, archive: DatabaseWsArchive.Archive) {
  return Effect.gen(function* () {
    yield* db.run(`CREATE TABLE _ws_stage (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), version INTEGER NOT NULL,
      token TEXT NOT NULL, archive_id TEXT NOT NULL, phase TEXT NOT NULL, session_id TEXT, seq INTEGER, message_id TEXT,
      archive_sha256 TEXT NOT NULL, prefix_sha256 TEXT NOT NULL)`)
    const state = { singleton: 1, version: 1, token, archive_id: archive.id, phase: "ws", session_id: null, seq: null,
      message_id: null, archive_sha256: yield* WsNativeSeal.archive(db, archive), prefix_sha256: "" } satisfies typeof State.Type
    yield* db.run(sql`INSERT INTO _ws_stage VALUES (1, 1, ${token}, ${archive.id}, 'ws', NULL, NULL, NULL, ${state.archive_sha256}, ${seal(empty, state)})`)
    return { ...state, prefix_sha256: seal(empty, state), prefix: empty }
  })
}

export function verify(db: Database, token: string) {
  return Effect.gen(function* () {
    const rows = yield* db.all("SELECT * FROM _ws_stage")
    if (rows.length !== 1) return yield* failure("Invalid stage checkpoint count")
    const state = yield* Schema.decodeUnknownEffect(State)(rows[0])
    if (state.token !== token || (state.session_id === null) !== (state.seq === null) ||
      (state.session_id === null) !== (state.message_id === null) || (state.seq !== null && !Number.isSafeInteger(state.seq)))
      return yield* failure("Invalid stage checkpoint cursor or token")
    if ((yield* db.all<{ integrity_check: string }>("PRAGMA integrity_check")).some((row) => row.integrity_check !== "ok") ||
      (yield* db.all("PRAGMA foreign_key_check")).length) return yield* failure("Invalid stage integrity")
    if (yield* db.get("SELECT 1 FROM session_message_part WHERE typeof(position) <> 'integer' OR position < 0 OR position > 9007199254740991 LIMIT 1"))
      return yield* failure("Invalid stage part position")
    const archive = yield* DatabaseWsArchive.read(db, state.archive_id)
    if (!archive || (yield* WsNativeSeal.archive(db, archive)) !== state.archive_sha256) return yield* failure("Stage archive seal mismatch")
    const parent = DatabaseWsArchive.sourceTable(archive, "session_message")
    if (!parent) return yield* failure("Missing stage source messages")
    let prefix = empty
    let cursor: { kind: number; session_id: string; seq: number; id: string } | undefined
    for (const kind of [0, 1]) {
      let seek: { session_id: string; seq: number; id: string } | undefined
      while (true) {
        const messages = yield* db.all<{ session_id: string; seq: number; id: string }>(sql`
          SELECT m.session_id, m.seq, m.id FROM session_message m
          WHERE ${seek ? sql`(m.session_id, m.seq, m.id) > (${seek.session_id}, ${seek.seq}, ${seek.id})` : sql`1`}
            AND ${kind === 0 ? sql`` : sql`NOT`} EXISTS (SELECT 1 FROM ${identifier(parent)} a WHERE a.session_id = m.session_id)
          ORDER BY m.session_id, m.seq, m.id LIMIT 128`)
        if (!messages.length) break
        for (const message of messages) prefix = yield* WsNativeSeal.message(db, message.id, prefix)
        seek = messages[messages.length - 1]
        cursor = { ...seek, kind }
      }
    }
    if (seal(prefix, state) !== state.prefix_sha256) return yield* failure("Stage prefix seal mismatch")
    if (state.phase === "ws" || state.phase === "legacy") {
      const kind = state.phase === "ws" ? 0 : 1
      const last = cursor?.kind === kind ? cursor : undefined
      if ((last?.session_id ?? null) !== state.session_id || (last?.seq ?? null) !== state.seq || (last?.id ?? null) !== state.message_id ||
        (state.phase === "ws" && cursor?.kind === 1)) return yield* failure("Stage cursor differs from committed prefix")
      const expected = yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM ${identifier(parent)}
        WHERE ${state.phase !== "ws" ? sql`1` : state.session_id === null ? sql`0` : sql`(session_id, seq, id) <= (${state.session_id}, ${state.seq}, ${state.message_id})`}`)
      const actual = yield* db.get<{ count: number }>(sql`SELECT count(*) AS count FROM session_message m WHERE EXISTS
        (SELECT 1 FROM ${identifier(parent)} a WHERE a.id = m.id AND a.session_id = m.session_id AND a.seq = m.seq)`)
      if (expected?.count !== actual?.count) return yield* failure("Stage WS prefix is incomplete")
    }
    return { ...state, prefix }
  })
}

export function complete(db: Database, archive: DatabaseWsArchive.Archive, prefix: string) {
  return Effect.gen(function* () {
    yield* save(db, prefix, { archive_sha256: yield* WsNativeSeal.archive(db, archive), phase: "complete", session_id: null, seq: null, message_id: null })
  })
}

export function writer(db: Database, checkpoint?: Checkpoint) {
  const pending: { row: Message; stored: NonNullable<WsNativeBatch.Output> }[] = []
  let bytes = 0
  let prefix = checkpoint?.prefix ?? empty
  const flush = Effect.gen(function* () {
    if (!pending.length) return
    const insert = (tx: Database) => Effect.gen(function* () {
      let next = prefix
      if (checkpoint) {
        yield* tx.run(sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data) VALUES ${sql.join(
          pending.map(({ row, stored }) => sql`(${row.id}, ${row.session_id}, ${stored.type}, ${row.seq}, ${row.time_created}, ${row.time_updated}, ${stored.data})`), sql`, `)}`)
        const parts: SQL[] = []
        for (const { row, stored } of pending) {
          for (const part of stored.parts) {
            parts.push(sql`(${row.id}, ${part.position}, ${part.id}, ${part.type}, ${part.data})`)
            if (parts.length < 16) continue
            yield* tx.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data) VALUES ${sql.join(parts, sql`, `)}`)
            parts.length = 0
          }
        }
        if (parts.length) yield* tx.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data) VALUES ${sql.join(parts, sql`, `)}`)
        next = yield* WsNativeSeal.messages(tx, pending.map(({ row }) => row.id), next)
      }
      for (const { row, stored } of pending) {
        if (checkpoint) break
        yield* tx.run(sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
          VALUES (${row.id}, ${row.session_id}, ${stored.type}, ${row.seq}, ${row.time_created}, ${row.time_updated}, ${stored.data})`)
        for (let offset = 0; offset < stored.parts.length; offset += 16)
          yield* tx.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data) VALUES ${sql.join(
            stored.parts.slice(offset, offset + 16).map((part) => sql`(${row.id}, ${part.position}, ${part.id}, ${part.type}, ${part.data})`), sql`, `)}`)
      }
      if (checkpoint) {
        const last = pending[pending.length - 1].row
        yield* save(tx, next, { session_id: last.session_id, seq: last.seq, message_id: last.id })
      }
      return next
    })
    const committed = yield* checkpoint ? db.transaction(insert, { behavior: "immediate" }) : insert(db)
    prefix = committed
    pending.length = 0
    bytes = 0
  })
  return {
    flush,
    prefix: () => prefix,
    phase: (before: Checkpoint["phase"], after: Checkpoint["phase"]) => db.transaction((tx) => Effect.gen(function* () {
      const state = yield* tx.get<{ phase: string }>("SELECT phase FROM _ws_stage")
      if (state?.phase === before) yield* save(tx, prefix, { phase: after, session_id: null, seq: null, message_id: null })
    }), { behavior: "immediate" }),
    accept: (row: Message, stored: NonNullable<WsNativeBatch.Output>) => Effect.gen(function* () {
      const size = Buffer.byteLength(stored.data) + stored.parts.reduce((total, part) => total + Buffer.byteLength(part.data), 0)
      if (pending.length && bytes + size > 1024 * 1024) yield* flush
      pending.push({ row, stored })
      bytes += size
      if (pending.length >= 128 || bytes >= 1024 * 1024) yield* flush
    }),
  }
}

function seal(prefix: string, state: typeof State.Type) {
  return createHash("sha256").update(JSON.stringify([state.version, state.token, state.archive_id, state.phase,
    state.session_id, state.seq, state.message_id, state.archive_sha256, prefix, [...DatabaseMigration.currentMigrationIDs].sort()])).digest("hex")
}

function save(db: Database, prefix: string, update: Partial<Pick<typeof State.Type, "phase" | "session_id" | "seq" | "message_id" | "archive_sha256">>) {
  return Effect.gen(function* () {
    const state = yield* db.get<typeof State.Type>("SELECT * FROM _ws_stage")
    if (!state) return yield* failure("Missing stage checkpoint")
    const next = { ...state, ...update }
    yield* db.run(sql`UPDATE _ws_stage SET phase = ${next.phase}, session_id = ${next.session_id}, seq = ${next.seq},
      message_id = ${next.message_id}, archive_sha256 = ${next.archive_sha256}, prefix_sha256 = ${seal(prefix, next)}`)
  })
}
