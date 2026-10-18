import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNative } from "@opencode/core/database/ws-native"
import { WsNativeSource } from "@opencode/core/database/ws-native-source"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"
import { setupWs, setupPreviousNative } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"

test("large mixed legacy and WS event history converts without omitted source rows", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  for (let index = 0; index < 270; index++) {
    const id = `msg_bulk_${String(index).padStart(3, "0")}`
    yield* db.run(sql`INSERT INTO message SELECT ${id}, session_id, ${index + 3}, ${index + 3}, data FROM message WHERE id = 'msg_legacy'`)
    yield* db.run(sql`INSERT INTO part VALUES (${`prt_bulk_${index}`}, ${id}, 'ses_legacy', ${index + 3}, ${index + 3}, ${JSON.stringify({ type: "text", text: "x".repeat(12000) })})`)
    yield* db.run(sql`INSERT INTO event VALUES (${`evt_bulk_${index}`}, 'ses_migration', ${index + 51}, 'session.next.text.ended.1',
      ${JSON.stringify({ sessionID: "ses_migration", timestamp: 4, assistantMessageID: "msg_migration", textID: "text-1", text: "x".repeat(12000) })})`)
  }
  yield* db.run("UPDATE event_sequence SET seq = 1000")
  const reader = {
    get: db.get.bind(db),
    all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => expect(rows.length).toBeLessThanOrEqual(128))))
      return result
    },
  }
  yield* WsNativeSource.read(reader)
  yield* WsNative.convert(db)
  expect(yield* db.get("SELECT count(*) AS count FROM session_message WHERE session_id = 'ses_legacy'")).toEqual({ count: 271 })
  expect(yield* db.get("SELECT json_extract(data, '$.text') AS text, json_extract(data, '$.time.created') AS created FROM session_message WHERE id = 'msg_bulk_269'"))
    .toEqual({ text: "x".repeat(12000), created: 272 })
  yield* WsNative.validate(db)
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped)))

for (const previous of [false, true]) test(`selected restore validates unrelated ${previous ? "pending" : "epoch"} corruption and rolls back`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* (previous ? setupPreviousNative : setupWs)(db, { directory: "/fixture", rich: true })
  yield* WsNative.convert(db)
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing archive manifest")
  const source = DatabaseWsArchive.sourceTable(archive, previous ? "session_pending" : "session_context_epoch")
  if (!source) throw new Error("Missing source table")
  yield* db.run(sql`DROP TRIGGER ${DatabaseWsArchive.identifier(`${source}_update_immutable`)}`)
  yield* db.run(previous ? sql`UPDATE ${DatabaseWsArchive.identifier(source)} SET data = '{'`
    : sql`UPDATE ${DatabaseWsArchive.identifier(source)} SET snapshot = '{'`)
  yield* db.run("DELETE FROM session_v2 WHERE id = 'ses_legacy'")
  const result = yield* db.transaction((tx) => WsNative.restoreSessions(tx, archive, ["ses_legacy"])).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  expect(yield* db.get("SELECT id FROM session_v2 WHERE id = 'ses_legacy'")).toBeUndefined()
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped)))
