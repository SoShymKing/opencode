import { expect, test } from "bun:test"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { WsNative } from "../src/database/ws-native"
import { WsNativeBatch } from "../src/database/ws-native-batch"
import { WsNativeProgress } from "../src/database/ws-native-progress"
import { WsNativeSeal } from "../src/database/ws-native-seal"
import { setupWs, model } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"

function fixture(db: EffectDrizzleSqlite.EffectSQLiteDatabase, count: number) {
  return Effect.gen(function* () {
    yield* setupWs(db, { directory: "/fixture" })
    const data = JSON.stringify({ agent: "build", model, time: { created: 1 }, content:
      Array.from({ length: 9 }, (_, position) => ({ id: `text-${position}`, type: "text", text: `part ${position}` })) })
    yield* db.run(sql`UPDATE session_message SET seq = 0, data = ${data}`)
    yield* db.run(sql`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < ${count - 1})
      INSERT INTO session_message SELECT printf('msg_ws_%04d', value), session_id, type, value, time_created, time_updated, data FROM session_message, n WHERE seq = 0`)
    yield* db.run(sql`UPDATE event_sequence SET seq = ${count - 1}`)
    const rows = yield* db.all<WsNativeBatch.Row>("SELECT * FROM session_message ORDER BY seq")
    const stored = yield* WsNativeBatch.process(rows.map((row) => ({ row, parts: undefined, previousNative: false, store: true })))
    const checkpoint = yield* WsNative.prepareStage(db, "writes-token")
    return { rows, stored, checkpoint }
  })
}

for (const count of [38, 128]) test(`checkpoint batches ${count} parents and streams nine parts per message`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    const input = yield* fixture(db, count)
    const direct = WsNativeProgress.writer(db)
    for (const [index, row] of input.rows.entries()) {
      const stored = input.stored[index]
      if (!stored) throw new Error("Missing stored fixture")
      yield* direct.accept(row, stored)
    }
    yield* direct.flush
    const parents = yield* db.all("SELECT * FROM session_message ORDER BY seq")
    const children = yield* db.all("SELECT * FROM session_message_part ORDER BY message_id, position")
    let prefix = input.checkpoint.prefix
    for (const row of input.rows) prefix = yield* WsNativeSeal.message(db, row.id, prefix)
    yield* db.run("DELETE FROM session_message_part")
    yield* db.run("DELETE FROM session_message")
    const inserts: { sql: string; bindings: number }[] = []
    const transaction = db.transaction.bind(db)
    db.transaction = (callback, options) => transaction((tx) => {
      const run = tx.run.bind(tx)
      tx.run = (query) => {
        const result = run(query)
        const statement = result.getQuery()
        if (/INSERT INTO session_message(?:_part)?\s/.test(statement.sql)) {
          expect(options?.behavior).toBe("immediate")
          inserts.push({ sql: statement.sql, bindings: statement.params.length })
        }
        return result
      }
      return callback(tx)
    }, options)
    const writer = WsNativeProgress.writer(db, input.checkpoint)
    for (const [index, row] of input.rows.entries()) {
      const stored = input.stored[index]
      if (!stored) throw new Error("Missing stored fixture")
      yield* writer.accept(row, stored)
    }
    yield* writer.flush
    expect(yield* db.all("SELECT * FROM session_message ORDER BY seq")).toEqual(parents)
    expect(yield* db.all("SELECT * FROM session_message_part ORDER BY message_id, position")).toEqual(children)
    expect(writer.prefix()).toBe(prefix)
    expect((yield* WsNativeProgress.verify(db, "writes-token")).prefix).toBe(prefix)
    expect(inserts.filter((query) => query.sql.includes("INSERT INTO session_message ("))).toHaveLength(1)
    expect(inserts.filter((query) => query.sql.includes("INSERT INTO session_message_part ("))).toHaveLength(Math.ceil(count * 9 / 16))
    expect(inserts[0].bindings).toBe(count * 7)
    expect(inserts[0].bindings).toBeLessThanOrEqual(896)
    for (const query of inserts.slice(1)) expect(query.bindings).toBeLessThanOrEqual(80)
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped)))

for (const child of [false, true]) test(`late ${child ? "child" : "parent"} trigger rolls back the whole checkpoint batch`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    const input = yield* fixture(db, 300)
    yield* db.run(child
      ? "CREATE TRIGGER fail_late BEFORE INSERT ON session_message_part WHEN (SELECT seq FROM session_message WHERE id = NEW.message_id) >= 270 BEGIN SELECT RAISE(ABORT, 'late child'); END"
      : "CREATE TRIGGER fail_late BEFORE INSERT ON session_message WHEN NEW.seq >= 270 BEGIN SELECT RAISE(ABORT, 'late parent'); END")
    expect((yield* WsNative.resumeStage(db, "writes-token").pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* db.get("SELECT count(*) AS count, max(seq) AS last FROM session_message")).toEqual({ count: 256, last: 255 })
    expect(yield* db.get("SELECT count(*) AS count FROM session_message_part")).toEqual({ count: 256 * 9 })
    const checkpoint = yield* WsNativeProgress.verify(db, "writes-token")
    expect(checkpoint.phase).toBe("ws")
    expect(checkpoint.seq).toBe(255)
    expect(checkpoint.message_id).toBe(input.rows[255].id)
    let prefix = input.checkpoint.prefix
    for (const row of input.rows.slice(0, 256)) prefix = yield* WsNativeSeal.message(db, row.id, prefix)
    expect(checkpoint.prefix).toBe(prefix)
    yield* db.run("DROP TRIGGER fail_late")
    yield* db.run("CREATE TRIGGER refuse_replay BEFORE INSERT ON session_message WHEN NEW.seq <= 255 BEGIN SELECT RAISE(ABORT, 'replayed prefix'); END")
    yield* WsNative.resumeStage(db, "writes-token")
    expect(yield* db.get("SELECT count(*) AS count FROM session_message")).toEqual({ count: 300 })
    expect(yield* db.get("SELECT count(*) AS count FROM session_message_part")).toEqual({ count: 2700 })
    expect((yield* WsNativeProgress.verify(db, "writes-token")).phase).toBe("complete")
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped)))

test("direct writer keeps parent/parts trigger order and caller-owned rollback", () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    const input = yield* fixture(db, 38)
    yield* db.run("CREATE TRIGGER ordered_parent BEFORE INSERT ON session_message WHEN (SELECT count(*) FROM session_message_part) <> NEW.seq * 9 BEGIN SELECT RAISE(ABORT, 'parts must precede next parent'); END")
    const write = (tx: EffectDrizzleSqlite.EffectSQLiteDatabase) => Effect.gen(function* () {
      const writer = WsNativeProgress.writer(tx)
      for (const [index, row] of input.rows.entries()) {
        const stored = input.stored[index]
        if (!stored) throw new Error("Missing stored fixture")
        yield* writer.accept(row, stored)
      }
      yield* writer.flush
    })
    yield* db.run("CREATE TRIGGER fail_late BEFORE INSERT ON session_message_part WHEN (SELECT seq FROM session_message WHERE id = NEW.message_id) = 37 BEGIN SELECT RAISE(ABORT, 'late child'); END")
    expect((yield* db.transaction(write, { behavior: "immediate" }).pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* db.get("SELECT count(*) AS count FROM session_message")).toEqual({ count: 0 })
    expect(yield* db.get("SELECT count(*) AS count FROM session_message_part")).toEqual({ count: 0 })
    yield* db.run("DROP TRIGGER fail_late")
    yield* db.transaction(write, { behavior: "immediate" })
    expect(yield* db.get("SELECT count(*) AS count FROM session_message_part")).toEqual({ count: 342 })
    expect(yield* db.get("SELECT seq, phase FROM _ws_stage")).toEqual({ seq: null, phase: "ws" })
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped)))
