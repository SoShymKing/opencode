import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { WsNativeProgress } from "../src/database/ws-native-progress"
import { WsNativeScan } from "../src/database/ws-native-scan"
import { WsNativeSeal } from "../src/database/ws-native-seal"

const schema = Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, seq INTEGER, time_created INTEGER, time_updated INTEGER, data TEXT)")
  yield* db.run("CREATE TABLE session_message_part (message_id TEXT, position INTEGER, id TEXT, type TEXT, data TEXT, PRIMARY KEY (message_id, position))")
  return db
})

function observe(db: EffectDrizzleSqlite.EffectSQLiteDatabase) {
  const queries: { sql: string; params: unknown[] }[] = []
  const sizes: number[] = []
  const inspect = (value: unknown) => {
    if (typeof value !== "object" || value === null) return 0
    return Object.values(value).reduce<number>((total, scalar) => total + (typeof scalar === "string" ? Buffer.byteLength(scalar) : 0), 0)
  }
  const get = db.get.bind(db)
  db.get = <T>(query: Parameters<typeof db.get>[0]) => {
    const result = get<T>(query)
    queries.push(result.getQuery())
    const execute = result.execute
    result.execute = () => execute().pipe(Effect.tap((value) => Effect.sync(() => { sizes.push(inspect(value)) })))
    return result
  }
  const all = db.all.bind(db)
  db.all = <T>(query: Parameters<typeof db.all>[0]) => {
    const result = all<T>(query)
    queries.push(result.getQuery())
    const execute = result.execute
    result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => { sizes.push(rows.reduce((total, row) => total + inspect(row), 0)) })))
    return result
  }
  return { queries, sizes }
}

for (const count of [38, 128]) for (const parts of [0, 9, 35]) test(`batch seal matches commit order with ${count} messages and ${parts} parts in two reads`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* schema
    const ids = Array.from({ length: count }, (_, index) => `message-${count - index}`)
    for (const id of ids) {
      yield* db.run(sql`INSERT INTO session_message VALUES (${id}, 'session', 'assistant', 9223372036854775807, 1.25, NULL, ${'{ "number":1.00e+2,"text":"\uD55C\uAE00\uD83D\uDE00" }\0tail'})`)
      for (let position = parts - 1; position >= 0; position--)
        yield* db.run(sql`INSERT INTO session_message_part VALUES (${id}, ${position * 3}, ${`part-${position}`}, 'text',
          ${position % 3 === 0 ? sql`X'00FF80'` : sql`${'{ "number":-0.00e+0,"text":"\u00E9\uD55C\uD83D\uDE00" }\0tail'}`})`)
    }
    let reference = "previous-seal"
    for (const id of ids) reference = yield* WsNativeSeal.message(db, id, reference)
    const observed = observe(db)

    expect(yield* WsNativeSeal.messages(db, ids, "previous-seal")).toBe(reference)
    expect(observed.queries).toHaveLength(2)
    expect(Math.max(...observed.sizes)).toBeLessThanOrEqual(WsNativeScan.byteLimit)
    for (const query of [...observed.queries]) {
      expect(query.params.length).toBeLessThanOrEqual(128)
      const params = [...query.params]
      const plan = yield* db.all<{ detail: string }>(sql`EXPLAIN QUERY PLAN ${sql.raw(query.sql.replace(/\?/g, () => {
        const value = params.shift()
        if (typeof value !== "string") throw new Error("Expected ID binding")
        return `'${value.replaceAll("'", "''")}'`
      }))}`)
      expect(plan.some((row) => row.detail.includes("TEMP B-TREE"))).toBe(false)
      if (parts) expect(plan.some((row) => row.detail.includes("message_id=?"))).toBe(true)
    }
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("batch seal pages known IDs and rejects a missing parent without sealing a partial list", () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* schema
    const ids = Array.from({ length: 129 }, (_, index) => `message-${129 - index}`)
    for (const id of ids) yield* db.run(sql`INSERT INTO session_message VALUES (${id}, 'session', 'assistant', 1, 1, 1, '{}')`)
    let reference = "previous-seal"
    for (const id of ids) reference = yield* WsNativeSeal.message(db, id, reference)
    const observed = observe(db)

    expect(yield* WsNativeSeal.messages(db, [], "previous-seal")).toBe("previous-seal")
    expect(observed.queries).toHaveLength(0)
    expect(yield* WsNativeSeal.messages(db, ids, "previous-seal")).toBe(reference)
    expect(observed.queries).toHaveLength(4)
    for (const query of observed.queries) expect(query.params.length).toBeLessThanOrEqual(128)
    expect((yield* WsNativeSeal.messages(db, [...ids, "missing"], "previous-seal").pipe(Effect.exit))._tag).toBe("Failure")
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const target of ["parent", "part", "group"] as const) test(`batch seal bounds ${target} payload reads before materializing canonical data`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* schema
    const ids = Array.from({ length: 128 }, (_, index) => `message-${128 - index}`)
    for (const [index, id] of ids.entries()) {
      yield* db.run(sql`INSERT INTO session_message VALUES (${id}, 'session', 'assistant', ${index}, 1, 1,
        ${target === "parent" && index === 63 ? sql`zeroblob(600000)` : target === "group" ? sql`zeroblob(10000)` : sql`'{}'`})`)
      yield* db.run(sql`INSERT INTO session_message_part VALUES (${id}, 0, 'part', 'text',
        ${target === "part" && index === 63 ? sql`zeroblob(600000)` : sql`'{}'`})`)
    }
    let reference = "previous-seal"
    for (const id of ids) reference = yield* WsNativeSeal.message(db, id, reference)
    const observed = observe(db)

    expect(yield* WsNativeSeal.messages(db, ids, "previous-seal")).toBe(reference)
    expect(Math.max(...observed.sizes)).toBeLessThanOrEqual(WsNativeScan.byteLimit)
    expect(observed.queries.length).toBeLessThan(30)
    expect(observed.queries.some((query) => query.sql.includes("hex(substr(CAST("))).toBe(target !== "group")
    if (target === "group") expect(observed.queries).toHaveLength(4)
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const checkpointed of [false, true]) for (const count of [38, 128]) test(`writer seals ${count} pending messages with ${checkpointed ? "three" : "zero"} reads`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* schema
    const checkpoint = { singleton: 1, version: 1, token: "token", archive_id: "archive", phase: "ws", session_id: null,
      seq: null, message_id: null, archive_sha256: "archive-seal", prefix_sha256: "checkpoint-seal", prefix: "previous-seal" } satisfies WsNativeProgress.Checkpoint
    yield* db.run("CREATE TABLE _ws_stage (singleton, version, token, archive_id, phase, session_id, seq, message_id, archive_sha256, prefix_sha256)")
    yield* db.run("INSERT INTO _ws_stage VALUES (1, 1, 'token', 'archive', 'ws', NULL, NULL, NULL, 'archive-seal', 'checkpoint-seal')")
    const observed = observe(db)
    const transaction = db.transaction.bind(db)
    db.transaction = (callback, options) => transaction((tx) => {
      const reads = observe(tx)
      return callback(tx).pipe(Effect.tap(() => Effect.sync(() => { observed.queries.push(...reads.queries) })))
    }, options)
    const writer = WsNativeProgress.writer(db, checkpointed ? checkpoint : undefined)

    for (let index = 0; index < count; index++) yield* writer.accept({ id: `message-${count - index}`, session_id: "session", seq: index, time_created: 1, time_updated: 1 },
      { type: "assistant", data: "{}", parts: Array.from({ length: 9 }, (_, position) => ({ position, id: `part-${position}`, type: "text", data: "{}" })) })
    yield* writer.flush

    expect(observed.queries).toHaveLength(checkpointed ? 3 : 0)
    if (!checkpointed) return
    let reference = checkpoint.prefix
    for (let index = 0; index < count; index++) reference = yield* WsNativeSeal.message(db, `message-${count - index}`, reference)
    expect(writer.prefix()).toBe(reference)
    expect(yield* db.get("SELECT seq, message_id FROM _ws_stage")).toEqual({ seq: count - 1, message_id: "message-1" })
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))
