import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Context, Deferred, Effect, Fiber } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNativeSource } from "@opencode/core/database/ws-native-source"
import { WsNativePrevious } from "@opencode/core/database/ws-native-previous"
import { setupWs, setupPreviousNative, instructionInitialHash, instructionCurrentHash } from "./fixture/database-ws"

for (const previous of [false, true]) test(`${previous ? "P" : "WS"} event validation bounds real database reads`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* (previous ? setupPreviousNative : setupWs)(db, { directory: "/fixture", rich: false })
  yield* db.run("UPDATE event_sequence SET seq = 1000")
  for (let index = 0; index < 270; index++) {
    const data = JSON.stringify({ sessionID: "ses_migration", assistantMessageID: "msg_migration", text: "x".repeat(12000),
      ...(previous ? { ordinal: 0 } : { timestamp: 4, textID: "text-1" }) })
    yield* previous
      ? db.run(sql`INSERT INTO event VALUES (${`evt_${index}`}, 'ses_migration', ${index}, 4, 'session.text.ended.1', ${data})`)
      : db.run(sql`INSERT INTO event VALUES (${`evt_${index}`}, 'ses_migration', ${index}, 'session.next.text.ended.1', ${data})`)
  }
  const sizes: number[] = []
  const reader = {
    get: db.get.bind(db),
    all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => {
        sizes.push(rows.length)
        expect(rows.length).toBeLessThanOrEqual(128)
        const bytes = rows.reduce((total, row) => total + (typeof row === "object" && row !== null && "data" in row && typeof row.data === "string"
          ? Buffer.byteLength(row.data) : 0), 0)
        if (rows.length > 1) expect(bytes).toBeLessThanOrEqual(1024 * 1024)
      })))
      return result
    },
  }
  yield* previous ? WsNativePrevious.read(reader) : WsNativeSource.read(reader)
  expect(sizes.length).toBeGreaterThan(2)
  yield* db.run("UPDATE event SET data = '{}' WHERE id = 'evt_99'")
  const invalid = yield* (previous ? WsNativePrevious.read(reader).pipe(Effect.result) : WsNativeSource.read(reader).pipe(Effect.result))
  expect(invalid._tag).toBe("Failure")
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

class VisitorValue extends Context.Service<VisitorValue, { readonly value: string }>()("SourceScanVisitorValue") {}

test("source visitors await consumption and preserve caller context", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const seen: string[] = []
  const fiber = yield* WsNativeSource.read(db, undefined, false, {
    epoch: () => Effect.gen(function* () {
      const value = yield* VisitorValue
      seen.push(value.value)
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
    }),
    input: (input) => Effect.sync(() => {
      expect("prompt" in input.row).toBe(false)
      seen.push(input.row.id)
    }),
  }).pipe(Effect.provideService(VisitorValue, { value: "context" }), Effect.forkChild)
  yield* Deferred.await(entered)
  expect(seen).toEqual(["context"])
  yield* Deferred.succeed(release, undefined)
  const result = yield* Fiber.join(fiber)
  expect(Object.keys(result).sort()).toEqual(["grants", "permissions"])
  expect(seen.length).toBe(4)
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const previous of [false, true]) test(`${previous ? "pending" : "input"} visitor failure propagates without consuming later rows`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* (previous ? setupPreviousNative : setupWs)(db, { directory: "/fixture", rich: true })
  const failure = new Error("visitor stopped")
  const seen: string[] = []
  const accept = (item: { row: { id: string } }) => Effect.gen(function* () {
    seen.push(item.row.id)
    return yield* Effect.fail(failure)
  })
  const result = yield* (previous ? WsNativePrevious.read(db, undefined, accept)
    : WsNativeSource.read(db, undefined, false, { input: accept })).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(result.failure).toBe(failure)
  expect(seen.length).toBe(1)
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const previous of [false, true]) test(`${previous ? "pending" : "input"} validation without a visitor rejects late corruption`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* (previous ? setupPreviousNative : setupWs)(db, { directory: "/fixture", rich: false })
  yield* db.run("UPDATE event_sequence SET seq = 1000")
  for (let index = 0; index < 140; index++) {
    const id = `msg_scan_${String(index).padStart(3, "0")}`
    const data = JSON.stringify({ text: "x".repeat(12000) })
    yield* previous
      ? db.run(sql`INSERT INTO session_pending VALUES (${id}, 'ses_migration', 'user', ${data}, 'queue', ${index}, 1)`)
      : db.run(sql`INSERT INTO session_input VALUES (${id}, 'ses_migration', ${data}, 'queue', ${index}, NULL, 1)`)
  }
  const reader = {
    get: db.get.bind(db),
    all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => expect(rows.length).toBeLessThanOrEqual(128))))
      return result
    },
  }
  yield* previous ? WsNativePrevious.read(reader) : WsNativeSource.read(reader)
  yield* db.run(previous ? "UPDATE session_pending SET data = '{}' WHERE id = 'msg_scan_139'"
    : "UPDATE session_input SET prompt = '{}' WHERE id = 'msg_scan_139'")
  const result = yield* (previous ? WsNativePrevious.read(reader).pipe(Effect.result) : WsNativeSource.read(reader).pipe(Effect.result))
  expect(result._tag).toBe("Failure")
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const hash of [instructionInitialHash, instructionCurrentHash]) test(`previous instructions require referenced blob ${hash.slice(0, 8)}`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupPreviousNative(db, { directory: "/fixture", rich: true })
  yield* db.run(sql`DELETE FROM instruction_blob WHERE hash = ${hash}`)
  const result = yield* WsNativePrevious.read(db).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(String(result.failure)).toContain("Missing P instruction blob")
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("legacy orphan check requires both message and session identity", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* db.run("INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES ('ses_other', 'project', 'other', '/fixture', 'other', 'fixture', 1, 1)")
  yield* db.run("UPDATE part SET session_id = 'ses_other'")
  const result = yield* WsNativeSource.read(db).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(String(result.failure)).toContain("Orphan legacy part")
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const previous of [false, true]) test(`${previous ? "previous instruction" : "epoch"} payload reads stay bounded across sessions`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* (previous ? setupPreviousNative : setupWs)(db, { directory: "/fixture", rich: false })
  for (let index = 0; index < 140; index++) {
    const id = `ses_bulk_${String(index).padStart(3, "0")}`
    yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated)
      VALUES (${id}, 'project', ${id}, '/fixture', 'bulk', 'fixture', 1, 1)`)
    yield* db.run(sql`INSERT INTO event_sequence VALUES (${id}, 10, NULL)`)
    if (!previous) {
      yield* db.run(sql`INSERT INTO session_context_epoch VALUES (${id}, ${"x".repeat(12000)}, '{"policy":{"value":"kept"}}', 1)`)
      continue
    }
    const hash = index.toString(16).padStart(64, "0")
    const value = JSON.stringify("x".repeat(12000))
    yield* db.run(sql`INSERT INTO instruction_blob VALUES (${hash}, ${value})`)
    yield* db.run(sql`INSERT INTO instruction_entry VALUES ('ses_migration', ${id}, ${value}, 0, 1, 1)`)
    yield* db.run(sql`INSERT INTO instruction_state VALUES (${id}, 1, 10, ${JSON.stringify({ policy: hash })}, ${JSON.stringify({ policy: hash })})`)
  }
  const reader = {
    get: db.get.bind(db),
    all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => expect(rows.length).toBeLessThanOrEqual(128))))
      return result
    },
  }
  yield* previous ? WsNativePrevious.read(reader) : WsNativeSource.read(reader)
  yield* db.run(previous ? "UPDATE instruction_blob SET value = '{' WHERE hash = '000000000000000000000000000000000000000000000000000000000000008b'"
    : "UPDATE session_context_epoch SET snapshot = '{' WHERE session_id = 'ses_bulk_139'")
  const result = yield* (previous ? WsNativePrevious.read(reader).pipe(Effect.result) : WsNativeSource.read(reader).pipe(Effect.result))
  expect(result._tag).toBe("Failure")
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))
