import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Deferred, Effect, Fiber } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNative } from "@opencode/core/database/ws-native"
import { setupWs, setupPreviousNative, messageData, richMessageData, nativeMessageData, rawJSON } from "./fixture/database-ws"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import { WsNativeScan } from "@opencode/core/database/ws-native-scan"
import { openWorker } from "@opencode/core/database/ws-native-bun"
import { WsNativeMessage } from "@opencode/core/database/ws-native-message"
import { tempGlobalLayer } from "./fixture/global"
import type { Worker } from "node:worker_threads"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"

const input = (data = richMessageData, previousNative = false): WsNativeBatch.Input => ({
  row: { id: "msg_worker", session_id: "ses_migration", type: "assistant", seq: 7, time_created: 1, time_updated: 16, data },
  parts: undefined, previousNative, store: true,
})

test("source validation bounds message reads when the source exceeds one wave", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false })
  for (let index = 0; index < 300; index++)
    yield* db.run(sql`INSERT INTO session_message VALUES (${`msg_bulk_${index}`}, 'ses_migration', 'assistant', ${index + 51}, 1, 16, ${messageData})`)
  yield* db.run("UPDATE event_sequence SET seq = 1000")
  const sizes: number[] = []
  const reader = {
    get: db.get.bind(db),
    all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => {
        sizes.push(rows.length)
        expect(rows.length).toBeLessThanOrEqual(128)
      })))
      return result
    },
  }
  yield* WsNative.validateSource(reader)
  expect(sizes.length).toBeGreaterThan(1)
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("two real workers preserve serial JSON, independent ordinals and native timestamps", () => Effect.runPromise(Effect.gen(function* () {
  const left = yield* openWorker()
  const right = yield* openWorker()
  expect(left.worker.threadId).toBeGreaterThan(0)
  expect(right.worker.threadId).toBeGreaterThan(0)
  expect(left.worker.threadId).not.toBe(right.worker.threadId)
  const metadata = rawJSON.replace('"negativeZero":-0', '"negativeZero":0')
  const items = [input(richMessageData.replace('"cost":0.125', '"cost":0.12500')),
    input(nativeMessageData, true), input(`{ "agent":"build", "model":{"providerID":"test","id":"model"}, "time":{"created":1}, "content":[], "metadata":${metadata} }`)]
  const expected = yield* WsNativeBatch.process(items)
  const results = yield* Effect.all([left.process(items), right.process(items)], { concurrency: 2 })
  expect(results).toEqual([expected, expected])
  expect(results[0]?.[0]?.parts.map((part) => part.id)).toEqual(["0", "1", "0", "pending-call", "running-call", "empty-call", "hosted-call", "error-call"])
  expect(results[0]?.[0]?.data).toContain('"cost":0.12500')
  expect(results[0]?.[2]?.data).toContain(metadata)
  expect(results[0]?.[1]?.parts.map((part) => part.data)).toEqual(['{"text":"first"}', '{"text":"second"}'])
}).pipe(Effect.scoped)))

test("scan waits for consumption before reading another wave and preserves tied sequence order", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false })
  yield* db.run("DROP INDEX session_message_session_seq_idx")
  for (let index = 0; index < 260; index++)
    yield* db.run(sql`INSERT INTO session_message VALUES (${`msg_bulk_${String(index).padStart(3, "0")}`}, 'ses_migration', 'assistant', 7, 1, 16, ${messageData})`)
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const pages: number[] = []
  const order: string[] = []
  const reader = {
    get: db.get.bind(db),
    all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      if (result.getQuery().sql.includes("AS bytes")) pages.push(1)
      return result
    },
  }
  const fiber = yield* WsNativeScan.consume(reader, { parent: "session_message", child: undefined, previousNative: false, store: () => true },
    (row, output) => Effect.gen(function* () {
      expect(output?.parts.length).toBe(2)
      order.push(row.id)
      if (order.length !== 1) return
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
    })).pipe(Effect.forkChild)
  yield* Deferred.await(entered)
  expect(pages.length).toBe(1)
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(fiber)
  expect(order).toEqual([...Array.from({ length: 260 }, (_, index) => `msg_bulk_${String(index).padStart(3, "0")}`), "msg_migration"])
  expect(pages.length).toBe(4)
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const previousNative of [false, true]) test(`large ${previousNative ? "previous-native" : "WS"} conversion preserves storage and rolls back invalid input`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* (previousNative ? setupPreviousNative : setupWs)(db, { directory: "/fixture", rich: false })
  for (let index = 0; index < 140; index++)
    yield* db.run(sql`INSERT INTO session_message VALUES (${`msg_bulk_${index}`}, 'ses_migration', 'assistant', ${index + 51}, 1, 16, ${previousNative ? nativeMessageData : messageData})`)
  yield* db.run("UPDATE event_sequence SET seq = 1000")
  yield* db.run("UPDATE session_message SET data = '{}' WHERE id = 'msg_bulk_139'")
  const before = yield* db.all("SELECT * FROM sqlite_schema")
  const result = yield* WsNative.convert(db).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(before)
  yield* db.run(sql`UPDATE session_message SET data = ${previousNative ? nativeMessageData : messageData} WHERE id = 'msg_bulk_139'`)
  yield* WsNative.convert(db)
  expect(yield* db.get("SELECT count(*) AS count FROM session_message")).toEqual({ count: 141 })
  expect(yield* db.get("SELECT count(*) AS count FROM session_message_part")).toEqual({ count: 282 })
  expect(yield* db.get("SELECT seq, time_created, time_updated FROM session_message WHERE id = 'msg_bulk_139'")).toEqual({ seq: 190, time_created: 1, time_updated: 16 })
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped)))

for (const mode of ["exit", "error"]) test(`worker ${mode} fails pending conversion and scope closes it`, async () => {
  const result = await Effect.runPromise(Effect.gen(function* () {
    const worker = yield* openWorker(new URL("./fixture/ws-native-worker-failure.ts", import.meta.url))
    const item = input()
    return yield* worker.process([{ ...item, row: { ...item.row, id: `msg_${mode}` } }])
  }).pipe(Effect.scoped, Effect.result))
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") {
    expect(result.failure).toBeInstanceOf(WsNativeMessage.WsNativeMessageError)
    expect(result.failure.messageID).toBe(`msg_${mode}`)
  }
})

test("worker clone failure rejects pending conversion", async () => {
  const item = input()
  const uncloneable = { ...item, row: { ...item.row, extra: () => undefined } }
  const result = await Effect.runPromise(Effect.gen(function* () {
    const worker = yield* openWorker()
    return yield* worker.process([uncloneable])
  }).pipe(Effect.scoped, Effect.result))
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(result.failure.messageID).toBe("msg_worker")
})

test("worker startup failure rejects conversion without serial fallback", async () => {
  const result = await Effect.runPromise(Effect.gen(function* () {
    const worker = yield* openWorker(new URL("./fixture/missing-ws-worker.ts", import.meta.url))
    return yield* worker.process([input()])
  }).pipe(Effect.scoped, Effect.result))
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(result.failure).toBeInstanceOf(WsNativeMessage.WsNativeMessageError)
})

test("interruption terminates a worker with pending work", () => Effect.runPromise(Effect.gen(function* () {
  const started = yield* Deferred.make<Worker>()
  const fiber = yield* Effect.scoped(Effect.gen(function* () {
    const worker = yield* openWorker(new URL("./fixture/ws-native-worker-failure.ts", import.meta.url))
    yield* Deferred.succeed(started, worker.worker)
    const item = input()
    yield* worker.process([{ ...item, row: { ...item.row, id: "msg_wait" } }])
  })).pipe(Effect.forkChild)
  const worker = yield* Deferred.await(started)
  const exit = new Promise<number>((resolve) => worker.once("exit", resolve))
  yield* Fiber.interrupt(fiber)
  expect(typeof (yield* Effect.promise(() => exit))).toBe("number")
}).pipe(Effect.scoped)))

test("child payload budget admits an oversized message alone", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false, lineage: "v02" })
  yield* db.run(sql`UPDATE session_message_part SET data = ${JSON.stringify({ text: "x".repeat(WsNativeScan.byteLimit) })} WHERE position = 0`)
  yield* db.run("INSERT INTO session_message SELECT 'msg_next', session_id, type, 8, time_created, time_updated, data FROM session_message")
  const reads: string[] = []
  const reader = {
    all: db.all.bind(db),
    get: <T>(query: Parameters<typeof db.get>[0]) => {
      const result = db.get<T>(query)
      reads.push(result.getQuery().sql)
      return result
    },
  }
  const consumed: string[] = []
  yield* WsNativeScan.consume(reader, { parent: "session_message", child: "session_message_part", previousNative: false, store: () => true },
    (row) => Effect.sync(() => {
      consumed.push(row.id)
      expect(reads.length).toBe(consumed.length)
    }))
  expect(consumed).toEqual(["msg_migration", "msg_next"])
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("selected restore still rejects invalid unselected archived messages", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* WsNative.convert(db)
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing fixture archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing fixture manifest")
  const messages = DatabaseWsArchive.sourceTable(archive, "session_message")
  if (!messages) throw new Error("Missing fixture messages")
  yield* db.run(sql`DROP TRIGGER ${DatabaseWsArchive.identifier(`${messages}_update_immutable`)}`)
  yield* db.run(sql`UPDATE ${DatabaseWsArchive.identifier(messages)} SET data = '{}' WHERE id = 'msg_migration'`)
  yield* db.run("DELETE FROM session_v2 WHERE id = 'ses_legacy'")
  const result = yield* db.transaction((tx) => WsNative.restoreSessions(tx, archive, ["ses_legacy"])).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") {
    expect(result.failure).toBeInstanceOf(WsNativeMessage.WsNativeMessageError)
  }
  expect(yield* db.get("SELECT id FROM session_v2 WHERE id = 'ses_legacy'")).toBeUndefined()
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped)))

test("worker distinguishes absent children from an empty normalized content list", () => Effect.runPromise(Effect.gen(function* () {
  const worker = yield* openWorker()
  const item = input('{"agent":"build","model":{"providerID":"test","id":"model"},"time":{"created":1}}')
  const absent = yield* worker.process([item]).pipe(Effect.result)
  expect(absent._tag).toBe("Failure")
  const empty = yield* worker.process([{ ...item, parts: [] }])
  expect(empty[0]?.parts).toEqual([])
  const raw = '{ "text" : "unchanged\\u0020text" }'
  const normalized = yield* worker.process([{ ...item, parts: [{ id: "source-text", position: 0, type: "text", data: raw }] }])
  expect(normalized[0]?.parts).toEqual([{ id: "0", position: 0, type: "text", data: raw }])
}).pipe(Effect.scoped)))
