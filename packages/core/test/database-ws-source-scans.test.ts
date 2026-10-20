import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Context, Deferred, Effect, Fiber } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNativeSource } from "@opencode/core/database/ws-native-source"
import { WsNativePrevious } from "@opencode/core/database/ws-native-previous"
import { WsNativeScan } from "@opencode/core/database/ws-native-scan"
import { choose, estimate, headroom, lease, pipeline, Resources, workerBytes } from "@opencode/core/database/ws-native-serial"
import { openWith, openWorker } from "@opencode/core/database/ws-native-bun"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import type { Worker } from "node:worker_threads"
import { setupWs, setupPreviousNative, instructionInitialHash, instructionCurrentHash, messageData } from "./fixture/database-ws"

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

test("pipeline reads during gated compute and computes all fitting later batches during blocked writes", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE pipeline_source (id TEXT, session_id TEXT, type TEXT, seq INTEGER, time_created INTEGER, time_updated INTEGER, data TEXT)")
  const data = messageData.replace('"first"', JSON.stringify("x".repeat(270000)))
  for (const [seq, id] of ["msg_first", "msg_gate", "msg_second", "msg_third", "msg_fourth", "msg_fifth"].entries())
    yield* db.run(sql`INSERT INTO pipeline_source VALUES (${id}, 'ses_migration', 'assistant', ${seq}, 1, 16, ${data})`)
  const workers: Worker[] = []
  const converter = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(
    Effect.tap((opened) => Effect.sync(() => { workers.push(opened.worker) }))))
  const available = yield* headroom
  const readAhead = yield* Deferred.make<void>()
  const writing = yield* Deferred.make<void>()
  const written = yield* Deferred.make<void>()
  const computed = yield* Deferred.make<void>()
  const firstComputed = yield* Deferred.make<void>()
  let reads = 0
  let batches = 0
  const seen: string[] = []
  const read = Effect.gen(function* () {
    if (reads === 3) return undefined
    yield* lease(2 * estimate(Buffer.byteLength(data)), "page")
    const rows = yield* db.all<WsNativeBatch.Row>(sql`SELECT * FROM pipeline_source ORDER BY seq LIMIT 2 OFFSET ${reads * 2}`)
    reads++
    if (reads === 3) yield* Deferred.succeed(readAhead, undefined)
    if (!rows.length) return undefined
    return rows.map((row): WsNativeBatch.Job => ({ kind: "message", input: { row, parts: undefined, previousNative: false, store: true } }))
  })
  const compute = (jobs: readonly WsNativeBatch.Job[], emit: (result: WsNativeBatch.Result, index: number) => Effect.Effect<void>) => Effect.gen(function* () {
    const batch = ++batches
    yield* converter.stream(jobs, emit, true)
    if (batch === 1) yield* Deferred.succeed(firstComputed, undefined)
    if (batch === 3) yield* Deferred.succeed(computed, undefined)
  })
  const fiber = yield* pipeline(read, compute, (job, output) => Effect.gen(function* () {
    expect(output.kind).toBe("message")
    seen.push(WsNativeBatch.jobID(job))
    if (seen.length !== 1) return
    yield* Deferred.succeed(writing, undefined)
    yield* Deferred.await(written)
  })).pipe(Effect.forkChild)
  yield* Deferred.await(writing)
  yield* Deferred.await(readAhead)
  expect(reads).toBe(3)
  expect(seen).toEqual(["msg_first"])
  workers.forEach((worker) => worker.postMessage("release"))
  yield* Deferred.await(computed)
  yield* Deferred.await(firstComputed)
  expect(converter.threadIDs().length).toBe(2)
  expect(reads).toBe(3)
  expect(seen).toEqual(["msg_first"])
  expect(yield* headroom).toBe(available - 6 * estimate(Buffer.byteLength(data)))
  yield* Deferred.succeed(written, undefined)
  yield* Fiber.join(fiber)
  expect(seen).toEqual(["msg_first", "msg_gate", "msg_second", "msg_third", "msg_fourth", "msg_fifth"])
  expect(yield* headroom).toBe(available)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("pipeline waits for a payload lease instead of rejecting a fitting next page", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false })
  for (let index = 0; index < 260; index++) yield* db.run(sql`INSERT INTO session_message VALUES
    (${`msg_memory_${index}`}, 'ses_migration', 'assistant', ${index + 51}, 1, 16, ${messageData})`)
  let reads = 0
  const reader = { get: db.get.bind(db), all: <T>(query: Parameters<typeof db.all>[0]) => {
    const result = db.all<T>(query)
    if (result.getQuery().sql.includes("SELECT id, session_id")) reads++
    return result
  } }
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  let consumed = 0
  const fiber = yield* WsNativeScan.consume(reader,
    { parent: "session_message", child: undefined, previousNative: false, store: () => true },
    () => Effect.gen(function* () {
      if (consumed++ !== 0) return
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
    })).pipe(Effect.forkChild)
  yield* Deferred.await(entered)
  yield* Effect.yieldNow
  expect(reads).toBe(1)
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(fiber)
  expect(consumed).toBe(261)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 2 * 128 * estimate(Buffer.byteLength(messageData)) })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("bounded pipeline retains the caller transaction after late conversion failure", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false })
  yield* db.run("CREATE TABLE pipeline_target (id TEXT PRIMARY KEY, data TEXT)")
  for (let index = 0; index < 260; index++) yield* db.run(sql`INSERT INTO session_message VALUES
    (${`msg_pipeline_${index}`}, 'ses_migration', 'assistant', ${index + 51}, 1, 16, ${index === 259 ? '{}' : messageData})`)
  let writes = 0
  const written = yield* Deferred.make<void>()
  const result = yield* db.transaction((tx) => WsNativeScan.consume({ get: tx.get.bind(tx), all: <T>(query: Parameters<typeof tx.all>[0]) => {
    const result = tx.all<T>(query)
    if (result.getQuery().sql.includes("SELECT id, session_id") && result.getQuery().params.includes("msg_pipeline_259")) {
      const execute = result.execute
      result.execute = () => Deferred.await(written).pipe(Effect.andThen(execute()))
    }
    return result
  } },
    { parent: "session_message", child: undefined, previousNative: false, store: () => true },
    (row, output) => Effect.gen(function* () {
      expect((yield* VisitorValue).value).toBe("transaction")
      yield* tx.run(sql`INSERT INTO pipeline_target VALUES (${row.id}, ${output?.data ?? ''})`)
      writes++
      yield* Deferred.succeed(written, undefined)
    }))).pipe(Effect.provideService(VisitorValue, { value: "transaction" }), Effect.result)
  expect(result._tag).toBe("Failure")
  expect(writes).toBeGreaterThan(0)
  expect(yield* db.get("SELECT count(*) AS count FROM pipeline_target")).toEqual({ count: 0 })
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 1024 ** 3 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("worker row waves span bounded SQL pages and feed twenty CPUs", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE source (id TEXT PRIMARY KEY, time_created INTEGER, data TEXT)")
  for (let index = 0; index < 220; index++) yield* db.run(sql`INSERT INTO source VALUES
    (${String(index).padStart(3, "0")}, ${Math.floor(index / 3)}, ${"x".repeat(27500)})`)
  let reads = 0
  const reader = { all: <T>(query: Parameters<typeof db.all>[0]) => {
    const result = db.all<T>(query)
    const execute = result.execute
    result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => {
      expect(rows.length).toBeLessThanOrEqual(128)
      const bytes = rows.reduce((sum, row) => sum + (typeof row === "object" && row !== null && "data" in row && typeof row.data === "string"
        ? Buffer.byteLength(row.data) : 0), 0)
      expect(bytes).toBeLessThanOrEqual(1024 * 1024)
      reads++
    })))
    return result
  } }
  const seen: string[] = []
  let waves = 0
  yield* WsNativeScan.rows(reader, { table: "source", key: { first: "time_created", then: "id" }, bytes: sql`octet_length(m.data)`, workers: true },
    (rows: readonly { id: string; data: string }[]) => Effect.sync(() => {
      if (!waves++) {
        expect(choose({ cpu: 20, freeBytes: 4 * 1024 ** 3 }, rows.map((row) => estimate(Buffer.byteLength(row.data)))).workers).toBe(20)
        expect(reads).toBeGreaterThan(4)
      }
      seen.push(...rows.map((row) => row.id))
    }))
  expect(seen).toEqual(Array.from({ length: 220 }, (_, index) => String(index).padStart(3, "0")))
  expect(waves).toBe(2)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 20, freeBytes: 4 * 1024 ** 3 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

for (const cpu of [1, 20]) test(`consume on ${cpu} CPUs batches bounded parent/child reads before awaited writes`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE source (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, seq INTEGER, time_created INTEGER, time_updated INTEGER, data TEXT)")
  yield* db.run("CREATE TABLE children (message_id TEXT, position INTEGER, id TEXT, type TEXT, data TEXT)")
  for (let index = 0; index < 260; index++) {
    const id = `msg_${String(index).padStart(3, "0")}`
    yield* db.run(sql`INSERT INTO source VALUES (${id}, 'ses_migration', 'assistant', 7, 1, 1,
      ${JSON.stringify({ agent: "build", model: { providerID: "test", id: "model" }, time: { created: 1 }, metadata: { padding: "x".repeat(27500) } })})`)
    if (index % 2) continue
    yield* db.run(sql`INSERT INTO children VALUES (${id}, 1, 'second', 'text', '{"text":"second"}'), (${id}, 0, 'first', 'text', '{ "text":"first\\u0020text" }')`)
  }
  let payloads = 0
  const prefetched = yield* Deferred.make<void>()
  const available = yield* headroom
  const weights = new Map<string, number>()
  const reader = { get: db.get.bind(db), all: <T>(query: Parameters<typeof db.all>[0]) => {
    const result = db.all<T>(query)
    const parent = result.getQuery().sql.includes("SELECT id, session_id")
    if (result.getQuery().sql.includes("AS bytes")) {
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => rows.length ? Effect.void : Deferred.succeed(prefetched, undefined)))
    }
    if (!result.getQuery().sql.includes("AS bytes")) {
      payloads++
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => {
        expect(rows.length).toBeLessThanOrEqual(128)
        let bytes = 0
        for (const row of rows) {
          if (typeof row !== "object" || row === null || !("data" in row) || typeof row.data !== "string") continue
          bytes += Buffer.byteLength(row.data)
          if (parent && "id" in row && typeof row.id === "string") weights.set(row.id, estimate(Buffer.byteLength(row.data)))
          if (!parent && "message_id" in row && typeof row.message_id === "string")
            weights.set(row.message_id, (weights.get(row.message_id) ?? 0) + estimate(Buffer.byteLength(row.data)))
        }
        expect(bytes).toBeLessThanOrEqual(1024 * 1024)
      })))
    }
    return result
  } }
  const seen: string[] = []
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const fiber = yield* WsNativeScan.consume(reader, { parent: "source", child: "children", previousNative: false, store: () => true }, (row, output) => Effect.gen(function* () {
    expect((yield* VisitorValue).value).toBe("context")
    seen.push(row.id)
    expect(output?.parts.map((part) => part.data)).toEqual(Number(row.id.slice(4)) % 2 ? [] : ['{ "text":"first\\u0020text" }', '{"text":"second"}'])
    if (seen.length !== 1) return
    if (cpu === 20) expect(choose({ cpu, freeBytes: 4 * 1024 ** 3 }, [...weights.values()]).workers).toBe(20)
    yield* Deferred.succeed(entered, undefined)
    yield* Deferred.await(release)
  })).pipe(Effect.provideService(VisitorValue, { value: "context" }), Effect.forkChild)
  yield* Deferred.await(entered)
  yield* Deferred.await(prefetched).pipe(Effect.timeout("10 seconds"))
  expect(payloads).toBe(16)
  const retained = [...weights.values()].reduce((sum, bytes) => sum + bytes, 0)
  expect(available - (yield* headroom)).toBeGreaterThanOrEqual(retained)
  expect(available - (yield* headroom)).toBeLessThanOrEqual(retained + (cpu === 1 ? 0 : cpu * workerBytes))
  expect(seen.length).toBe(1)
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(fiber)
  expect(seen).toEqual(Array.from({ length: 260 }, (_, index) => `msg_${String(index).padStart(3, "0")}`))
  expect(payloads).toBe(16)
}).pipe(Effect.provideService(Resources, () => ({ cpu, freeBytes: 4 * 1024 ** 3 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

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
