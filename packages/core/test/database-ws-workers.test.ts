import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Deferred, Effect, Fiber, Schema } from "effect"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { decodeStoredMessage, encodeStoredMessage } from "@opencode/core/session/message-storage"
import { normalizeMessage, normalizeMessages } from "@opencode/core/database/native-message-parts"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNative } from "@opencode/core/database/ws-native"
import { setupWs, setupPreviousNative, messageData, richMessageData, nativeMessageData, rawJSON } from "./fixture/database-ws"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import { WsNativeScan } from "@opencode/core/database/ws-native-scan"
import { open, openWith, openWorker } from "@opencode/core/database/ws-native-bun"
import { choose, estimate, headroom, plan, reserve, Resources, workerBytes } from "@opencode/core/database/ws-native-serial"
import { WsNativeMessage } from "@opencode/core/database/ws-native-message"
import { tempGlobalLayer } from "./fixture/global"
import type { Worker } from "node:worker_threads"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"

const input = (data = richMessageData, previousNative = false): WsNativeBatch.Input => ({
  row: { id: "msg_worker", session_id: "ses_migration", type: "assistant", seq: 7, time_created: 1, time_updated: 16, data },
  parts: undefined, previousNative, store: true,
})

test("resource selection uses all feasible CPUs and accounts for active reservations", () => {
  const weights = Array.from({ length: 8 }, (_, index) => 2 * 1024 ** 2 + index)
  for (const cpu of [1, 3, 8]) {
    expect(choose({ cpu, freeBytes: 4 * 1024 ** 3 }, weights).workers).toBe(cpu === 1 ? 0 : cpu)
  }
  expect(choose({ cpu: 8, freeBytes: 4 * 1024 ** 3 }, []).workers).toBe(0)
  expect(choose({ cpu: 20, freeBytes: 4 * 1024 ** 3 }, Array(20).fill(100)).workers).toBe(0)
  expect(choose({ cpu: 20, freeBytes: 4 * 1024 ** 3 }, Array(38).fill(8 * 27500 + 4096)).workers).toBe(4)
  expect(choose({ cpu: 20, freeBytes: 4 * 1024 ** 3 }, Array(20).fill(2 * 1024 ** 2)).workers).toBe(20)
  expect(choose({ cpu: 8, freeBytes: 4 * 1024 ** 3 }, [100]).workers).toBe(0)
  expect(choose({ cpu: 8, freeBytes: 4 * workerBytes + 2 * weights.reduce((sum, weight) => sum + weight, 0) }, weights).workers).toBe(2)
  expect(choose({ cpu: 8, freeBytes: 4 * 1024 ** 3 }, weights, { cpu: 5, bytes: 0 }).workers).toBe(3)
  expect(choose({ cpu: 8, freeBytes: 100 }, [100]).count).toBe(0)
  expect(choose({ cpu: 8, freeBytes: 1000 }, [100, 200, 400]).count).toBe(2)
})

test("wave planning uses CPU and memory headroom without reserving a second time", () => {
  const snapshot = { cpu: 20, freeBytes: 4 * 1024 ** 3 }
  const size = estimate(27500)
  expect(plan(snapshot, size * 38, 38)).toMatchObject({ workers: 20, ready: false })
  const count = Math.ceil(20 * 2 * 1024 ** 2 / size)
  expect(plan(snapshot, size * count, count)).toMatchObject({ workers: 20, ready: true })
  expect(choose(snapshot, Array(count).fill(size)).workers).toBe(20)
  expect(plan({ cpu: 64, freeBytes: 16 * 1024 ** 3 }, 0, 0).workers).toBe(64)
  expect(plan(snapshot, 0, 0, { cpu: 18, bytes: 0 }).workers).toBe(2)
  expect(plan({ cpu: 1, freeBytes: snapshot.freeBytes }, 0, 0)).toMatchObject({ workers: 0, ready: true })
  expect(plan({ cpu: 20, freeBytes: 4 * workerBytes }, workerBytes, 1)).toMatchObject({ workers: 1, ready: true })
  expect(plan({ cpu: 20, freeBytes: 4 * workerBytes }, 2 * workerBytes, 1)).toMatchObject({ workers: 0, ready: true })
})

test("scoped scheduler starts lazily, uses three real threads and reuses them in order", () => Effect.runPromise(Effect.gen(function* () {
  const worker = yield* open
  expect(worker.threadIDs()).toEqual([])
  expect(yield* worker.run([])).toEqual([])
  expect(worker.threadIDs()).toEqual([])
  const jobs = [262144, 300000, 280000, 270000].map((size) => ({ kind: "message" as const,
    input: input(richMessageData.replace('"first"', JSON.stringify("x".repeat(size)))) }))
  const expected = yield* WsNativeBatch.run(jobs)
  expect(yield* worker.run(jobs)).toEqual([...expected])
  const ids = worker.threadIDs()
  expect(new Set(ids).size).toBe(3)
  expect(ids.every((id) => id > 0)).toBe(true)
  expect(yield* worker.run(jobs)).toEqual([...expected])
  expect(worker.threadIDs()).toEqual(ids)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 3, freeBytes: 2 * 1024 ** 3 })), Effect.scoped)))

test("scan refuses an oversized singleton before reading its payload", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE source (id TEXT PRIMARY KEY, data TEXT)")
  yield* db.run("INSERT INTO source VALUES ('oversized', 'payload')")
  const reads: string[] = []
  const reader = { all: <T>(query: Parameters<typeof db.all>[0]) => {
    const result = db.all<T>(query)
    reads.push(result.getQuery().sql)
    return result
  } }
  const result = yield* WsNativeScan.rows(reader, { table: "source", bytes: sql`octet_length(m.data)`, workers: true },
    () => Effect.void).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(result.failure).toMatchObject({ messageID: "oversized" })
  expect(reads.length).toBe(1)
  expect(reads[0]).toContain("AS bytes")
}).pipe(Effect.provideService(Resources, () => ({ cpu: 8, freeBytes: 100 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("late memory pressure rolls back prior scan writes without reading another payload", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE source (id TEXT PRIMARY KEY, data TEXT)")
  yield* db.run("CREATE TABLE target (id TEXT)")
  for (let index = 0; index < 220; index++) yield* db.run(sql`INSERT INTO source VALUES (${String(index).padStart(3, "0")}, ${"x".repeat(27500)})`)
  let freeBytes = 4 * 1024 ** 3
  let writes = 0
  let payloads = 0
  const result = yield* db.transaction((tx) => WsNativeScan.rows({ all: <T>(query: Parameters<typeof tx.all>[0]) => {
    const result = tx.all<T>(query)
    if (!result.getQuery().sql.includes("AS bytes")) {
      expect(writes).toBe(0)
      payloads++
    }
    return result
  } }, { table: "source", bytes: sql`octet_length(m.data)`, workers: true },
    (rows: readonly { id: string }[]) => Effect.gen(function* () {
      for (const row of rows) {
        yield* tx.run(sql`INSERT INTO target VALUES (${row.id})`)
        writes++
      }
      freeBytes = 100
    }))).pipe(Effect.provideService(Resources, () => ({ cpu: 20, freeBytes })), Effect.result)
  expect(result._tag).toBe("Failure")
  expect(payloads).toBeGreaterThan(1)
  expect(writes).toBeGreaterThan(128)
  expect(yield* db.get("SELECT count(*) AS count FROM target")).toEqual({ count: 0 })
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("single CPU conversion admits memory without a thread and releases a failed wave", () => Effect.runPromise(Effect.gen(function* () {
  const worker = yield* open
  const available = yield* headroom
  expect(yield* worker.process([input()])).toEqual(yield* WsNativeBatch.process([input()]))
  expect(worker.threadIDs()).toEqual([])
  const result = yield* worker.process([input("{}")]).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  expect(yield* headroom).toBe(available)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 1024 ** 3 })), Effect.scoped)))

test("active reservations constrain another scope and release after worker failure", () => Effect.runPromise(Effect.gen(function* () {
  const large = input(richMessageData.replace('"first"', JSON.stringify("x".repeat(262144))))
  const available = yield* headroom
  yield* Effect.scoped(Effect.gen(function* () {
    yield* reserve([{ kind: "message", input: large }, { kind: "message", input: large }])
    const remaining = yield* headroom
    expect(remaining).toBeLessThan(available - 2 * workerBytes)
    const worker = yield* open
    yield* worker.process([large, large, large])
    expect(worker.threadIDs().length).toBe(1)
    expect(yield* headroom).toBe(remaining)
  }))
  const result = yield* Effect.scoped(Effect.gen(function* () {
    const worker = yield* open
    return yield* worker.process([large, input("{}"), large])
  })).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  expect(yield* headroom).toBe(available)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 3, freeBytes: 420 * 1024 ** 2 })), Effect.scoped)))

test("resource changes grow and shrink a live scoped worker set", () => {
  let cpu = 1
  return Effect.runPromise(Effect.gen(function* () {
    const exited: number[] = []
    const finalized: number[] = []
    const worker = yield* openWith(openWorker().pipe(Effect.tap((opened) => Effect.gen(function* () {
      const id = opened.worker.threadId
      opened.worker.once("exit", () => exited.push(id))
      yield* Effect.addFinalizer(() => Effect.sync(() => { finalized.push(id) }))
    }))))
    const large = input(richMessageData.replace('"first"', JSON.stringify("x".repeat(262144))))
    const items = [large, large, large]
    yield* worker.process(items)
    expect(worker.threadIDs()).toEqual([])
    cpu = 3
    yield* worker.process(items)
    expect(new Set(worker.threadIDs()).size).toBe(3)
    const first = worker.threadIDs()
    cpu = 1
    yield* worker.process(items)
    expect(worker.threadIDs()).toEqual([])
    expect(exited.toSorted()).toEqual(first.toSorted())
    expect(finalized.toSorted()).toEqual(first.toSorted())
    cpu = 3
    yield* worker.process(items)
    expect(worker.threadIDs().length).toBe(3)
    expect(worker.threadIDs().every((id) => !first.includes(id))).toBe(true)
    return { exited, finalized }
  }).pipe(Effect.provideService(Resources, () => ({ cpu, freeBytes: 2 * 1024 ** 3 })), Effect.scoped,
    Effect.tap((closed) => Effect.sync(() => {
      expect(closed.finalized.length).toBe(6)
      expect(new Set(closed.finalized).size).toBe(6)
      expect(closed.exited.toSorted()).toEqual(closed.finalized.toSorted())
    }))))
})

for (const mode of ["interruption", "failure"] as const) test(`scheduler ${mode} holds reservations until every Worker exits`, () => Effect.runPromise(Effect.gen(function* () {
  const snapshot = () => ({ cpu: 3, freeBytes: 2 * 1024 ** 3 })
  const large = input(richMessageData.replace('"first"', JSON.stringify("x".repeat(262144))))
  const jobs: WsNativeBatch.Job[] = Array.from({ length: 3 }, (_, index) => ({ kind: "message", input: { ...large,
    row: { ...large.row, id: mode === "failure" && index === 0 ? "msg_error" : "msg_wait" } } }))
  const started = yield* Deferred.make<void>()
  const online: number[] = []
  const exits: { available: number; workers: number }[] = []
  const worker = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-failure.ts", import.meta.url)).pipe(Effect.tap((opened) => Effect.sync(() => {
    opened.worker.once("online", () => {
      online.push(opened.worker.threadId)
      if (online.length === 3) Effect.runSync(Deferred.succeed(started, undefined))
    })
    opened.worker.once("exit", () => {
      exits.push(Effect.runSync(Effect.scoped(Effect.gen(function* () {
        const available = yield* headroom
        const selected = yield* reserve(jobs)
        return { available, workers: selected.workers }
      })).pipe(Effect.provideService(Resources, snapshot))))
    })
  }))))
  const available = yield* headroom.pipe(Effect.provideService(Resources, snapshot))
  const fiber = yield* worker.run(jobs).pipe(Effect.provideService(Resources, snapshot), Effect.forkChild({ startImmediately: true }))
  const held = yield* headroom.pipe(Effect.provideService(Resources, snapshot))
  expect(held).toBeLessThan(available)
  if (mode === "interruption") {
    yield* Deferred.await(started)
    yield* Fiber.interrupt(fiber)
  }
  if (mode === "failure") expect((yield* Fiber.await(fiber))._tag).toBe("Failure")
  expect(exits).toEqual(Array.from({ length: 3 }, () => ({ available: held, workers: 0 })))
  expect(yield* headroom.pipe(Effect.provideService(Resources, snapshot))).toBe(available)
}).pipe(Effect.scoped)))

test("both real workers execute native preparation jobs and reject invalid validation", () => Effect.runPromise(Effect.gen(function* () {
  const left = yield* openWorker()
  const right = yield* openWorker()
  expect(left.worker.threadId).not.toBe(right.worker.threadId)
  for (const worker of [left, right]) {
    const results = yield* worker.run([{ kind: "native-normalize", row: { id: "msg_worker", data: nativeMessageData } }])
    expect(results[0]?.kind).toBe("native-normalize")
    if (results[0]?.kind === "native-normalize") {
      expect(results[0].output.parts.map((part) => part.id)).toEqual(["0", "1"])
      expect(JSON.parse(results[0].output.data)).not.toHaveProperty("content")
    }
    const invalid = yield* worker.run([{ kind: "legacy-message", mode: "validate", row: {
      id: "msg_bad", session_id: "ses_migration", time_created: 1, time_updated: 1, data: "{}",
    } }]).pipe(Effect.result)
    expect(invalid._tag).toBe("Failure")
  }
}).pipe(Effect.scoped)))

for (const kind of ["legacy-message", "legacy-part"] as const) {
  for (const mode of ["validate", "prepare"] as const) {
    for (const amount of ["42", "1e400"]) test(`${kind} ${mode} handles JSON number ${amount}`, () => Effect.runPromise(Effect.gen(function* () {
      const worker = yield* openWorker()
      const row = { id: kind === "legacy-part" ? "prt_legacy" : "msg_legacy", message_id: "msg_parent", session_id: "ses_migration", time_created: 1, time_updated: 1,
        data: kind === "legacy-part"
          ? `{"type":"tool","callID":"pending","tool":"read","state":{"status":"pending","input":{"amount":${amount}},"raw":"{}"}}`
          : `{"role":"user","time":{"created":1},"agent":"build","model":{"providerID":"test","modelID":"model"},"extra":{"amount":${amount}}}` }
      const result = yield* worker.run([{ kind, mode, row }]).pipe(Effect.result)
      expect(result._tag).toBe(mode === "validate" && amount === "1e400" ? "Failure" : "Success")
      if (result._tag === "Success" && mode === "prepare") {
        expect(result.success[0]).toMatchObject({ kind, mode, parsed: { value: expect.any(Object) } })
      }
    }).pipe(Effect.scoped)))
  }
}

for (const operation of ["validate", "convert"] as const) test(`source ${operation} rejects non-finite legacy tool input`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  const data = '{"type":"tool","callID":"pending","tool":"read","state":{"status":"pending","input":{"amount":42},"raw":"{}"}}'
  yield* db.run(sql`UPDATE part SET data = ${data} WHERE id = 'prt_legacy'`)
  yield* WsNative.validateSource(db)
  yield* db.run(sql`UPDATE part SET data = ${data.replace('"amount":42', '"amount":1e400')} WHERE id = 'prt_legacy'`)
  const result = yield* (operation === "validate" ? WsNative.validateSource(db) : WsNative.convert(db)).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped)))

test("mixed jobs match native storage codecs on both threads", () => Effect.runPromise(Effect.gen(function* () {
  const left = yield* openWorker()
  const right = yield* openWorker()
  const data = { agent: "build", model: { providerID: "test", id: "model" }, time: { created: 1 },
    content: [{ type: "text", text: "first" }, { type: "reasoning", text: "reason" }, { type: "text", text: "second" }] }
  const stored = yield* encodeStoredMessage(yield* Schema.decodeUnknownEffect(SessionMessage.Info)({ ...data, id: "msg_worker", type: "assistant" }))
  const parent = { sessionID: Schema.decodeUnknownSync(Session.ID)("ses_migration"),
    messageID: Schema.decodeUnknownSync(SessionMessage.ID)("msg_worker"), type: "assistant" as const, dataText: JSON.stringify(stored.envelope.data) }
  const children = stored.parts.map((part) => ({ messageID: parent.messageID, position: part.position, id: part.id, type: part.type, dataText: JSON.stringify(part.data) }))
  expect((yield* decodeStoredMessage(parent, children)).type).toBe("assistant")
  const event = { id: "evt_worker", aggregate_id: "ses_migration", seq: 1, type: "session.next.revert.cleared.1", data: '{"sessionID":"ses_migration","timestamp":1}' }
  const legacy = { id: "msg_legacy", session_id: "ses_migration", time_created: 1, time_updated: 1, data: '{"role":"user","time":{"created":1},"agent":"build","model":{"providerID":"test","modelID":"model"}}' }
  const part = { ...legacy, id: "prt_legacy", message_id: legacy.id, data: '{"type":"text","text":"legacy"}' }
  const jobs: readonly WsNativeBatch.Job[] = [
    { kind: "message", input: input() }, { kind: "ws-event", row: event }, { kind: "p-event", row: { ...event, created: 1,
      type: "session.text.ended.1", data: '{"sessionID":"ses_migration","assistantMessageID":"msg_worker","ordinal":0,"text":"first"}' } },
    { kind: "native-encode", row: { id: "msg_worker", type: "assistant", data } }, { kind: "native-validate", parent, children },
    { kind: "legacy-message", mode: "prepare", row: legacy }, { kind: "legacy-part", mode: "prepare", row: part },
    { kind: "legacy-message", mode: "validate", row: legacy }, { kind: "legacy-part", mode: "validate", row: part },
  ]
  const expected = yield* WsNativeBatch.run(jobs)
  for (const worker of [left, right]) {
    const actual = yield* worker.run(jobs)
    expect(actual).toEqual(expected)
    const encoded = actual[3]
    if (encoded?.kind !== "native-encode") throw new Error("Missing encoded result")
    expect(encoded.output.data).toBe(parent.dataText)
    expect(encoded.output.parts.map((child) => child.id)).toEqual(["0", "0", "1"])
    const invalid = yield* worker.run([{ kind: "native-validate", parent, children: [{ ...children[0], dataText: '{}' }] }]).pipe(Effect.result)
    expect(invalid._tag).toBe("Failure")
    const invalidJobs: readonly WsNativeBatch.Job[] = [
      { kind: "ws-event", row: { ...event, data: "{}" } },
      { kind: "p-event", row: { ...event, created: 0.5 } },
      { kind: "native-encode", row: { id: "msg_bad", type: "assistant", data: {} } },
      { kind: "native-normalize", row: { id: "msg_bad", data: "{}" } },
      { kind: "legacy-part", mode: "validate", row: { ...part, data: "{}" } },
    ]
    for (const job of invalidJobs) {
      const result = yield* worker.run([job]).pipe(Effect.result)
      expect(result._tag).toBe("Failure")
      if (result._tag === "Failure") expect(result.failure.messageID).toBe(WsNativeBatch.jobID(job))
    }
    const skipped = yield* worker.run([{ kind: "legacy-part", mode: "prepare", row: { ...part, data: '{"type":"unknown"}' } }])
    expect(skipped).toEqual([{ kind: "legacy-part", mode: "prepare", parsed: {
      row: { id: part.id, message_id: part.message_id, session_id: part.session_id, time_created: 1, time_updated: 1 },
      value: undefined, observedType: "unknown",
    } }])
  }
}).pipe(Effect.scoped)))

test("raw row waves bound bytes and stop at a later consumer failure", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run('CREATE TABLE "raw rows" (hash TEXT PRIMARY KEY, data TEXT, selected INTEGER)')
  for (const [hash, size] of [["a", WsNativeScan.byteLimit + 1], ["b", 600000], ["c", 600000], ["d", 600000]] as const)
    yield* db.run(sql`INSERT INTO "raw rows" VALUES (${hash}, ${"x".repeat(size)}, 1)`)
  const reads: number[] = []
  const reader = { all: <T>(query: Parameters<typeof db.all>[0]) => {
    const result = db.all<T>(query)
    if (!result.getQuery().sql.includes("AS bytes")) reads.push(1)
    return result
  } }
  const waves: string[][] = []
  const result = yield* WsNativeScan.rows(reader, { table: "raw rows", key: "hash", bytes: sql`octet_length(m.data)`, where: sql`m.selected = 1` },
    (rows: readonly { hash: string; data: string }[]) => Effect.gen(function* () {
      waves.push(rows.map((row) => row.hash))
      expect(reads.length).toBe(waves.length)
      expect(rows.length).toBe(1)
      if (rows[0].hash === "c") return yield* Effect.fail("stop")
    })).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  expect(waves).toEqual([["a"], ["b"], ["c"]])
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("native normalization preserves raw tokens and rejects duplicate structural keys", () => Effect.runPromise(Effect.gen(function* () {
  const raw = '{ "agent":"build", "model":{"providerID":"test","id":"model"}, "time":{"created":1}, "content":[{"type":"text","text":"first\\u0020text"}], "metadata":{ "decimal":1.2300 } }'
  const output = yield* normalizeMessage({ id: "msg_worker", data: raw })
  expect(output.data).toContain('"decimal":1.2300')
  expect(output.parts[0].data).toContain('"first\\u0020text"')
  const duplicate = yield* normalizeMessage({ id: "msg_worker", data: raw.replace('"agent":"build"', '"agent":"build","agent":"build"') })
  expect(duplicate.data).toContain('"agent":"build","agent":"build"')
  const invalid = yield* normalizeMessage({ id: "msg_worker", data: raw.replace('"content":', '"content":[],"content":') }).pipe(Effect.result)
  expect(invalid._tag).toBe("Failure")
}).pipe(Effect.scoped)))

test("native validation reads all children across waves without adding ordinal constraints", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, data TEXT)")
  yield* db.run("CREATE TABLE session_message_part (message_id TEXT, position INTEGER, id TEXT, type TEXT, data TEXT)")
  for (let index = 0; index < 130; index++) {
    const id = `msg_${String(index).padStart(3, "0")}`
    yield* db.run(sql`INSERT INTO session_message VALUES (${id}, 'ses_migration', 'assistant', '{"agent":"build","model":{"providerID":"test","id":"model"},"time":{"created":1}}')`)
    yield* db.run(sql`INSERT INTO session_message_part VALUES (${id}, 7, 'unrelated-ordinal', 'text', ${JSON.stringify({ text: index === 0 ? "x".repeat(WsNativeScan.byteLimit) : "text" })})`)
  }
  yield* WsNativeScan.validateNative(db)
  yield* db.run("INSERT INTO session_message_part VALUES ('msg_129', 9, 'invalid', 'text', '{}')")
  const invalid = yield* WsNativeScan.validateNative(db).pipe(Effect.result)
  expect(invalid._tag).toBe("Failure")
  if (invalid._tag === "Failure" && invalid.failure instanceof WsNativeMessage.WsNativeMessageError)
    expect(invalid.failure.messageID).toBe("msg_129")
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("worker normalization keeps caller transaction atomic on later-wave failure", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, data TEXT)")
  yield* db.run("CREATE TABLE session_message_part (message_id TEXT, position INTEGER, id TEXT, type TEXT, data TEXT)")
  for (let index = 0; index < 130; index++) yield* db.run(sql`INSERT INTO session_message VALUES
    (${`msg_${String(index).padStart(3, "0")}`}, 'ses_migration', 'assistant', ${index === 129 ? '{}' : nativeMessageData})`)
  let writes = 0
  const result = yield* db.transaction((tx) => normalizeMessages({ all: tx.all.bind(tx), run: (query) => {
    writes++
    return tx.run(query)
  } })).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 2 * 128 * estimate(Buffer.byteLength(nativeMessageData)) })), Effect.result)
  expect(result._tag).toBe("Failure")
  expect(writes).toBeGreaterThan(0)
  expect(yield* db.get("SELECT count(*) AS count FROM session_message_part")).toEqual({ count: 0 })
  expect(yield* db.get("SELECT data FROM session_message WHERE id = 'msg_000'")).toEqual({ data: nativeMessageData })
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

test("session normalization seeks the source sequence index across waves", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupPreviousNative(db, { directory: "/fixture", rich: false })
  yield* db.run("CREATE TABLE session_message_part (message_id TEXT, position INTEGER, id TEXT, type TEXT, data TEXT)")
  for (let index = 0; index < 260; index++) yield* db.run(sql`INSERT INTO session_message VALUES
    (${`msg_${String(260 - index).padStart(3, "0")}`}, 'ses_migration', 'assistant', ${index + 51}, 1, 16, ${nativeMessageData})`)
  const queries: Parameters<typeof db.all>[0][] = []
  const reader = { run: db.run.bind(db), all: <T>(query: Parameters<typeof db.all>[0]) => {
    queries.push(query)
    return db.all<T>(query)
  } }
  yield* normalizeMessages(reader, "ses_migration")
  expect(queries.length).toBe(7)
  for (const query of queries) {
    const plan = yield* db.all<{ detail: string }>(sql`EXPLAIN QUERY PLAN ${typeof query === "string" ? sql.raw(query) : query}`)
    const details = plan.map((row) => row.detail).join("\n")
    expect(details).not.toContain("TEMP B-TREE")
    expect(details).toContain("session_message_session_type_seq_idx")
    if (db.all(query).getQuery().sql.includes(" > ")) expect(details).toContain("seq>?")
  }
  expect(yield* db.get("SELECT count(*) AS count FROM session_message_part")).toEqual({ count: 522 })
  expect(yield* db.get("SELECT data FROM session_message_part WHERE message_id = 'msg_001' AND position = 0"))
    .toEqual({ data: '{"text":"first"}' })
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

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
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 1024 ** 3 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

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
  const available = yield* headroom
  const started = yield* Deferred.make<Worker>()
  const fiber = yield* Effect.scoped(Effect.gen(function* () {
    yield* reserve([{ kind: "message", input: input() }])
    const worker = yield* openWorker(new URL("./fixture/ws-native-worker-failure.ts", import.meta.url))
    yield* Deferred.succeed(started, worker.worker)
    const item = input()
    yield* worker.process([{ ...item, row: { ...item.row, id: "msg_wait" } }])
  })).pipe(Effect.forkChild)
  const worker = yield* Deferred.await(started)
  const exit = new Promise<number>((resolve) => worker.once("exit", resolve))
  yield* Fiber.interrupt(fiber)
  expect(typeof (yield* Effect.promise(() => exit))).toBe("number")
  expect(yield* headroom).toBe(available)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 3, freeBytes: 1024 ** 3 })), Effect.scoped)))

test("child payload budget admits an oversized message alone", () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false, lineage: "v02" })
  yield* db.run(sql`UPDATE session_message_part SET data = ${JSON.stringify({ text: "x".repeat(WsNativeScan.byteLimit) })} WHERE position = 0`)
  yield* db.run("INSERT INTO session_message SELECT 'msg_next', session_id, type, 8, time_created, time_updated, data FROM session_message")
  const reads: string[] = []
  const reader = {
    get: db.get.bind(db),
    all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      if (result.getQuery().sql.includes("SELECT id, session_id")) reads.push(result.getQuery().sql)
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
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 1024 ** 3 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

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
