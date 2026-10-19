import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { choose, estimate, readingPlan, Resources, workerBytes } from "@opencode/core/database/ws-native-serial"
import { WsNativeScan } from "@opencode/core/database/ws-native-scan"
import { open, withPool } from "@opencode/core/database/ws-native-bun"
import { messageData } from "./fixture/database-ws"

for (const cpu of [20, 24]) test(`tiny-row admission feeds ${cpu} feasible CPUs`, () => {
  const snapshot = { cpu, freeBytes: 4 * 1024 ** 3 }
  const target = Effect.runSync(readingPlan().pipe(Effect.provideService(Resources, () => snapshot)))
  expect(target.countLimit).toBe(cpu * 2 * 1024 ** 2 / estimate(0))
  expect(choose(snapshot, Array.from({ length: target.countLimit }, () => estimate(0))).workers).toBe(cpu)
})

test("admission falls back to 128 serial rows and scales to memory-feasible workers", () => {
  for (const snapshot of [{ cpu: 1, freeBytes: 4 * 1024 ** 3 }, { cpu: 24, freeBytes: 2 * workerBytes }]) {
    expect(Effect.runSync(readingPlan().pipe(Effect.provideService(Resources, () => snapshot))).countLimit).toBe(128)
  }
  const snapshot = { cpu: 24, freeBytes: 4 * (workerBytes + 2 * 1024 ** 2) }
  const target = Effect.runSync(readingPlan().pipe(Effect.provideService(Resources, () => snapshot)))
  expect(target.countLimit).toBe(1024)
  expect(choose(snapshot, Array.from({ length: target.countLimit }, () => estimate(0))).workers).toBe(2)
})

test("tiny SQL rows admit a 20-worker wave while each source read stays within 128 rows", () => Effect.runPromise(
  withPool(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TABLE source (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, seq INTEGER, time_created INTEGER, time_updated INTEGER, data TEXT)")
    yield* db.run(sql`WITH RECURSIVE numbers(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM numbers WHERE value < 10240)
      INSERT INTO source SELECT printf('msg_%05d', value), 'ses_tiny', 'assistant', value, 1, 16, ${messageData} FROM numbers`)
    const pages: number[] = []
    const reader = { get: db.get.bind(db), all: <T>(query: Parameters<typeof db.all>[0]) => {
      const result = db.all<T>(query)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => {
        pages.push(rows.length)
        expect(rows.length).toBeLessThanOrEqual(128)
      })))
      return result
    } }
    const pool = yield* open
    let count = 0
    yield* WsNativeScan.consume(reader, { parent: "source", child: undefined, previousNative: false }, () => Effect.sync(() => {
      if (count === 0) expect(pool.threadIDs().length).toBe(20)
      count++
    }))
    expect(count).toBe(10240)
    expect(pages).toContain(128)
  })).pipe(Effect.provideService(Resources, () => ({ cpu: 20, freeBytes: 4 * 1024 ** 3 })),
    Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))
