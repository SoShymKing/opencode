import { expect, test } from "bun:test"
import { join } from "node:path"
import { readdir } from "node:fs/promises"
import { Deferred, Effect, Fiber, Layer, Schedule } from "effect"
import { Global } from "@opencode/util/global"
import { Database } from "../src/database/database"
import { V1Migration } from "../src/database/v1-migration"
import { setupWs, setupPreviousNative } from "./fixture/database-ws"
import { setupNativeInline } from "./fixture/database-native-inline"
import { useDatabase } from "./fixture/database-migration"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"
import { withEnv } from "./fixture/env"

test.each(["ws", "previous-native", "native"])("preparation covers %s acquisition and clears before caller use", async (source) => {
  await using tmp = await tmpdir()
  const filename = join(tmp.path, "source.db")
  await useDatabase(filename, (db) => source === "ws" ? setupWs(db, { directory: tmp.path })
    : source === "previous-native" ? setupPreviousNative(db, { directory: tmp.path }) : setupNativeInline(db))
  const phases: boolean[] = []
  await Effect.gen(function* () {
    const service = yield* Database.Service
    expect(phases).toEqual([true, false])
    expect(yield* service.db.all("SELECT 1 AS value")).toEqual([{ value: 1 }])
  }).pipe(Effect.provide(Database.layer({ path: filename }, async (active) => {
    if (active) expect((await readdir(tmp.path)).filter((name) => name.includes("backup") || name.includes(".native-"))).toEqual([])
    phases.push(active)
  })), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  expect(phases).toEqual([true, false])
})

test("fresh, current native and memory acquisition do not publish preparation", async () => {
  await using tmp = await tmpdir()
  const phases: boolean[] = []
  for (const filename of [join(tmp.path, "fresh.db"), join(tmp.path, "fresh.db"), ":memory:"]) {
    await Database.Service.pipe(Effect.provide(Database.layer({ path: filename }, async (active) => {
      phases.push(active)
    })), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  }
  expect(phases).toEqual([])
})

test("recognized replacement clears preparation when validation fails", async () => {
  await using tmp = await tmpdir()
  const filename = join(tmp.path, "source.db")
  await Bun.write(`${filename}.replacement.json`, "{}")
  const phases: boolean[] = []
  await expect(Database.Service.pipe(Effect.provide(Database.layer({ path: filename }, async (active) => {
    phases.push(active)
  })), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)).rejects.toThrow()
  expect(phases).toEqual([true, false])
})

test("boot profile preparation publishes paired phases before returning imported data", async () => {
  await using tmp = await tmpdir()
  await useDatabase(join(tmp.path, "opencode-next.db"), (db) => setupWs(db, { directory: tmp.path }))
  const phases: boolean[] = []
  await withEnv({ OPENCODE_DB: undefined }, () => Effect.gen(function* () {
    const service = yield* Database.Service
    const cleared = yield* Deferred.make<void>()
    const context = yield* Effect.context<never>()
    yield* Layer.build(V1Migration.configured(async (active) => {
      phases.push(active)
      if (!active) await Effect.runPromiseWith(context)(Deferred.succeed(cleared, undefined))
    }))
    yield* Deferred.await(cleared)
    expect(phases).toEqual([true, false])
    expect((yield* service.db.all("SELECT id FROM session_v2")).length).toBe(1)
  }).pipe(Effect.provide(Database.layer()),
    Effect.provideService(Global.Service, Global.make({ data: tmp.path })), Effect.scoped)).pipe(Effect.runPromise)
})

test("interrupted acquisition awaits publication and clears without conversion artifacts", async () => {
  await using tmp = await tmpdir()
  const filename = join(tmp.path, "source.db")
  await useDatabase(filename, (db) => setupWs(db, { directory: tmp.path }))
  const phases: boolean[] = []
  const published = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  await Effect.gen(function* () {
    const fiber = yield* Database.Service.pipe(Effect.provide(Database.layer({ path: filename }, async (active) => {
      phases.push(active)
      if (!active) return
      published.resolve()
      await release.promise
    })), Effect.provide(tempGlobalLayer), Effect.forkScoped)
    yield* Effect.promise(() => published.promise)
    expect((yield* Effect.promise(() => readdir(tmp.path))).filter((name) => name.includes(".native-"))).toEqual([])
    const interrupted = yield* Fiber.interrupt(fiber).pipe(Effect.forkChild({ startImmediately: true }))
    release.resolve()
    yield* Fiber.join(interrupted)
    expect(phases).toEqual([true, false])
  }).pipe(Effect.scoped, Effect.runPromise)
})

test.each([false, true])("boot legacy preparation clears after mutation with failure=%s", async (failure) => {
  const phases: boolean[] = []
  await Effect.gen(function* () {
    const service = yield* Database.Service
    yield* service.db.run("CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)")
    yield* service.db.run("INSERT INTO event_sequence (aggregate_id, seq) VALUES ('old', 0)")
    yield* service.db.run("INSERT INTO event (id, aggregate_id, seq, created, type, data) VALUES ('old', 'old', 0, 1, 'old', '{}')")
    if (failure) yield* service.db.run("CREATE TRIGGER fail_delete BEFORE DELETE ON event BEGIN SELECT RAISE(ABORT, 'test failure'); END")
    const cleared = yield* Deferred.make<void>()
    const context = yield* Effect.context<never>()
    yield* Layer.build(V1Migration.configured(async (active) => {
      phases.push(active)
      if (!active) await Effect.runPromiseWith(context)(Deferred.succeed(cleared, undefined))
    }))
    yield* Deferred.await(cleared)
    expect(phases).toEqual([true, false])
    expect((yield* service.db.all("SELECT id FROM event")).length).toBe(failure ? 1 : 0)
  }).pipe(Effect.provide(Database.layer()), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
})

test("boot with no profile or legacy work does not publish preparation", async () => {
  const phases: boolean[] = []
  await Effect.gen(function* () {
    yield* Layer.build(V1Migration.configured(async (active) => { phases.push(active) }))
    yield* V1Migration.status().pipe(
      Effect.filterOrFail((status) => status.status === "completed"),
      Effect.retry(Schedule.spaced("1 millis")),
    )
    expect(phases).toEqual([])
  }).pipe(Effect.provide(Database.layer()), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
})
