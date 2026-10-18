import { NodeFileSystem } from "@effect/platform-node"
import { expect, test } from "bun:test"
import { Clock, Effect, Fiber, FileSystem, Schema } from "effect"
import { writeFileSync } from "node:fs"
import { ensure } from "../src/promise/service"
import { Service } from "../src/effect/service"
import type { EnsureOptions } from "../src/service"
import { withEnsureTiming } from "../src/service-timing"
import { decodeRegistration, ordinaryTime, preparing } from "../src/service-startup"
import { same } from "../src/service-probe"
import { serviceFixture } from "./fixture/service-fixture"

const adapters = {
  promise: (options: EnsureOptions) => ensure(options),
  effect: (options: EnsureOptions) => Effect.runPromise(Service.ensure(options).pipe(Effect.provide(NodeFileSystem.layer))),
}

const identity = { id: "fixture", version: "test", url: "http://localhost:1", pid: process.pid, password: "fixture-only" }

test("registration boundaries reject invalid phases and preserve the exact optional phase", () => {
  for (const startup of [undefined, "database-preparation", "other", null, 5]) {
    const input = { ...identity, startup }
    const valid = startup === undefined || startup === "database-preparation"
    expect(decodeRegistration(input) !== undefined).toBe(valid)
    expect(Schema.is(Service.Info)(input)).toBe(valid)
  }
  expect(decodeRegistration({ ...identity, pid: "123" })).toBeUndefined()
  expect(decodeRegistration({ ...identity, password: 3 })).toBeUndefined()
})

test("ordinary time resumes the remaining budget after preparation", () => {
  const elapsed = ordinaryTime(0)
  expect(elapsed(40, false)).toBe(40)
  expect(elapsed(50, true)).toBe(40)
  expect(elapsed(10_000, true)).toBe(40)
  expect(elapsed(10_010, false)).toBe(40)
  expect(elapsed(10_070, false)).toBe(100)
})

test("protection binds terminal health to the full identity, not the mutable phase", () => {
  const info = { ...identity, startup: "database-preparation" as const }
  expect(same(identity, info)).toBe(true)
  expect(same(identity, { ...info, password: "changed" })).toBe(false)
  expect(preparing(info)).toBe(true)
  for (const state of ["ready", "failed", "waiting"] as const) {
    const service = { info: identity, endpoint: { url: identity.url }, state, compatible: true }
    expect(preparing(info, service)).toBe(state === "waiting")
    expect(preparing(info, { ...service, info: { ...identity, id: "successor" } })).toBe(true)
    expect(preparing(info, { ...service, compatible: false })).toBe(true)
  }
})

for (const [name, ensure] of Object.entries(adapters)) {
  test(`${name} defers version replacement until database preparation ends`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("database-old")
    await fixture.waitForFile(fixture.registration + ".blocked")
    let settled = false
    const pending = ensure(withEnsureTiming({ file: fixture.registration, version: "test", command: fixture.command("graceful") }, {
      promiseTimeout: 300, requestTimeout: 100, pollInterval: 5, spawnDelay: 200,
    })).finally(() => { settled = true }).catch((error: unknown) => error)
    await using ordinary = await serviceFixture()
    ordinary.spawn("starting")
    await ordinary.waitForFile()
    await expect(ensure(withEnsureTiming({ file: ordinary.registration, command: [] }, {
      promiseTimeout: 500, requestTimeout: 1_000, pollInterval: 5,
    }))).rejects.toThrow("Timed out")
    expect(settled).toBe(false)
    expect(child.exitCode).toBeNull()
    expect(await Bun.file(fixture.registration + ".pty-handoff").exists()).toBe(false)
    await Bun.write(fixture.registration + ".release", "")
    const endpoint = await pending
    const info = await Bun.file(fixture.registration).json()
    fixture.track(info.pid)
    expect(info.pid).not.toBe(child.pid)
    expect(endpoint).toMatchObject({ url: info.url })
  }, 10_000)

  test(`${name} adopts a new registration without acting on the protected old owner`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("database")
    await fixture.waitForFile(fixture.registration + ".blocked")
    const pending = ensure(withEnsureTiming({ file: fixture.registration, command: [] }, {
      promiseTimeout: 1_000, requestTimeout: 30, pollInterval: 5,
    }))
    const successor = fixture.spawn("graceful")
    const endpoint = await pending
    const info = await Bun.file(fixture.registration).json()
    expect(info.pid).toBe(successor.pid)
    expect(endpoint.url).toBe(info.url)
    expect(child.exitCode).toBeNull()
  }, 10_000)

  test(`${name} rechecks protection before automatic terminal cleanup and signaling`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("hanging")
    await fixture.waitForFile()
    const original = await Bun.file(fixture.registration).json()
    const published = Promise.withResolvers<void>()
    const pending = ensure(withEnsureTiming({
      file: fixture.registration,
      command: fixture.command("record-start"),
      onStart: () => {
        writeFileSync(fixture.registration, JSON.stringify({ ...original, startup: "database-preparation" }), { mode: 0o600 })
        published.resolve()
      },
    }, { promiseTimeout: 1_000, requestTimeout: 30, pollInterval: 5, spawnDelay: 10_000 })).catch((error: unknown) => error)
    await published.promise
    await using ordinary = await serviceFixture()
    ordinary.spawn("starting")
    await ordinary.waitForFile()
    await expect(ensure(withEnsureTiming({ file: ordinary.registration, command: [] }, {
      promiseTimeout: 200, requestTimeout: 1_000, pollInterval: 5,
    }))).rejects.toThrow("Timed out")
    expect(child.exitCode).toBeNull()
    expect(await Bun.file(fixture.registration + ".pty-handoff").exists()).toBe(false)
    fixture.spawn("graceful")
    expect(await pending).toMatchObject({ url: (await Bun.file(fixture.registration).json()).url })
  }, 10_000)

  test(`${name} honors ready and failed health despite a published preparation phase`, async () => {
    for (const mode of ["database-ready", "database-failed"]) {
      await using fixture = await serviceFixture()
      const child = fixture.spawn(mode)
      await fixture.waitForFile()
      const pending = ensure(withEnsureTiming({ file: fixture.registration, command: [] }, { promiseTimeout: 300 }))
      if (mode === "database-failed") await expect(pending).rejects.toThrow("failed to start")
      else expect(await pending).toMatchObject({ url: (await Bun.file(fixture.registration).json()).url })
      expect(child.exitCode).toBeNull()
    }
  }, 10_000)

  test(`${name} rereads a phase published during the third timed-out probe`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("database-probe")
    await fixture.waitForFile()
    const pending = ensure(withEnsureTiming({ file: fixture.registration, command: fixture.command("record-start") }, {
      promiseTimeout: 1_000, requestTimeout: 100, pollInterval: 5, spawnDelay: 10_000,
    })).catch((error: unknown) => error)
    await fixture.waitForFile(fixture.registration + ".blocked")
    await using ordinary = await serviceFixture()
    ordinary.spawn("starting")
    await ordinary.waitForFile()
    await expect(ensure(withEnsureTiming({ file: ordinary.registration, command: [] }, {
      promiseTimeout: 450, requestTimeout: 1_000, pollInterval: 5,
    }))).rejects.toThrow("Timed out")
    expect(child.exitCode).toBeNull()
    expect(await Bun.file(fixture.registration + ".started").exists()).toBe(false)
    expect(await Bun.file(fixture.registration + ".pty-handoff").exists()).toBe(false)
    await Bun.write(fixture.registration + ".release", "")
    expect(await pending).toMatchObject({ url: (await Bun.file(fixture.registration).json()).url })
  }, 10_000)

  test(`${name} revokes a dead converter's protection`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("database")
    await fixture.waitForFile(fixture.registration + ".blocked")
    child.kill("SIGKILL")
    await child.exited
    expect(preparing(await Bun.file(fixture.registration).json())).toBe(false)
    const endpoint = await ensure(withEnsureTiming({ file: fixture.registration, command: fixture.command("graceful") }, {
      promiseTimeout: 3_000, requestTimeout: 100, pollInterval: 5, spawnDelay: 200,
    }))
    const info = await Bun.file(fixture.registration).json()
    fixture.track(info.pid)
    expect(info.pid).not.toBe(child.pid)
    expect(endpoint.url).toBe(info.url)
  }, 10_000)
}

test("Effect recognizes a protected expected URL without HTTP and permits interruption", async () => {
  await using fixture = await serviceFixture()
  const child = fixture.spawn("database-old")
  await fixture.waitForFile(fixture.registration + ".blocked")
  const info = await Bun.file(fixture.registration).json()
  const run = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem>) =>
    Effect.runPromise(effect.pipe(Effect.provide(NodeFileSystem.layer)))
  expect(await run(Service.incumbent({ file: fixture.registration, url: info.url, version: "test" }))).toMatchObject({ state: "waiting" })
  await run(Effect.gen(function* () {
    const fiber = yield* Service.ensure({ file: fixture.registration, version: "test", command: [] }).pipe(Effect.forkChild({ startImmediately: true }))
    yield* Fiber.interrupt(fiber)
  }))
  expect(child.exitCode).toBeNull()
  expect(await fixture.signal()).toBeUndefined()
})

test("Effect ordinary startup budget uses the caller's clock", async () => {
  await using fixture = await serviceFixture()
  fixture.spawn("starting")
  await fixture.waitForFile()
  let now = 0
  const live = Clock.Clock.defaultValue()
  const clock: Clock.Clock = {
    currentTimeMillisUnsafe: () => now,
    currentTimeMillis: Effect.sync(() => now),
    currentTimeNanosUnsafe: () => BigInt(now) * 1_000_000n,
    currentTimeNanos: Effect.sync(() => BigInt(now) * 1_000_000n),
    monotonicTimeNanosUnsafe: () => live.monotonicTimeNanosUnsafe(),
    monotonicTimeNanos: live.monotonicTimeNanos,
    sleep: (duration) => live.sleep(duration),
  }
  const pending = Effect.runPromise(Service.ensure({ file: fixture.registration, command: [] }).pipe(
    Effect.provide(NodeFileSystem.layer),
    Effect.provideService(Clock.Clock, clock),
  )).catch((error: unknown) => error)
  await fixture.waitForFile(fixture.registration + ".status-request")
  now = 120_001
  expect(await pending).toMatchObject({ message: "Timed out waiting for the background service to start" })
})

for (const [name, ensure] of Object.entries(adapters)) {
  test(`${name} preserves a blocked database owner beyond ordinary startup and health deadlines`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("database")
    await fixture.waitForFile(fixture.registration + ".blocked")
    const original = await Bun.file(fixture.registration).json()
    const starts: string[] = []
    let settled = false
    const pending = ensure(withEnsureTiming({
      file: fixture.registration,
      command: fixture.command("record-start"),
      onStart: (reason: string) => starts.push(reason),
    }, { promiseTimeout: 150, requestTimeout: 30, pollInterval: 5 })).finally(() => { settled = true }).catch((error: unknown) => error)
    await using ordinary = await serviceFixture()
    ordinary.spawn("starting")
    await ordinary.waitForFile()
    await expect(ensure(withEnsureTiming({ file: ordinary.registration, command: [] }, {
      promiseTimeout: 450, requestTimeout: 1_000, pollInterval: 5,
    }))).rejects.toThrow("Timed out")
    expect(settled).toBe(false)
    expect(child.exitCode).toBeNull()
    expect(starts).toEqual([])
    expect(await fixture.signal()).toBeUndefined()
    expect(await Bun.file(fixture.registration + ".started").exists()).toBe(false)
    expect(await Bun.file(fixture.registration + ".pty-handoff").exists()).toBe(false)
    await Bun.write(fixture.registration + ".release", "")
    expect(await pending).toMatchObject({ url: original.url })
  }, 10_000)
}
