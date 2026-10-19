import { NodeFileSystem } from "@effect/platform-node"
import { expect, test } from "bun:test"
import { Effect, Fiber } from "effect"
import { once } from "node:events"
import { join } from "node:path"
import { Service } from "../src/effect/service"
import { ensure } from "../src/promise/service"
import type { EnsureOptions } from "../src/service"
import { defaultEnsureTiming, withEnsureTiming } from "../src/service-timing"
import { contenderPool, spawnServiceContender } from "../src/service-contender"
import { serviceFixture, expectNotListening } from "./fixture/service-fixture"

test("cleanup accepts an exited owned child while a descendant holds stderr open", async () => {
  await using fixture = await serviceFixture()
  const pool = contenderPool({ ...defaultEnsureTiming, stopPollAttempts: 0 })
  const contender = spawnServiceContender(process.execPath, [
    join(import.meta.dir, "fixture/service-inherited-stderr.ts"), fixture.registration,
  ])
  pool.add(contender)
  const closed = once(contender.child, "close")
  try {
    await once(contender.child, "exit")
    expect(contender.child.exitCode).toBe(0)
    expect(contender.closed()).toBe(false)
    await pool.releaseAll()
    expect(contender.closed()).toBe(false)
  } finally {
    await Bun.write(fixture.registration, "")
    await closed
    contender.release()
  }
}, 10_000)

test("interrupted ensure waits for its blocked database child to exit", async () => {
  await using fixture = await serviceFixture()
  let adopted = false
  const fiber = Effect.runFork(Service.ensure(withEnsureTiming({
    file: fixture.registration, command: fixture.command("database"),
  }, { pollInterval: 5, requestTimeout: 30, stopPollInterval: 5, stopPollAttempts: 20 })).pipe(
    Effect.tap(() => Effect.sync(() => { adopted = true })),
    Effect.provide(NodeFileSystem.layer),
  ))
  try {
    await fixture.waitForFile(fixture.registration + ".blocked")
    const info = await Bun.file(fixture.registration).json()
    fixture.track(info.pid)
    expect(adopted).toBe(false)
    await Effect.runPromise(Fiber.interrupt(fiber))
    expect(() => process.kill(info.pid, 0)).toThrow()
    await expectNotListening(info.url)
  } finally {
    await Effect.runPromise(Fiber.interrupt(fiber))
  }
}, 10_000)

const adapters = {
  promise: (options: EnsureOptions) => ensure(options),
  effect: (options: EnsureOptions) => Effect.runPromise(Service.ensure(options).pipe(Effect.provide(NodeFileSystem.layer))),
}

for (const [name, run] of Object.entries(adapters)) {
  test(`${name} failed startup closes its owned listener before rejecting`, async () => {
    await using fixture = await serviceFixture()
    const pending = run(withEnsureTiming({
      file: fixture.registration, command: fixture.command("database-failed"),
    }, { pollInterval: 5, requestTimeout: 1_000, stopPollInterval: 5, stopPollAttempts: 20 }))
      .catch((error: unknown) => error)
    await fixture.waitForFile()
    const info = await Bun.file(fixture.registration).json()
    fixture.track(info.pid)
    expect(await pending).toMatchObject({ message: "Background service failed to start" })
    expect(() => process.kill(info.pid, 0)).toThrow()
    await expectNotListening(info.url)
  }, 10_000)

  test(`${name} adopts the ready winner and closes its owned loser`, async () => {
    await using fixture = await serviceFixture()
    const pending = run(withEnsureTiming({
      file: fixture.registration, command: fixture.command("controlled"),
    }, { spawnDelay: 100, pollInterval: 5, requestTimeout: 1_000, stopPollInterval: 5, stopPollAttempts: 20 }))
    const [winner, loser] = await fixture.waitForStarts(2)
    await fixture.release(winner, "ready")
    const endpoint = await pending
    expect(() => process.kill(winner, 0)).not.toThrow()
    expect(() => process.kill(loser, 0)).toThrow()
    expect(await fetch(new URL("/api/info", endpoint.url)).then((response) => response.status)).toBe(200)
  }, 10_000)

  test(`${name} closes its contender when a pre-existing owner wins`, async () => {
    await using fixture = await serviceFixture()
    const pending = run(withEnsureTiming({
      file: fixture.registration, command: fixture.command("controlled"),
    }, { spawnDelay: 10_000, pollInterval: 5, requestTimeout: 1_000, stopPollInterval: 5, stopPollAttempts: 20 }))
    const [loser] = await fixture.waitForStarts(1)
    const incumbent = fixture.spawn("graceful")
    await pending
    expect(() => process.kill(loser, 0)).toThrow()
    expect(incumbent.exitCode).toBeNull()
  }, 10_000)
}
