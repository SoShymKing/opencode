import { NodeFileSystem } from "@effect/platform-node"
import { expect, test } from "bun:test"
import { Effect } from "effect"
import { writeFileSync } from "node:fs"
import { ensure } from "../src/promise/service"
import { Service } from "../src/effect/service"
import type { EnsureOptions } from "../src/service"
import { withEnsureTiming } from "../src/service-timing"
import { preparing, decodeRegistration } from "../src/service-startup"
import { serviceFixture } from "./fixture/service-fixture"

const adapters = {
  promise: (options: EnsureOptions) => ensure(options),
  effect: (options: EnsureOptions) => Effect.runPromise(Service.ensure(options).pipe(Effect.provide(NodeFileSystem.layer))),
}

test("preparation protection requires a nonempty instance id", () => {
  for (const id of [undefined, ""]) {
    const info = { id, pid: process.pid, url: "http://localhost:1", startup: "database-preparation" as const }
    expect(preparing(info)).toBe(false)
    expect(decodeRegistration({ ...info, startup: undefined })).toBeDefined()
    expect(preparing({ ...info, startup: undefined })).toBe(false)
  }
})

for (const [name, run] of Object.entries(adapters)) {
  test(`${name} does not use cached ready health to signal a newly protected owner`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("old")
    await fixture.waitForFile()
    const original = await Bun.file(fixture.registration).json()
    const published = Promise.withResolvers<void>()
    const pending = run(withEnsureTiming({
      file: fixture.registration, version: "test", command: fixture.command("graceful"),
      onStart: () => {
        writeFileSync(fixture.registration, JSON.stringify({ ...original, startup: "database-preparation" }))
        published.resolve()
      },
    }, { promiseTimeout: 1_000, requestTimeout: 1_000, pollInterval: 5, spawnDelay: 200 }))
      .catch((error: unknown) => error)
    await published.promise
    try {
      await using ordinary = await serviceFixture()
      ordinary.spawn("starting")
      await ordinary.waitForFile()
      await expect(run(withEnsureTiming({ file: ordinary.registration, command: [] }, {
        promiseTimeout: 250, requestTimeout: 1_000, pollInterval: 5,
      }))).rejects.toThrow("Timed out")
      expect(child.exitCode).toBeNull()
      expect(await Bun.file(fixture.registration + ".pty-handoff").exists()).toBe(false)
    } finally {
      writeFileSync(fixture.registration, JSON.stringify(original))
      const endpoint = await pending
      const info = await Bun.file(fixture.registration).json()
      fixture.track(info.pid)
      expect(endpoint).toMatchObject({ url: info.url })
    }
  }, 10_000)

  test(`${name} reports failed preparation even when the requested version differs`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("database-failed")
    await fixture.waitForFile()
    await expect(run(withEnsureTiming({ file: fixture.registration, version: "other", command: [] }, {
      promiseTimeout: 300, requestTimeout: 1_000,
    }))).rejects.toThrow("Background service failed to start")
    expect(child.exitCode).toBeNull()
  })

  test(`${name} protects a ready old-version converter until phase removal`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("database-ready-old")
    await fixture.waitForFile()
    let settled = false
    const starts: string[] = []
    const pending = run(withEnsureTiming({
      file: fixture.registration, version: "test", command: fixture.command("graceful"),
      onStart: (reason) => starts.push(reason),
    }, { promiseTimeout: 300, requestTimeout: 1_000, pollInterval: 5, spawnDelay: 200 }))
      .finally(() => { settled = true }).catch((error: unknown) => error)
    try {
      await using ordinary = await serviceFixture()
      ordinary.spawn("starting")
      await ordinary.waitForFile()
      await expect(run(withEnsureTiming({ file: ordinary.registration, command: [] }, {
        promiseTimeout: 600, requestTimeout: 1_000, pollInterval: 5,
      }))).rejects.toThrow("Timed out")
      expect(settled).toBe(false)
      expect(child.exitCode).toBeNull()
      expect(starts).toEqual([])
      expect(await Bun.file(fixture.registration + ".pty-handoff").exists()).toBe(false)
    } finally {
      await Bun.write(fixture.registration + ".release", "")
      const endpoint = await pending
      const info = await Bun.file(fixture.registration).json()
      fixture.track(info.pid)
      expect(endpoint).toMatchObject({ url: info.url })
    }
  }, 10_000)

  test(`${name} enforces ordinary deadline during registration identity churn`, async () => {
    await using fixture = await serviceFixture()
    const child = fixture.spawn("churn")
    await fixture.waitForFile()
    let settled = false
    const pending = run(withEnsureTiming({ file: fixture.registration, command: [] }, {
      promiseTimeout: 150, requestTimeout: 1_000, pollInterval: 5,
    })).finally(() => { settled = true }).catch((error: unknown) => error)
    try {
      await using ordinary = await serviceFixture()
      ordinary.spawn("starting")
      await ordinary.waitForFile()
      await expect(run(withEnsureTiming({ file: ordinary.registration, command: [] }, {
        promiseTimeout: 450, requestTimeout: 1_000, pollInterval: 5,
      }))).rejects.toThrow("Timed out")
      expect(settled).toBe(true)
    } finally {
      child.kill("SIGKILL")
      await child.exited
      await pending
    }
    expect(await pending).toMatchObject({ message: "Timed out waiting for the background service to start" })
  }, 10_000)
}
