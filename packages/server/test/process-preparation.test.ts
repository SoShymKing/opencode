import { expect, test } from "bun:test"
import { Deferred, Effect, Fiber, Schedule } from "effect"
import { join } from "node:path"
import { HttpServer } from "effect/http"
import { ServerProcess } from "../src/process"
import { tmpdir } from "../../core/test/fixture/tmpdir"

test("managed boot clears recognized preparation before retaining a failed listener", async () => {
  await using tmp = await tmpdir()
  const filename = join(tmp.path, "database.db")
  await Bun.write(`${filename}.replacement.json`, "{}")
  const phases: boolean[] = []
  await Effect.gen(function* () {
    const listening = yield* Deferred.make<string>()
    const cleared = yield* Deferred.make<void>()
    const context = yield* Effect.context<never>()
    const fiber = yield* ServerProcess.start({
      hostname: "127.0.0.1", port: 0, password: "test-password", database: { path: filename },
      models: { fetch: false }, config: { directory: tmp.path, project: false, claude: false },
    }, {
      onListen: (address) => Deferred.succeed(listening, HttpServer.formatAddress(address)).pipe(Effect.as(Effect.void)),
      onDatabasePreparation: async (active) => {
        phases.push(active)
        if (active) {
          const url = await Effect.runPromiseWith(context)(Deferred.await(listening))
          const response = await fetch(new URL("/api/info", url), {
            headers: { authorization: `Basic ${btoa("opencode:test-password")}` },
          })
          expect(response.status).toBe(503)
          await response.arrayBuffer()
        }
        if (!active) await Effect.runPromiseWith(context)(Deferred.succeed(cleared, undefined))
      },
    }).pipe(Effect.forkScoped)
    yield* Deferred.await(cleared)
    expect(phases).toEqual([true, false])
    const url = yield* Deferred.await(listening)
    const status = yield* Effect.promise(async () => {
      const response = await fetch(new URL("/api/info", url), {
        headers: { authorization: `Basic ${btoa("opencode:test-password")}` },
      })
      await response.arrayBuffer()
      return response.status
    }).pipe(Effect.filterOrFail((status) => status === 500), Effect.retry(Schedule.spaced("10 millis")))
    expect(status).toBe(500)
    yield* Fiber.interrupt(fiber)
    expect(phases).toEqual([true, false])
  }).pipe(Effect.scoped, Effect.runPromise)
})
