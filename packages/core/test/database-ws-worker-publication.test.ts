import { expect, test } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber } from "effect"
import type { Worker } from "node:worker_threads"
import { openWith, openWorker } from "@opencode/core/database/ws-native-bun"
import { headroom, lease, minWorkPerWorker, pipeline, Resources, weight, workerBytes } from "@opencode/core/database/ws-native-serial"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import { richMessageData } from "./fixture/database-ws"

test("free Worker dispatches admitted FIFO work before first synchronous ordered acceptance", () => Effect.runPromise(Effect.gen(function* () {
  const workers: Worker[] = []
  const events: string[] = []
  const requests: { ids: string[]; worker: number }[] = []
  const atFirstAccept: string[][] = []
  const accepted: string[] = []
  const results: WsNativeBatch.Result[] = []
  const jobs = ["msg_first", "msg_gate", "msg_tail_1", "msg_tail_2"].map((id, seq): WsNativeBatch.Job => ({
    kind: "message", input: {
      row: { id, session_id: "ses_publication", type: "assistant", seq, time_created: 1, time_updated: 16,
        data: richMessageData.replace('"first"', JSON.stringify("x".repeat(270000))) },
      parts: undefined, previousNative: false, store: true,
    },
  }))
  const available = yield* headroom
  yield* Effect.scoped(Effect.gen(function* () {
    const refilled = yield* Deferred.make<void>()
    const pool = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
      workers.push(opened.worker)
      let current: readonly WsNativeBatch.Job[] = []
      const post = opened.worker.postMessage.bind(opened.worker)
      opened.worker.postMessage = (...args: Parameters<Worker["postMessage"]>) => {
        post(...args)
        if (args[0] === "release") return
        requests.push({ ids: current.map(WsNativeBatch.jobID), worker: opened.worker.threadId })
        events.push(...current.map((job) => `dispatch:${WsNativeBatch.jobID(job)}`))
      }
      return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => Effect.suspend(() => {
        current = items
        return opened.run(items)
      }).pipe(Effect.tap(() => Effect.gen(function* () {
        events.push(...items.map((job) => `complete:${WsNativeBatch.jobID(job)}`))
        if (items.some((job) => WsNativeBatch.jobID(job) === "msg_tail_2")) yield* Deferred.succeed(refilled, undefined)
      }))) }
    })))
    let read = false
    const fiber = yield* pipeline(Effect.gen(function* () {
      if (read) return undefined
      yield* lease(jobs.reduce((sum, job) => sum + weight(job), 0), WsNativeBatch.jobID(jobs[0]))
      read = true
      return jobs
    }), (batch, emit: (output: WsNativeBatch.Result, index: number) => Effect.Effect<void>) => pool.stream(batch, emit, true), (job, output) => Effect.sync(() => {
      const id = WsNativeBatch.jobID(job)
      events.push(`accept:${id}`)
      if (id === "msg_first") atFirstAccept.push([...events])
      accepted.push(id)
      results.push(output)
    })).pipe(Effect.forkChild)
    const release = Effect.sync(() => {
      events.push("release")
      workers.forEach((worker) => worker.postMessage("release"))
    })
    yield* Effect.gen(function* () {
      yield* Deferred.await(refilled)
      yield* release
      yield* Fiber.join(fiber)
    }).pipe(Effect.ensuring(release.pipe(Effect.andThen(Fiber.interrupt(fiber)))))
  }))
  expect(yield* headroom).toBe(available)
  expect(workers.length).toBe(2)
  expect(workers.every((worker) => worker.threadId === -1)).toBe(true)
  expect(jobs.every((job) => weight(job) > minWorkPerWorker)).toBe(true)
  expect(requests.map((request) => request.ids)).toEqual(jobs.map((job) => [WsNativeBatch.jobID(job)]))
  expect(requests[0].worker).not.toBe(requests[1].worker)
  expect(requests[2].worker).toBe(requests[0].worker)
  expect(requests[3].worker).toBe(requests[0].worker)
  expect(accepted).toEqual(jobs.map(WsNativeBatch.jobID))
  expect(results).toEqual([...(yield* WsNativeBatch.run(jobs))])
  expect(atFirstAccept.length).toBe(1)
  expect(atFirstAccept[0]).toContain("complete:msg_first")
  expect(atFirstAccept[0]).toContain("dispatch:msg_gate")
  expect(atFirstAccept[0]).not.toContain("complete:msg_gate")
  expect(atFirstAccept[0]).not.toContain("release")
  expect(events.indexOf("complete:msg_gate")).toBeGreaterThan(events.indexOf("release"))
  expect(events.indexOf("dispatch:msg_tail_1")).toBeGreaterThan(events.indexOf("complete:msg_first"))
  expect(atFirstAccept[0]).toContain("dispatch:msg_tail_1")
  expect(events.indexOf("dispatch:msg_tail_1")).toBeLessThan(events.indexOf("accept:msg_first"))
}).pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 })), Effect.timeout("25 seconds"), Effect.scoped)))

for (const mode of ["failure", "interruption"] as const) test(`${mode} at first acceptance closes the already-posted successor before releasing credits`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const resources = () => ({ cpu: 2, freeBytes: 1024 ** 3 })
    const workers: Worker[] = []
    const exits: number[] = []
    const posted: string[] = []
    const atAccept: string[][] = []
    const completed: string[] = []
    const entered = yield* Deferred.make<void>()
    const error = new Error("ordered acceptance failed")
    const jobs = ["msg_first", "msg_gate", "msg_tail_1", "msg_tail_2", "msg_other_1", "msg_other_2"].map((id, seq): WsNativeBatch.Job => ({
      kind: "message", input: {
        row: { id, session_id: "ses_publication", type: "assistant", seq, time_created: 1, time_updated: 16,
          data: richMessageData.replace('"first"', JSON.stringify("x".repeat(270000))) },
        parts: undefined, previousNative: false, store: true,
      },
    }))
    const available = yield* headroom
    const pool = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
      workers.push(opened.worker)
      opened.worker.once("exit", () => exits.push(Effect.runSync(headroom.pipe(Effect.provideService(Resources, resources)))))
      let current: readonly WsNativeBatch.Job[] = []
      const post = opened.worker.postMessage.bind(opened.worker)
      opened.worker.postMessage = (...args: Parameters<Worker["postMessage"]>) => {
        post(...args)
        if (args[0] !== "release") posted.push(...current.map(WsNativeBatch.jobID))
      }
      return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => Effect.suspend(() => {
        current = items
        return opened.run(items)
      }).pipe(Effect.tap(() => Effect.sync(() => { completed.push(...items.map(WsNativeBatch.jobID)) }))) }
    })))
    const first = yield* pool.stream(jobs.slice(0, 4), () => Effect.sync(() => {
      atAccept.push([...posted])
    }).pipe(Effect.andThen(Deferred.succeed(entered, undefined)), Effect.andThen(mode === "failure" ? Effect.fail(error) : Effect.never)))
      .pipe(Effect.forkChild({ startImmediately: true }))
    const other = yield* pool.run(jobs.slice(4)).pipe(Effect.forkChild({ startImmediately: true }))
    yield* Effect.gen(function* () {
      yield* Deferred.await(entered)
      if (mode === "interruption") yield* Fiber.interrupt(first)
      const result = yield* Fiber.await(first)
      const peer = yield* Fiber.await(other)
      expect(Exit.isFailure(result)).toBe(true)
      if (Exit.isFailure(result)) {
        if (mode === "failure") expect(Cause.squash(result.cause)).toBe(error)
        if (mode === "interruption") expect(Cause.hasInterrupts(result.cause)).toBe(true)
      }
      expect(Exit.isFailure(peer)).toBe(true)
      expect(workers.length).toBe(2)
      expect(workers.every((worker) => worker.threadId === -1)).toBe(true)
      const held = available - jobs.reduce((sum, job) => sum + weight(job), 0) - 2 * workerBytes
      expect(exits).toEqual([held, held])
      expect(yield* headroom).toBe(available)
      expect(completed).not.toContain("msg_gate")
      expect(atAccept).toHaveLength(1)
      expect(atAccept[0]).toEqual(["msg_first", "msg_gate", "msg_tail_1"])
    }).pipe(Effect.ensuring(Effect.all([Fiber.interrupt(first), Fiber.interrupt(other)], { discard: true })))
  }).pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 })), Effect.timeout("25 seconds"), Effect.scoped)))
