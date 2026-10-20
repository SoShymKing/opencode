import { Worker } from "node:worker_threads"
import { Context, Deferred, Effect, Exit, Fiber, Scope, Semaphore } from "effect"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"
import { minWorkPerWorker, reserve, reserveWorker, resourceChange, workerPlan } from "./ws-native-serial.js"

declare const OPENCODE_WS_NATIVE_WORKER: string | undefined
const workerURL = new URL(typeof OPENCODE_WS_NATIVE_WORKER === "undefined"
  ? `./ws-native-worker.${import.meta.url.endsWith(".ts") ? "ts" : "js"}` : OPENCODE_WS_NATIVE_WORKER, import.meta.url)

export function openWorker(url = workerURL) {
  return Effect.acquireRelease(Effect.try({
    try: () => {
      const worker = new Worker(url)
      const state: {
        failure?: WsNativeMessage.WsNativeMessageError
        pending?: { readonly id: string; readonly resolve: (items: readonly WsNativeBatch.Result[]) => void; readonly reject: (error: WsNativeMessage.WsNativeMessageError) => void }
      } = {}
      const fail = () => {
        const error = new WsNativeMessage.WsNativeMessageError({
          messageID: state.pending?.id ?? "", message: "WS message worker failed",
        })
        state.failure = error
        state.pending?.reject(error)
        state.pending = undefined
      }
      worker.on("error", fail)
      worker.on("messageerror", fail)
      worker.on("exit", fail)
      worker.on("message", (reply: WsNativeBatch.Reply) => {
        const pending = state.pending
        state.pending = undefined
        if ("items" in reply) pending?.resolve(reply.items)
        else pending?.reject(new WsNativeMessage.WsNativeMessageError(reply))
      })
      const run = (items: readonly WsNativeBatch.Job[]) => Effect.tryPromise({
          try: () => new Promise<readonly WsNativeBatch.Result[]>((resolve, reject) => {
            const id = items[0] ? WsNativeBatch.jobID(items[0]) : ""
            if (state.failure) return reject(new WsNativeMessage.WsNativeMessageError({
              messageID: id, message: state.failure.message,
            }))
            if (state.pending) return reject(new WsNativeMessage.WsNativeMessageError({ messageID: id, message: "WS worker already has a pending request" }))
            state.pending = { id, resolve, reject }
            try { worker.postMessage(items) } catch (error) {
              fail()
              if (!(error instanceof Error)) throw error
            }
          }),
          catch: (error) => error instanceof WsNativeMessage.WsNativeMessageError ? error
            : new WsNativeMessage.WsNativeMessageError({ messageID: items[0] ? WsNativeBatch.jobID(items[0]) : "", message: "WS message worker failed" }),
        })
      return {
        worker,
        close: async () => { fail(); await worker.terminate() },
        run,
        process: (items: readonly WsNativeBatch.Input[]) => run(items.map((input) => ({ kind: "message", input }))).pipe(Effect.map(WsNativeBatch.messageOutputs)),
      }
    },
    catch: () => new WsNativeMessage.WsNativeMessageError({ messageID: "", message: "WS message worker could not start" }),
  }), (worker) => Effect.promise(worker.close))
}

export const openWith = (acquire: ReturnType<typeof openWorker>) => Effect.gen(function* () {
  const scope = yield* Scope.make()
  const reservations = yield* Scope.fork(scope)
  const observers = yield* Scope.fork(scope)
  const lock = yield* Semaphore.make(1)
  const workers: { readonly worker: Effect.Success<ReturnType<typeof openWorker>>; readonly scope: Scope.Closeable; busy: boolean; weight: number }[] = []
  type Ready = Deferred.Deferred<WsNativeBatch.Result, WsNativeMessage.WsNativeMessageError>
  const pending = new Set<Ready>()
  const queue: { readonly job: WsNativeBatch.Job; readonly ready: Ready; readonly weight: number }[] = []
  const workload = { estimatedBytes: 0, count: 0 }
  let closed = false
  const close = (exit: Exit.Exit<unknown, unknown>) => lock.withPermits(1)(Effect.gen(function* () {
    closed = true
    yield* Scope.close(observers, exit)
    yield* Scope.close(scope, exit)
    queue.length = 0
    pending.clear()
    workload.estimatedBytes = 0
    workload.count = 0
  }))
  yield* Effect.addFinalizer(close)
  const active = () => workers.filter((record) => record.busy).length
  const dispatch = lock.withPermits(1)(Effect.gen(function* () {
    if (closed) return yield* new WsNativeMessage.WsNativeMessageError({ messageID: "", message: "WS conversion pool is closed" })
    const selected = yield* workerPlan(workload, active())
    while (workers.length && !workers[workers.length - 1].busy &&
      (workers.length > selected.capacity || workers[workers.length - 1].worker.worker.threadId < 0)) {
      const record = workers.pop()
      if (record) yield* Scope.close(record.scope, Exit.void)
    }
    const desired = Math.max(active(), selected.workers, active() ? Math.min(workers.length, active() + queue.length) : 0)
    for (let index = 0; active() < desired && index < selected.capacity; index++) {
      if (workers[index]?.busy) continue
      const reservation = yield* Scope.fork(reservations)
      if (!(yield* reserveWorker.pipe(Scope.provide(reservation)))) {
        yield* Scope.close(reservation, Exit.void)
        break
      }
      if (!workers[index] || workers[index].worker.worker.threadId < 0) {
        const child = yield* Scope.fork(scope)
        const worker = yield* acquire.pipe(Scope.provide(child))
        workers[index] = { worker, scope: child, busy: false, weight: 0 }
      }
      const record = workers[index]
      record.busy = true
      yield* Effect.gen(function* () {
        let flight: { readonly items: typeof queue; readonly rpc: Fiber.Fiber<readonly WsNativeBatch.Result[], WsNativeMessage.WsNativeMessageError> } | undefined
        while (true) {
          const finished = flight ? { items: flight.items, outputs: yield* Fiber.join(flight.rpc) } : undefined
          flight = yield* lock.withPermits(1)(Effect.gen(function* () {
            if (finished) {
              workload.estimatedBytes -= record.weight
              workload.count -= finished.items.length
              record.weight = 0
            }
            const capacity = yield* workerPlan(workload, active())
            if (queue.length && index < capacity.capacity) {
              const chunk: (typeof queue)[number][] = []
              while (queue.length && (!chunk.length || record.weight + queue[0].weight <= minWorkPerWorker)) {
                const item = queue.shift()
                if (!item) break
                record.weight += item.weight
                chunk.push(item)
              }
              const rpc = yield* record.worker.run(chunk.map((item) => item.job)).pipe(Effect.forkChild({ startImmediately: true }))
              return { items: chunk, rpc }
            }
            if (index >= capacity.capacity) yield* Scope.close(record.scope, Exit.void)
            yield* Scope.close(reservation, Exit.void)
            record.busy = false
            return undefined
          }))
          if (finished) for (const [position, item] of finished.items.entries()) yield* Deferred.succeed(item.ready, finished.outputs[position])
          if (!flight) {
            if (!active()) {
              while (queue.length) {
                const remaining = queue.shift()
                if (!remaining) break
                const outputs = yield* WsNativeBatch.run([remaining.job])
                workload.estimatedBytes -= remaining.weight
                workload.count--
                yield* Deferred.succeed(remaining.ready, outputs[0])
              }
            }
            return
          }
        }
      }).pipe(
        Effect.onError((cause) => Effect.forEach(pending, (ready) => Deferred.failCause(ready, cause), { discard: true })),
        Effect.forkIn(scope))
    }
    return active() > 0
  }))
  const stream = <E, R>(items: readonly WsNativeBatch.Job[], accept: (output: WsNativeBatch.Result, index: number) => Effect.Effect<void, E, R>, prepaid = false) => Effect.gen(function* () {
    let offset = 0
    while (offset < items.length) {
      offset += yield* Effect.gen(function* () {
        const selected = yield* reserve(items.slice(offset), false, prepaid)
        const jobs = items.slice(offset, offset + selected.count)
        if (selected.useful < 2) {
          yield* dispatch
          for (const [index, job] of jobs.entries()) {
            const outputs = yield* WsNativeBatch.run([job])
            yield* accept(outputs[0], offset + index)
          }
          return jobs.length
        }
        const ready = yield* Effect.forEach(jobs, () => Deferred.make<WsNativeBatch.Result, WsNativeMessage.WsNativeMessageError>())
        for (const [index, job] of jobs.entries()) {
          pending.add(ready[index])
          workload.estimatedBytes += selected.weights[index]
          workload.count++
          queue.push({ job, ready: ready[index], weight: selected.weights[index] })
        }
        if (!(yield* dispatch)) {
          while (queue.length) {
            const item = queue.shift()
            if (!item) break
            const outputs = yield* WsNativeBatch.run([item.job])
            workload.estimatedBytes -= item.weight
            workload.count--
            yield* Deferred.succeed(item.ready, outputs[0])
          }
        }
        const observer = yield* Effect.gen(function* () {
          while (true) {
            const change = yield* resourceChange
            if (queue.length) yield* dispatch
            yield* change
          }
        }).pipe(Effect.forkIn(observers))
        return yield* Effect.gen(function* () {
          for (const [index, result] of ready.entries()) {
            yield* accept(yield* Deferred.await(result), offset + index)
            pending.delete(result)
          }
          return ready.length
        }).pipe(Effect.raceFirst(Fiber.join(observer)), Effect.ensuring(Fiber.interrupt(observer)))
      }).pipe(Effect.onExit((exit) => Exit.isFailure(exit) ? close(exit) : Effect.void), Effect.scoped)
    }
  })
  const run = (items: readonly WsNativeBatch.Job[]) => Effect.gen(function* () {
    const results: WsNativeBatch.Result[] = []
    yield* stream(items, (result) => Effect.sync(() => { results.push(result) }))
    return results
  })
  return {
    run,
    stream,
    threadIDs: () => workers.map((record) => record.worker.worker.threadId),
    process: (items: readonly WsNativeBatch.Input[]) => run(items.map((input) => ({ kind: "message", input }))).pipe(Effect.map(WsNativeBatch.messageOutputs)),
  }
})

const Pool = Context.Reference<Effect.Effect<Effect.Success<ReturnType<typeof openWith>>> | undefined>("@opencode/WsConversionPool", {
  defaultValue: () => undefined,
})

export function withPool<A, E, R>(effect: Effect.Effect<A, E, R>) {
  return Effect.gen(function* () {
    if (yield* Pool) return yield* effect
    return yield* Effect.scoped(Effect.gen(function* () {
      const scope = yield* Effect.scope
      const pool = yield* Effect.cached(openWith(openWorker()).pipe(Scope.provide(scope)))
      return yield* effect.pipe(Effect.provideService(Pool, pool))
    }))
  })
}

export const open = Effect.flatMap(Pool, (pool) => pool ?? openWith(openWorker()))
