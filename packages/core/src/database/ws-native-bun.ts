import { Worker } from "node:worker_threads"
import { Context, Deferred, Effect, Exit, Scope, Semaphore } from "effect"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"
import { reserve } from "./ws-native-serial.js"

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
  const parent = yield* Effect.scope
  const scope = yield* Scope.fork(parent)
  const lock = yield* Semaphore.make(1)
  const workers: { readonly worker: Effect.Success<ReturnType<typeof openWorker>>; readonly scope: Scope.Closeable }[] = []
  const stream = <E, R>(items: readonly WsNativeBatch.Job[], accept: (output: WsNativeBatch.Result, index: number) => Effect.Effect<void, E, R>, prepaid = false) => Effect.gen(function* () {
    let offset = 0
    while (offset < items.length) {
      offset += yield* Effect.gen(function* () {
        const payloadScope = yield* Effect.scope
        const initialized = yield* Deferred.make<readonly Deferred.Deferred<WsNativeBatch.Result>[]>()
        const compute = lock.withPermits(1)(Effect.gen(function* () {
          const selected = yield* reserve(items.slice(offset), true, prepaid).pipe(Scope.provide(payloadScope))
          const jobs = items.slice(offset, offset + selected.count)
          const ready = yield* Effect.forEach(jobs, () => Deferred.make<WsNativeBatch.Result>())
          yield* Deferred.succeed(initialized, ready)
          while (workers.length > selected.capacity) {
            const worker = workers.pop()
            if (worker) yield* Scope.close(worker.scope, Exit.void)
          }
          if (!selected.workers) {
            for (const [index, job] of jobs.entries()) {
              const outputs = yield* WsNativeBatch.run([job])
              yield* Deferred.succeed(ready[index], outputs[0])
            }
            yield* selected.releaseCPU
            return
          }
          while (workers.length < selected.workers) {
            const child = yield* Scope.fork(scope)
            const worker = yield* acquire.pipe(Scope.provide(child),
              Effect.onExit((exit) => Exit.isFailure(exit) ? Scope.close(child, exit) : Effect.void))
            workers.push({ worker, scope: child })
          }
          const groups = workers.slice(0, selected.workers).map<{ weight: number; items: { readonly index: number; readonly job: WsNativeBatch.Job }[] }>(() => ({ weight: 0, items: [] }))
          for (const item of jobs.map((job, index) => ({ job, index, weight: selected.weights[index] })).sort((left, right) => right.weight - left.weight)) {
            const group = groups.reduce((left, right) => left.weight <= right.weight ? left : right)
            group.weight += item.weight
            group.items.push(item)
          }
          yield* Effect.all(groups.map((group, index) => workers[index].worker.run(group.items.map((item) => item.job)).pipe(
            Effect.flatMap((outputs) => Effect.forEach(outputs, (output, index) => Deferred.succeed(ready[group.items[index].index], output))),
          )), { concurrency: selected.workers, discard: true })
          yield* selected.releaseCPU
        }))
        const write = Effect.gen(function* () {
          const ready = yield* Deferred.await(initialized)
          for (const [index, result] of ready.entries()) yield* accept(yield* Deferred.await(result), offset + index)
          return ready.length
        })
        const results = yield* Effect.all([compute, write], { concurrency: 2 })
        return results[1]
      }).pipe(Effect.onExit((exit) => Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void), Effect.scoped)
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
