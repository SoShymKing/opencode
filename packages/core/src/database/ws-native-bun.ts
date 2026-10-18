import { Worker } from "node:worker_threads"
import { Effect, Exit, Scope, Semaphore } from "effect"
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
  const run = (items: readonly WsNativeBatch.Job[]) => lock.withPermits(1)(Effect.gen(function* () {
    const results: WsNativeBatch.Result[] = []
    let offset = 0
    while (offset < items.length) {
      const output = yield* Effect.gen(function* () {
        const selected = yield* reserve(items.slice(offset))
        const jobs = items.slice(offset, offset + selected.count)
        while (workers.length > selected.workers) {
          const worker = workers.pop()
          if (worker) yield* Scope.close(worker.scope, Exit.void)
        }
        if (!selected.workers) return yield* WsNativeBatch.run(jobs)
        while (workers.length < selected.workers) {
          const child = yield* Scope.fork(scope)
          const worker = yield* acquire.pipe(Scope.provide(child),
            Effect.onExit((exit) => Exit.isFailure(exit) ? Scope.close(child, exit) : Effect.void))
          workers.push({ worker, scope: child })
        }
        const groups = workers.map<{ weight: number; items: { readonly index: number; readonly job: WsNativeBatch.Job }[] }>(() => ({ weight: 0, items: [] }))
        for (const item of jobs.map((job, index) => ({ job, index, weight: selected.weights[index] })).sort((left, right) => right.weight - left.weight)) {
          const group = groups.reduce((left, right) => left.weight <= right.weight ? left : right)
          group.weight += item.weight
          group.items.push(item)
        }
        const replies = yield* Effect.all(groups.map((group, index) => workers[index].worker.run(group.items.map((item) => item.job)).pipe(
          Effect.map((outputs) => outputs.map((output, index) => ({ index: group.items[index].index, output }))),
        )), { concurrency: selected.workers })
        return replies.flat().sort((left, right) => left.index - right.index).map((item) => item.output)
      }).pipe(Effect.onExit((exit) => Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void), Effect.scoped)
      results.push(...output)
      offset += output.length
    }
    return results
  }))
  return {
    run,
    threadIDs: () => workers.map((record) => record.worker.worker.threadId),
    process: (items: readonly WsNativeBatch.Input[]) => run(items.map((input) => ({ kind: "message", input }))).pipe(Effect.map(WsNativeBatch.messageOutputs)),
  }
})

export const open = openWith(openWorker())
