import { availableParallelism } from "node:os"
import { Worker } from "node:worker_threads"
import { Effect } from "effect"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"

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

export const open = Effect.gen(function* () {
  if (availableParallelism() < 2) return { run: WsNativeBatch.run, process: WsNativeBatch.process }
  const left = yield* openWorker()
  const right = yield* openWorker()
  const run = (items: readonly WsNativeBatch.Job[]) => Effect.all([
      left.run(items.slice(0, Math.ceil(items.length / 2))),
      right.run(items.slice(Math.ceil(items.length / 2))),
    ], { concurrency: 2 }).pipe(Effect.map(([first, second]) => [...first, ...second]))
  return {
    run,
    process: (items: readonly WsNativeBatch.Input[]) => run(items.map((input) => ({ kind: "message", input }))).pipe(Effect.map(WsNativeBatch.messageOutputs)),
  }
})
