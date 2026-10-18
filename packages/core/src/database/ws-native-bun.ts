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
        pending?: { readonly id: string; readonly resolve: (items: readonly WsNativeBatch.Output[]) => void; readonly reject: (error: WsNativeMessage.WsNativeMessageError) => void }
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
      return {
        worker,
        close: async () => { fail(); await worker.terminate() },
        process: (items: readonly WsNativeBatch.Input[]) => Effect.tryPromise({
          try: () => new Promise<readonly WsNativeBatch.Output[]>((resolve, reject) => {
            if (state.failure) return reject(new WsNativeMessage.WsNativeMessageError({
              messageID: items[0]?.row.id ?? "", message: state.failure.message,
            }))
            state.pending = { id: items[0]?.row.id ?? "", resolve, reject }
            try { worker.postMessage(items) } catch (error) {
              fail()
              if (!(error instanceof Error)) throw error
            }
          }),
          catch: (error) => error instanceof WsNativeMessage.WsNativeMessageError ? error
            : new WsNativeMessage.WsNativeMessageError({ messageID: items[0]?.row.id ?? "", message: "WS message worker failed" }),
        }),
      }
    },
    catch: () => new WsNativeMessage.WsNativeMessageError({ messageID: "", message: "WS message worker could not start" }),
  }), (worker) => Effect.promise(worker.close))
}

export const open = Effect.gen(function* () {
  if (availableParallelism() < 2) return { process: WsNativeBatch.process }
  const left = yield* openWorker()
  const right = yield* openWorker()
  return {
    process: (items: readonly WsNativeBatch.Input[]) => Effect.all([
      left.process(items.slice(0, Math.ceil(items.length / 2))),
      right.process(items.slice(Math.ceil(items.length / 2))),
    ], { concurrency: 2 }).pipe(Effect.map(([first, second]) => [...first, ...second])),
  }
})
