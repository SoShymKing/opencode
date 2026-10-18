import { availableParallelism, freemem } from "#ws-native-os"
import { Context, Effect } from "effect"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"

// Cooperative estimates cover clones, decoded data and output, not Bun heap limits.
export const workerBytes = 64 * 1024 ** 2
export const Resources = Context.Reference<() => { readonly cpu: number; readonly freeBytes: number }>("@opencode/WsConversionResources", {
  defaultValue: () => () => ({ cpu: availableParallelism(), freeBytes: freemem() }),
})
const reserved = { cpu: 0, bytes: 0 }
// The conversion sweep showed startup overhead dominates below 2 MiB estimated work per Worker.
const minWorkPerWorker = 2 * 1024 ** 2
export const estimate = (bytes: number, rows = 1) => 8 * bytes + 4096 * rows
export function choose(snapshot: { readonly cpu: number; readonly freeBytes: number }, weights: readonly number[], active = { cpu: 0, bytes: 0 }) {
  const available = Math.max(0, Math.floor(snapshot.freeBytes / 2) - active.bytes)
  let bytes = 0
  let count = 0
  for (const weight of weights) {
    if (bytes + weight > available) break
    bytes += weight
    count++
  }
  const useful = Math.floor(bytes / minWorkPerWorker)
  const workers = snapshot.cpu < 2 || useful < 2 ? 0 : Math.max(0, Math.min(snapshot.cpu - active.cpu, count, Math.floor((available - bytes) / workerBytes), useful))
  return { count, workers, bytes: bytes + workers * workerBytes, available }
}
export const headroom = Effect.map(Resources, (snapshot) => Math.max(0, Math.floor(snapshot().freeBytes / 2) - reserved.bytes))
export function memoryError(messageID: string, needed: number, available: number) {
  return new WsNativeMessage.WsNativeMessageError({ messageID, message: `WS conversion needs an estimated ${needed} bytes; ${available} bytes available` })
}
export function weight(job: WsNativeBatch.Job): number {
  switch (job.kind) {
    case "message": return estimate(Buffer.byteLength(job.input.row.data) + (job.input.parts ?? []).reduce((sum, part) => sum + Buffer.byteLength(part.data), 0), 1 + (job.input.parts?.length ?? 0))
    case "native-validate": return estimate(Buffer.byteLength(job.parent.dataText) + job.children.reduce((sum, child) => sum + Buffer.byteLength(child.dataText), 0), 1 + job.children.length)
    case "native-encode": return estimate(Buffer.byteLength(JSON.stringify(job.row.data)))
    default: return estimate(Buffer.byteLength(job.row.data))
  }
}
export function reserve(jobs: readonly WsNativeBatch.Job[], threads = true) {
  return Effect.acquireRelease(Effect.gen(function* () {
    const snapshot = yield* Resources
    return yield* Effect.suspend(() => {
      const weights = jobs.map(weight)
      const current = snapshot()
      const selected = choose(threads ? current : { ...current, cpu: 1 }, weights, reserved)
      if (!selected.count) return Effect.fail(memoryError(WsNativeBatch.jobID(jobs[0]), weights[0], selected.available))
      reserved.cpu += selected.workers
      reserved.bytes += selected.bytes
      return Effect.succeed({ ...selected, weights })
    })
  }), (selected) => Effect.sync(() => {
    reserved.cpu -= selected.workers
    reserved.bytes -= selected.bytes
  }))
}
const run = (jobs: readonly WsNativeBatch.Job[]) => Effect.gen(function* () {
  const results: WsNativeBatch.Result[] = []
  let offset = 0
  while (offset < jobs.length) {
    const output = yield* Effect.scoped(Effect.gen(function* () {
      const selected = yield* reserve(jobs.slice(offset), false)
      return yield* WsNativeBatch.run(jobs.slice(offset, offset + selected.count))
    }))
    results.push(...output)
    offset += output.length
  }
  return results
})
export const open = Effect.succeed({ run, process: (items: readonly WsNativeBatch.Input[]) => run(items.map((input) => ({ kind: "message", input }))).pipe(Effect.map(WsNativeBatch.messageOutputs)) })
