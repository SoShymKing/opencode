import { availableParallelism, freemem } from "#ws-native-os"
import { Context, Deferred, Effect, Exit, Queue, Scope, Semaphore } from "effect"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"

// Cooperative estimates cover clones, decoded data and output, not Bun heap limits.
export const workerBytes = 64 * 1024 ** 2
export const Resources = Context.Reference<() => { readonly cpu: number; readonly freeBytes: number }>("@opencode/WsConversionResources", {
  defaultValue: () => () => ({ cpu: availableParallelism(), freeBytes: freemem() }),
})
const reserved = { cpu: 0, bytes: 0 }
const held = new WeakMap<Scope.Scope, number>()
let changed = Deferred.makeUnsafe<void>()
export const resourceChange = Effect.sync(() => Deferred.await(changed))
function notify() {
  const previous = changed
  changed = Deferred.makeUnsafe<void>()
  Deferred.doneUnsafe(previous, Exit.void)
}
export const readingPlan = (bytes = 0, count = 0) => Effect.map(Resources, (snapshot) => {
  const current = snapshot()
  const target = plan(current, bytes, count)
  return { ...target, available: Math.max(0, Math.floor(current.freeBytes / 2) - reserved.bytes),
    countLimit: target.workers ? Math.ceil(Math.max(2, target.workers) * minWorkPerWorker / estimate(0)) : 128 }
})
export function lease(bytes: number, messageID: string) {
  return Effect.gen(function* () {
    const scope = yield* Effect.scope
    return yield* Effect.acquireRelease(Effect.gen(function* () {
      const snapshot = yield* Resources
      while (true) {
        const available = Math.max(0, Math.floor(snapshot().freeBytes / 2) - reserved.bytes)
        if (bytes <= available) {
          reserved.bytes += bytes
          held.set(scope, (held.get(scope) ?? 0) + bytes)
          return bytes
        }
        if (reserved.bytes <= (held.get(scope) ?? 0)) return yield* memoryError(messageID, bytes, available)
        yield* Deferred.await(changed)
      }
    }), (bytes) => Effect.sync(() => {
      reserved.bytes -= bytes
      held.set(scope, (held.get(scope) ?? 0) - bytes)
      notify()
    }), { interruptible: true })
  })
}
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
  const capacity = snapshot.cpu < 2 ? 0 : Math.max(0, Math.min(snapshot.cpu - active.cpu, Math.floor((available - bytes) / workerBytes)))
  const workers = useful < 2 ? 0 : Math.min(capacity, count, useful)
  return { count, workers, capacity, bytes: bytes + workers * workerBytes, available }
}
export const headroom = Effect.map(Resources, (snapshot) => Math.max(0, Math.floor(snapshot().freeBytes / 2) - reserved.bytes))
export function plan(snapshot: { readonly cpu: number; readonly freeBytes: number }, bytes: number, count: number, active = { cpu: 0, bytes: 0 }) {
  const available = Math.max(0, Math.floor(snapshot.freeBytes / 2) - active.bytes)
  const workers = snapshot.cpu < 2 ? 0 : Math.max(0, Math.min(snapshot.cpu - active.cpu,
    Math.floor(available / (workerBytes + minWorkPerWorker)), Math.floor((available - bytes) / workerBytes)))
  return { available, workers, ready: !workers || (count >= workers && bytes >= Math.max(2, workers) * minWorkPerWorker) }
}
export const planning = (bytes = 0, count = 0) => Effect.map(Resources, (snapshot) => plan(snapshot(), bytes, count, reserved))
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
export function reserve(jobs: readonly WsNativeBatch.Job[], threads = true, prepaid = false) {
  return Effect.acquireRelease(Effect.gen(function* () {
    const snapshot = yield* Resources
    const scope = yield* Effect.scope
    return yield* Effect.suspend(() => {
      const weights = jobs.map(weight)
      const current = snapshot()
      const selected = choose(threads ? current : { ...current, cpu: 1 }, weights,
        { cpu: reserved.cpu, bytes: reserved.bytes - (prepaid ? weights.reduce((sum, value) => sum + value, 0) : 0) })
      if (!selected.count) return Effect.fail(memoryError(WsNativeBatch.jobID(jobs[0]), weights[0], selected.available))
      const bytes = selected.bytes - (prepaid ? weights.slice(0, selected.count).reduce((sum, value) => sum + value, 0) : 0)
      reserved.cpu += selected.workers
      reserved.bytes += bytes
      held.set(scope, (held.get(scope) ?? 0) + bytes)
      let active = true
      const releaseCPU = Effect.sync(() => {
        if (!active) return
        active = false
        reserved.cpu -= selected.workers
        notify()
      })
      return Effect.succeed({ ...selected, bytes, weights, releaseCPU, scope })
    })
  }), (selected) => selected.releaseCPU.pipe(Effect.andThen(Effect.sync(() => {
    reserved.bytes -= selected.bytes
    held.set(selected.scope, (held.get(selected.scope) ?? 0) - selected.bytes)
    notify()
  }))))
}
const stream = <E, R>(jobs: readonly WsNativeBatch.Job[], accept: (output: WsNativeBatch.Result, index: number) => Effect.Effect<void, E, R>, prepaid = false) => Effect.gen(function* () {
  let offset = 0
  while (offset < jobs.length) {
    offset += yield* Effect.scoped(Effect.gen(function* () {
      const selected = yield* reserve(jobs.slice(offset), false, prepaid)
      for (let index = 0; index < selected.count; index++) {
        const outputs = yield* WsNativeBatch.run([jobs[offset + index]])
        yield* accept(outputs[0], offset + index)
      }
      return selected.count
    }))
  }
})
const run = (jobs: readonly WsNativeBatch.Job[]) => Effect.gen(function* () {
  const results: WsNativeBatch.Result[] = []
  yield* stream(jobs, (result) => Effect.sync(() => { results.push(result) }))
  return results
})
export const open = Effect.succeed({ run, stream, process: (items: readonly WsNativeBatch.Input[]) => run(items.map((input) => ({ kind: "message", input }))).pipe(Effect.map(WsNativeBatch.messageOutputs)) })

export const withPool = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect

export function pipeline<A, B, E, R, E2, R2, E3, R3>(
  read: Effect.Effect<readonly A[] | undefined, E, R | Scope.Scope>,
  compute: (items: readonly A[], emit: (output: B, index: number) => Effect.Effect<void>) => Effect.Effect<void, E2, R2>,
  accept: (item: A, output: B) => Effect.Effect<void, E3, R3>,
) {
  return Effect.scoped(Effect.gen(function* () {
    const parent = yield* Effect.scope
    const window = yield* Semaphore.make(2)
    type Batch = { readonly items: readonly A[]; readonly ready: readonly Deferred.Deferred<B>[]; readonly scope: Scope.Closeable }
    const inputs = yield* Queue.make<Batch | undefined>({ capacity: 2 })
    const outputs = yield* Queue.make<Batch | undefined>({ capacity: 2 })
    const producer = Effect.gen(function* () {
      while (true) {
        yield* window.take(1)
        const scope = yield* Scope.fork(parent)
        const items = yield* read.pipe(Scope.provide(scope))
        if (items === undefined) {
          yield* Scope.close(scope, Exit.void)
          yield* Queue.offer(inputs, undefined)
          return
        }
        const ready = yield* Effect.forEach(items, () => Deferred.make<B>())
        yield* Queue.offer(inputs, { items, ready, scope })
      }
    })
    const processor = Effect.gen(function* () {
      while (true) {
        const batch = yield* Queue.take(inputs)
        yield* Queue.offer(outputs, batch)
        if (batch === undefined) return
        yield* compute(batch.items, (output, index) => Deferred.succeed(batch.ready[index], output).pipe(Effect.asVoid))
      }
    })
    const writer = Effect.gen(function* () {
      while (true) {
        const batch = yield* Queue.take(outputs)
        if (batch === undefined) return
        for (const [index, item] of batch.items.entries()) yield* accept(item, yield* Deferred.await(batch.ready[index]))
        yield* Scope.close(batch.scope, Exit.void)
        yield* window.release(1)
      }
    })
    yield* Effect.all([producer, processor, writer], { concurrency: 3, discard: true })
  }))
}
