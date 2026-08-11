import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Deferred, Effect, Layer, Queue, Sink, Stream } from "effect"
import { ChildProcessSpawner } from "effect/unstable/process"

export type Scenario = {
  readonly chunks: readonly string[]
  readonly hang: boolean
  readonly drained: Deferred.Deferred<void>
  readonly release: Deferred.Deferred<void>
  readonly outputDone: Deferred.Deferred<void>
  readonly killed: Deferred.Deferred<void>
  readonly exited: Deferred.Deferred<void>
}

type Interface = {
  readonly offer: (scenario: Scenario) => Effect.Effect<void>
  readonly take: Effect.Effect<Scenario>
}

export class Service extends Context.Service<Service, Interface>()("@test/ShellProcess") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const scenarios = yield* Queue.unbounded<Scenario>()
    return Service.of({
      offer: (scenario) => Queue.offer(scenarios, scenario).pipe(Effect.asVoid),
      take: Queue.take(scenarios),
    })
  }),
)

export const node = LayerNode.make({ service: Service, layer, deps: [] })

export const spawnerLayer = Layer.effect(
  ChildProcessSpawner.ChildProcessSpawner,
  Effect.gen(function* () {
    const scenarios = yield* Service
    return ChildProcessSpawner.make(() =>
      Effect.gen(function* () {
        const scenario = yield* scenarios.take
        const encoder = new TextEncoder()
        const kill = () =>
          Deferred.succeed(scenario.killed, undefined).pipe(
            Effect.andThen(Deferred.succeed(scenario.exited, undefined)),
            Effect.asVoid,
          )
        const finish = scenario.hang
          ? Deferred.await(scenario.release)
          : Deferred.succeed(scenario.exited, undefined).pipe(Effect.asVoid)
        const output = Stream.fromIterable(scenario.chunks).pipe(
          Stream.map((chunk) => encoder.encode(chunk)),
          Stream.concat(Stream.fromEffect(Deferred.succeed(scenario.drained, undefined)).pipe(Stream.drain)),
          Stream.concat(Stream.fromEffect(finish).pipe(Stream.drain)),
          Stream.ensuring(Deferred.succeed(scenario.outputDone, undefined)),
        )
        const handle = ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(0),
          exitCode: Deferred.await(scenario.exited).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
          isRunning: Deferred.isDone(scenario.exited).pipe(Effect.map((done) => !done)),
          kill,
          stdin: Sink.drain,
          stdout: Stream.empty,
          stderr: Stream.empty,
          all: output,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        })
        return yield* Effect.acquireRelease(
          Effect.succeed(handle),
          () => Deferred.isDone(scenario.exited).pipe(Effect.flatMap((done) => (done ? Effect.void : kill()))),
        )
      }),
    )
  }),
)

export const spawnerNode = LayerNode.make({
  service: ChildProcessSpawner.ChildProcessSpawner,
  layer: spawnerLayer,
  deps: [node],
})

export const testSpawnerLayer = Layer.provideMerge(spawnerLayer, layer)

export const makeScenario = Effect.fn("test.makeShellScenario")(function* (
  chunks: readonly string[],
  hang = false,
) {
  return {
    chunks,
    hang,
    drained: yield* Deferred.make<void>(),
    release: yield* Deferred.make<void>(),
    outputDone: yield* Deferred.make<void>(),
    killed: yield* Deferred.make<void>(),
    exited: yield* Deferred.make<void>(),
  } satisfies Scenario
})
