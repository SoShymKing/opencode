import { SessionV1 } from "@opencode-ai/core/v1/session"
import { LLMEvent } from "@opencode-ai/llm"
import { LLM } from "@/session/llm"
import { SessionProcessor } from "@/session/processor"
import { Session } from "@/session/session"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Clock, Deferred, Duration, Effect, Layer, Stream } from "effect"

const starts: Array<Deferred.Deferred<void>> = []
const providerExecutedModes: boolean[] = []

export const clearControlledProcesses = () => {
  starts.splice(0)
  providerExecutedModes.splice(0)
}

export const controlledToolLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () => {
      const started = starts.shift()
      const providerExecuted = providerExecutedModes.shift() ?? false
      if (!started) return Stream.die(new Error("controlled tool stream was not prepared"))
      return Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({
          id: "call-1",
          name: "lookup",
          input: { query: "weather" },
          ...(providerExecuted ? { providerExecuted: true } : {}),
        }),
      ).pipe(
        Stream.concat(
          Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(Stream.flatMap(() => Stream.never)),
        ),
      )
    },
  }),
)

export type PartUpdateGate = {
  readonly entered: Deferred.Deferred<void>
  readonly release: Deferred.Deferred<void>
  readonly matches: (part: SessionV1.Part) => boolean
}

export const partUpdateGate: { current?: PartUpdateGate } = {}

export const sessionWithPartUpdateGate = Layer.effect(
  Session.Service,
  Effect.gen(function* () {
    const session = yield* Session.Service
    return Session.Service.of({
      ...session,
      updatePart: (part) =>
        Effect.gen(function* () {
          const gate = partUpdateGate.current
          if (gate?.matches(part)) {
            yield* Deferred.succeed(gate.entered, undefined)
            yield* Deferred.await(gate.release)
          }
          return yield* session.updatePart(part)
        }),
    })
  }),
).pipe(Layer.provide(LayerNode.compile(Session.node)))

type ControlledProcessOptions = {
  readonly cleanupReady?: Deferred.Deferred<void>
  readonly providerExecuted?: boolean
}

export const forkControlledProcess = Effect.fn("test.forkControlledProcess")(function* (
  handle: SessionProcessor.Handle,
  input: LLM.StreamInput,
  options?: ControlledProcessOptions,
) {
  const started = yield* Deferred.make<void>()
  starts.push(started)
  providerExecutedModes.push(options?.providerExecuted ?? false)
  const clock = yield* Clock.Clock
  const process = yield* handle
    .process(input)
    .pipe(
      Effect.provideService(Clock.Clock, {
        ...clock,
        sleep: (duration: Duration.Duration) =>
          Effect.gen(function* () {
            if (Duration.toMillis(duration) === 250 && options?.cleanupReady) {
              yield* Deferred.succeed(options.cleanupReady, undefined)
            }
            yield* clock.sleep(duration)
          }),
      } satisfies Clock.Clock),
      Effect.forkChild,
    )
  return { process, started }
})

export const startControlledProcess = Effect.fn("test.startControlledProcess")(function* (
  handle: SessionProcessor.Handle,
  input: LLM.StreamInput,
  options?: ControlledProcessOptions,
) {
  const controlled = yield* forkControlledProcess(handle, input, options)
  yield* Deferred.await(controlled.started)
  return controlled.process
})
