import { SessionV1 } from "@opencode-ai/core/v1/session"
import { LLMEvent } from "@opencode-ai/llm"
import { LLM } from "@/session/llm"
import { SessionProcessor } from "@/session/processor"
import { Session } from "@/session/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Clock, Deferred, Duration, Effect, Layer, Stream } from "effect"

type ControlledProcess = {
  readonly started: Deferred.Deferred<void>
  readonly providerExecuted: boolean
  readonly query?: string
  readonly next?: {
    readonly event: Deferred.Deferred<LLMEvent>
    readonly handled: Deferred.Deferred<void>
  }
}

const controlledProcesses: ControlledProcess[] = []

export const clearControlledProcesses = () => {
  controlledProcesses.splice(0)
}

export const controlledToolLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () => {
      const controlled = controlledProcesses.shift()
      if (!controlled) return Stream.die(new Error("controlled tool stream was not prepared"))
      return Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({
          id: "call-1",
          name: "lookup",
          input: { query: controlled.query ?? "weather" },
          ...(controlled.providerExecuted ? { providerExecuted: true } : {}),
        }),
      ).pipe(
        Stream.concat(
          Stream.fromEffect(Deferred.succeed(controlled.started, undefined)).pipe(
            Stream.flatMap(() => {
              if (!controlled.next) return Stream.never
              return Stream.fromEffect(Deferred.await(controlled.next.event)).pipe(
                Stream.concat(
                  Stream.fromEffect(Deferred.succeed(controlled.next.handled, undefined)).pipe(
                    Stream.flatMap(() => Stream.never),
                  ),
                ),
              )
            }),
          ),
        ),
      )
    },
  }),
)

type Gate = {
  readonly entered: Deferred.Deferred<void>
  readonly release: Deferred.Deferred<void>
}

export type PartUpdateGate = {
  readonly matches: (part: SessionV1.Part) => boolean
} & (Gate | { readonly sequence: Gate[] })

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
            const current = "sequence" in gate ? gate.sequence.shift() : gate
            if (current) {
              yield* Deferred.succeed(current.entered, undefined)
              yield* Deferred.await(current.release)
            }
          }
          return yield* session.updatePart(part)
        }),
    })
  }),
).pipe(Layer.provide(LayerNode.compile(Session.node)))

export const eventPublishGate: {
  current?: Gate & { readonly type: EventV2.Definition["type"] }
} = {}

export const eventV2BridgeWithPublishGate = Layer.effect(
  EventV2Bridge.Service,
  Effect.gen(function* () {
    const bridge = yield* EventV2Bridge.Service
    const publish: EventV2.Interface["publish"] = (definition, data, options) =>
      Effect.gen(function* () {
        const gate = eventPublishGate.current
        if (gate?.type === definition.type) {
          yield* Deferred.succeed(gate.entered, undefined)
          yield* Deferred.await(gate.release)
        }
        return yield* bridge.publish(definition, data, options)
      })
    return EventV2Bridge.Service.of({ ...bridge, publish })
  }),
).pipe(Layer.provide(LayerNode.compile(EventV2Bridge.node)))

type ControlledProcessOptions = {
  readonly cleanupReady?: Deferred.Deferred<void>
  readonly providerExecuted?: boolean
  readonly query?: string
  readonly next?: {
    readonly event: Deferred.Deferred<LLMEvent>
    readonly handled: Deferred.Deferred<void>
  }
}

export const forkControlledProcess = Effect.fn("test.forkControlledProcess")(function* (
  handle: SessionProcessor.Handle,
  input: LLM.StreamInput,
  options?: ControlledProcessOptions,
) {
  const started = yield* Deferred.make<void>()
  controlledProcesses.push({
    started,
    providerExecuted: options?.providerExecuted ?? false,
    ...(options?.query !== undefined ? { query: options.query } : {}),
    ...(options?.next ? { next: options.next } : {}),
  })
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
