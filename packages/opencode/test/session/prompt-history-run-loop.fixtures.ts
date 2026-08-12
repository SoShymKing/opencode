import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { afterEach, mock, spyOn } from "bun:test"
import { Effect, Tracer } from "effect"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"

afterEach(() => mock.restore())

export const recordConversions = Effect.acquireRelease(
  Effect.sync(() => {
    const calls: string[][] = []
    const original = MessageV2.toModelMessagesEffect
    const spy = spyOn(MessageV2, "toModelMessagesEffect").mockImplementation((messages, model, options) =>
      original(messages, model, options).pipe(
        Effect.tap(() => Effect.sync(() => calls.push(messages.map((message) => message.info.id)))),
      ),
    )
    return { calls, spy }
  }),
  (recorder) => Effect.sync(() => recorder.spy.mockRestore()),
)

export const recordFullHistoryClones = Effect.acquireRelease(
  Effect.sync(() => {
    const calls: Array<{ messages: number; generation: number }> = []
    const generations = new WeakMap<object, number>()
    const original = structuredClone
    const spy = spyOn(globalThis, "structuredClone").mockImplementation(
      <T>(value: T, options?: StructuredSerializeOptions) => {
        const result = original(value, options)
        if (isHistoryRoot(value)) {
          calls.push({ messages: value.length, generation: generations.get(value) ?? 0 })
          if (isHistoryRoot(result)) generations.set(result, (generations.get(value) ?? 0) + 1)
        }
        return result
      },
    )
    return { calls, spy }
  }),
  (recorder) => Effect.sync(() => recorder.spy.mockRestore()),
)

export const seedLongHistory = Effect.fn("PromptHistoryRunLoopTest.seedLongHistory")(function* (
  sessionID: SessionID,
) {
  const session = yield* Session.Service
  for (let index = 0; index < 50; index++) {
    const user = yield* addUser(session, sessionID, { text: `history-${index}`, time: index * 2 })
    yield* session.updateMessage({
      id: MessageID.ascending(),
      role: "assistant",
      parentID: user.id,
      sessionID,
      mode: "build",
      agent: "build",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: ModelV2.ID.make("test-model"),
      providerID: ProviderV2.ID.make("test"),
      time: { created: index * 2 + 1 },
      finish: "stop",
    })
  }
  return yield* addUser(session, sessionID, { text: "continue", time: 100 })
})

export const measureHistoryOperations = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const parent = yield* Effect.tracer
    const counts = { sequence: 0, pages: 0, hydrated: 0, ranges: 0 }
    const value = yield* effect.pipe(
      Effect.withTracer(
        Tracer.make({
          span(options) {
            if (options.name === "EventV2.latestSequence") counts.sequence += 1
            if (options.name === "MessageV2.page") counts.pages += 1
            if (options.name === "MessageV2.get") counts.hydrated += 1
            if (options.name === "PromptHistory.readRange") counts.ranges += 1
            return parent.span(options)
          },
        }),
      ),
    )
    return { value, counts }
  })

const addUser = Effect.fn("PromptHistoryRunLoopTest.addUser")(function* (
  session: Session.Interface,
  sessionID: SessionID,
  input: { readonly text: string; readonly time: number },
) {
  const info = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") },
    time: { created: input.time },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: info.id,
    sessionID,
    type: "text",
    text: input.text,
  })
  return info
})

function isHistoryRoot(value: unknown): value is SessionV1.WithParts[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every(
      (item) =>
        typeof item === "object" && item !== null && "info" in item && "parts" in item && Array.isArray(item.parts),
    )
  )
}
