import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect, Tracer } from "effect"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"

export const layer = LayerNode.compile(LayerNode.group([EventV2.node, Session.node, MessageV2.node, SessionProjector.node]))

export const withSession = <A, E, R>(
  use: (input: { readonly session: Session.Interface; readonly sessionID: SessionID }) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.gen(function* () {
      const session = yield* Session.Service
      return { session, sessionID: (yield* session.create({})).id }
    }),
    use,
    (input) => input.session.remove(input.sessionID).pipe(Effect.ignore),
  )

export const addUser = Effect.fn("PromptHistoryTest.addUser")(function* (
  sessionID: SessionID,
  text: string,
  options?: { readonly id?: MessageID; readonly time?: number },
) {
  const session = yield* Session.Service
  const info = {
    id: options?.id ?? MessageID.ascending(),
    sessionID,
    role: "user",
    time: { created: options?.time ?? Date.now() },
    agent: "test",
    model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
    tools: {},
  } satisfies SessionV1.User
  yield* session.updateMessage(info)
  const part = {
    id: PartID.ascending(),
    sessionID,
    messageID: info.id,
    type: "text",
    text,
  } satisfies SessionV1.TextPart
  yield* session.updatePart(part)
  return { info, part }
})

export const addAssistant = Effect.fn("PromptHistoryTest.addAssistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  options?: { readonly summary?: boolean },
) {
  const session = yield* Session.Service
  const info = {
    id: MessageID.ascending(),
    sessionID,
    role: "assistant",
    time: { created: Date.now() },
    parentID,
    modelID: ModelV2.ID.make("test"),
    providerID: ProviderV2.ID.make("test"),
    mode: "",
    agent: "test",
    path: { cwd: "/", root: "/" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    summary: options?.summary,
    finish: "stop",
  } satisfies SessionV1.Assistant
  yield* session.updateMessage(info)
  return info
})

export const addText = Effect.fn("PromptHistoryTest.addText")(function* (
  sessionID: SessionID,
  messageID: MessageID,
  text: string,
) {
  const part = { id: PartID.ascending(), sessionID, messageID, type: "text", text } satisfies SessionV1.TextPart
  yield* (yield* Session.Service).updatePart(part)
  return part
})

export const measure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
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
