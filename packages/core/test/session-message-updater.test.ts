import { expect, test } from "bun:test"
import { DateTime, Effect } from "effect"
import { Agent } from "@opencode/schema/agent"
import { Event } from "@opencode/schema/event"
import { Model } from "@opencode/schema/model"
import { Money } from "@opencode/schema/money"
import { Provider } from "@opencode/schema/provider"
import { Session } from "@opencode/schema/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionMessageUpdater } from "@opencode/core/session/message-updater"

const sessionID = Session.ID.make("ses_updater")
const assistantMessageID = SessionMessage.ID.make("msg_updater")
const envelope = {
  id: Event.ID.make("evt_updater"),
  created: 10,
  durable: { aggregateID: sessionID, seq: Event.Seq.make(1), version: Event.Version.make(1) },
}

function fixture() {
  let assistant = SessionMessage.Assistant.make({
    id: assistantMessageID,
    type: "assistant",
    agent: Agent.ID.make("build"),
    model: { id: Model.ID.make("model"), providerID: Provider.ID.make("provider") },
    time: { created: DateTime.makeUnsafe(0) },
    content: [{ type: "text", text: "retained" }],
    retry: { attempt: 1, at: DateTime.makeUnsafe(20), error: { type: "unknown", message: "retry" } },
  })
  const messages: SessionMessage.Info[] = []
  const adapter: SessionMessageUpdater.Adapter = {
    getAgent: () => Effect.succeed(undefined),
    getModel: () => Effect.succeed(undefined),
    getLocation: () => Effect.succeed(undefined),
    getCurrentAssistant: () => Effect.succeed(assistant),
    getAssistant: (id) => Effect.succeed(id === assistant.id ? assistant : undefined),
    getShell: () => Effect.succeed(undefined),
    getCompaction: () => Effect.succeed(undefined),
    updateAssistant: (message) => Effect.sync(() => { assistant = message }),
    updateShell: () => Effect.void,
    updateCompaction: () => Effect.void,
    appendMessage: (message) => Effect.sync(() => { messages.push(message) }),
  }
  return { adapter, assistant: () => assistant, messages }
}

test("ended step preserves native state and stores its absolute stream count", async () => {
  const state = fixture()
  await Effect.runPromise(SessionMessageUpdater.update(state.adapter, {
    ...envelope,
    type: SessionEvent.Step.Ended.type,
    data: {
      sessionID, assistantMessageID, streamEventCount: 17,
      finish: "stop", rawFinish: "stop_sequence", providerState: { response: "ended" },
      cost: Money.USD.make(2), tokens: { input: 1, output: 2, reasoning: 3, cache: { read: 4, write: 5 } },
    },
  }))
  expect(state.assistant()).toMatchObject({
    streamEventCount: 17, finish: "stop", rawFinish: "stop_sequence", providerState: { response: "ended" },
    cost: 2, content: [{ type: "text", text: "retained" }], time: { completed: DateTime.makeUnsafe(10) },
  })
})

test("failed step stores stream count and clears retry without discarding content", async () => {
  const state = fixture()
  await Effect.runPromise(SessionMessageUpdater.update(state.adapter, {
    ...envelope,
    type: SessionEvent.Step.Failed.type,
    data: {
      sessionID, assistantMessageID, streamEventCount: 9,
      error: { type: "unknown", message: "failed" }, providerState: { response: "failed" },
    },
  }))
  expect(state.assistant()).toMatchObject({
    streamEventCount: 9, finish: "error", providerState: { response: "failed" },
    content: [{ type: "text", text: "retained" }], time: { completed: DateTime.makeUnsafe(10) },
  })
  expect(state.assistant().retry).toBeUndefined()
})

test("archive and restore do not append transcript messages", async () => {
  const state = fixture()
  const before = state.assistant()
  await Effect.runPromise(Effect.gen(function* () {
    yield* SessionMessageUpdater.update(state.adapter, {
      ...envelope, type: SessionEvent.Archived.type, data: { sessionID },
    })
    yield* SessionMessageUpdater.update(state.adapter, {
      ...envelope, type: SessionEvent.Restored.type, data: { sessionID },
    })
  }))
  expect(state.assistant()).toBe(before)
  expect(state.messages).toEqual([])
})
