import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2Bridge } from "@/event-v2-bridge"
import { expect } from "bun:test"
import { tool } from "ai"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import * as Stream from "effect/Stream"
import { TestClock } from "effect/testing"
import path from "path"
import z from "zod"
import type { Agent } from "../../src/agent/agent"
import { Provider } from "@/provider/provider"

import { Session } from "@/session/session"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionProcessor } from "../../src/session/processor"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionSummary } from "../../src/session/summary"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { provideTmpdirInstance, provideTmpdirServer } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { raw, reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { LLMEvent, Usage } from "@opencode-ai/llm"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionEvent } from "@opencode-ai/core/session/event"
import {
  clearControlledProcesses,
  controlledToolLLM,
  eventPublishGate,
  eventV2BridgeWithPublishGate,
  forkControlledProcess,
  partUpdateGate,
  sessionWithPartUpdateGate,
  startControlledProcess,
} from "./processor-race.fixture"

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: {
        apiKey: "test-key",
        baseURL: "http://localhost:1/v1",
      },
    },
  },
}

function providerCfg(url: string) {
  return {
    ...cfg,
    provider: {
      ...cfg.provider,
      test: {
        ...cfg.provider.test,
        options: {
          ...cfg.provider.test.options,
          baseURL: url,
        },
      },
    },
  }
}

function agent(): Agent.Info {
  return {
    name: "build",
    mode: "primary",
    options: {},
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  }
}

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const waitFor = <A>(check: Effect.Effect<A | undefined>, message: string) =>
  Effect.gen(function* () {
    const stop = Date.now() + 500
    while (Date.now() < stop) {
      const value = yield* check
      if (value !== undefined) return value
      yield* Effect.sleep("10 millis")
    }
    return yield* Effect.fail(new Error(message))
  })

const user = Effect.fn("TestSession.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

const assistant = Effect.fn("TestSession.assistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  root: string,
) {
  const session = yield* Session.Service
  const msg: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID,
    mode: "build",
    agent: "build",
    path: { cwd: root, root },
    cost: 0,
    tokens: {
      total: 0,
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID,
    time: { created: Date.now() },
    finish: "end_turn",
  }
  yield* session.updateMessage(msg)
  return msg
})

const root = LayerNode.group([
  SessionProcessor.node,
  Session.node,
  SessionProjector.node,
  Provider.node,
  Database.node,
  EventV2Bridge.node,
  SessionStatus.node,
  CrossSpawnSpawner.node,
])
const replacements = [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
] as const
const env = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  replacements,
)

const it = testEffect(env)

const providerErrorLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: {}, providerExecuted: true }),
        LLMEvent.toolResult({
          id: "call-1",
          name: "lookup",
          result: { type: "error", value: "provider boom" },
          providerExecuted: true,
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const providerErrorEnv = LayerNode.compile(root, [...replacements, [RuntimeFlags.node, RuntimeFlags.layer()], [LLM.node, providerErrorLLM]])
const itProviderError = testEffect(providerErrorEnv)

const fragmentFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-1" }),
        LLMEvent.reasoningDelta({ id: "reasoning-1", text: "thinking" }),
        LLMEvent.textStart({ id: "text-1" }),
        LLMEvent.textDelta({ id: "text-1", text: "partial" }),
        LLMEvent.providerError({ message: "provider boom" }),
      ),
  }),
)
const fragmentFailureEnv = LayerNode.compile(root, [
  ...replacements,
  [RuntimeFlags.node, RuntimeFlags.layer()],
  [LLM.node, fragmentFailureLLM],
])
const itFragmentFailure = testEffect(fragmentFailureEnv)

const activeToolLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: { query: "weather" } }),
      ).pipe(Stream.concat(Stream.never)),
  }),
)
const activeToolEnv = LayerNode.compile(root, [...replacements, [LLM.node, activeToolLLM]])
const itActiveTool = testEffect(activeToolEnv)

const cleanupWaiterEnv = LayerNode.compile(root, [...replacements, [LLM.node, controlledToolLLM]])
const itCleanupWaiter = testEffect(cleanupWaiterEnv)
const ownerRaceEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, controlledToolLLM],
  [Session.node, sessionWithPartUpdateGate],
  [EventV2Bridge.node, eventV2BridgeWithPublishGate],
])
const itOwnerRace = testEffect(ownerRaceEnv)

const toolFailureGate = defer<void>()
const toolFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: { query: "weather" } }),
      ).pipe(
        Stream.concat(
          Stream.fromEffect(
            Effect.promise(() => toolFailureGate.promise).pipe(
              Effect.as(LLMEvent.toolError({ id: "call-1", name: "lookup", message: "tool boom" })),
            ),
          ),
        ),
      ),
  }),
)
const toolFailureEnv = LayerNode.compile(root, [...replacements, [LLM.node, toolFailureLLM]])
const itToolFailure = testEffect(toolFailureEnv)

const partLookupProbe = { active: false, count: 0 }
const sessionWithPartLookupProbe = Layer.effect(
  Session.Service,
  Effect.gen(function* () {
    const session = yield* Session.Service
    return Session.Service.of({
      ...session,
      getPart: (input) =>
        Effect.gen(function* () {
          if (partLookupProbe.active) partLookupProbe.count += 1
          return yield* session.getPart(input)
        }),
    })
  }),
).pipe(Layer.provide(LayerNode.compile(Session.node)))
const probedActiveToolEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, activeToolLLM],
  [Session.node, sessionWithPartLookupProbe],
])
const itProbedActiveTool = testEffect(probedActiveToolEnv)

const boot = Effect.fn("test.boot")(function* () {
  const processors = yield* SessionProcessor.Service
  const session = yield* Session.Service
  const provider = yield* Provider.Service
  return { processors, session, provider }
})

const basicUsage = () => new Usage({ inputTokens: 1, outputTokens: 1, totalTokens: 2 })

function assistantSuccessStream(text: string) {
  return Stream.make(
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.textStart({ id: "txt-0" }),
    LLMEvent.textDelta({ id: "txt-0", text }),
    LLMEvent.textEnd({ id: "txt-0" }),
    LLMEvent.stepFinish({ index: 0, reason: "stop", usage: basicUsage() }),
    LLMEvent.finish({ reason: "stop", usage: basicUsage() }),
  )
}

function abortStream(...events: LLMEvent[]) {
  return Stream.fromAsyncIterable(
    {
      async *[Symbol.asyncIterator]() {
        yield* events
        throw new DOMException("The operation was aborted.", "AbortError")
      },
    },
    (error) => error,
  )
}

function delayedFirstEventStream(delayMs: number) {
  return Stream.fromAsyncIterable(
    {
      async *[Symbol.asyncIterator]() {
        await Bun.sleep(delayMs)
        yield LLMEvent.stepStart({ index: 0 })
      },
    },
    (error) => error,
  )
}

function structuralEmptyAssistantStream() {
  return Stream.make(
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.stepFinish({ index: 0, reason: "unknown", usage: new Usage({}) }),
    LLMEvent.finish({ reason: "unknown", usage: new Usage({}) }),
  )
}

function llmMock(...streams: Stream.Stream<LLMEvent, unknown>[]) {
  let calls = 0
  return {
    calls: () => calls,
    layer: Layer.succeed(
      LLM.Service,
      LLM.Service.of({
        stream: () => {
          calls += 1
          return streams.shift() ?? Stream.empty
        },
      }),
    ),
  }
}

function processorEnv(llmLayer: Layer.Layer<LLM.Service>) {
  return LayerNode.compile(root, [...replacements, [LLM.node, llmLayer]])
}

function processorInput(input: {
  parent: SessionV1.User
  sessionID: SessionID
  model: Provider.Model
  text: string
}) {
  return {
    user: {
      id: input.parent.id,
      sessionID: input.sessionID,
      role: "user",
      time: input.parent.time,
      agent: input.parent.agent,
      model: { providerID: ref.providerID, modelID: ref.modelID },
    } satisfies SessionV1.User,
    sessionID: input.sessionID,
    model: input.model,
    agent: agent(),
    system: [],
    messages: [{ role: "user", content: input.text }],
    tools: {},
  } satisfies LLM.StreamInput
}

const processorHarness = Effect.fn("test.processorHarness")(function* (directory: string, text: string) {
  const { processors, session, provider } = yield* boot()
  const chat = yield* session.create({})
  const parent = yield* user(chat.id, text)
  const msg = yield* assistant(chat.id, parent.id, path.resolve(directory))
  const model = yield* provider.getModel(ref.providerID, ref.modelID)
  const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
  return { handle, input: processorInput({ parent, sessionID: chat.id, model, text }) }
})

const startToolCall = Effect.fn("test.startToolCall")(function* (directory: string, text: string) {
  const database = yield* Database.Service
  const { processors, session, provider } = yield* boot()
  const chat = yield* session.create({})
  const parent = yield* user(chat.id, text)
  const msg = yield* assistant(chat.id, parent.id, path.resolve(directory))
  const model = yield* provider.getModel(ref.providerID, ref.modelID)
  const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
  const run = yield* handle
    .process(processorInput({ parent, sessionID: chat.id, model, text }))
    .pipe(Effect.forkChild)

  yield* waitFor(
    MessageV2.parts(msg.id).pipe(
      Effect.map((parts) =>
        parts.find((part): part is SessionV1.ToolPart => part.type === "tool" && part.state.status === "running"),
      ),
      Effect.provideService(Database.Service, database),
    ),
    "timed out waiting for running tool part",
  )
  return { handle, messageID: msg.id, projectID: chat.projectID, run, sessionID: chat.id }
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

it.live("session.processor effect tests capture llm input cleanly", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("hello")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const input = {
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "hi" }],
          tools: {},
        } satisfies LLM.StreamInput

        const value = yield* handle.process(input)
        const parts = yield* MessageV2.parts(msg.id)
        const calls = yield* llm.calls

        expect(value).toBe("continue")
        expect(calls).toBe(1)
        expect(parts.some((part) => part.type === "text" && part.text === "hello")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests preserve text start time", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const gate = defer<void>()
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({
            head: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { role: "assistant" } }],
              },
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { content: "hello" } }],
              },
            ],
            wait: gate.promise,
            tail: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: {}, finish_reason: "stop" }],
              },
            ],
          }),
        )

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "hi" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.TextPart => part.type === "text")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for text part",
        )
        yield* Effect.sleep("20 millis")
        gate.resolve()

        const exit = yield* Fiber.await(run)
        const text = (yield* MessageV2.parts(msg.id)).find((part): part is SessionV1.TextPart => part.type === "text")

        expect(Exit.isSuccess(exit)).toBe(true)
        expect(text?.text).toBe("hello")
        expect(text?.time?.start).toBeDefined()
        expect(text?.time?.end).toBeDefined()
        if (!text?.time?.start || !text.time.end) return
        expect(text.time.start).toBeLessThan(text.time.end)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests stop after token overflow requests compaction", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("after", { usage: { input: 100, output: 0 } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const base = yield* provider.getModel(ref.providerID, ref.modelID)
        const mdl = { ...base, limit: { context: 20, output: 10 } }
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("compact")
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(parts.some((part) => part.type === "step-finish")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests capture reasoning from http mock", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("think").text("done").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)
        const reasoning = parts.find((part): part is SessionV1.ReasoningPart => part.type === "reasoning")
        const text = parts.find((part): part is SessionV1.TextPart => part.type === "text")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(reasoning?.text).toBe("think")
        expect(text?.text).toBe("done")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests reset reasoning state across retries", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("one").reset(), reply().reason("two").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)
        const reasoning = parts.filter((part): part is SessionV1.ReasoningPart => part.type === "reasoning")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(reasoning.some((part) => part.text === "two")).toBe(true)
        expect(reasoning.some((part) => part.text === "onetwo")).toBe(false)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests do not retry unknown json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { error: { message: "no_kv_space" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "json" }],
          tools: {},
        })

        expect(value).toBe("stop")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error?.name).toBe("APIError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry recognized structured json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(429, { type: "error", error: { type: "too_many_requests" } })
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry json" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

const unexpectedAbortLLM = llmMock(abortStream(LLMEvent.stepStart({ index: 0 })), assistantSuccessStream("after"))
const unexpectedAbortIt = testEffect(processorEnv(unexpectedAbortLLM.layer))

unexpectedAbortIt.live("session.processor effect tests retry unexpected aborts before assistant output", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies MessageV2.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry abort" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(unexpectedAbortLLM.calls()).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: cfg },
  ),
)

const partialAbortLLM = llmMock(
  abortStream(
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.textStart({ id: "txt-0" }),
    LLMEvent.textDelta({ id: "txt-0", text: "partial" }),
  ),
  assistantSuccessStream("after"),
)
const partialAbortIt = testEffect(processorEnv(partialAbortLLM.layer))

partialAbortIt.live("session.processor effect tests do not retry unexpected aborts after text output", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "partial abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies MessageV2.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "partial abort" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("stop")
        expect(partialAbortLLM.calls()).toBe(1)
        expect(parts.some((part) => part.type === "text" && part.text === "partial")).toBe(true)
        expect(handle.message.error?.name).toBe("UnexpectedProviderAbortError")
      }),
    { config: cfg },
  ),
)

const postToolTimeoutRetryLLM = llmMock(delayedFirstEventStream(50), assistantSuccessStream("after-timeout"))
const postToolTimeoutRetryIt = testEffect(processorEnv(postToolTimeoutRetryLLM.layer))

postToolTimeoutRetryIt.live("session.processor effect tests retry stalled post-tool continuation once", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "post-tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies MessageV2.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "post-tool" }],
          tools: {},
          internal: {
            postToolContinuation: true,
            postToolFirstEventTimeoutMs: 10,
          },
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(postToolTimeoutRetryLLM.calls()).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after-timeout")).toBe(true)
      }),
    { config: cfg },
  ),
)

const postToolTimeoutExhaustLLM = llmMock(delayedFirstEventStream(50))
const postToolTimeoutExhaustIt = testEffect(processorEnv(postToolTimeoutExhaustLLM.layer))

postToolTimeoutExhaustIt.live("session.processor effect tests include retry attempt in post-tool timeout errors", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "post-tool fail")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        yield* session.updatePart({
          id: PartID.ascending(),
          messageID: msg.id,
          sessionID: chat.id,
          type: "text",
          text: "started",
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies MessageV2.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "post-tool fail" }],
          tools: {},
          internal: {
            postToolContinuation: true,
            postToolFirstEventTimeoutMs: 10,
          },
        })

        expect(value).toBe("stop")
        expect(postToolTimeoutExhaustLLM.calls()).toBe(1)
        const error = handle.message.error
        expect(MessageV2.PostToolContinuationTimeoutError.isInstance(error)).toBe(true)
        if (!MessageV2.PostToolContinuationTimeoutError.isInstance(error)) return
        expect(error.data.message).toContain("No stream event within 10ms after tool continuation")
        expect(error.data.message).toContain("retry attempt 0")
      }),
    { config: cfg },
  ),
)

const emptyAssistantLLM = llmMock(Stream.empty, assistantSuccessStream("after-empty"))
const emptyAssistantIt = testEffect(processorEnv(emptyAssistantLLM.layer))

emptyAssistantIt.live("session.processor effect tests retry zero-part assistant finalization", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "empty assistant")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies MessageV2.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "empty assistant" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(emptyAssistantLLM.calls()).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after-empty")).toBe(true)
      }),
    { config: cfg },
  ),
)

const subagentEmptyAssistantLLM = llmMock(Stream.empty, Stream.empty)
const subagentEmptyAssistantIt = testEffect(processorEnv(subagentEmptyAssistantLLM.layer))

subagentEmptyAssistantIt.live("session.processor effect tests limit child zero-part assistant retries", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        const parentChat = yield* session.create({})
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "empty assistant")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies MessageV2.User,
          sessionID: chat.id,
          parentSessionID: parentChat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "empty assistant" }],
          tools: {},
        })

        expect(value).toBe("stop")
        expect(subagentEmptyAssistantLLM.calls()).toBe(2)
        expect(yield* MessageV2.parts(msg.id)).toHaveLength(0)
        expect(handle.message.error?.name).toBe("EmptyAssistantResponseError")
      }),
    { config: cfg },
  ),
)

const structuralEmptyAssistantLLM = llmMock(
  structuralEmptyAssistantStream(),
  assistantSuccessStream("after-structural"),
)
const structuralEmptyAssistantIt = testEffect(processorEnv(structuralEmptyAssistantLLM.layer))

structuralEmptyAssistantIt.live("session.processor effect tests retry structural-only assistant finalization", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "empty assistant")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies MessageV2.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "empty assistant" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(structuralEmptyAssistantLLM.calls()).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after-structural")).toBe(true)
      }),
    { config: cfg },
  ),
)

const subagentPostToolTimeoutLLM = llmMock(...Array.from({ length: 11 }, () => delayedFirstEventStream(50)))
const subagentPostToolTimeoutIt = testEffect(processorEnv(subagentPostToolTimeoutLLM.layer))

subagentPostToolTimeoutIt.live(
  "session.processor effect tests limit child post-tool continuation timeout retries",
  () =>
    provideTmpdirInstance(
      (dir) =>
        Effect.gen(function* () {
          const { processors, session, provider } = yield* boot()

          const parentChat = yield* session.create({})
          const chat = yield* session.create({})
          const parent = yield* user(chat.id, "post-tool child fail")
          const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
          const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
          const handle = yield* processors.create({
            assistantMessage: msg,
            sessionID: chat.id,
            model: mdl,
          })

          const value = yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies MessageV2.User,
            sessionID: chat.id,
            parentSessionID: parentChat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "post-tool child fail" }],
            tools: {},
            internal: {
              postToolContinuation: true,
              postToolFirstEventTimeoutMs: 10,
            },
          })

          expect(value).toBe("stop")
          expect(subagentPostToolTimeoutLLM.calls()).toBe(2)
          expect(handle.message.error?.name).toBe("PostToolContinuationTimeoutError")
        }),
      { config: cfg },
    ),
)

it.live("session.processor effect tests retry OpenAI-compatible midstream server errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(raw({ chunks: [{ error: { type: "server_error", code: "server_error", message: "xxx" } }] }))
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry midstream server error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry midstream server error" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests publish retry status updates", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        yield* llm.error(503, { error: "boom" })
        yield* llm.text("")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const states: number[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== SessionStatus.Event.Status.type) return Effect.void
          const data = evt.data as typeof SessionStatus.Event.Status.data.Type
          if (data.sessionID === chat.id && data.status.type === "retry") states.push(data.status.attempt)
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry" }],
          tools: {},
        })

        yield* off

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(states).toStrictEqual([1])
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests compact on structured context overflow", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { type: "error", error: { code: "context_length_exceeded" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact json" }],
          tools: {},
        })

        expect(value).toBe("compact")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests complete AI SDK tool calls when native flag is off", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.tool("lookup", { query: "weather" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "tool" }],
          tools: {
            lookup: tool({
              description: "Look up information",
              inputSchema: z.object({ query: z.string() }),
              execute: async (input) => ({
                title: "Weather lookup",
                output: `result:${input.query}`,
                metadata: { source: "test" },
              }),
            }),
          },
        })

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(call?.callID).toBe("call_1")
        expect(call?.tool).toBe("lookup")
        expect(call?.state.status).toBe("completed")
        if (call?.state.status !== "completed") return
        expect(call.state.input).toEqual({ query: "weather" })
        expect(call.state.output).toBe("result:weather")
        expect(call.state.title).toBe("Weather lookup")
        expect(call.state.metadata).toEqual({ source: "test" })
        expect(call.state.time.start).toBeDefined()
        expect(call.state.time.end).toBeDefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itActiveTool.live("session.processor effect tests preserve complete tool shape after metadata updates", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const active = yield* startToolCall(dir, "tool metadata")
        const running = yield* active.handle.updateToolCall("call-1", (part) => ({
          ...part,
          state: { ...part.state, metadata: { step: 1 } },
        }))
        yield* active.handle.completeToolCall("call-1", {
          title: "Weather lookup",
          metadata: { step: 1 },
          output: "result:weather",
        })
        yield* Fiber.interrupt(active.run)

        const call = (yield* MessageV2.parts(active.messageID)).find(
          (part): part is SessionV1.ToolPart => part.type === "tool",
        )
        expect(running?.state.status).toBe("running")
        if (!running || running.state.status !== "running") return
        expect(call).toEqual({
          ...running,
          state: {
            status: "completed",
            input: running.state.input,
            output: "result:weather",
            metadata: { step: 1 },
            title: "Weather lookup",
            time: { start: running.state.time.start, end: expect.any(Number) },
            attachments: undefined,
          },
        })
      }),
    { config: cfg },
  ),
)

itProbedActiveTool.live("session.processor metadata updates avoid post-create part lookups", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        partLookupProbe.active = false
        partLookupProbe.count = 0
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            partLookupProbe.active = false
            partLookupProbe.count = 0
          }),
        )

        const active = yield* startToolCall(dir, "tool metadata churn")
        partLookupProbe.active = true
        const seen: Array<number | undefined> = []
        const updates = yield* Effect.forEach(Array.from({ length: 100 }, (_, step) => step), (step) =>
          active.handle.updateToolCall("call-1", (part) => {
            seen.push(
              part.state.status === "running" && typeof part.state.metadata?.step === "number"
                ? part.state.metadata.step
                : undefined,
            )
            return { ...part, state: { ...part.state, metadata: { step } } }
          }),
        )
        yield* active.handle.completeToolCall("call-1", {
          title: "Weather lookup",
          metadata: { step: 99 },
          output: "result:weather",
        })
        partLookupProbe.active = false
        const lookups = partLookupProbe.count
        yield* Fiber.interrupt(active.run)

        const call = (yield* MessageV2.parts(active.messageID)).find(
          (part): part is SessionV1.ToolPart => part.type === "tool",
        )
        expect(updates.every((part) => part?.state.status === "running")).toBe(true)
        expect(seen).toEqual([undefined, ...Array.from({ length: 99 }, (_, step) => step)])
        expect(lookups).toBe(0)
        expect(call?.state).toEqual({
          status: "completed",
          input: { query: "weather" },
          output: "result:weather",
          metadata: { step: 99 },
          title: "Weather lookup",
          time: { start: expect.any(Number), end: expect.any(Number) },
          attachments: undefined,
        })
      }),
    { config: cfg },
  ),
)

itToolFailure.live("session.processor failures preserve latest tool metadata", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const active = yield* startToolCall(dir, "tool failure metadata")
        yield* active.handle.updateToolCall("call-1", (part) => ({
          ...part,
          state: { ...part.state, metadata: { step: 1 } },
        }))
        toolFailureGate.resolve()
        expect(yield* Fiber.join(active.run)).toBe("continue")

        const call = (yield* MessageV2.parts(active.messageID)).find(
          (part): part is SessionV1.ToolPart => part.type === "tool",
        )
        expect(call?.state).toEqual({
          status: "error",
          input: { query: "weather" },
          error: "tool boom",
          metadata: { step: 1 },
          time: { start: expect.any(Number), end: expect.any(Number) },
        })
      }),
    { config: cfg },
  ),
)

itCleanupWaiter.effect("session.processor cleanup settles retained tool waiters after interruption", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => Effect.sync(clearControlledProcesses))
        const harness = yield* processorHarness(dir, "tool cleanup waiter")
        const firstReady = yield* Deferred.make<void>()
        const secondReady = yield* Deferred.make<void>()
        const first = yield* startControlledProcess(harness.handle, harness.input, { cleanupReady: firstReady })
        const second = yield* startControlledProcess(harness.handle, harness.input, { cleanupReady: secondReady })

        const firstCleanup = yield* Fiber.interrupt(first).pipe(Effect.forkScoped({ startImmediately: true }))
        yield* Deferred.await(firstReady)
        yield* TestClock.adjust("249 millis")
        const retainedSettled = yield* Deferred.make<void>()
        const retainedCleanup = yield* Fiber.interrupt(second).pipe(
          Effect.ensuring(Deferred.succeed(retainedSettled, undefined).pipe(Effect.asVoid)),
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* Deferred.await(secondReady)
        yield* TestClock.adjust("1 milli")
        yield* Fiber.join(firstCleanup)
        yield* Effect.yieldNow

        expect(yield* Deferred.isDone(retainedSettled)).toBe(true)
        yield* Fiber.join(retainedCleanup)
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor late metadata persistence cannot replace newer tool ownership", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const session = yield* Session.Service
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        partUpdateGate.current = {
          entered,
          release,
          matches: (part) =>
            part.type === "tool" &&
            part.state.status === "running" &&
            part.state.metadata?.race === "late-update",
        }
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "late metadata ownership")
        const originalReady = yield* Deferred.make<void>()
        const original = yield* startControlledProcess(harness.handle, harness.input, {
          cleanupReady: originalReady,
        })
        const delayed = yield* harness.handle
          .updateToolCall("call-1", (part) => ({
            ...part,
            state: { ...part.state, metadata: { race: "late-update" } },
          }))
          .pipe(Effect.forkChild)
        yield* Deferred.await(entered)

        const originalCleanup = yield* Fiber.interrupt(original).pipe(
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* Deferred.await(originalReady)
        const delayedRelease = yield* Effect.sleep("251 millis").pipe(
          Effect.andThen(Deferred.succeed(release, undefined)),
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* TestClock.adjust("251 millis")
        yield* Fiber.join(delayedRelease)
        const delayedPart = yield* Fiber.join(delayed)
        yield* Fiber.join(originalCleanup)
        if (!delayedPart) return yield* Effect.die(new Error("delayed metadata owner was not created"))
        const storedOldPart = yield* session.getPart({
          sessionID: delayedPart.sessionID,
          messageID: delayedPart.messageID,
          partID: delayedPart.id,
        })
        const newerReady = yield* Deferred.make<void>()
        const newer = yield* startControlledProcess(harness.handle, harness.input, { cleanupReady: newerReady })
        const newerPart = yield* harness.handle.updateToolCall("call-1", (part) => part)
        if (!newerPart) return yield* Effect.die(new Error("newer metadata owner was not created"))
        const current = yield* harness.handle.updateToolCall("call-1", (part) => part)
        const newerCleanup = yield* Fiber.interrupt(newer).pipe(Effect.forkScoped({ startImmediately: true }))
        yield* Deferred.await(newerReady)
        yield* TestClock.adjust("250 millis")
        yield* Fiber.join(newerCleanup)

        expect(storedOldPart?.type).toBe("tool")
        if (storedOldPart?.type === "tool") {
          expect(storedOldPart.state.status).toBe("error")
          if (storedOldPart.state.status === "error") {
            expect(storedOldPart.state.error).toBe("Tool execution aborted")
            expect(storedOldPart.state.metadata).toEqual({ race: "late-update", interrupted: true })
          }
        }
        expect(current?.id).toBe(newerPart.id)
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor late completion persistence cannot delete newer tool ownership", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        partUpdateGate.current = {
          entered,
          release,
          matches: (part) =>
            part.type === "tool" && part.state.status === "completed" && part.state.title === "late completion",
        }
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "late completion ownership")
        const originalReady = yield* Deferred.make<void>()
        const original = yield* startControlledProcess(harness.handle, harness.input, {
          cleanupReady: originalReady,
        })
        const delayed = yield* harness.handle
          .completeToolCall("call-1", {
            title: "late completion",
            metadata: { race: "late-completion" },
            output: "late",
          })
          .pipe(Effect.forkChild)
        yield* Deferred.await(entered)

        const originalCleanup = yield* Fiber.interrupt(original).pipe(
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* Deferred.await(originalReady)
        yield* TestClock.adjust("250 millis")
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(delayed)
        yield* Fiber.join(originalCleanup)
        const newerReady = yield* Deferred.make<void>()
        const newer = yield* startControlledProcess(harness.handle, harness.input, { cleanupReady: newerReady })
        const newerPart = yield* harness.handle.updateToolCall("call-1", (part) => part)
        if (!newerPart) return yield* Effect.die(new Error("newer completion owner was not created"))
        const current = yield* harness.handle.updateToolCall("call-1", (part) => part)
        const newerCleanup = yield* Fiber.interrupt(newer).pipe(Effect.forkScoped({ startImmediately: true }))
        yield* Deferred.await(newerReady)
        yield* TestClock.adjust("250 millis")
        yield* Fiber.join(newerCleanup)

        expect(current?.id).toBe(newerPart.id)
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor late provider ensure persistence cannot replace newer tool ownership", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        partUpdateGate.current = {
          entered,
          release,
          matches: (part) => part.type === "tool" && part.metadata?.providerExecuted === true,
        }
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "late provider ensure ownership")
        const originalReady = yield* Deferred.make<void>()
        const original = yield* startControlledProcess(harness.handle, harness.input, {
          cleanupReady: originalReady,
        })
        const delayed = yield* forkControlledProcess(harness.handle, harness.input, {
          providerExecuted: true,
        })
        yield* Deferred.await(entered)

        const originalCleanup = yield* Fiber.interrupt(original).pipe(
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* Deferred.await(originalReady)
        yield* TestClock.adjust("250 millis")
        yield* Deferred.succeed(release, undefined)
        yield* Deferred.await(delayed.started)
        yield* Fiber.join(originalCleanup)
        const newer = yield* startControlledProcess(harness.handle, harness.input)
        const newerPart = yield* harness.handle.updateToolCall("call-1", (part) => part)
        if (!newerPart) return yield* Effect.die(new Error("newer provider owner was not created"))
        const current = yield* harness.handle.updateToolCall("call-1", (part) => part)
        yield* harness.handle.completeToolCall("call-1", {
          title: "race cleanup",
          metadata: {},
          output: "done",
        })
        yield* Fiber.interrupt(delayed.process)
        yield* Fiber.interrupt(newer)

        expect(current?.id).toBe(newerPart.id)
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor tool-call continuation cannot update a replacement owner", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const gate = {
          type: SessionEvent.Tool.Called.type,
          entered: yield* Deferred.make<void>(),
          release: yield* Deferred.make<void>(),
        }
        eventPublishGate.current = gate
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            eventPublishGate.current = undefined
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "tool-call replacement ownership")
        const original = yield* forkControlledProcess(harness.handle, harness.input, { query: "owner-a" })
        yield* Deferred.await(gate.entered)
        eventPublishGate.current = undefined

        yield* harness.handle.updateToolCall("call-1", (part) => ({
          ...part,
          state: { status: "running", input: { query: "owner-a" }, time: { start: Date.now() } },
        }))
        yield* harness.handle.completeToolCall("call-1", {
          title: "owner-a complete",
          metadata: {},
          output: "done",
        })
        const replacement = yield* startControlledProcess(harness.handle, harness.input, { query: "owner-b" })
        const replacementPart = yield* harness.handle.updateToolCall("call-1", (part) => part)
        if (!replacementPart) return yield* Effect.die(new Error("replacement tool-call owner was not created"))

        yield* Deferred.succeed(gate.release, undefined)
        yield* Deferred.await(original.started)
        const current = yield* harness.handle.updateToolCall("call-1", (part) => part)
        yield* harness.handle.completeToolCall("call-1", { title: "cleanup", metadata: {}, output: "done" })
        yield* Fiber.interrupt(original.process)
        yield* Fiber.interrupt(replacement)

        expect(current?.id).toBe(replacementPart.id)
        expect(current?.state.status).toBe("running")
        if (current?.state.status === "running") expect(current.state.input).toEqual({ query: "owner-b" })
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor tool-result cannot complete a replacement owner", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const gate = {
          type: SessionEvent.Tool.Success.type,
          entered: yield* Deferred.make<void>(),
          release: yield* Deferred.make<void>(),
        }
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            eventPublishGate.current = undefined
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "tool-result replacement ownership")
        const next = {
          event: yield* Deferred.make<LLMEvent>(),
          handled: yield* Deferred.make<void>(),
        }
        const original = yield* startControlledProcess(harness.handle, harness.input, { next, query: "owner-a" })
        eventPublishGate.current = gate
        yield* Deferred.succeed(
          next.event,
          LLMEvent.toolResult({ id: "call-1", name: "lookup", result: { type: "text", value: "done" } }),
        )
        yield* Deferred.await(gate.entered)
        eventPublishGate.current = undefined

        yield* harness.handle.completeToolCall("call-1", {
          title: "owner-a complete",
          metadata: {},
          output: "done",
        })
        const replacement = yield* startControlledProcess(harness.handle, harness.input, { query: "owner-b" })
        const replacementPart = yield* harness.handle.updateToolCall("call-1", (part) => part)
        if (!replacementPart) return yield* Effect.die(new Error("replacement tool-result owner was not created"))
        yield* Deferred.succeed(gate.release, undefined)
        yield* Deferred.await(next.handled)

        const current = yield* harness.handle.updateToolCall("call-1", (part) => part)
        yield* harness.handle.completeToolCall("call-1", { title: "cleanup", metadata: {}, output: "done" })
        yield* Fiber.interrupt(original)
        yield* Fiber.interrupt(replacement)

        expect(current?.id).toBe(replacementPart.id)
        expect(current?.state.status).toBe("running")
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor tool-error cannot fail a replacement owner", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const gate = {
          type: SessionEvent.Tool.Failed.type,
          entered: yield* Deferred.make<void>(),
          release: yield* Deferred.make<void>(),
        }
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            eventPublishGate.current = undefined
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "tool-error replacement ownership")
        const next = {
          event: yield* Deferred.make<LLMEvent>(),
          handled: yield* Deferred.make<void>(),
        }
        const original = yield* startControlledProcess(harness.handle, harness.input, { next, query: "owner-a" })
        eventPublishGate.current = gate
        yield* Deferred.succeed(
          next.event,
          LLMEvent.toolError({ id: "call-1", name: "lookup", message: "owner-a failed" }),
        )
        yield* Deferred.await(gate.entered)
        eventPublishGate.current = undefined

        yield* harness.handle.completeToolCall("call-1", {
          title: "owner-a complete",
          metadata: {},
          output: "done",
        })
        const replacement = yield* startControlledProcess(harness.handle, harness.input, { query: "owner-b" })
        const replacementPart = yield* harness.handle.updateToolCall("call-1", (part) => part)
        if (!replacementPart) return yield* Effect.die(new Error("replacement tool-error owner was not created"))
        yield* Deferred.succeed(gate.release, undefined)
        yield* Deferred.await(next.handled)

        const current = yield* harness.handle.updateToolCall("call-1", (part) => part)
        yield* harness.handle.completeToolCall("call-1", { title: "cleanup", metadata: {}, output: "done" })
        yield* Fiber.interrupt(original)
        yield* Fiber.interrupt(replacement)

        expect(current?.id).toBe(replacementPart.id)
        expect(current?.state.status).toBe("running")
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor cleanup retains same-owner metadata written during the grace period", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            eventPublishGate.current = undefined
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "cleanup grace metadata")
        const cleanupReady = yield* Deferred.make<void>()
        const process = yield* startControlledProcess(harness.handle, harness.input, { cleanupReady })
        const cleanup = yield* Fiber.interrupt(process).pipe(Effect.forkScoped({ startImmediately: true }))
        yield* Deferred.await(cleanupReady)

        yield* harness.handle.updateToolCall("call-1", (part) => ({
          ...part,
          state: { ...part.state, metadata: { grace: true } },
        }))
        yield* TestClock.adjust("250 millis")
        yield* Fiber.join(cleanup)

        const call = (yield* MessageV2.parts(harness.handle.message.id)).find(
          (part): part is SessionV1.ToolPart => part.type === "tool",
        )
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") expect(call.state.metadata).toEqual({ grace: true, interrupted: true })
      }),
    { config: cfg },
  ),
)

itOwnerRace.effect("session.processor concurrent initial tool creation retains the first owner", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const firstGate = {
          entered: yield* Deferred.make<void>(),
          release: yield* Deferred.make<void>(),
        }
        const secondGate = {
          entered: yield* Deferred.make<void>(),
          release: yield* Deferred.make<void>(),
        }
        const startGate = {
          type: SessionEvent.Tool.Input.Started.type,
          entered: yield* Deferred.make<void>(),
          release: yield* Deferred.make<void>(),
        }
        partUpdateGate.current = {
          sequence: [firstGate, secondGate],
          matches: (part) => part.type === "tool" && part.state.status === "pending",
        }
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            eventPublishGate.current = undefined
            partUpdateGate.current = undefined
            clearControlledProcesses()
          }),
        )
        const harness = yield* processorHarness(dir, "concurrent initial tool ownership")
        const order: string[] = []
        const off = yield* (yield* EventV2Bridge.Service).listen((event) => {
          if (
            event.type === SessionEvent.Tool.Input.Started.type ||
            event.type === SessionEvent.Tool.Input.Ended.type ||
            event.type === SessionEvent.Tool.Called.type
          ) {
            order.push(event.type)
          }
          return Effect.void
        })
        const first = yield* forkControlledProcess(harness.handle, harness.input, { query: "owner-a" })
        yield* Deferred.await(firstGate.entered)
        const second = yield* forkControlledProcess(harness.handle, harness.input, { query: "owner-b" })
        yield* Deferred.await(secondGate.entered)

        eventPublishGate.current = startGate
        yield* Deferred.succeed(firstGate.release, undefined)
        yield* Deferred.await(startGate.entered)
        eventPublishGate.current = undefined
        const firstOwner = yield* harness.handle.updateToolCall("call-1", (part) => part)
        if (!firstOwner) return yield* Effect.die(new Error("first concurrent tool owner was not created"))
        yield* Deferred.succeed(secondGate.release, undefined)
        const beforeStart = yield* Deferred.await(second.started).pipe(
          Effect.as(true),
          Effect.timeoutOrElse({ duration: "1 milli", orElse: () => Effect.succeed(false) }),
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* Effect.yieldNow
        yield* TestClock.adjust("1 milli")
        const completedBeforeStart = yield* Fiber.join(beforeStart)
        yield* Deferred.succeed(startGate.release, undefined)
        yield* Deferred.await(first.started)
        yield* Deferred.await(second.started)
        const current = yield* harness.handle.updateToolCall("call-1", (part) => part)
        yield* off
        const parts = (yield* MessageV2.parts(harness.handle.message.id)).filter(
          (part): part is SessionV1.ToolPart => part.type === "tool",
        )

        yield* harness.handle.completeToolCall("call-1", { title: "cleanup", metadata: {}, output: "done" })
        yield* Fiber.interrupt(first.process)
        yield* Fiber.interrupt(second.process)

        expect(current?.id).toBe(firstOwner.id)
        expect(completedBeforeStart).toBe(false)
        expect(parts).toHaveLength(1)
        expect(order.filter((type) => type === SessionEvent.Tool.Input.Started.type)).toHaveLength(1)
        expect(order.indexOf(SessionEvent.Tool.Input.Started.type)).toBeLessThan(
          order.indexOf(SessionEvent.Tool.Input.Ended.type),
        )
        expect(order.indexOf(SessionEvent.Tool.Input.Started.type)).toBeLessThan(
          order.indexOf(SessionEvent.Tool.Called.type),
        )
      }),
    { config: cfg },
  ),
)

it.live("session.processor effect tests mark pending tools as aborted on cleanup", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.toolHang("bash", { cmd: "pwd" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "tool abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.ToolPart => part.type === "tool")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for tool part",
        )
        yield* handle.updateToolCall("call_1", (part) => ({
          ...part,
          state: { ...part.state, metadata: { step: 1 } },
        }))
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(yield* llm.calls).toBe(1)
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") {
          expect(call.state.error).toBe("Tool execution aborted")
          expect(call.state.metadata).toEqual({ step: 1, interrupted: true })
          expect(call.state.time.end).toBeDefined()
        }
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests record aborted errors and idle state", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const seen = defer<void>()
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service
        const sts = yield* SessionStatus.Service

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const errs: string[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== Session.Event.Error.type) return Effect.void
          const data = evt.data as typeof Session.Event.Error.data.Type
          if (data.sessionID !== chat.id || !data.error) return Effect.void
          errs.push(data.error.name)
          seen.resolve()
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        yield* Effect.promise(() => seen.promise)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        const state = yield* sts.get(chat.id)
        yield* off

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(state).toMatchObject({ type: "idle" })
        expect(errs).toContain("MessageAbortedError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests mark interruptions aborted without manual abort", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const sts = yield* SessionStatus.Service

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "interrupt" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        const state = yield* sts.get(chat.id)

        expect(Exit.isFailure(exit)).toBe(true)
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(state).toMatchObject({ type: "idle" })
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itProviderError.live("session.processor effect tests fail provider-executed error results", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider tool error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "provider tool error" }],
          tools: {},
        })
        yield* off

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") expect(call.state.error).toBe("provider boom")
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(MessageV2.Event.Updated.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
      }),
    { config: cfg },
  ),
)

itFragmentFailure.live("session.processor effect tests retain partial legacy parts without v2 events", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider failure")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        expect(
          yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "provider failure" }],
            tools: {},
          }),
        ).toBe("stop")
        yield* off

        const parts = yield* MessageV2.parts(msg.id)
        expect(parts).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ type: "text", text: "partial" }),
            expect.objectContaining({ type: "reasoning", text: "thinking" }),
          ]),
        )
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(Session.Event.Error.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
      }),
    { config: cfg },
  ),
)
