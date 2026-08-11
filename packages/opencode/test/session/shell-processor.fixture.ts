import { Database } from "@opencode-ai/core/database/database"
import { EventV2Bridge } from "@/event-v2-bridge"
import { EventV2 } from "@opencode-ai/core/event"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Agent } from "@/agent/agent"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { Provider } from "@/provider/provider"
import { Session } from "@/session/session"
import { LLM } from "@/session/llm"
import { MessageV2 } from "@/session/message-v2"
import { SessionProcessor } from "@/session/processor"
import { MessageID, PartID } from "@/session/schema"
import { SessionStatus } from "@/session/status"
import { SessionSummary } from "@/session/summary"
import { resolveWithBridge } from "@/session/tools"
import { ToolRegistry } from "@/tool/registry"
import { createShellToolForTest } from "@/tool/shell"
import { Tool } from "@/tool/tool"
import { Truncate } from "@/tool/truncate"
import { LLMEvent } from "@opencode-ai/llm"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { Context, Deferred, Effect, Layer, Stream } from "effect"
import path from "path"
import { TestInstance, withTmpdirInstance } from "../fixture/fixture"
import { awaitWithTimeout, pollWithTimeout, testEffect } from "../lib/effect"
import { shellCheckpointNodes } from "../tool/shell-checkpoint.fixture"
import { Service, node, testSpawnerLayer } from "./shell-process.fixture"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { makeRetainedBridge } from "./shell-bridge.fixture"

export const callID = "call-shell"
const ref = { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }
const agent: Agent.Info = {
  name: "build",
  mode: "primary",
  options: {},
  permission: [{ permission: "*", pattern: "*", action: "allow" }],
}

export const config = {
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
      options: { apiKey: "test-key", baseURL: "http://localhost:1/v1" },
    },
  },
}

const summary = Layer.mock(SessionSummary.Service, {
  summarize: () => Effect.void,
  diff: () => Effect.succeed([]),
  computeDiff: () => Effect.succeed([]),
})

type LLMControlShape = {
  readonly complete: (result: unknown) => Effect.Effect<void>
  readonly result: Deferred.Deferred<unknown>
}
class LLMControl extends Context.Service<LLMControl, LLMControlShape>()("@test/ShellProcessorLLM") {}
const llmControlLayer = Layer.effect(
  LLMControl,
  Effect.gen(function* () {
    const result = yield* Deferred.make<unknown>()
    return LLMControl.of({ result, complete: (value) => Deferred.succeed(result, value).pipe(Effect.asVoid) })
  }),
)
const llmControlNode = LayerNode.make({ service: LLMControl, layer: llmControlLayer, deps: [] })
const activeToolLLM = Layer.effect(
  LLM.Service,
  Effect.gen(function* () {
    const control = yield* LLMControl
    return LLM.Service.of({
      stream: () =>
        Stream.make(
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolInputStart({ id: callID, name: "bash" }),
          LLMEvent.toolInputEnd({ id: callID, name: "bash" }),
          LLMEvent.toolCall({ id: callID, name: "bash", input: { command: "fixture" } }),
        ).pipe(
          Stream.concat(
            Stream.fromEffect(Deferred.await(control.result)).pipe(
              Stream.flatMap((value) =>
                Stream.make(
                  LLMEvent.toolResult({ id: callID, name: "bash", result: { type: "json", value } }),
                  LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
                  LLMEvent.finish({ reason: "tool-calls" }),
                ),
              ),
            ),
          ),
        ),
    })
  }),
)
const testLLMLayer = Layer.provideMerge(activeToolLLM, llmControlLayer)

const registryLayer = Layer.effect(
  ToolRegistry.Service,
  Effect.gen(function* () {
    const info = yield* createShellToolForTest(60_000)
    return ToolRegistry.Service.of({
      ids: () => Effect.succeed([info.id]),
      all: () => Tool.init(info).pipe(Effect.map((shell) => [shell])),
      named: () => Effect.die(new Error("unused in shell processor tests")),
      tools: () => Tool.init(info).pipe(Effect.map((shell) => [shell])),
    })
  }),
)
const registryNode = LayerNode.make({ service: ToolRegistry.Service, layer: registryLayer, deps: [shellCheckpointNodes] })
const root = LayerNode.group([
  SessionProcessor.node,
  Session.node,
  SessionProjector.node,
  Provider.node,
  Database.node,
  EventV2.node,
  EventV2Bridge.node,
  SessionStatus.node,
  MCP.node,
  Permission.node,
  Plugin.node,
  Truncate.node,
  RuntimeFlags.node,
  registryNode,
  node,
  llmControlNode,
])
export const it = testEffect(
  LayerNode.compile(root, [
    [SessionSummary.node, summary],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
    [LLM.node, testLLMLayer],
    [CrossSpawnSpawner.node, testSpawnerLayer],
  ]),
)

export const inSession = <A, E, R>(effect: (directory: string) => Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    return yield* effect((yield* TestInstance).directory)
  }).pipe(withTmpdirInstance({ config }))

export const start = Effect.fn("test.startShellProcessor")(function* (directory: string) {
  const session = yield* Session.Service
  const provider = yield* Provider.Service
  const processors = yield* SessionProcessor.Service
  const chat = yield* session.create({})
  const parent = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: chat.id,
    agent: agent.name,
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: parent.id,
    sessionID: chat.id,
    type: "text",
    text: "run fixture",
  })
  const message: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID: chat.id,
    mode: agent.name,
    agent: agent.name,
    path: { cwd: path.resolve(directory), root: path.resolve(directory) },
    cost: 0,
    tokens: { total: 0, input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID: parent.id,
    time: { created: Date.now() },
    finish: "end_turn",
  }
  yield* session.updateMessage(message)
  const model = yield* provider.getModel(ref.providerID, ref.modelID)
  const handle = yield* processors.create({ assistantMessage: message, sessionID: chat.id, model })
  const processor = yield* handle
    .process({
      user: parent,
      sessionID: chat.id,
      model,
      agent,
      system: [],
      messages: [{ role: "user", content: "run fixture" }],
      tools: {},
    })
    .pipe(Effect.forkScoped({ startImmediately: true }))
  yield* pollWithTimeout(
    MessageV2.parts(message.id).pipe(
      Effect.map((parts) =>
        parts.find((part): part is SessionV1.ToolPart =>
          part.type === "tool" && part.callID === callID && part.state.status === "running",
        ),
      ),
    ),
    "shell tool call never became running",
  )
  const bridge = yield* makeRetainedBridge()
  const tools = yield* resolveWithBridge(
    {
      agent,
      model,
      session: chat,
      processor: handle,
      bypassAgentCheck: false,
      messages: [],
      promptOps: {
        cancel: () => Effect.void,
        resolvePromptParts: (template) => Effect.succeed([{ type: "text", text: template }]),
        prompt: () => Effect.die(new Error("unused in shell processor tests")),
      },
    },
    bridge.retained,
  )
  const shell = tools.bash
  if (!shell?.execute) return yield* Effect.die(new Error("bash tool unavailable"))
  return { chat, message, processor, shell, toolFiber: bridge.toolFiber, process: yield* Service, llm: yield* LLMControl }
})

export const wait = <A, E, R>(effect: Effect.Effect<A, E, R>, message: string) =>
  awaitWithTimeout(effect, message, "5 seconds")
