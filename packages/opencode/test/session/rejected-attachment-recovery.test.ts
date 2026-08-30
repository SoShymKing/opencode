import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import type { ConfigV1 } from "@opencode-ai/core/v1/config/config"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { EventV2Bridge } from "@/event-v2-bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { InstanceStore } from "@/project/instance-store"
import { InstanceBootstrap } from "@/project/bootstrap"
import { Provider } from "@/provider/provider"
import type { Agent } from "@/agent/agent"
import { InvalidFile } from "@/session/invalid-file"
import { MessageV2 } from "@/session/message-v2"
import { SessionProcessor } from "@/session/processor"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionStatus } from "@/session/status"
import { SessionSummary } from "@/session/summary"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { expect } from "bun:test"
import { Effect, Layer, Logger } from "effect"
import path from "node:path"
import { provideInstance, provideTmpdirServer, reloadInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { TestLLMServer } from "../lib/llm-server"

const ref = { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") }
const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({ summarize: () => Effect.void, diff: () => Effect.succeed([]), computeDiff: () => Effect.succeed([]) }),
)
const root = LayerNode.group([
  SessionProcessor.node,
  Session.node,
  SessionProjector.node,
  Provider.node,
  Database.node,
  EventV2Bridge.node,
  SessionStatus.node,
  CrossSpawnSpawner.node,
  InstanceStore.node,
])
const replacements = [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
  [InstanceBootstrap.node, Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))],
] as const
const env = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  replacements,
)
const it = testEffect(env)

const config = (url: string): Partial<ConfigV1.Info> => ({
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
          attachment: true,
          modalities: { input: ["text", "pdf"], output: ["text"] },
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: { apiKey: "test-key", baseURL: url },
    },
  },
})

function agent(): Agent.Info {
  return { name: "build", mode: "primary", options: {}, permission: [{ permission: "*", pattern: "*", action: "allow" }] }
}

const makeAssistant = Effect.fn("test.makeAssistant")(function* (input: {
  sessionID: SessionID
  parentID: MessageID
  root: string
}) {
  return yield* (yield* Session.Service).updateMessage({
    id: MessageID.ascending(),
    role: "assistant",
    sessionID: input.sessionID,
    parentID: input.parentID,
    providerID: ref.providerID,
    modelID: ref.modelID,
    mode: "build",
    agent: "build",
    path: { cwd: input.root, root: input.root },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: Date.now() },
  } satisfies SessionV1.Assistant)
})

const makeTurn = Effect.fn("test.makeTurn")(function* (input: {
  sessionID: SessionID
  root: string
  parts: readonly ({ readonly type: "text"; readonly text: string } | { readonly type: "file"; readonly mime: string; readonly url: string; readonly filename?: string })[]
}) {
  const sessions = yield* Session.Service
  const user = yield* sessions.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID: input.sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  const parts = yield* Effect.forEach(input.parts, (part) =>
    sessions.updatePart({ ...part, id: PartID.ascending(), sessionID: input.sessionID, messageID: user.id }),
  )
  const assistant = yield* makeAssistant({ sessionID: input.sessionID, parentID: user.id, root: input.root })
  return { user, parts, assistant }
})

const runTurn = Effect.fn("test.runTurn")(function* (turn: {
  readonly user: SessionV1.User
  readonly parts: readonly SessionV1.Part[]
  readonly assistant: SessionV1.Assistant
}) {
  const sessions = yield* Session.Service
  const provider = yield* Provider.Service
  const processors = yield* SessionProcessor.Service
  const model = yield* provider.getModel(ref.providerID, ref.modelID)
  const handle = yield* processors.create({ assistantMessage: turn.assistant, sessionID: turn.user.sessionID, model })
  return yield* handle.process({
    user: turn.user,
    sessionID: turn.user.sessionID,
    model,
    agent: agent(),
    system: [],
    messages: yield* MessageV2.toModelMessagesEffect(
      yield* sessions.messages({ sessionID: turn.user.sessionID }),
      model,
    ),
    tools: {},
  })
})

it.live("quarantines rejected attachment and recovers next turn after instance reload", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const sessions = yield* Session.Service
        const events = yield* EventV2Bridge.Service
        const chat = yield* sessions.create({})
        const secret = "provider-secret-marker"
        const base64 = Buffer.from("%PDF-synthetic-malformed").toString("base64")
        const pdf = `data:application/pdf;base64,${base64}`
        const first = yield* makeTurn({
          sessionID: chat.id,
          root: path.resolve(dir),
          parts: [{ type: "text", text: "inspect attachment" }],
        })
        const attachment = {
          id: PartID.ascending(),
          sessionID: chat.id,
          messageID: first.assistant.id,
          type: "file",
          mime: "application/pdf",
          url: pdf,
        } satisfies SessionV1.FilePart
        yield* sessions.updatePart({
          id: PartID.ascending(),
          sessionID: chat.id,
          messageID: first.assistant.id,
          type: "tool",
          callID: "call-read",
          tool: "read",
          state: {
            status: "completed",
            input: { filePath: "/tmp/broken.pdf" },
            output: "PDF read successfully",
            title: "Read",
            metadata: {},
            time: { start: 0, end: 1 },
            attachments: [attachment],
          },
        })
        yield* sessions.updateMessage({
          ...first.assistant,
          finish: "tool-calls",
          time: { ...first.assistant.time, completed: Date.now() },
        })
        const failedAssistant = yield* makeAssistant({
          sessionID: chat.id,
          parentID: first.user.id,
          root: path.resolve(dir),
        })
        const originalFile = structuredClone(attachment)
        const logs: unknown[] = []
        const eventErrors: SessionV1.Assistant["error"][] = []
        const off = yield* events.listen((event) => {
          if (event.type === Session.Event.Error.type) eventErrors.push((event.data as typeof Session.Event.Error.data.Type).error)
          return Effect.void
        })
        yield* llm.error(400, { error: { message: secret, type: "invalid_request_error", param: "input", code: "invalid_file", pdf } })

        expect(
          yield* runTurn({ ...first, assistant: failedAssistant }).pipe(
            Effect.provide(Logger.layer([Logger.make((entry) => logs.push(Logger.formatStructured.log(entry)))])),
          ),
        ).toBe("stop")
        yield* off

        const failed = yield* MessageV2.get({ sessionID: chat.id, messageID: failedAssistant.id })
        if (failed.info.role !== "assistant") return yield* Effect.die("expected assistant")
        const error = failed.info.error
        if (!SessionV1.APIError.isInstance(error)) return yield* Effect.die("expected APIError")
        expect(failed.info.finish).toBe("error")
        expect(error.data.isRetryable).toBe(false)
        expect(error.data.message).toContain(attachment.id)
        expect(error.data.message).toContain("originals remain preserved")
        const serialized = JSON.stringify({ logs, eventErrors, error })
        for (const marker of ["stream error", "process rejected attachment"]) {
          const logged = logs.map((entry) => JSON.stringify(entry)).find((entry) => entry.includes(marker))
          expect(logged).toContain("invalid_request_error")
          expect(logged).toContain("input")
          expect(logged).toContain("invalid_file")
        }
        expect(serialized).not.toContain(secret)
        expect(serialized).not.toContain(pdf)
        expect(serialized).not.toContain(base64)
        expect(serialized).not.toContain(JSON.stringify((yield* llm.inputs)[0]))
        expect(serialized).not.toContain(llm.url)
        expect(serialized).not.toContain("\n    at ")
        expect(JSON.stringify((yield* llm.inputs)[0])).toContain(pdf)

        yield* reloadInstance({ directory: dir })
        yield* llm.text("recovered")
        yield* Effect.gen(function* () {
          const reloadedSessions = yield* Session.Service
          const next = yield* makeTurn({
            sessionID: chat.id,
            root: path.resolve(dir),
            parts: [{ type: "text", text: "continue safely" }],
          })
          expect(yield* runTurn(next)).toBe("continue")
          const history = yield* reloadedSessions.messages({ sessionID: chat.id })
          expect(history.filter((message) => message.info.role === "user")).toHaveLength(2)
          const persistedTool = history
            .find((message) => message.info.id === first.assistant.id)
            ?.parts.find((part) => part.type === "tool")
          if (persistedTool?.type !== "tool" || persistedTool.state.status !== "completed")
            return yield* Effect.die("expected completed read tool")
          expect(persistedTool.state.attachments?.[0]).toEqual(originalFile)
          const persistedFailure = yield* MessageV2.get({ sessionID: chat.id, messageID: failedAssistant.id })
          if (persistedFailure.info.role !== "assistant") return yield* Effect.die("expected assistant")
          expect(persistedFailure.info.finish).toBe("error")
          const persistedError = persistedFailure.info.error
          if (!SessionV1.APIError.isInstance(persistedError)) return yield* Effect.die("expected APIError")
          expect(InvalidFile.decode(persistedError.data.metadata?.[InvalidFile.metadataKey] ?? "")).toEqual([
            { partID: attachment.id, messageID: first.assistant.id, mime: "application/pdf" },
          ])
        }).pipe(provideInstance(dir))

        expect(yield* llm.calls).toBe(2)
        const secondPayload = JSON.stringify((yield* llm.inputs)[1])
        expect(secondPayload).toContain("continue safely")
        expect(secondPayload).not.toContain(pdf)
        expect(secondPayload).not.toContain(base64)
      }),
    { config },
  ),
  30_000,
)
