import { expect } from "bun:test"
import { LLMClient, Message } from "@opencode/ai"
import { OpenAI } from "@opencode/ai/providers"
import { RequestExecutor, WebSocketTransport } from "@opencode/ai/route"
import { App } from "@opencode/core/app"
import { WebSocketConstructor } from "@opencode/core/effect/websocket-constructor"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { ToolOutput } from "@opencode/core/tool-output"
import { Agent } from "@opencode/schema/agent"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Effect, Layer, Schema } from "effect"
import { FetchHttpClient } from "effect/http"
import { Socket } from "effect/socket"
import { testEffect } from "./lib/effect"

testEffect(WebSocketConstructor.layer).live("attachment calls and quarantined replay use fresh full WebSocket submissions", () => Effect.gen(function* () {
  const constructor = yield* Socket.WebSocketConstructor
  const frames: Record<string, unknown>[] = []
  const state = { connections: 0, http: 0 }
  const server = yield* Effect.acquireRelease(Effect.sync(() => Bun.serve({
    hostname: "127.0.0.1", port: 0,
    fetch(request, server) {
      if (server.upgrade(request)) return
      state.http++
      return new Response("Unexpected HTTP fallback", { status: 500 })
    },
    websocket: {
      open() { state.connections++ },
      message(socket, data) {
        frames.push(Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)))(String(data)))
        const id = `resp_${frames.length}`
        const output = frames.length === 4 ? [{ type: "compaction", id: "cmp_continuation", encrypted_content: "opaque" }] : []
        socket.send(JSON.stringify({ type: "response.created", response: { id } }))
        for (const item of output) socket.send(JSON.stringify({ type: "response.output_item.done", item }))
        socket.send(JSON.stringify({ type: "response.completed", response: { id, status: "completed", output } }))
      },
    },
  })), (server) => Effect.sync(() => { void server.stop(true) }))
  const transport = SessionModelTransport.makeLayer({
    open: (input) => WebSocketTransport.open(input).pipe(Effect.provideService(Socket.WebSocketConstructor, constructor)),
  })
  const client = LLMClient.layer.pipe(Layer.provide(RequestExecutor.layer), Layer.provide(FetchHttpClient.layer))
  yield* Effect.gen(function* () {
    const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer))
    const model = SessionRunnerModel.resolved(OpenAI.configure({ apiKey: "fixture", baseURL: server.url.toString() }).responses("fixture"), {
      capabilities: { tools: true, input: ["text", "image"], output: ["text"] }, cost: [], limit: { context: 100_000, output: 1000 }, transport: "websocket",
    })
    const session = Schema.decodeUnknownSync(Session.Info)({ id: "ses_attachment_transport", projectID: "global", cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      time: { created: 0, updated: 0 }, location: { directory: "/project" },
    })
    const user = Schema.decodeUnknownSync(SessionMessage.Info)({ id: "msg_attachment_transport", type: "user", text: "input", time: { created: 0 },
      files: [{ mime: "image/png", data: "aW1hZ2U=", source: { type: "inline" } }],
    })
    const agent = Agent.Info.default(Agent.ID.make("build"))
    const tools = { definitions: [], execute: () => Effect.die("No tools expected") }
    const submissions: SessionModelRequest.Prepared[] = []
    for (const quarantine of [false, false, true]) {
      const transcript = SessionModelRequest.baseTranscript({ agent, model, tools, initial: "", messages: [user],
        rejectedAttachments: quarantine ? [{ source: "user", messageID: user.id, fileIndex: 0 }] : [],
      })
      const prepared = yield* requests.primary({ session, agent: agent.id, model, ...transcript, webSocket: "session" })
      expect(prepared.options.webSocket).toBeDefined()
      submissions.push(prepared)
      yield* LLMClient.generate(prepared.request, prepared.options)
    }
    expect(state).toEqual({ connections: 3, http: 0 })
    expect(frames).toHaveLength(3)
    for (const frame of frames) expect(frame).not.toHaveProperty("previous_response_id")
    expect(JSON.stringify(frames[0])).toContain("input_image")
    expect(JSON.stringify(frames[1])).toContain("input_image")
    expect(JSON.stringify(frames[2])).not.toContain("input_image")
    const previous = submissions.at(-1)
    if (!previous) throw new Error("Missing previous submission")
    const hooks = yield* PluginHooks.Service
    yield* hooks.register("session", "http.request", () => Effect.void)
    const followup = yield* requests.primary({ session, agent: agent.id, model, system: [...previous.request.system],
      messages: [...previous.request.messages, Message.user("follow-up")], webSocket: "session",
    })
    expect(followup.attachmentBatch).toEqual({ coverage: "unknown" })
    const response = yield* LLMClient.generate(followup.request, followup.options)
    const checkpoint = yield* requests.primary({ session, agent: agent.id, model, system: [...followup.request.system],
      messages: [...followup.request.messages, response.message, Message.user("after checkpoint")], webSocket: "session",
    })
    expect(checkpoint.attachmentBatch).toEqual({ coverage: "unknown" })
    yield* LLMClient.generate(checkpoint.request, checkpoint.options)
    expect(state).toEqual({ connections: 3, http: 0 })
    expect(frames[3]).toHaveProperty("previous_response_id", "resp_3")
    expect(frames[4]).toHaveProperty("previous_response_id", "resp_4")
  }).pipe(Effect.provide(Layer.mergeAll(transport, client, LayerNode.compile(LayerNode.group([App.node, PluginHooks.node, ToolOutput.node])))))
}))
