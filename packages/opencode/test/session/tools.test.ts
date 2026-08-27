import { describe, expect } from "bun:test"
import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import type { CallToolResult, Tool as MCPToolDef } from "@modelcontextprotocol/sdk/types.js"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Agent } from "@/agent/agent"
import { EffectBridge } from "@/effect/bridge"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { MCP } from "@/mcp"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { Session } from "@/session/session"
import { resolveWithBridge } from "@/session/tools"
import { Tool } from "@/tool/tool"
import { ToolRegistry } from "@/tool/registry"
import { Truncate } from "@/tool/truncate"
import { isRecord } from "@/util/record"
import { Effect, Layer, Schema } from "effect"
import { testEffect } from "../lib/effect"
import { messageID, model, sessionID } from "./prompt-history-conversion.fixtures"

const LIMIT = 8
const INITIAL_OUTPUT = "initial"
const EXPANDED_OUTPUT = "x".repeat(LIMIT + 1)
const RAW_MCP_OUTPUT = "raw-mcp-output"
const selectedModel = model()
const message = {
  id: messageID("tools-assistant"),
  sessionID,
  role: "assistant",
  time: { created: 0 },
  parentID: messageID("tools-parent"),
  modelID: selectedModel.id,
  providerID: selectedModel.providerID,
  mode: "build",
  agent: "build",
  path: { cwd: "/", root: "/" },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
} satisfies SessionV1.Assistant
const agent: Agent.Info = { name: "build", mode: "primary", options: {}, permission: [] }
const session: Session.Info = {
  id: sessionID,
  slug: "tools-test",
  projectID: ProjectV2.ID.make("tools-test"),
  directory: "/",
  title: "tools test",
  version: "test",
  time: { created: 0, updated: 0 },
  permission: [],
}
const builtIn: Tool.Def = {
  id: "built_in",
  description: "built-in test tool",
  parameters: Schema.Struct({}),
  execute: () => Effect.succeed({ title: "", metadata: {}, output: INITIAL_OUTPUT }),
}
const mcpClient = new Client({ name: "tools-test", version: "1.0.0" })
const mcpResult = { content: [{ type: "text", text: RAW_MCP_OUTPUT }] } satisfies CallToolResult
mcpClient.callTool = () => Promise.resolve(mcpResult)
const mcpDefinition = {
  name: "raw",
  description: "MCP test tool",
  inputSchema: { type: "object", properties: {} },
} satisfies MCPToolDef
const trigger: Plugin.Interface["trigger"] = (name, _input, output) => {
  if (name === "tool.execute.after" && isRecord(output) && output.output === INITIAL_OUTPUT) {
    Object.assign(output, { output: EXPANDED_OUTPUT })
  }
  return Effect.succeed(output)
}

const it = testEffect(
  Layer.mergeAll(
    Layer.mock(Plugin.Service, { trigger }),
    Layer.mock(Permission.Service, { ask: () => Effect.void }),
    Layer.mock(ToolRegistry.Service, { tools: () => Effect.succeed([builtIn]) }),
    Layer.mock(MCP.Service, {
      clients: () => Effect.succeed({}),
      tools: () => Effect.succeed({ server_raw: { def: mcpDefinition, client: mcpClient } }),
    }),
    Layer.mock(Truncate.Service, {
      output: (text) =>
        text.length > LIMIT
          ? Effect.succeed({ content: text.slice(0, LIMIT), truncated: true, outputPath: "tools-test-output" })
          : Effect.succeed({ content: text, truncated: false }),
    }),
    RuntimeFlags.layer({ experimentalCodeMode: false }),
  ),
)

const input = {
  agent,
  model: selectedModel,
  session,
  processor: {
    message,
    updateToolCall: () => Effect.succeed(undefined),
    completeToolCall: () => Effect.void,
  },
  bypassAgentCheck: false,
  messages: [],
  promptOps: {
    cancel: () => Effect.void,
    resolvePromptParts: () => Effect.succeed([]),
    prompt: () => Effect.die(new Error("unused prompt operation")),
  },
}

const execute = Effect.fn("test.executeTool")(function* (name: string) {
  const tools = yield* resolveWithBridge(input, yield* EffectBridge.make())
  const run = tools[name]?.execute
  if (!run) return yield* Effect.die(new Error(`${name} tool unavailable`))
  return yield* Effect.promise(() => Promise.resolve(run({}, { toolCallId: `call-${name}`, messages: [] })))
})

describe("session tools output boundaries", () => {
  it.effect("bounds built-in output after an after-hook expands it", () =>
    Effect.gen(function* () {
      const result = yield* execute("built_in")
      if (!isRecord(result) || typeof result.output !== "string") {
        return yield* Effect.die(new Error("built-in tool returned an invalid result"))
      }
      expect(result.output.length).toBeLessThanOrEqual(LIMIT)
    }),
  )

  it.effect("drops raw MCP content after producing bounded output", () =>
    Effect.gen(function* () {
      const result = yield* execute("server_raw")
      if (!isRecord(result) || typeof result.output !== "string") {
        return yield* Effect.die(new Error("MCP tool returned an invalid result"))
      }
      expect(result.output.length).toBeLessThanOrEqual(LIMIT)
      expect("content" in result).toBe(false)
    }),
  )
})
