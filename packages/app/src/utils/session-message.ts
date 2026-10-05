import type { SessionMessage } from "@opencode-ai/schema/session-message"
import type { AssistantMessage, FilePart, Message, Part, ToolPart, UserMessage } from "@opencode-ai/sdk/v2"
import { Option, Schema } from "effect"

export type NativeSessionMessage = typeof SessionMessage.Message.Encoded
type NativeAssistant = Extract<NativeSessionMessage, { type: "assistant" }>
type NativeTool = Extract<NativeAssistant["content"][number], { type: "tool" }>
type NativeShell = Extract<NativeSessionMessage, { type: "shell" }>
type NativeUser = Extract<NativeSessionMessage, { type: "user" }>

const emptyTokens = { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } }
const emptyModel: { id: string; providerID: string; variant?: string } = { id: "", providerID: "" }
const decodeToolInput = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)

export function compareMessages(a: Pick<Message, "id" | "time">, b: Pick<Message, "id" | "time">) {
  const left = messageKey(a)
  const right = messageKey(b)
  return left < right ? -1 : left > right ? 1 : 0
}

export const messageKey = (message: Pick<Message, "id" | "time">) => message.time.created + message.id

function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value)
}

function normalizeToolInput(name: string, input: Record<string, unknown>) {
  if (!["edit", "write"].includes(name) || typeof input.path !== "string" || typeof input.filePath === "string")
    return input
  return { ...input, filePath: input.path }
}

function normalizeToolMetadata(name: string, metadata: Record<string, unknown>) {
  if (name !== "edit" || !Array.isArray(metadata.files)) return metadata
  const file = metadata.files.find(record)
  if (!file || typeof file.file !== "string") return metadata
  return {
    ...metadata,
    filediff: {
      file: file.file,
      patch: typeof file.patch === "string" ? file.patch : undefined,
      additions: typeof file.additions === "number" ? file.additions : 0,
      deletions: typeof file.deletions === "number" ? file.deletions : 0,
    },
  }
}

export function normalizeSessionMessages(sessionID: string, source: readonly NativeSessionMessage[]) {
  const messages: Message[] = []
  const parts = new Map<string, Part[]>()
  let agent = ""
  let model = emptyModel
  let parentID: string | undefined

  source.forEach((message) => {
    if (message.type === "agent-switched") {
      agent = message.agent
      return
    }
    if (message.type === "model-switched") {
      model = message.model
      return
    }
    if (message.type === "user") {
      parentID = message.id
      messages.push(userMessage(sessionID, message, agent, model))
      parts.set(message.id, userParts(sessionID, message))
      return
    }
    if (message.type === "synthetic" && message.text.trim()) {
      parentID = message.id
      messages.push({
        id: message.id,
        sessionID,
        role: "user",
        time: message.time,
        agent,
        model: { providerID: model.providerID, modelID: model.id, variant: model.variant },
      })
      parts.set(message.id, [textPart(sessionID, message.id, "text:0", message.text, true)])
      return
    }
    if (message.type === "shell") {
      messages.push(...shellMessages(sessionID, message, agent, model))
      parts.set(message.id, [textPart(sessionID, message.id, "text:0", message.command)])
      parts.set(`${message.id}:assistant`, [shellPart(sessionID, message)])
      parentID = undefined
      return
    }
    if (message.type === "assistant") {
      agent = message.agent
      model = message.model
      if (!parentID) return
      const parent = messages.findLast((item) => item.id === parentID)
      if (parent?.role === "user") {
        parent.agent = message.agent
        parent.model = {
          providerID: message.model.providerID,
          modelID: message.model.id,
          variant: message.model.variant,
        }
      }
      messages.push(assistantMessage(sessionID, parentID, message))
      parts.set(message.id, assistantParts(sessionID, message))
      return
    }
    if (message.type !== "compaction" || !parentID) return
    parts.set(parentID, [
      ...(parts.get(parentID) ?? []),
      {
        id: `${message.id}:compaction`,
        sessionID,
        messageID: parentID,
        type: "compaction",
        auto: message.reason === "auto",
      },
    ])
  })

  return { messages, parts }
}

function shellMessages(
  sessionID: string,
  message: NativeShell,
  agent: string,
  model: { id: string; providerID: string; variant?: string },
): [UserMessage, AssistantMessage] {
  return [
    {
      id: message.id,
      sessionID,
      role: "user",
      time: { created: message.time.created },
      agent,
      model: { providerID: model.providerID, modelID: model.id, variant: model.variant },
    },
    {
      id: `${message.id}:assistant`,
      sessionID,
      role: "assistant",
      time: message.time,
      parentID: message.id,
      modelID: model.id,
      providerID: model.providerID,
      variant: model.variant,
      mode: agent,
      agent,
      path: { cwd: "", root: "" },
      cost: 0,
      tokens: emptyTokens,
    },
  ]
}

function shellPart(sessionID: string, message: NativeShell): ToolPart {
  const input = { command: message.command }
  const start = message.time.created
  const state: ToolPart["state"] =
    message.time.completed === undefined
      ? { status: "running", input, time: { start } }
      : {
          status: "completed",
          input,
          output: message.output,
          title: "Shell",
          metadata: {},
          time: { start, end: message.time.completed },
        }
  return {
    id: sessionMessagePartID(message.id, message.callID),
    sessionID,
    messageID: `${message.id}:assistant`,
    type: "tool",
    callID: message.callID,
    tool: "bash",
    state,
  }
}

export function sessionMessagePartID(messageID: string, contentID: string): string
export function sessionMessagePartID(messageID: string, type: "text" | "reasoning", ordinal: number): string
export function sessionMessagePartID(messageID: string, contentID: string, ordinal?: number) {
  return `${messageID}:${contentID}${ordinal === undefined ? "" : `:${ordinal}`}`
}

function userMessage(
  sessionID: string,
  message: NativeUser,
  agent: string,
  model: { id: string; providerID: string; variant?: string },
): UserMessage {
  return {
    id: message.id,
    sessionID,
    role: "user",
    time: message.time,
    agent,
    model: { providerID: model.providerID, modelID: model.id, variant: model.variant },
  }
}

function userParts(sessionID: string, message: NativeUser): Part[] {
  return [
    textPart(sessionID, message.id, "text:0", message.text),
    ...(message.files ?? []).map(
      (file, index): FilePart => ({
        id: `${message.id}:file:${index}`,
        sessionID,
        messageID: message.id,
        type: "file",
        mime: file.mime,
        filename: file.name,
        url: file.uri,
        source: file.source
          ? {
              type: "file",
              text: { value: file.source.text, start: file.source.start, end: file.source.end },
              path: file.source.text.startsWith("@") ? file.source.text.slice(1) : (file.name ?? file.source.text),
            }
          : undefined,
      }),
    ),
    ...(message.agents ?? []).map(
      (item, index): Part => ({
        id: `${message.id}:agent:${index}`,
        sessionID,
        messageID: message.id,
        type: "agent",
        name: item.name,
        source: item.source
          ? { value: item.source.text, start: item.source.start, end: item.source.end }
          : undefined,
      }),
    ),
  ]
}

function assistantMessage(sessionID: string, parentID: string, message: NativeAssistant): AssistantMessage {
  const error = message.error
    ? message.error.type.toLowerCase().includes("abort") || message.error.type.toLowerCase().includes("interrupt")
      ? { name: "MessageAbortedError" as const, data: { message: message.error.message } }
      : { name: "UnknownError" as const, data: { message: message.error.message } }
    : undefined
  return {
    id: message.id,
    sessionID,
    role: "assistant",
    time: message.time,
    error,
    parentID,
    modelID: message.model.id,
    providerID: message.model.providerID,
    variant: message.model.variant,
    mode: message.agent,
    agent: message.agent,
    path: { cwd: "", root: "" },
    cost: message.cost ?? 0,
    tokens: message.tokens ?? emptyTokens,
    finish: message.finish,
  }
}

function assistantParts(sessionID: string, message: NativeAssistant): Part[] {
  return message.content.flatMap((content): Part[] => {
    if (content.type === "text") {
      const part = textPart(sessionID, message.id, content.id, content.text)
      return content.text.trim() ? [part] : []
    }
    if (content.type === "reasoning") {
      const part: Part = {
        id: sessionMessagePartID(message.id, content.id),
        sessionID,
        messageID: message.id,
        type: "reasoning",
        text: content.text,
        metadata: content.providerMetadata,
        time: {
          start: content.time?.created ?? message.time.created,
          end: content.time?.completed,
        },
      }
      return content.text.trim() ? [part] : []
    }
    return [toolPart(sessionID, message.id, content)]
  })
}

function textPart(sessionID: string, messageID: string, contentID: string, text: string, synthetic?: boolean): Part {
  return {
    id: sessionMessagePartID(messageID, contentID),
    sessionID,
    messageID,
    type: "text",
    text,
    synthetic,
  }
}

function toolPart(sessionID: string, messageID: string, tool: NativeTool): ToolPart {
  const start = tool.time.ran ?? tool.time.created
  const state = (() => {
    if (tool.state.status === "pending") {
      const value = Option.getOrUndefined(decodeToolInput(tool.state.input))
      const input = normalizeToolInput(tool.name, record(value) ? value : {})
      return { status: "pending" as const, input, raw: tool.state.input }
    }
    if (tool.state.status === "running") {
      return {
        status: "running" as const,
        input: normalizeToolInput(tool.name, tool.state.input),
        metadata: normalizeToolMetadata(tool.name, tool.state.structured),
        time: { start },
      }
    }
    if (tool.state.status === "error") {
      return {
        status: "error" as const,
        input: normalizeToolInput(tool.name, tool.state.input),
        error: tool.state.error.message,
        metadata: normalizeToolMetadata(tool.name, tool.state.structured),
        time: { start, end: tool.time.completed ?? start },
      }
    }
    const attachments = [...tool.state.content, ...(tool.state.attachments ?? [])].flatMap((item, index): FilePart[] =>
      !("type" in item) || item.type === "file"
        ? [
            {
              id: `${sessionMessagePartID(messageID, tool.id)}:file:${index}`,
              sessionID,
              messageID,
              type: "file",
              mime: item.mime,
              filename: item.name,
              url: item.uri,
            },
          ]
        : [],
    )
    return {
      status: "completed" as const,
      input: normalizeToolInput(tool.name, tool.state.input),
      output: tool.state.content.flatMap((item) => (item.type === "text" ? [item.text] : [])).join("\n"),
      title: tool.name,
      metadata: normalizeToolMetadata(tool.name, tool.state.structured),
      time: { start, end: tool.time.completed ?? start },
      attachments: attachments.length ? attachments : undefined,
    }
  })()
  return {
    id: sessionMessagePartID(messageID, tool.id),
    sessionID,
    messageID,
    type: "tool",
    callID: tool.id,
    tool: tool.name,
    state,
    metadata: tool.provider,
  }
}
