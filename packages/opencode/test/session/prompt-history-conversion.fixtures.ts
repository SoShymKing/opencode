import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { Provider } from "@/provider/provider"
import { SessionID } from "@/session/schema"

export const sessionID = SessionID.make("session-conversion")

export function model(
  input: { providerID?: string; modelID?: string; apiNpm?: string; apiID?: string } = {},
): Provider.Model {
  return {
    id: ModelV2.ID.make(input.modelID ?? "test-model"),
    providerID: ProviderV2.ID.make(input.providerID ?? "test"),
    api: {
      id: input.apiID ?? input.modelID ?? "test-model",
      url: "https://example.com",
      npm: input.apiNpm ?? "@ai-sdk/openai",
    },
    name: "Test Model",
    capabilities: {
      temperature: true,
      reasoning: false,
      attachment: false,
      toolcall: true,
      input: { text: true, audio: false, image: false, video: false, pdf: false },
      output: { text: true, audio: false, image: false, video: false, pdf: false },
      interleaved: false,
    },
    cost: { input: 0, output: 0, cache: { read: 0, write: 0 } },
    limit: { context: 0, input: 0, output: 0 },
    status: "active",
    options: {},
    headers: {},
    release_date: "2026-01-01",
  }
}

export function messageID(label: string) {
  return SessionV1.MessageID.ascending(`msg_${label}`)
}

function partID(label: string) {
  return SessionV1.PartID.ascending(`prt_${label}`)
}

export function text(message: SessionV1.MessageID, label: string, value: string): SessionV1.TextPart {
  return { id: partID(`${label}_text`), sessionID, messageID: message, type: "text", text: value }
}

export function user(
  label: string,
  value = label,
  extra: SessionV1.Part[] = [],
): SessionV1.WithParts {
  const id = messageID(`user_${label}`)
  return {
    info: {
      id,
      sessionID,
      role: "user",
      time: { created: 0 },
      agent: "build",
      model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test-model") },
    } satisfies SessionV1.User,
    parts: [text(id, label, value), ...extra],
  }
}

type AssistantInput = {
  readonly finish?: string | false
  readonly error?: SessionV1.Assistant["error"]
  readonly parts?: SessionV1.Part[]
  readonly selectedModel?: Provider.Model
  readonly summary?: boolean
}

export function assistant(label: string, parentID: SessionV1.MessageID, input: AssistantInput = {}): SessionV1.WithParts {
  const id = messageID(`assistant_${label}`)
  const selected = input.selectedModel ?? model()
  return {
    info: {
      id,
      sessionID,
      role: "assistant",
      time: { created: 0 },
      parentID,
      modelID: selected.id,
      providerID: selected.providerID,
      mode: "build",
      agent: "build",
      path: { cwd: "/", root: "/" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      ...(input.finish === false ? {} : { finish: input.finish ?? "stop" }),
      ...(input.error === undefined ? {} : { error: input.error }),
      ...(input.summary === undefined ? {} : { summary: input.summary }),
    } satisfies SessionV1.Assistant,
    parts: input.parts ?? [text(id, label, `answer-${label}`)],
  }
}

export function turn(label: string, input: AssistantInput = {}): SessionV1.WithParts[] {
  const prompt = user(label)
  return [prompt, assistant(label, prompt.info.id, input)]
}

export function toolPart(
  label: string,
  status: SessionV1.ToolState["status"],
  metadata: Record<string, unknown> = {},
): SessionV1.ToolPart {
  const base = {
    id: partID(`${label}_tool`),
    sessionID,
    messageID: messageID(`assistant_${label}`),
    type: "tool" as const,
    callID: `call-${label}`,
    tool: "read",
    metadata: { openai: { marker: { value: "clean" } } },
  }
  if (status === "pending") return { ...base, state: { status, input: { path: { value: "clean" } }, raw: "" } }
  if (status === "running") {
    return { ...base, state: { status, input: { path: { value: "clean" } }, metadata, time: { start: 0 } } }
  }
  if (status === "error") {
    return { ...base, state: { status, input: { path: { value: "clean" } }, error: "failed", metadata, time: { start: 0, end: 1 } } }
  }
  return {
    ...base,
    state: {
      status,
      input: { path: { value: "clean" } },
      output: "complete",
      title: "Read",
      metadata,
      time: { start: 0, end: 1 },
    },
  }
}

export function mediaToolPart(label: string): SessionV1.ToolPart {
  const message = messageID(`assistant_${label}`)
  return {
    ...toolPart(label, "completed"),
    state: {
      status: "completed",
      input: { path: "/tmp/document.pdf" },
      output: "PDF read successfully",
      title: "Read",
      metadata: {},
      time: { start: 0, end: 1 },
      attachments: [
        {
          id: partID(`${label}_pdf`),
          sessionID,
          messageID: message,
          type: "file",
          mime: "application/pdf",
          filename: "document.pdf",
          url: `data:application/pdf;base64,${Buffer.from("%PDF-1.4\n").toString("base64")}`,
        },
      ],
    },
  }
}

export function stepParts(label: string): SessionV1.Part[] {
  const id = messageID(`assistant_${label}`)
  return [
    text(id, `${label}_first`, "first"),
    { id: partID(`${label}_step`), sessionID, messageID: id, type: "step-start" },
    text(id, `${label}_second`, "second"),
  ]
}

export function historicalParts(label: string): SessionV1.Part[] {
  const id = messageID(`user_${label}`)
  return [
    { id: partID(`${label}_compaction`), sessionID, messageID: id, type: "compaction", auto: true },
    { id: partID(`${label}_subtask`), sessionID, messageID: id, type: "subtask", prompt: "p", description: "d", agent: "build" },
  ]
}

export const failure = new SessionV1.APIError({ message: "failed", isRetryable: false }).toObject()
