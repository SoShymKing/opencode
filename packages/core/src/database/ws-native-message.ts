export * as WsNativeMessage from "./ws-native-message.js"

import { Effect, Schema } from "effect"
import { FinishReason } from "@opencode/schema/llm"
import { Base64, FileAttachment } from "@opencode/schema/prompt"
import { SessionMessage } from "@opencode/schema/session-message"
import { WsMessageCodec } from "./ws-message-codec.js"

export class WsNativeMessageError extends Schema.TaggedError<WsNativeMessageError>()("WsNativeMessageError", {
  messageID: Schema.String,
  message: Schema.String,
}) {}

export type Identity = {
  readonly position: number
  readonly type: "text" | "reasoning" | "tool"
  readonly sourceID: string
  readonly nativeID: string
}

type Input = {
  readonly id: string
  readonly sessionID: string
  readonly type: string
  readonly data: string
  readonly parts?: readonly { readonly position: number; readonly id: string; readonly type: string; readonly data: string }[]
}

type Source = typeof WsMessageCodec.Message.Type
type SourcePart = Extract<Source, { type: "assistant" }>["content"][number]
const parse = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json)))
const decode = Schema.decodeUnknownEffect(WsMessageCodec.Message)
const native = Schema.decodeUnknownEffect(Schema.toType(SessionMessage.Info))
const isFinish = Schema.is(FinishReason)

export function convert(input: Input): Effect.Effect<{
  readonly message: SessionMessage.Info
  readonly identities: readonly Identity[]
}, WsNativeMessageError> {
  const fail = (message: string) => new WsNativeMessageError({ messageID: input.id, message })
  return Effect.gen(function* () {
    const data = yield* parse(input.data)
    if ("id" in data || "type" in data) return yield* fail("message data contains identity")
    if (input.parts?.length && (input.type !== "assistant" || "content" in data))
      return yield* fail("ambiguous or non-assistant child parts")
    const parts = yield* Effect.forEach(input.parts?.toSorted((left, right) => left.position - right.position) ?? [], (part, position) =>
      Effect.gen(function* () {
        if (part.position !== position) return yield* fail("child positions must be contiguous nonnegative integers")
        const value = yield* parse(part.data)
        if ("id" in value || "type" in value) return yield* fail("child data contains identity")
        return { ...value, id: part.id, type: part.type }
      }),
    )
    const source = yield* decode({
      ...data, id: input.id, type: input.type,
      ...(input.type === "assistant" && !("content" in data) && input.parts !== undefined ? { content: parts } : {}),
    }, { onExcessProperty: "error" })
    if (source.type === "synthetic" && source.sessionID !== input.sessionID)
      return yield* fail("synthetic session does not match its owner")
    const ordinals = { text: 0, reasoning: 0 }
    const identities: readonly Identity[] = source.type !== "assistant" ? [] : source.content.map((part, position) => ({
      position, type: part.type, sourceID: part.id,
      nativeID: part.type === "tool" ? part.id : String(ordinals[part.type]++),
    }))
    const message = yield* convertMessage(source)
    return { message, identities }
  }).pipe(Effect.mapError((error) => error instanceof WsNativeMessageError ? error : fail(error.message)))
}

function convertMessage(source: Source) {
  const base = { id: source.id, metadata: source.metadata, time: source.time }
  switch (source.type) {
    case "shell":
      return native({ ...base, type: "shell-recorded", callID: source.callID, command: source.command, output: source.output })
    case "synthetic":
      return native({ ...base, type: "synthetic", text: source.text })
    case "compaction":
      return native({ ...source, status: "completed" })
    case "user":
      return Effect.gen(function* () {
        const files = source.files === undefined ? undefined : yield* Effect.forEach(
          source.files.filter((file) => file.uri.startsWith("data:")), attachment,
        )
        const recordedFiles = source.files?.filter((file) => !file.uri.startsWith("data:"))
        return yield* native({ ...base, type: "user", text: source.text, files,
          ...(recordedFiles?.length ? { recordedFiles } : {}),
          agents: source.agents?.map((agent) => ({ name: agent.name, mention: agent.source })),
        })
      })
    case "assistant":
      return native({ ...source, content: source.content.map(content),
        finish: isFinish(source.finish) ? source.finish : undefined,
        rawFinish: source.finish !== undefined && !isFinish(source.finish) ? source.finish : undefined,
      })
    default:
      return native(source)
  }
}

function content(part: SourcePart) {
  if (part.type === "text") return { type: part.type, text: part.text }
  if (part.type === "reasoning") return {
    type: part.type, text: part.text, time: part.time, recordedProviderMetadata: part.providerMetadata,
  }
  const state = part.state
  return {
    type: part.type, id: part.id, name: part.name, executed: part.provider?.executed,
    time: { created: part.time.created, ran: part.time.ran, completed: part.time.completed },
    recorded: {
      providerMetadata: part.provider?.metadata,
      providerResultMetadata: part.provider?.resultMetadata,
      pruned: part.time.pruned,
      ...(state.status === "running" ? { content: state.content } : {}),
      ...(state.status === "completed" ? { attachments: state.attachments, outputPaths: state.outputPaths } : {}),
      ...("result" in state ? { result: state.result } : {}),
    },
    state: state.status === "pending" ? { status: "streaming", input: state.input } : {
      status: state.status, input: state.input, metadata: state.structured,
      ...(state.status !== "running" ? { content: state.content } : {}),
      ...(state.status === "error" ? { error: state.error } : {}),
    },
  }
}

function attachment(file: NonNullable<Extract<Source, { type: "user" }>["files"]>[number]) {
  return Effect.try({
    try: () => {
      const comma = file.uri.indexOf(",")
      if (comma < 0) throw new Error("data URI has no payload separator")
      const header = file.uri.slice(0, comma)
      const payload = file.uri.slice(comma + 1)
      if (/%(?![0-9a-f]{2})/i.test(payload)) throw new Error("data URI has malformed percent encoding")
      const bytes = Buffer.concat((payload.match(/%[0-9a-f]{2}|[^%]+/gi) ?? []).map((chunk) =>
        chunk.startsWith("%") ? Buffer.from([Number.parseInt(chunk.slice(1), 16)]) : Buffer.from(chunk),
      ))
      const data = header.endsWith(";base64") ? bytes.toString("utf8") : bytes.toString("base64")
      if (!Schema.is(Base64)(data))
        throw new Error("data URI has malformed base64")
      return FileAttachment.create({
        data,
        mime: file.mime, source: { type: "inline" }, name: file.name, description: file.description, mention: file.source,
      })
    },
    catch: (cause) => new Error("invalid data URI attachment", { cause }),
  })
}
