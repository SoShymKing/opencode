export * as WsMessageCodec from "./ws-message-codec.js"

import { Schema } from "effect"
import { DateTimeUtcFromMillis, NonNegativeInt, RelativePath, optional } from "@opencode/schema/schema"
import { Model } from "@opencode/schema/model"

const metadata = Schema.Record(Schema.String, Schema.Record(Schema.String, Schema.Unknown))
const source = Schema.Struct({ start: Schema.Finite, end: Schema.Finite, text: Schema.String })
const file = Schema.Struct({
  uri: Schema.String, mime: Schema.String, name: optional(Schema.String),
  description: optional(Schema.String), source: optional(source),
})
const toolContent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("file"), uri: Schema.String, mime: Schema.String, name: optional(Schema.String) }),
]).pipe(Schema.toTaggedUnion("type"))
const error = Schema.Struct({ type: Schema.Literal("unknown"), message: Schema.String })
const base = {
  id: Schema.String.check(Schema.isStartingWith("msg_")),
  metadata: optional(Schema.Record(Schema.String, Schema.Unknown)),
  time: Schema.Struct({ created: DateTimeUtcFromMillis }),
}
const toolBase = {
  input: Schema.Record(Schema.String, Schema.Unknown),
  structured: Schema.Record(Schema.String, Schema.Unknown),
  content: Schema.Array(toolContent),
}
const toolState = Schema.Union([
  Schema.Struct({ status: Schema.Literal("pending"), input: Schema.String }),
  Schema.Struct({ ...toolBase, status: Schema.Literal("running") }),
  Schema.Struct({ ...toolBase, status: Schema.Literal("completed"), attachments: optional(Schema.Array(file)),
    outputPaths: optional(Schema.Array(Schema.String)), result: optional(Schema.Unknown) }),
  Schema.Struct({ ...toolBase, status: Schema.Literal("error"), error, result: optional(Schema.Unknown) }),
]).pipe(Schema.toTaggedUnion("status"))
const content = Schema.Union([
  Schema.Struct({ type: Schema.Literal("text"), id: Schema.String, text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("reasoning"), id: Schema.String, text: Schema.String,
    providerMetadata: optional(metadata), time: optional(Schema.Struct({
      created: DateTimeUtcFromMillis, completed: optional(DateTimeUtcFromMillis),
    })) }),
  Schema.Struct({ type: Schema.Literal("tool"), id: Schema.String, name: Schema.String,
    provider: optional(Schema.Struct({ executed: Schema.Boolean, metadata: optional(metadata), resultMetadata: optional(metadata) })),
    state: toolState, time: Schema.Struct({ created: DateTimeUtcFromMillis,
      ran: optional(DateTimeUtcFromMillis), completed: optional(DateTimeUtcFromMillis), pruned: optional(DateTimeUtcFromMillis) }) }),
]).pipe(Schema.toTaggedUnion("type"))

export const Message = Schema.Union([
  Schema.Struct({ ...base, type: Schema.Literal("agent-switched"), agent: Schema.String }),
  Schema.Struct({ ...base, type: Schema.Literal("model-switched"), model: Model.Ref }),
  Schema.Struct({ ...base, type: Schema.Literal("user"), text: Schema.String, files: optional(Schema.Array(file)),
    agents: optional(Schema.Array(Schema.Struct({ name: Schema.String, source: optional(source) }))) }),
  Schema.Struct({ ...base, type: Schema.Literal("synthetic"), sessionID: Schema.String.check(Schema.isStartingWith("ses_")), text: Schema.String }),
  Schema.Struct({ ...base, type: Schema.Literal("system"), text: Schema.String }),
  Schema.Struct({ ...base, type: Schema.Literal("shell"), callID: Schema.String, command: Schema.String, output: Schema.String,
    time: Schema.Struct({ created: DateTimeUtcFromMillis, completed: optional(DateTimeUtcFromMillis) }) }),
  Schema.Struct({ ...base, type: Schema.Literal("assistant"), agent: Schema.String, model: Model.Ref,
    content: Schema.Array(content), snapshot: optional(Schema.Struct({ start: optional(Schema.String), end: optional(Schema.String), files: optional(Schema.Array(RelativePath)) })),
    finish: optional(Schema.String), cost: optional(Schema.Finite), streamEventCount: optional(NonNegativeInt),
    tokens: optional(Schema.Struct({ input: Schema.Finite, output: Schema.Finite, reasoning: Schema.Finite,
      cache: Schema.Struct({ read: Schema.Finite, write: Schema.Finite }) })), error: optional(error),
    time: Schema.Struct({ created: DateTimeUtcFromMillis, completed: optional(DateTimeUtcFromMillis) }) }),
  Schema.Struct({ ...base, type: Schema.Literal("compaction"), reason: Schema.Literals(["auto", "manual"]), summary: Schema.String, recent: Schema.String }),
]).pipe(Schema.toTaggedUnion("type"))
