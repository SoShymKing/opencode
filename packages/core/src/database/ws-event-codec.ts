export * as WsEventCodec from "./ws-event-codec.js"

import { Schema } from "effect"
import { SessionV1 } from "@opencode/schema/v1/session"
import { DateTimeUtcFromMillis, NonNegativeInt, RelativePath, optional } from "@opencode/schema/schema"
import { Model } from "@opencode/schema/model"
import { Location } from "@opencode/schema/location"
import { WsMessageCodec } from "./ws-message-codec.js"

const messageID = WsMessageCodec.Message.cases.user.fields.id
const base = { sessionID: Schema.String, timestamp: DateTimeUtcFromMillis }
const message = { ...base, messageID }
const assistant = { ...base, assistantMessageID: messageID }
const tool = { ...assistant, callID: Schema.String }
const metadata = Schema.Record(Schema.String, Schema.Record(Schema.String, Schema.Unknown))
const provider = Schema.Struct({ executed: Schema.Boolean, metadata: optional(metadata) })
const error = Schema.Struct({ type: Schema.Literal("unknown"), message: Schema.String })
const content = Schema.Array(Schema.Union([
  Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("file"), uri: Schema.String, mime: Schema.String, name: optional(Schema.String) }),
]))
const structured = Schema.Record(Schema.String, Schema.Unknown)
const prompt = { ...message, prompt: Schema.Struct({ text: Schema.String,
  files: WsMessageCodec.Message.cases.user.fields.files, agents: WsMessageCodec.Message.cases.user.fields.agents }),
  delivery: Schema.Literals(["steer", "queue"]) }
const reason = Schema.Literals(["auto", "manual"])
const revert = Schema.Struct({ messageID, partID: optional(Schema.String), snapshot: optional(Schema.String), diff: optional(Schema.String),
  files: optional(Schema.Array(Schema.Struct({ path: RelativePath, status: Schema.Literals(["added", "modified", "deleted"]),
    additions: NonNegativeInt, deletions: NonNegativeInt, patch: Schema.String }))) })
const info = Schema.Struct({ ...SessionV1.SessionInfo.fields, title: Schema.String })
const event = <const Type extends string, Fields extends Schema.Struct.Fields>(type: Type, fields: Fields) =>
  Schema.Struct({ type: Schema.Literal(type), data: Schema.Struct(fields) })

export const Event = Schema.Union([
  event("session.created.1", { sessionID: Schema.String, info }),
  event("session.updated.1", { sessionID: Schema.String, info }),
  event("session.deleted.1", { sessionID: Schema.String, info }),
  event("message.updated.1", SessionV1.Event.MessageUpdated.data.fields),
  event("message.removed.1", SessionV1.Event.MessageRemoved.data.fields),
  event("message.part.updated.1", SessionV1.Event.PartUpdated.data.fields),
  event("message.part.removed.1", SessionV1.Event.PartRemoved.data.fields),
  event("session.next.agent.switched.1", { ...message, agent: Schema.String }),
  event("session.next.model.switched.1", { ...message, model: Model.Ref }),
  event("session.next.moved.1", { ...base, location: Location.Ref, subdirectory: optional(RelativePath) }),
  event("session.next.prompt.admitted.1", prompt),
  event("session.next.prompted.1", prompt),
  event("session.next.context.updated.1", { ...message, text: Schema.String }),
  event("session.next.synthetic.1", { ...message, text: Schema.String }),
  event("session.next.shell.started.1", { ...message, callID: Schema.String, command: Schema.String }),
  event("session.next.shell.ended.1", { ...base, callID: Schema.String, output: Schema.String }),
  event("session.next.step.started.1", { ...assistant, agent: Schema.String, model: Model.Ref,
    snapshot: optional(Schema.String), streamEventCount: optional(NonNegativeInt) }),
  event("session.next.step.stream.updated.1", { ...assistant, streamEventCount: NonNegativeInt }),
  event("session.next.step.ended.2", { ...assistant, finish: Schema.String, cost: Schema.Finite,
    tokens: SessionV1.StepFinishPart.fields.tokens, snapshot: optional(Schema.String), files: optional(Schema.Array(RelativePath)) }),
  event("session.next.step.failed.2", { ...assistant, error }),
  event("session.next.text.started.1", { ...assistant, textID: Schema.String }),
  event("session.next.text.ended.1", { ...assistant, textID: Schema.String, text: Schema.String }),
  event("session.next.reasoning.started.1", { ...assistant, reasoningID: Schema.String, providerMetadata: optional(metadata) }),
  event("session.next.reasoning.ended.1", { ...assistant, reasoningID: Schema.String, text: Schema.String, providerMetadata: optional(metadata) }),
  event("session.next.tool.input.started.1", { ...tool, name: Schema.String }),
  event("session.next.tool.input.ended.1", { ...tool, text: Schema.String }),
  event("session.next.tool.called.1", { ...tool, tool: Schema.String, input: structured, provider }),
  event("session.next.tool.progress.1", { ...tool, structured, content }),
  event("session.next.tool.success.1", { ...tool, structured, content, outputPaths: optional(Schema.Array(Schema.String)), result: optional(Schema.Unknown), provider }),
  event("session.next.tool.failed.1", { ...tool, error, result: optional(Schema.Unknown), provider }),
  event("session.next.retried.1", { ...base, attempt: Schema.Finite, error: Schema.Struct({ message: Schema.String,
    statusCode: optional(Schema.Finite), isRetryable: Schema.Boolean,
    responseHeaders: optional(Schema.Record(Schema.String, Schema.String)), responseBody: optional(Schema.String),
    metadata: optional(Schema.Record(Schema.String, Schema.String)) }) }),
  event("session.next.compaction.started.1", { ...message, reason }),
  event("session.next.compaction.ended.1", { ...message, reason, text: Schema.String, recent: Schema.String }),
  event("session.next.revert.staged.1", { ...base, revert }),
  event("session.next.revert.cleared.1", base),
  event("session.next.revert.committed.1", message),
]).pipe(Schema.toTaggedUnion("type"))
export type Event = typeof Event.Type

export const Row = Schema.Struct({ id: Schema.String, aggregate_id: Schema.String, seq: NonNegativeInt,
  type: Schema.String, data: Schema.String })
export type Row = typeof Row.Type

export function decode(input: unknown) {
  const row = Schema.decodeUnknownSync(Row)(input)
  const data = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(row.data)
  const value = Schema.decodeUnknownSync(Event)({ type: row.type, data }, { onExcessProperty: "error" })
  if (value.data.sessionID !== row.aggregate_id) throw new SourceEventError("Source aggregate mismatch")
  if ("info" in value.data && "sessionID" in value.data.info && value.data.info.sessionID !== row.aggregate_id)
    throw new SourceEventError("Source message aggregate mismatch")
  if ("part" in value.data && value.data.part.sessionID !== row.aggregate_id)
    throw new SourceEventError("Source part aggregate mismatch")
  if ("info" in value.data && "projectID" in value.data.info && value.data.info.id !== row.aggregate_id)
    throw new SourceEventError("Source session identity mismatch")
  return { row, event: value }
}

export class SourceEventError extends Error {
  override readonly name = "WsSourceEventError"
}
