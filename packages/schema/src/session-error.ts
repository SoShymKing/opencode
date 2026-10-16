export * as SessionError from "./session-error.js"

import { Schema } from "effect"
import { NonNegativeInt, optional } from "./schema.js"
import { ID } from "./session-message-id.js"

export const RejectedAttachment = Schema.Union([
  Schema.Struct({
    source: Schema.tag("user"),
    messageID: ID,
    fileIndex: NonNegativeInt,
  }),
  Schema.Struct({
    source: Schema.tag("tool"),
    messageID: ID,
    partIndex: NonNegativeInt,
    contentIndex: NonNegativeInt,
  }),
]).annotate({ identifier: "Session.RejectedAttachment" })
export type RejectedAttachment = typeof RejectedAttachment.Type

export interface Error extends Schema.Schema.Type<typeof Error> {}
export const Error = Schema.Struct({
  type: Schema.String,
  message: Schema.String,
  status: Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 599 })).pipe(optional),
  response: Schema.Struct({ body: Schema.String }).pipe(optional),
  rejectedAttachments: Schema.Array(RejectedAttachment).pipe(optional),
}).annotate({ identifier: "Session.StructuredError" })
