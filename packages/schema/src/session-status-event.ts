export * as SessionStatusEvent from "./session-status-event"

import { Schema } from "effect"
import { optional } from "./schema"
import { Event } from "./event"
import { NonNegativeInt } from "./schema"
import { SessionID } from "./session-id"
import { SessionMessage } from "./session-message"

export interface Activity extends Schema.Schema.Type<typeof Activity> {}
export const Activity = Schema.Struct({
  userMessageID: optional(SessionMessage.ID),
  model: Schema.Literals(["none", "preparing", "waiting", "receiving", "settling"]),
  streamEventCount: optional(NonNegativeInt),
  lastStreamEventAt: optional(NonNegativeInt.annotate({ description: "Unix timestamp in milliseconds" })),
}).annotate({ identifier: "SessionStatus.Activity" })

export interface Terminal extends Schema.Schema.Type<typeof Terminal> {}
export const Terminal = Schema.Struct({
  userMessageID: optional(SessionMessage.ID),
  reason: Schema.Literals(["completed", "cancelled", "error"]),
  message: optional(Schema.String),
}).annotate({ identifier: "SessionStatus.Terminal" })

export const Info = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("idle"),
    terminal: optional(Terminal),
  }),
  Schema.Struct({
    type: Schema.Literal("retry"),
    attempt: NonNegativeInt,
    message: Schema.String,
    action: optional(
      Schema.Struct({
        reason: Schema.String,
        provider: Schema.String,
        title: Schema.String,
        message: Schema.String,
        label: Schema.String,
        link: optional(Schema.String),
      }),
    ),
    next: NonNegativeInt,
    activity: optional(Activity),
  }),
  Schema.Struct({
    type: Schema.Literal("busy"),
    activity: optional(Activity),
  }),
]).annotate({ identifier: "SessionStatus" })
export type Info = Schema.Schema.Type<typeof Info>

export const Status = Event.define({
  type: "session.status",
  schema: {
    sessionID: SessionID,
    status: Info,
  },
})

// deprecated
export const Idle = Event.define({
  type: "session.idle",
  schema: {
    sessionID: SessionID,
  },
})

export const Definitions = Event.inventory(Status, Idle)
