export * as SessionStatusEvent from "./session-status-event.js"

import { Schema } from "effect"
import { optional } from "./schema.js"
import { Event } from "./event.js"
import { NonNegativeInt } from "./schema.js"
import { SessionID } from "./session-id.js"
import { SessionMessage } from "./session-message.js"

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

export const Status = Event.ephemeral({
  type: "session.status",
  // The bare SessionStatus identifier belongs to the status union above.
  identifier: "SessionStatusUpdated",
  schema: {
    sessionID: SessionID,
    status: Info,
  },
})

// deprecated
export const Idle = Event.ephemeral({
  type: "session.idle",
  schema: {
    sessionID: SessionID,
  },
})

export const Definitions = Event.inventory(Status, Idle)
