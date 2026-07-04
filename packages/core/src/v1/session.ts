export * as SessionV1 from "./session"

import { ModelV2 } from "../model"
import { ProviderV2 } from "../provider"
import { NonNegativeInt } from "../schema"
import { SessionSchema } from "../session/schema"
import { NamedError } from "../util/error"
import { MessageID } from "@opencode-ai/schema/session-v1"
import { Schema } from "effect"

export {
  AgentPart,
  AgentPartInput,
  Assistant,
  CompactionPart,
  Event,
  FilePart,
  FilePartInput,
  FilePartSource,
  FileSource,
  Format,
  Info,
  MessageID,
  OutputFormatJsonSchema,
  OutputFormatText,
  Part,
  PartID,
  PatchPart,
  Range,
  ReasoningPart,
  ResourceSource,
  RetryPart,
  SessionInfo,
  SnapshotPart,
  StepFinishPart,
  StepStartPart,
  SubtaskPart,
  SubtaskPartInput,
  SymbolSource,
  TextPart,
  TextPartInput,
  ToolPart,
  ToolState,
  ToolStateCompleted,
  ToolStateError,
  ToolStatePending,
  ToolStateRunning,
  User,
  WithParts,
} from "@opencode-ai/schema/session-v1"

export const OutputLengthError = NamedError.create("MessageOutputLengthError", {})
export const AuthError = NamedError.create("ProviderAuthError", {
  providerID: Schema.String,
  message: Schema.String,
})

export const AbortSource = Schema.Literals([
  "user_cancel",
  "session_cancel",
  "provider_abort",
  "network_abort",
  "first_byte_timeout",
  "stream_idle_timeout",
  "post_tool_first_event_timeout",
  "no_visible_part_timeout",
  "server_restart",
  "client_disconnect",
  "unknown",
])
export type AbortSource = Schema.Schema.Type<typeof AbortSource>

export const RequestPhase = Schema.Literals([
  "model_stream",
  "post_tool_continuation",
  "message_finalization",
  "unknown",
])
export type RequestPhase = Schema.Schema.Type<typeof RequestPhase>

export const NoResponseDiagnostics = Schema.Struct({
  providerID: Schema.optional(ProviderV2.ID),
  modelID: Schema.optional(ModelV2.ID),
  sessionID: Schema.optional(SessionSchema.ID),
  messageID: Schema.optional(MessageID),
  elapsedMs: Schema.optional(NonNegativeInt),
  isPostToolContinuation: Schema.optional(Schema.Boolean),
  retryAttempt: Schema.optional(NonNegativeInt),
  firstStreamEventAt: Schema.optional(NonNegativeInt),
  lastStreamEventAt: Schema.optional(NonNegativeInt),
  firstVisiblePartAt: Schema.optional(NonNegativeInt),
  lastVisiblePartAt: Schema.optional(NonNegativeInt),
  partCount: Schema.optional(NonNegativeInt),
  tokenCount: Schema.optional(NonNegativeInt),
})
export type NoResponseDiagnostics = Schema.Schema.Type<typeof NoResponseDiagnostics>

const NoResponseErrorData = {
  message: Schema.String,
  abortSource: AbortSource,
  phase: RequestPhase,
  retryable: Schema.Boolean,
  diagnostics: Schema.optional(NoResponseDiagnostics),
}

export const AbortedError = NamedError.create("MessageAbortedError", {
  message: Schema.String,
  abortSource: Schema.optional(AbortSource),
})
export const UnexpectedProviderAbortError = NamedError.create("UnexpectedProviderAbortError", {
  ...NoResponseErrorData,
})
export const PostToolContinuationTimeoutError = NamedError.create("PostToolContinuationTimeoutError", {
  ...NoResponseErrorData,
})
export const EmptyAssistantResponseError = NamedError.create("EmptyAssistantResponseError", {
  ...NoResponseErrorData,
})
export const NoResponseError = NamedError.create("NoResponseError", {
  ...NoResponseErrorData,
})
export const StructuredOutputError = NamedError.create("StructuredOutputError", {
  message: Schema.String,
  retries: NonNegativeInt,
})
export const APIError = NamedError.create("APIError", {
  message: Schema.String,
  statusCode: Schema.optional(NonNegativeInt),
  isRetryable: Schema.Boolean,
  responseHeaders: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  responseBody: Schema.optional(Schema.String),
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.String)),
})
export type APIError = Schema.Schema.Type<typeof APIError.Schema>
export const ContextOverflowError = NamedError.create("ContextOverflowError", {
  message: Schema.String,
  responseBody: Schema.optional(Schema.String),
})
export const ContentFilterError = NamedError.create("ContentFilterError", {
  message: Schema.String,
})
