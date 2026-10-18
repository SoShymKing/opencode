export * as WsNativeBatch from "./ws-native-batch.js"

import { Effect, Schema } from "effect"
import { WsNativeMessage } from "./ws-native-message.js"
import { WsNativePrevious } from "./ws-native-previous.js"
import { WsNativeJson } from "./ws-native-json.js"
import { decodeStoredMessage, encodeStoredMessage, type EnvelopeRow, type ChildRow } from "../session/message-storage.js"
import { SessionMessage } from "@opencode/schema/session-message"
import { WsEventCodec } from "./ws-event-codec.js"
import { V1MigrationRows } from "./v1-migration-rows.js"
import { normalizeMessage } from "./native-message-parts.js"

const decodeLegacyJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json)))

export type Row = {
  readonly id: string
  readonly session_id: string
  readonly type: string
  readonly seq: number
  readonly time_created: number
  readonly time_updated: number
  readonly data: string
}
export type Input = {
  readonly row: Row
  readonly parts: readonly { readonly position: number; readonly id: string; readonly type: string; readonly data: string }[] | undefined
  readonly previousNative: boolean
  readonly store: boolean
}
export type Output = {
  readonly type: string
  readonly data: string
  readonly parts: readonly { readonly position: number; readonly id: string; readonly type: string; readonly data: string }[]
} | undefined

function messages(items: readonly Input[]) {
  return Effect.forEach(items, (item) => Effect.gen(function* () {
    const message = item.previousNative ? yield* WsNativePrevious.message(item.row)
      : (yield* WsNativeMessage.convert({ id: item.row.id, sessionID: item.row.session_id,
        type: item.row.type, data: item.row.data, parts: item.parts })).message
    const originals = yield* Effect.try(() => item.parts?.map((part) => part.data) ?? WsNativeJson.children(item.row.data))
    if (!item.store) return undefined
    const stored = yield* encodeStoredMessage(message)
    return yield* Effect.try(() => ({
      type: stored.envelope.type,
      data: WsNativeJson.preserve(item.row.data, stored.envelope.data),
      parts: stored.parts.map((part) => ({
        position: part.position, id: part.id, type: part.type,
        data: originals[part.position] === undefined ? JSON.stringify(part.data)
          : WsNativeJson.preserve(originals[part.position], part.data),
      })),
    }))
  }).pipe(Effect.mapError(() => new WsNativeMessage.WsNativeMessageError({
    messageID: item.row.id, message: `Invalid source message ${item.row.id}`,
  }))))
}

export type Job =
  | { readonly kind: "message"; readonly input: Input }
  | { readonly kind: "ws-event"; readonly row: WsEventCodec.Row }
  | { readonly kind: "p-event"; readonly row: WsEventCodec.Row & { created: number } }
  | { readonly kind: "native-validate"; readonly parent: EnvelopeRow; readonly children: readonly Omit<ChildRow, "sessionID">[] }
  | { readonly kind: "native-encode"; readonly row: { id: string; type: string; data: Record<string, unknown> } }
  | { readonly kind: "native-normalize"; readonly row: { id: string; data: string } }
  | { readonly kind: "legacy-message"; readonly mode: "validate" | "prepare"; readonly row: V1MigrationRows.SourceMessage }
  | { readonly kind: "legacy-part"; readonly mode: "validate" | "prepare"; readonly row: V1MigrationRows.SourcePart }

export type Result =
  | { readonly kind: "message"; readonly output: Output }
  | { readonly kind: "ws-event" | "p-event" | "native-validate" }
  | { readonly kind: "native-encode" | "native-normalize"; readonly output: NonNullable<Output> }
  | { readonly kind: "legacy-message" | "legacy-part"; readonly mode: "validate" }
  | { readonly kind: "legacy-message"; readonly mode: "prepare"; readonly parsed: V1MigrationRows.ParsedMessage }
  | { readonly kind: "legacy-part"; readonly mode: "prepare"; readonly parsed: V1MigrationRows.ParsedPart }

export function jobID(job: Job) {
  switch (job.kind) {
    case "message": return job.input.row.id
    case "native-validate": return job.parent.messageID
    default: return job.row.id
  }
}

export function run(jobs: readonly Job[]): Effect.Effect<readonly Result[], WsNativeMessage.WsNativeMessageError> {
  return Effect.forEach(jobs, (job) => Effect.gen(function* () {
    switch (job.kind) {
      case "message": return { kind: job.kind, output: (yield* messages([job.input]))[0] } as const
      case "ws-event":
        yield* Effect.try(() => WsEventCodec.decode(job.row))
        return { kind: job.kind } as const
      case "p-event":
        yield* WsNativePrevious.validateEvent(job.row)
        return { kind: job.kind } as const
      case "native-validate":
        yield* decodeStoredMessage(job.parent, job.children)
        return { kind: job.kind } as const
      case "native-encode": {
        const message = yield* Schema.decodeUnknownEffect(SessionMessage.Info)({ ...job.row.data, id: job.row.id, type: job.row.type })
        const stored = yield* encodeStoredMessage(message)
        return { kind: job.kind, output: { type: stored.envelope.type, data: JSON.stringify(stored.envelope.data),
          parts: stored.parts.map((part) => ({ position: part.position, id: part.id, type: part.type, data: JSON.stringify(part.data) })) } } as const
      }
      case "native-normalize": return { kind: job.kind, output: yield* normalizeMessage(job.row) } as const
      case "legacy-message": {
        if (job.mode === "validate") yield* decodeLegacyJson(job.row.data)
        const parsed = yield* Effect.try(() => V1MigrationRows.parseMessage(job.row))
        if (job.mode === "prepare") return { kind: job.kind, mode: job.mode, parsed } as const
        if (parsed.value === undefined) return yield* Effect.fail(new Error("Invalid legacy message"))
        return { kind: job.kind, mode: job.mode } as const
      }
      case "legacy-part": {
        if (job.mode === "validate") yield* decodeLegacyJson(job.row.data)
        const parsed = yield* Effect.try(() => V1MigrationRows.parsePart(job.row))
        if (job.mode === "prepare") return { kind: job.kind, mode: job.mode, parsed } as const
        if (parsed.value === undefined) return yield* Effect.fail(new Error("Invalid legacy part"))
        return { kind: job.kind, mode: job.mode } as const
      }
    }
  }).pipe(Effect.mapError((cause) => cause instanceof WsNativeMessage.WsNativeMessageError ? cause
    : new WsNativeMessage.WsNativeMessageError({ messageID: jobID(job), message: `Invalid ${job.kind} ${jobID(job)}` }))))
}

export function process(items: readonly Input[]) {
  return run(items.map((input) => ({ kind: "message", input }))).pipe(Effect.map(messageOutputs))
}

export function messageOutputs(results: readonly Result[]) {
  return results.map((result) => {
    if (result.kind !== "message") throw new Error("Expected message result")
    return result.output
  })
}

export type Reply = { readonly items: readonly Result[] } | { readonly messageID: string; readonly message: string }
