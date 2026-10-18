export * as V1MigrationRows from "./v1-migration-rows.js"

import { Option, Schema } from "effect"
import { SessionV1 } from "@opencode/schema/session-v1"

export type SourceMessage = {
  readonly id: string
  readonly session_id: string
  readonly time_created: number
  readonly time_updated: number
  readonly data: string
}

export type SourcePart = SourceMessage & { readonly message_id: string }

export type ParsedMessage = {
  readonly row: Omit<SourceMessage, "data">
  readonly value: typeof SessionV1.Info.Encoded | undefined
}

export type ParsedPart = {
  readonly row: Omit<SourcePart, "data">
  readonly value: typeof SessionV1.Part.Encoded | undefined
  readonly observedType: string | undefined
}

const decodeJson = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown))
const decodeMessage = Schema.decodeUnknownOption(SessionV1.Info)
const encodeMessage = Schema.encodeSync(SessionV1.Info)
const decodePart = Schema.decodeUnknownOption(SessionV1.Part)
const encodePart = Schema.encodeSync(SessionV1.Part)

export function parseMessage(source: SourceMessage): ParsedMessage {
  const { data, ...row } = source
  const value = Option.getOrUndefined(decodeJson(data))
  const decoded = value && typeof value === "object"
    ? Option.getOrUndefined(decodeMessage({ ...value, id: row.id, sessionID: row.session_id }))
    : undefined
  return { row, value: decoded === undefined ? undefined : encodeMessage(decoded) }
}

export function parsePart(source: SourcePart): ParsedPart {
  const { data, ...row } = source
  const value = Option.getOrUndefined(decodeJson(data))
  const observedType = value && typeof value === "object" && "type" in value ? String(value.type) : undefined
  const decoded = value && typeof value === "object"
    ? Option.getOrUndefined(decodePart({ ...value, id: row.id, messageID: row.message_id, sessionID: row.session_id }))
    : undefined
  return { row, value: decoded === undefined ? undefined : encodePart(decoded), observedType }
}
