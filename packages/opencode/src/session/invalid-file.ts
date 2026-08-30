import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Option, Schema } from "effect"

export const metadataKey = "invalidFileQuarantine"

export type Record = {
  readonly partID: SessionV1.PartID
  readonly messageID: SessionV1.MessageID
  readonly mime: string
  readonly filename?: string
}

const options = { errors: "all", onExcessProperty: "error", propertyOrder: "original" } as const
const Envelope = Schema.Struct({ version: Schema.Literal(1), files: Schema.Array(Schema.Unknown) })
const File = Schema.Struct({
  partID: SessionV1.PartID,
  messageID: SessionV1.MessageID,
  mime: Schema.String.check(Schema.isNonEmpty()),
  filename: Schema.optional(Schema.Unknown),
})
const decodeJson = Schema.decodeUnknownOption(Schema.UnknownFromJsonString)
const decodeEnvelope = Schema.decodeUnknownOption(Envelope, options)
const decodeFile = Schema.decodeUnknownOption(File, options)

export function decode(value: string): readonly Record[] {
  const json = decodeJson(value)
  if (Option.isNone(json)) return []
  const envelope = decodeEnvelope(json.value)
  if (Option.isNone(envelope)) return []
  return envelope.value.files.flatMap((value) => {
    const file = decodeFile(value)
    if (Option.isNone(file)) return []
    const filename = typeof file.value.filename === "string" && file.value.filename.trim() ? file.value.filename : undefined
    return [
      {
        partID: file.value.partID,
        messageID: file.value.messageID,
        mime: file.value.mime,
        ...(filename === undefined ? {} : { filename }),
      },
    ]
  })
}

export function encode(files: readonly Record[]) {
  return JSON.stringify({ version: 1, files })
}

export function collect(messages: readonly SessionV1.WithParts[]): readonly Record[] {
  const files = messages
    .slice()
    .sort(order)
    .flatMap((message) => {
      if (message.info.role !== "assistant" || message.info.error?.name !== "APIError") return []
      const metadata = message.info.error.data.metadata
      if (
        metadata?.providerErrorType !== "invalid_request_error" ||
        metadata.providerErrorParam !== "input" ||
        metadata.providerErrorCode !== "invalid_file"
      )
        return []
      const value = metadata[metadataKey]
      return value ? decode(value) : []
    })
  return merge([], files)
}

export function select(
  messages: readonly SessionV1.WithParts[],
  failed: SessionV1.Assistant,
): readonly Record[] {
  const ordered = messages.slice().sort(order)
  const parent = ordered.find((message) => message.info.role === "user" && message.info.id === failed.parentID)
  if (!parent) return []
  if (failed.error?.name === "APIError" && failed.error.data.metadata?.[metadataKey]) return []
  const quarantined = new Set(collect(ordered).map((file) => file.partID))
  const boundary = ordered.findLast(
    (message) =>
      compare(message.info, failed) < 0 &&
      message.info.role === "assistant" &&
      message.info.providerID === failed.providerID &&
      message.info.modelID === failed.modelID &&
      !message.info.error &&
      !message.info.summary &&
      message.parts.some((part) => part.type === "step-finish"),
  )
  const batches = ordered.flatMap((message) => {
    if (boundary && compare(message.info, boundary.info) <= 0) return []
    if (message.info.role === "user") {
      if (compare(message.info, parent.info) > 0) return []
      return [message.parts.filter(sendable)]
    }
    if (message.info.role !== "assistant" || compare(message.info, failed) > 0) return []
    if (compare(message.info, parent.info) > 0 && message.info.parentID !== failed.parentID) return []
    return message.parts.flatMap((part) =>
      part.type === "tool" && part.state.status === "completed" ? [part.state.attachments?.filter(sendable) ?? []] : [],
    )
  })
  const batch = batches.findLast((files) => files.some((file) => !quarantined.has(file.id)))
  if (!batch) return []
  return batch.flatMap((part) => {
    if (quarantined.has(part.id)) return []
    const filename = part.filename?.trim() ? part.filename : undefined
    return [
      {
        partID: part.id,
        messageID: part.messageID,
        mime: part.mime,
        ...(filename === undefined ? {} : { filename }),
      },
    ]
  })
}

export function merge(previous: readonly Record[], selected: readonly Record[]): readonly Record[] {
  const seen = new Set<SessionV1.PartID>()
  return [...previous, ...selected].filter((file) => {
    if (seen.has(file.partID)) return false
    seen.add(file.partID)
    return true
  })
}

export function notice(selected: readonly Record[]) {
  if (selected.length === 0) return "No attachments were quarantined because no eligible attachment batch was found."
  const files = selected.map(
    (file) => `${file.partID} (${file.mime}${file.filename?.trim() ? `, ${JSON.stringify(file.filename)}` : ""})`,
  )
  return `Quarantined rejected attachments: ${files.join(", ")}.`
}

function sendable(part: SessionV1.Part): part is SessionV1.FilePart {
  return part.type === "file" && part.mime !== "text/plain" && part.mime !== "application/x-directory"
}

function order(left: SessionV1.WithParts, right: SessionV1.WithParts) {
  return compare(left.info, right.info)
}

function compare(left: SessionV1.Info, right: SessionV1.Info) {
  if (left.time.created !== right.time.created) return left.time.created - right.time.created
  return left.id < right.id ? -1 : left.id > right.id ? 1 : 0
}

export * as InvalidFile from "./invalid-file"
