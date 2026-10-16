export * as AttachmentQuarantine from "./attachment-quarantine.js"

import type { MediaPart, Message } from "@opencode/ai"
import { SessionError } from "@opencode/schema/session-error"
import { Schema } from "effect"
import type { SessionMessage } from "./message.js"

export type Ref = SessionError.RejectedAttachment
export type Origin = { readonly value: object; readonly snapshot: string; readonly ref: Ref; readonly wrapper?: string }
export const mediaWrapper = (part: MediaPart) => JSON.stringify({
  filename: part.filename, cache: part.cache, metadata: part.metadata, providerMetadata: part.providerMetadata,
})
export type Batch =
  | { readonly coverage: "complete"; readonly refs: readonly Ref[] }
  | { readonly coverage: "unknown" }

export class Unavailable extends Schema.TaggedError<Unavailable>()("Session.AttachmentQuarantineUnavailable", {
  message: Schema.String,
}) {}

export const key = (ref: Ref) => JSON.stringify(ref.source === "user"
  ? [ref.source, ref.messageID, ref.fileIndex]
  : [ref.source, ref.messageID, ref.partIndex, ref.contentIndex])
export const unique = (refs: readonly Ref[]) => [...new Map(refs.map((ref) => [key(ref), ref])).values()]

export function collect(messages: readonly SessionMessage.Info[]) {
  return unique(messages.flatMap((message) =>
    (message.type === "assistant" || (message.type === "compaction" && message.status === "failed")) &&
      message.error?.type === "provider.invalid-file"
      ? message.error.rejectedAttachments ?? []
      : [],
  ))
}

export function exists(ref: Ref, messages: ReadonlyMap<SessionMessage.ID, SessionMessage.Info>) {
  const message = messages.get(ref.messageID)
  if (ref.source === "user") {
    const file = message?.type === "user" ? message.files?.[ref.fileIndex] : undefined
    return file !== undefined && file.mime !== "text/plain" && file.mime !== "application/x-directory"
  }
  const part = message?.type === "assistant" ? message.content[ref.partIndex] : undefined
  return part?.type === "tool" && (part.state.status === "completed" || part.state.status === "error") &&
    part.state.content?.[ref.contentIndex]?.type === "file"
}

export function batch(messages: readonly Message[], origins: readonly Origin[] = [], unknown = false): Batch {
  const refs: Ref[] = []
  const byValue = new Map(origins.map((origin) => [origin.value, origin]))
  for (const message of messages) {
    if (message.native !== undefined) unknown = true
    for (const part of message.content) {
      if (part.type === "compaction") unknown = true
      const values = part.type === "media" ? [part.media]
        : part.type === "tool-result" && part.result.type === "content"
          ? part.result.value.filter((item) => item.type === "file")
          : []
      if (values.length > 0 && (part.type === "tool-result" || part.type === "media") && part.providerMetadata !== undefined)
        unknown = true
      for (const value of values) {
        const origin = byValue.get(value)
        if (!origin || origin.snapshot !== JSON.stringify(value) ||
          (part.type === "media" && origin.wrapper !== mediaWrapper(part))) unknown = true
        else refs.push(origin.ref)
      }
    }
  }
  return unknown ? { coverage: "unknown" } : { coverage: "complete", refs: unique(refs) }
}

export function failure(error: SessionError.Error, submitted: Batch): SessionError.Error {
  return error.type === "provider.invalid-file" && submitted.coverage === "complete" && submitted.refs.length > 0
    ? { ...error, rejectedAttachments: submitted.refs }
    : error
}

export function afterReplacement(before: readonly Message[], after: readonly Message[], origins: readonly Origin[] = []) {
  const byValue = new Map(origins.map((origin) => [origin.value, origin]))
  const carried: Origin[] = []
  const retain = (previous: object, value: object) => {
    const origin = byValue.get(previous)
    if (origin && origin.snapshot === JSON.stringify(previous) && origin.snapshot === JSON.stringify(value))
      carried.push({ ...origin, value })
  }
  before.forEach((message, messageIndex) => message.content.forEach((part, partIndex) => {
    const next = after[messageIndex]?.content[partIndex]
    if (part.type === "media" && next?.type === "media") {
      const origin = byValue.get(part.media)
      if (origin?.wrapper === mediaWrapper(part) && origin.wrapper === mediaWrapper(next)) retain(part.media, next.media)
    }
    if (part.type !== "tool-result" || part.result.type !== "content" || next?.type !== "tool-result" || next.result.type !== "content") return
    const content = next.result.value
    part.result.value.forEach((item, index) => {
      const replacement = content[index]
      if (item.type === "file" && replacement?.type === "file") retain(item, replacement)
    })
  }))
  return carried
}
