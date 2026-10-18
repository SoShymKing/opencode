export * as WsNativeJson from "./ws-native-json.js"

import { isDeepStrictEqual } from "node:util"
import { Schema } from "effect"
import { DatabaseLegacyV01Json } from "./legacy-v01-json.js"

const decode = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)))

export function preserve(source: string, target: Record<string, unknown>) {
  const original = DatabaseLegacyV01Json.object(source)
  const previous = decode(source)
  const next = decode(JSON.stringify(target))
  const removed = new Set(Object.keys(previous).filter((key) =>
    next[key] === undefined || !isDeepStrictEqual(previous[key], next[key])))
  const added = Object.entries(next).filter(([key, value]) => value !== undefined &&
    (!Object.hasOwn(previous, key) || removed.has(key)))
    .map(([key, value]) => ({ key, raw: JSON.stringify(value) }))
  if (removed.size === 0 && added.length === 0) return source
  return DatabaseLegacyV01Json.add(
    DatabaseLegacyV01Json.object(DatabaseLegacyV01Json.remove(original, removed)), added,
  )
}

export function children(source: string) {
  return DatabaseLegacyV01Json.objects(DatabaseLegacyV01Json.object(source), "content")?.map((part) =>
    DatabaseLegacyV01Json.remove(part, new Set(["id", "type"]))) ?? []
}
