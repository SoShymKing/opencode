export * as WsNativeBatch from "./ws-native-batch.js"

import { Effect } from "effect"
import { WsNativeMessage } from "./ws-native-message.js"
import { WsNativePrevious } from "./ws-native-previous.js"
import { WsNativeJson } from "./ws-native-json.js"
import { encodeStoredMessage } from "../session/message-storage.js"

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

export function process(items: readonly Input[]) {
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

export type Reply = { readonly items: readonly Output[] } | { readonly messageID: string; readonly message: string }
