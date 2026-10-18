import { Effect } from "effect"
import { WsNativeBatch } from "./ws-native-batch.js"

export const open = Effect.succeed({ process: WsNativeBatch.process })
