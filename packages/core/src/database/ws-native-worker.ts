import { parentPort } from "node:worker_threads"
import { Effect } from "effect"
import { WsNativeBatch } from "./ws-native-batch.js"

if (!parentPort) throw new Error("WS converter requires a worker parent")
const port = parentPort
port.on("message", (items: readonly WsNativeBatch.Job[]) => {
  const result = Effect.runSync(WsNativeBatch.run(items).pipe(Effect.result))
  const reply: WsNativeBatch.Reply = result._tag === "Success" ? { items: result.success }
    : { messageID: result.failure.messageID, message: result.failure.message }
  port.postMessage(reply)
})
