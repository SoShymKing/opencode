import { parentPort } from "node:worker_threads"
import type { WsNativeBatch } from "../../src/database/ws-native-batch"

if (!parentPort) throw new Error("Missing worker parent")
parentPort.on("message", (items: readonly WsNativeBatch.Input[]) => {
  if (items[0]?.row.id === "msg_wait") return
  if (items[0]?.row.id === "msg_error") throw new Error("Fixture worker error")
  process.exit(0)
})
