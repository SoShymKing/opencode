import { parentPort } from "node:worker_threads"
import type { WsNativeBatch } from "../../src/database/ws-native-batch"

if (!parentPort) throw new Error("Missing worker parent")
parentPort.on("message", (items: readonly WsNativeBatch.Job[]) => {
  const first = items[0]
  const id = first?.kind === "message" ? first.input.row.id : undefined
  if (id === "msg_wait") return
  if (id === "msg_error") throw new Error("Fixture worker error")
  process.exit(0)
})
