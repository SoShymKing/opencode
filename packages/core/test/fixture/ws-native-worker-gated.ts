import { parentPort } from "node:worker_threads"
import { Effect } from "effect"
import { WsNativeBatch } from "../../src/database/ws-native-batch"

const port = parentPort
if (!port) throw new Error("Worker requires parent port")
let release: (() => void) | undefined
let released = false
port.on("message", async (message: readonly WsNativeBatch.Job[] | "release") => {
  if (message === "release") {
    released = true
    return release?.()
  }
  if (!released && message.some((job) => WsNativeBatch.jobID(job) === "msg_gate"))
    await new Promise<void>((resolve) => { release = resolve })
  const result = await Effect.runPromise(WsNativeBatch.run(message).pipe(Effect.result))
  port.postMessage(result._tag === "Success" ? { items: result.success }
    : { messageID: result.failure.messageID, message: result.failure.message })
})
