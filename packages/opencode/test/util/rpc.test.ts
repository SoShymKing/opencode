import { describe, expect, test } from "bun:test"
import { Rpc } from "../../src/util/rpc"
import type { rpc } from "../fixture/rpc-worker"

const workerUrl = new URL("../fixture/rpc-worker.ts", import.meta.url)

function nextMessage(worker: Worker) {
  return new Promise<unknown>((resolve) => {
    worker.addEventListener("message", (event: MessageEvent<unknown>) => resolve(event.data), { once: true })
  })
}

describe("util.rpc", () => {
  test("uses structured clone for fetch requests and results", async () => {
    // Given
    const worker = new Worker(workerUrl)
    const client = Rpc.client<typeof rpc>(worker)

    try {
      // When
      const rawResult = nextMessage(worker)
      const pending = client.callObject("fetch", {
        url: "http://opencode.internal/test",
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "request body",
      })
      const result = await pending

      // Then
      expect(result).toEqual({
        url: "http://opencode.internal/test",
        method: "POST",
        status: 200,
        headers: { "content-type": "text/plain" },
        body: "request body",
        requestType: "object",
      })
      expect(typeof (await rawResult)).toBe("object")
    } finally {
      worker.terminate()
    }
  })

  test("keeps regular call requests and results as JSON strings", async () => {
    // Given
    const worker = new Worker(workerUrl)
    const client = Rpc.client<typeof rpc>(worker)
    const rawResult = nextMessage(worker)

    try {
      // When
      const requestType = await client.call("regular", undefined)

      // Then
      expect(requestType).toBe("string")
      expect(typeof (await rawResult)).toBe("string")
    } finally {
      worker.terminate()
    }
  })

  test("keeps emitted events as JSON strings", async () => {
    // Given
    const worker = new Worker(workerUrl)
    const client = Rpc.client<typeof rpc>(worker)
    const rawEvent = nextMessage(worker)
    const event = new Promise<{ readonly body: string }>((resolve) => {
      client.on("fixture.event", resolve)
    })

    try {
      // When
      await client.call("event", { body: "event body" })

      // Then
      expect(await event).toEqual({ body: "event body" })
      expect(typeof (await rawEvent)).toBe("string")
    } finally {
      worker.terminate()
    }
  })
})
