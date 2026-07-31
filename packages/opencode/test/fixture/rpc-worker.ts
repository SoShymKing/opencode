import { Rpc } from "../../src/util/rpc"

let requestType = "missing"

addEventListener("message", (event: MessageEvent<unknown>) => {
  requestType = typeof event.data
})

export const rpc = {
  fetch(input: {
    readonly url: string
    readonly method: string
    readonly headers: Record<string, string>
    readonly body?: string
  }) {
    return { url: input.url, method: input.method, status: 200, headers: input.headers, body: input.body ?? "", requestType }
  },
  regular() {
    return requestType
  },
  event(input: { readonly body: string }) {
    Rpc.emit("fixture.event", input)
  },
}

Rpc.listen(rpc)
