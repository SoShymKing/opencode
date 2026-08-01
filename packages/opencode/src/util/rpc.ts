type Definition = {
  [method: string]: (input: any) => any
}

export function listen(rpc: Definition) {
  onmessage = async (evt) => {
    const object = typeof evt.data !== "string"
    const parsed = object ? evt.data : JSON.parse(evt.data)
    if (object && parsed.method !== "fetch") return
    if (parsed.type === "rpc.request") {
      const result = await rpc[parsed.method](parsed.input)
      const response = { type: "rpc.result", result, id: parsed.id }
      postMessage(object ? response : JSON.stringify(response))
    }
  }
}

export function emit(event: string, data: unknown) {
  postMessage(JSON.stringify({ type: "rpc.event", event, data }))
}

export function client<T extends Definition>(target: {
  postMessage: (
    data: string | { readonly type: "rpc.request"; readonly method: "fetch"; readonly input: unknown; readonly id: number },
  ) => void | null
  onmessage: ((this: Worker, ev: MessageEvent<any>) => any) | null
}) {
  const pending = new Map<number, (result: any) => void>()
  const listeners = new Map<string, Set<(data: any) => void>>()
  let id = 0
  target.onmessage = async (evt) => {
    const parsed = typeof evt.data === "string" ? JSON.parse(evt.data) : evt.data
    if (parsed.type === "rpc.result") {
      const resolve = pending.get(parsed.id)
      if (resolve) {
        resolve(parsed.result)
        pending.delete(parsed.id)
      }
    }
    if (parsed.type === "rpc.event") {
      const handlers = listeners.get(parsed.event)
      if (handlers) {
        for (const handler of handlers) {
          handler(parsed.data)
        }
      }
    }
  }
  return {
    call<Method extends keyof T>(method: Method, input: Parameters<T[Method]>[0]): Promise<ReturnType<T[Method]>> {
      const requestId = id++
      return new Promise((resolve) => {
        pending.set(requestId, resolve)
        target.postMessage(JSON.stringify({ type: "rpc.request", method, input, id: requestId }))
      })
    },
    callObject<Method extends Extract<keyof T, "fetch">>(
      method: Method,
      input: Parameters<T[Method]>[0],
    ): Promise<ReturnType<T[Method]>> {
      const requestId = id++
      return new Promise((resolve) => {
        pending.set(requestId, resolve)
        target.postMessage({ type: "rpc.request", method, input, id: requestId })
      })
    },
    on<Data>(event: string, handler: (data: Data) => void) {
      let handlers = listeners.get(event)
      if (!handlers) {
        handlers = new Set()
        listeners.set(event, handlers)
      }
      handlers.add(handler)
      return () => {
        handlers!.delete(handler)
      }
    },
  }
}

export * as Rpc from "./rpc"
