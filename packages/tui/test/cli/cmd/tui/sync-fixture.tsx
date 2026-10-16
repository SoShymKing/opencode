/** @jsxImportSource @opentui/solid */
import type { OpenCodeEvent } from "@opencode/client"
import { testRender, type JSX } from "@opentui/solid"
import { createEffect, createRoot, onCleanup } from "solid-js"
import { ClientProvider, useClient } from "../../../../src/context/client"
import { DataProvider, useData } from "../../../../src/context/data"
import { createApi, createEventStream, createFetch, directory, type FetchHandler } from "../../../fixture/tui-client"
import { TestTuiContexts } from "../../../fixture/tui-environment"

export { createApi, createEventStream, createFetch, directory, json, worktree } from "../../../fixture/tui-client"

export function wait(predicate: () => boolean) {
  return new Promise<void>((resolve, reject) => {
    createRoot((dispose) => {
      const deadline = setTimeout(() => {
        dispose()
        reject(new Error("Timed out waiting for native TUI state"))
      }, 4000)
      onCleanup(() => clearTimeout(deadline))
      createEffect(() => {
        if (!predicate()) return
        dispose()
        resolve()
      })
    })
  })
}

export async function mount(override?: FetchHandler, view?: () => JSX.Element) {
  const events = createEventStream()
  const calls = createFetch(override, events)
  const mounted = Promise.withResolvers<{ data: ReturnType<typeof useData>; client: ReturnType<typeof useClient> }>()
  function Probe() {
    mounted.resolve({ data: useData(), client: useClient() })
    return <box>{view?.()}</box>
  }
  const app = await testRender(
    () => (
      <TestTuiContexts>
        <ClientProvider api={createApi(calls.fetch)}>
          <DataProvider directory={directory}>
            <Probe />
          </DataProvider>
        </ClientProvider>
      </TestTuiContexts>
    ),
    { width: 100, height: 20, kittyKeyboard: true },
  )
  const context = await mounted.promise
  try {
    await wait(() => context.client.connection.status() === "connected")
  } catch (error) {
    app.renderer.destroy()
    throw error
  }
  return {
    app,
    ...context,
    async emit(event: OpenCodeEvent) {
      const received = Promise.withResolvers<void>()
      const off = context.client.event.listen(({ details }) => {
        if (details.type === event.type && details.id === event.id) received.resolve()
      })
      try {
        events.emit(event)
        await received.promise
      } finally {
        off()
      }
    },
    async frame() {
      await app.renderOnce()
      await app.renderOnce()
      return app.captureCharFrame()
    },
    [Symbol.dispose]() {
      app.renderer.destroy()
    },
  }
}
