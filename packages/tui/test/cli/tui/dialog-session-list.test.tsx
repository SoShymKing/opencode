/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { testRender } from "@opentui/solid"
import { InputRenderable } from "@opentui/core"
import { onMount } from "solid-js"
import { DialogSessionList } from "../../../src/component/dialog-session-list"
import { DialogArchivedSessionList } from "../../../src/component/dialog-archived-session-list"
import { createStorage } from "../../../src/context/storage"
import {
  listSessionPages,
  SESSION_LIST_REQUEST_WINDOW_KEY,
  sessionListRequestStart,
  sessionListRequestWindow,
  type SessionListRequestWindow,
} from "../../../src/util/session-list-window"
import { ConfigProvider } from "../../../src/config"
import { ArgsProvider } from "../../../src/context/args"
import { ClientProvider } from "../../../src/context/client"
import { DataProvider, useData } from "../../../src/context/data"
import { Keymap } from "../../../src/context/keymap"
import { LocalProvider } from "../../../src/context/local"
import { LocationProvider } from "../../../src/context/location"
import { PermissionProvider } from "../../../src/context/permission"
import { RouteProvider, useRoute } from "../../../src/context/route"
import { TuiAppProvider } from "../../../src/context/runtime"
import { SessionTabsProvider } from "../../../src/context/session-tabs"
import { StorageProvider, useStorage } from "../../../src/context/storage"
import { ThemeProvider } from "../../../src/context/theme"
import { DialogProvider, useDialog } from "../../../src/ui/dialog"
import { ToastProvider } from "../../../src/ui/toast"
import { createApi, createEventStream, createFetch, json } from "../../fixture/tui-client"
import { emptyThemeSource, tmpdir } from "../../fixture/fixture"
import { TestTuiContexts } from "../../fixture/tui-environment"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"

test.each(["archive", "restore", "delete"] as const)("native session dialog %s", async (action) => {
  const archived = action === "restore"
  const active = "/tmp/opencode/project-b"
  const events = createEventStream()
  const requestedProjects: string[] = []
  const listQueries: URLSearchParams[] = []
  const mutations: string[] = []
  let isArchived = archived
  const info = () => ({
    id: "ses_b", projectID: "proj_b", cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1, updated: Date.now(), ...(isArchived ? { archived: Date.now() } : {}) },
    title: "Project B session", location: { directory: active },
  })
  const calls = createFetch((url, request) => {
    if (request.method === "DELETE" && url.pathname === "/api/session/ses_b") {
      mutations.push(url.pathname)
      return new Response(null, { status: 204 })
    }
    if (request.method === "POST" && /\/(archive|restore)$/.test(url.pathname)) {
      mutations.push(url.pathname)
      isArchived = url.pathname.endsWith("/archive")
      return new Response(null, { status: 204 })
    }
    if (url.pathname === "/api/session/ses_b") return json({ data: info() })
    if (url.pathname === "/api/session/ses_b/message") return json({
      data: [{ id: "msg_retained", type: "user", text: "Keep transcript", time: { created: 1 } }], cursor: {},
    })
    if (url.pathname === "/api/location") {
      const directory = url.searchParams.get("location[directory]") ?? process.cwd()
      const project = directory === active ? "proj_b" : "proj_a"
      return json({ directory, project: { id: project, directory, canonical: directory } })
    }
    if (url.pathname !== "/api/session") return undefined
    // Family syncs list children by parentID; only project-scoped list requests matter here.
    const parentID = url.searchParams.get("parentID")
    if (parentID && parentID !== "null") return json({ data: [], cursor: {} })
    listQueries.push(url.searchParams)
    const project = url.searchParams.get("project") ?? ""
    requestedProjects.push(project)
    if (archived) return json({ data: isArchived ? [info()] : [], cursor: {} })
    return json({
      data: [
        {
          id: project === "proj_b" ? "ses_b" : "ses_a",
          projectID: project,
          cost: 0,
          tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
          time: { created: 1, updated: Date.now(), ...(isArchived ? { archived: Date.now() } : {}) },
          title: project === "proj_b" ? "Project B session" : "Project A session",
          location: { directory: project === "proj_b" ? active : process.cwd() },
        },
      ],
      cursor: {},
    })
  }, events)
  const temporary = await tmpdir()
  let storage!: ReturnType<typeof useStorage>
  let cache!: ReturnType<typeof useData>

  function Probe() {
    const data = useData()
    cache = data
    const dialog = useDialog()
    const route = useRoute()
    storage = useStorage()
    onMount(async () => {
      await data.session.sync("ses_b")
      await data.session.message.sync("ses_b")
      data.session.remember({
        id: "ses_active",
        projectID: "proj_b",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        time: { created: 1, updated: 3 },
        title: "Active session",
        location: { directory: active },
      })
      route.navigate({ type: "session", sessionID: "ses_active" })
      dialog.replace(() => archived ? <DialogArchivedSessionList /> : <DialogSessionList />)
    })
    return null
  }

  const app = await testRender(
    () => (
      <TestTuiContexts paths={{ state: temporary.path }}>
        <TuiAppProvider value={{ name: "test", version: "test", channel: "test" }}>
          <StorageProvider>
            <ArgsProvider>
              <ConfigProvider config={createTuiResolvedConfig()}>
                <Keymap.Provider>
                  <ToastProvider>
                    <RouteProvider>
                      <ClientProvider api={createApi(calls.fetch)}>
                        <PermissionProvider>
                          <DataProvider directory={process.cwd()}>
                            <LocationProvider>
                              <SessionTabsProvider>
                                <ThemeProvider mode="dark" source={emptyThemeSource}>
                                  <LocalProvider>
                                    <DialogProvider>
                                      <Probe />
                                    </DialogProvider>
                                  </LocalProvider>
                                </ThemeProvider>
                              </SessionTabsProvider>
                            </LocationProvider>
                          </DataProvider>
                        </PermissionProvider>
                      </ClientProvider>
                    </RouteProvider>
                  </ToastProvider>
                </Keymap.Provider>
              </ConfigProvider>
            </ArgsProvider>
          </StorageProvider>
        </TuiAppProvider>
      </TestTuiContexts>
    ),
    { width: archived ? 44 : 100, height: 30, kittyKeyboard: true },
  )
  app.renderer.start()

  try {
    const frame = await app.waitForFrame((value) => value.includes("Project B session"))
    expect(frame).not.toContain("Project A session")
    if (!archived) expect(requestedProjects.at(-1)).toBe("proj_b")
    expect(listQueries.at(-1)!.get("archived")).toBe(String(archived))
    expect(listQueries.at(-1)!.get("parentID")).toBe("null")
    if (!archived) {
      const start = Number(listQueries.at(-1)!.get("updatedAfter"))
      expect(start).toBeGreaterThan(Date.now() - 31 * 24 * 60 * 60 * 1000)
      expect(start).toBeLessThan(Date.now() - 29 * 24 * 60 * 60 * 1000)
    }
    await app.waitForFrame(() => app.renderer.currentFocusedRenderable instanceof InputRenderable)
    if (archived) app.mockInput.pressEnter()
    if (action === "archive") app.mockInput.pressKey("x", { ctrl: true })
    if (action === "delete") {
      app.mockInput.pressKey("d", { ctrl: true })
      await app.waitForFrame((value) => value.includes("again to confirm"))
      expect(mutations).toHaveLength(0)
      app.mockInput.pressKey("d", { ctrl: true })
    }
    await app.waitForFrame(() => mutations.length === 1)
    expect(mutations).toEqual([action === "delete" ? "/api/session/ses_b" : `/api/session/ses_b/${action}`])
    if (archived) await app.waitForFrame((value) => value.includes("No archived sessions"))
    if (action !== "delete") {
      await app.waitForFrame(() => Boolean(cache.session.get("ses_b")?.time.archived) === !archived)
      expect(cache.session.message.list("ses_b").map((message) => message.id)).toEqual(["msg_retained"])
    }
  } finally {
    app.renderer.destroy()
    await storage.flush()
    await temporary[Symbol.asyncDispose]()
  }
})

test("missing or invalid session window uses thirty days", () => {
  for (const value of [undefined, null, 0, -1, 1.5, "0", "invalid", Number.MAX_SAFE_INTEGER]) {
    expect(sessionListRequestWindow(value)).toBe(30)
  }
})

test.each([30, 7, "all"] as const)("persists session window %s and retains it across opaque pages", async (window) => {
  await using temporary = await tmpdir()
  const first = createStorage(temporary.path, "test")
  const [, update] = first.storage.store<{ allProjects: boolean; session_list_request_window_days?: SessionListRequestWindow }>("session-list", { initial: { allProjects: false } })
  await update((draft) => { draft[SESSION_LIST_REQUEST_WINDOW_KEY] = window })
  await first.storage.flush()
  first.close()
  const second = createStorage(temporary.path, "test")
  try {
    const [prefs] = second.storage.store<{ allProjects: boolean; session_list_request_window_days?: SessionListRequestWindow }>("session-list", { initial: { allProjects: false } })
    expect(sessionListRequestWindow(prefs[SESSION_LIST_REQUEST_WINDOW_KEY])).toBe(window)
    const updatedAfter = sessionListRequestStart(prefs[SESSION_LIST_REQUEST_WINDOW_KEY], 2_000_000_000_000)
    const queries: URLSearchParams[] = []
    const calls = createFetch((url) => {
      queries.push(url.searchParams)
      return json({ data: [], cursor: url.searchParams.has("cursor") ? {} : { next: "opaque:next/page==" } })
    })
    await listSessionPages(createApi(calls.fetch), { archived: false, parentID: null, project: "proj_b", search: "test", updatedAfter, limit: 50 })
    expect(queries).toHaveLength(2)
    expect(queries[1]!.get("cursor")).toBe("opaque:next/page==")
    for (const query of queries) {
      expect(query.get("updatedAfter")).toBe(updatedAfter === undefined ? null : String(updatedAfter))
      expect(query.get("project")).toBe("proj_b")
      expect(query.get("search")).toBe("test")
      expect(query.get("parentID")).toBe("null")
    }
  } finally {
    second.close()
  }
})
