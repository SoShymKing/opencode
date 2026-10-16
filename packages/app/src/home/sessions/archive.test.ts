import { expect, test } from "bun:test"
import { SESSION_TABS_REMOVED_EVENT, readSessionTabsRemovedDetail } from "@/shell/titlebar/session-events"
import { archiveHomeSession, restoreHomeSession } from "./archive"
import { ServerConnection } from "@/runtime/server/registry"
import { OpenCode, type SessionInfo } from "@opencode/client/promise"
import { listAllSessions } from "@/session/list"

const remote = ServerConnection.Key.make("remote")

test("archives through the reversible API, lists archived pages, and restores the same session", async () => {
  let detail: ReturnType<typeof readSessionTabsRemovedDetail>
  let remembered: SessionInfo["time"] | undefined

  const session = {
    id: "ses_1",
    projectID: "project-1",
    title: "Retained history",
    agent: "build",
    model: { id: "model-1", providerID: "provider-1" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1, updated: 2 },
    location: { directory: "/workspace" },
  } satisfies SessionInfo

  const requests: Array<{ method: string; path: string; archived: string | null; cursor: string | null }> = []

  const api = OpenCode.make({
    baseUrl: "http://archive.test",
    fetch: Object.assign(async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
      const request = new Request(input, init)
      const url = new URL(request.url)

      requests.push({
        method: request.method,
        path: url.pathname,
        archived: url.searchParams.get("archived"),
        cursor: url.searchParams.get("cursor"),
      })

      if (request.method === "POST") return new Response(null, { status: 204 })

      if (url.pathname === "/api/session/ses_1") return Response.json({ data: session })

      if (url.searchParams.has("cursor")) return Response.json({ data: [], cursor: {} })

      return Response.json({ data: [{ ...session, time: { ...session.time, archived: 3 } }], cursor: { next: "page-2" } })
    }, { preconnect: fetch.preconnect }),
  }).session

  window.addEventListener(
    SESSION_TABS_REMOVED_EVENT,
    (event) => {
      detail = readSessionTabsRemovedDetail(event)
    },
    { once: true },
  )

  await archiveHomeSession({
    server: remote,
    session,
    api,
    remember: (time) => {
      remembered = time
    },
  })

  expect(remembered?.archived).toBeNumber()
  expect(detail).toEqual({ server: remote, directory: "/workspace", sessionIDs: ["ses_1"] })
  expect((await listAllSessions(api, { archived: true, directory: "/workspace" })).map((item) => item.id)).toEqual(["ses_1"])
  expect(await restoreHomeSession(api, "ses_1")).toEqual(session)
  expect(requests).toEqual([
    { method: "POST", path: "/api/session/ses_1/archive", archived: null, cursor: null },
    { method: "GET", path: "/api/session", archived: "true", cursor: null },
    { method: "GET", path: "/api/session", archived: "true", cursor: "page-2" },
    { method: "POST", path: "/api/session/ses_1/restore", archived: null, cursor: null },
    { method: "GET", path: "/api/session/ses_1", archived: null, cursor: null },
  ])
})

test("reports archive failures without removing the session", async () => {
  const failure = new Error("offline")
  let error: unknown
  let removed = false

  await archiveHomeSession({
    server: remote,
    session: { id: "ses_1", location: { directory: "/workspace" }, time: { created: 1, updated: 1 } },
    api: { archive: async () => Promise.reject(failure) },
    remember: () => {
      removed = true
    },
    onError: (value) => {
      error = value
    },
  })

  expect(error).toBe(failure)
  expect(removed).toBe(false)
})
