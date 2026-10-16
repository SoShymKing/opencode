import { expect } from "bun:test"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { Effect, Schema } from "effect"
import { it } from "../../core/test/lib/effect"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { ServerFetch } from "../src/fetch"

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })
const ExportResponse = Schema.Struct({
  data: Schema.Struct({ messages: Schema.Array(Schema.toEncoded(SessionMessage.Info)) }),
})
const SessionsResponse = Schema.Struct({
  data: Schema.Array(Schema.toEncoded(Session.Info)),
  cursor: Schema.Struct({ next: Schema.optional(Schema.String), previous: Schema.optional(Schema.String) }),
})

const fixture = Effect.gen(function* () {
  const directory = yield* tmpdirScoped("opencode-server-archive-")
  const handler = yield* ServerFetch.make({
    app: { version: "test" },
    database: { path: ":memory:" },
    config: { directory: directory.path, project: false, content: "{}" },
    fs: { filewatcher: false },
    models: { fetch: false },
  })
  const request = (path: string, method = "GET", body?: unknown) =>
    Effect.promise(() => handler(new Request(`http://opencode.local${path.startsWith("/api/") ? path : `/api/session${path}`}`, {
      method,
      headers: { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    })))
  const create = Effect.fn(function* (title: string) {
    const response = yield* request("", "POST", { title, location: { directory: directory.path } })
    expect(response.status).toBe(200)
    return Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => response.json())).data
  })
  const list = Effect.fn(function* (query: string) {
    const response = yield* request(`?${query}`)
    expect(response.status).toBe(200)
    return Schema.decodeUnknownSync(SessionsResponse)(yield* Effect.promise(() => response.json()))
  })
  return { request, create, list, directory: directory.path }
})

it.live("archives without deleting and restores the same session", () =>
  Effect.gen(function* () {
    const api = yield* fixture
    const session = yield* api.create("Retained")
    const messages = [{
      id: "msg_archive_retained", type: "assistant", agent: "build",
      model: { id: "test", providerID: "test" }, time: { created: 10, completed: 20 },
      content: [{ type: "text", text: "Retained answer" }],
    }] as const
    expect((yield* api.request(`/${session.id}`, "DELETE")).status).toBe(204)
    expect((yield* api.request("/api/experimental/session/import", "POST", {
      info: session, messages,
    })).status).toBe(200)
    expect((yield* api.request(`/${session.id}/archive`, "POST")).status).toBe(204)
    expect(yield* api.list("")).toEqual({ data: [], cursor: {} })
    const archived = yield* api.list("archived=true")
    expect(archived.data.map((item) => item.id)).toEqual([session.id])
    expect(archived.data[0]?.time.archived).toBeNumber()
    expect((yield* api.request(`/${session.id}`)).status).toBe(200)
    const archivedExport = yield* api.request(`/api/experimental/session/${session.id}/export`)
    expect(archivedExport.status).toBe(200)
    expect(Schema.decodeUnknownSync(ExportResponse)(yield* Effect.promise(() => archivedExport.json())).data.messages)
      .toEqual(messages)
    expect((yield* api.request(`/${session.id}/restore`, "POST")).status).toBe(204)
    const restored = yield* api.list("archived=false")
    expect(restored.data.map((item) => item.id)).toEqual([session.id])
    expect(restored.data[0]?.time.archived).toBeUndefined()
    const restoredExport = yield* api.request(`/api/experimental/session/${session.id}/export`)
    expect(restoredExport.status).toBe(200)
    expect(Schema.decodeUnknownSync(ExportResponse)(yield* Effect.promise(() => restoredExport.json())).data.messages)
      .toEqual(messages)
    expect((yield* api.list("archived=true")).data).toEqual([])
  }).pipe(Effect.scoped),
)

it.live("preserves archive, period, directory, parent, search and order filters across cursors", () =>
  Effect.gen(function* () {
    const api = yield* fixture
    const first = yield* api.create("Archived match one")
    const second = yield* api.create("Archived match two")
    const excluded = yield* api.create("Other title")
    for (const session of [first, second, excluded])
      expect((yield* api.request(`/${session.id}/archive`, "POST")).status).toBe(204)
    yield* api.create("Archived match active")
    const query = new URLSearchParams({
      archived: "true", updatedAfter: "0", directory: api.directory,
      parentID: "null", search: "Archived match", order: "asc", limit: "1",
    })
    const page = yield* api.list(query.toString())
    expect(page.data).toHaveLength(1)
    expect(page.cursor.next).toBeString()
    const next = yield* api.list(new URLSearchParams({ cursor: page.cursor.next ?? "", limit: "1" }).toString())
    expect(next.data).toHaveLength(1)
    expect(new Set([...page.data, ...next.data].map((item) => item.id))).toEqual(new Set([first.id, second.id]))
    const previous = yield* api.list(new URLSearchParams({ cursor: next.cursor.previous ?? "", limit: "1" }).toString())
    expect(previous.data.map((item) => item.id)).toEqual(page.data.map((item) => item.id))
    expect(yield* api.list("archived=true&updatedAfter=9007199254740991")).toEqual({ data: [], cursor: {} })
  }).pipe(Effect.scoped),
)

it.live("pages mixed archives forward and backward without omissions in either order", () =>
  Effect.gen(function* () {
    const api = yield* fixture
    const excluded = yield* api.create("Other title")
    const sessions = yield* Effect.forEach([0, 1, 2, 3, 4], (index) => Effect.gen(function* () {
      const session = yield* api.create(`Mixed match ${index}`)
      if (index % 2 === 1)
        expect((yield* api.request(`/${session.id}/archive`, "POST")).status).toBe(204)
      const response = yield* api.request(`/${session.id}`)
      expect(response.status).toBe(200)
      return Schema.decodeUnknownSync(SessionResponse)(yield* Effect.promise(() => response.json())).data
    }))
    const ascending = sessions.toSorted((left, right) =>
      left.time.updated - right.time.updated || (left.id < right.id ? -1 : left.id > right.id ? 1 : 0))
    for (const order of ["asc", "desc"] as const) {
      const expected = (order === "asc" ? ascending : ascending.toReversed()).map((session) => session.id)
      const query = new URLSearchParams({
        includeArchived: "true", project: excluded.projectID, parentID: "null",
        search: "Mixed match", updatedAfter: "0", order, limit: "2",
      })
      const first = yield* api.list(query.toString())
      expect(first.data.map((session) => session.id)).toEqual(expected.slice(0, 2))
      const second = yield* api.list(new URLSearchParams({ cursor: first.cursor.next ?? "", limit: "2" }).toString())
      expect(second.data.map((session) => session.id)).toEqual(expected.slice(2, 4))
      const third = yield* api.list(new URLSearchParams({ cursor: second.cursor.next ?? "", limit: "2" }).toString())
      expect(third.data.map((session) => session.id)).toEqual(expected.slice(4))
      expect(yield* api.list(new URLSearchParams({ cursor: third.cursor.next ?? "", limit: "2" }).toString()))
        .toEqual({ data: [], cursor: {} })
      const back = yield* api.list(new URLSearchParams({ cursor: third.cursor.previous ?? "", limit: "2" }).toString())
      expect(back.data.map((session) => session.id)).toEqual(expected.slice(2, 4))
      const start = yield* api.list(new URLSearchParams({ cursor: back.cursor.previous ?? "", limit: "2" }).toString())
      expect(start.data.map((session) => session.id)).toEqual(expected.slice(0, 2))
      expect(yield* api.list(new URLSearchParams({ cursor: start.cursor.previous ?? "", limit: "2" }).toString()))
        .toEqual({ data: [], cursor: {} })
    }
  }).pipe(Effect.scoped),
)

it.live("maps missing archive and restore sessions and rejects invalid list input", () =>
  Effect.gen(function* () {
    const api = yield* fixture
    const sessionID = Session.ID.create()
    for (const operation of ["archive", "restore"]) {
      const response = yield* api.request(`/${sessionID}/${operation}`, "POST")
      expect(response.status).toBe(404)
      expect(yield* Effect.promise(() => response.json())).toMatchObject({ _tag: "SessionNotFoundError", sessionID })
    }
    for (const query of ["archived=invalid", "includeArchived=invalid", "updatedAfter=-1", "cursor=invalid"])
      expect((yield* api.request(`?${query}`)).status).toBe(400)
  }).pipe(Effect.scoped),
)
