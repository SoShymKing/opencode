import { expect, test } from "bun:test"
import { tmpdir } from "../../core/test/fixture/tmpdir"
import { OpenCode, Session } from "../src"

test("native SDK archives, pages and restores sessions through the embedded router", async () => {
  await using directory = await tmpdir("opencode-sdk-archive-")
  await using opencode = await OpenCode.create({
    database: { path: ":memory:" },
    config: { directory: directory.path, project: false, content: "{}" },
    models: { fetch: false },
    fs: { filewatcher: false },
  })
  const session = await opencode.sessions.create({ location: { directory: directory.path }, title: "Retained" })
  await opencode.sessions.archive({ sessionID: session.id })
  expect((await opencode.sessions.list({ directory: directory.path })).data).toEqual([])
  const archived = await opencode.sessions.list({ archived: true, updatedAfter: 0, directory: directory.path })
  expect(archived.data.map((item) => item.id)).toEqual([session.id])
  expect(archived.data[0]?.time.archived).toBeNumber()
  await opencode.sessions.restore({ sessionID: session.id })
  expect((await opencode.sessions.get({ sessionID: session.id })).time.archived).toBeUndefined()
  expect((await opencode.sessions.list({ archived: false, updatedAfter: 0 })).data.map((item) => item.id)).toEqual([session.id])
  expect(await opencode.sessions.active()).toEqual({})
  const sessionID = Session.ID.create()
  await expect(opencode.sessions.restore({ sessionID })).rejects.toMatchObject({ _tag: "SessionNotFoundError", sessionID })
})
