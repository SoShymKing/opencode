import { describe, expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import { SessionsCursor, SessionsQuery } from "../src/groups/session.js"
import { Session } from "@opencode/schema/session"
import { Project } from "@opencode/schema/project"
import { AbsolutePath, RelativePath } from "@opencode/schema/schema"

describe("SessionsCursor", () => {
  test("round trips without Node globals", async () => {
    const input = {
      search: "protocol",
      order: "desc" as const,
      anchor: { id: Session.ID.make("ses_test"), time: 1, direction: "next" as const },
    }
    const cursor = SessionsCursor.make(input)

    expect(await Effect.runPromise(SessionsCursor.parse(cursor))).toEqual(input)
  })

  test("retains archive and time filters in every cursor scope", async () => {
    for (const scope of [
      { directory: AbsolutePath.make("/project") },
      { project: Project.ID.make("project_test"), subpath: RelativePath.make("src") },
      {},
    ]) {
      for (const filter of [{ archived: true }, { archived: false }, { includeArchived: true }, { includeArchived: false }, { archived: true, includeArchived: true }]) {
        const input = {
          ...scope,
          ...filter,
          search: "match",
          parentID: null,
          order: "asc" as const,
          updatedAfter: 123,
          anchor: { id: Session.ID.make("ses_test"), time: 1, direction: "next" as const },
        }
        expect(await Effect.runPromise(SessionsCursor.parse(SessionsCursor.make(input)))).toEqual(input)
      }
    }
  })

  test("decodes archive and updatedAfter URL filters", () => {
    const decode = Schema.decodeUnknownSync(SessionsQuery)
    expect(decode({ archived: "false", updatedAfter: "0" })).toEqual({ archived: false, updatedAfter: 0 })
    expect(decode({ includeArchived: "true" })).toEqual({ includeArchived: true })
    expect(decode({ includeArchived: "false" })).toEqual({ includeArchived: false })
    expect(() => decode({ includeArchived: "yes" })).toThrow()
    expect(() => decode({ archived: "yes" })).toThrow()
    expect(() => decode({ updatedAfter: "-1" })).toThrow()
  })
})
