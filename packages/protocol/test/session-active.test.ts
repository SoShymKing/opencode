import { expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionActive } from "../src/groups/session"

test("SessionActive preserves running without status and includes exact optional status", () => {
  const decode = Schema.decodeUnknownSync(SessionActive)
  expect(decode({ type: "running" })).toEqual({ type: "running" })
  const active = {
    type: "running",
    status: { type: "busy", activity: { model: "receiving", streamEventCount: 200, lastStreamEventAt: 123 } },
  } as const
  expect(decode(active)).toEqual(active)
  expect(decode({ type: "running", status: { type: "busy" } })).toEqual({ type: "running", status: { type: "busy" } })
})
