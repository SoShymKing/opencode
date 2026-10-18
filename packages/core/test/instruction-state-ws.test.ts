import { expect } from "bun:test"
import { Effect, Schema } from "effect"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Instructions } from "@opencode/core/instructions/index"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { InstructionState } from "@opencode/core/session/instruction-state"
import { SessionSchema } from "@opencode/core/session/schema"
import { InstructionStateTable, SessionTable } from "@opencode/core/session/sql"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(Database.node))
const sessionID = SessionSchema.ID.make("ses_ws_instructions")
const key = Instructions.Key.make("core/date")
const sources = Instructions.make({
  key,
  codec: Schema.toCodecJson(Schema.String),
  read: Effect.succeed("current"),
  render: { initial: String, changed: (_previous, current) => current },
})
const setup = Effect.gen(function* () {
  const database = yield* Database.Service
  const db = database.db
  yield* db.insert(ProjectTable).values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] }).run().pipe(Effect.orDie)
  yield* db.insert(SessionTable).values({ id: sessionID, project_id: Project.ID.global, slug: "ws", directory: "/project", version: "test" }).run().pipe(Effect.orDie)
  return db
})

for (const baseline of ["  original\n\nexact\n", ""]) {
  it.effect(`preserves exact imported baseline ${JSON.stringify(baseline)} across resume and preview`, () => Effect.gen(function* () {
    const db = yield* setup
    yield* InstructionState.importWS(db, { sessionID, baseline, baselineSeq: 7, throughSeq: 12, snapshot: { [key]: { value: "current" } } })
    expect(yield* InstructionState.initial(db, sessionID, sources)).toBe(baseline)
    expect(yield* InstructionState.preview(db, sessionID, sources, yield* Instructions.read(sources))).toEqual({ initial: baseline, update: "" })
    expect((yield* InstructionState.observe(db, sources, sessionID)).delta).toEqual({})
    expect(yield* db.select().from(InstructionStateTable).get().pipe(Effect.orDie)).toMatchObject({ epoch_start: 7, through_seq: 12, initial_values: {}, current_values: { [key]: Instructions.hash("current") } })
  }))
}

it.effect("retires imported prose only at a native epoch transition and seeds native forks with current values", () => Effect.gen(function* () {
  const db = yield* setup
  yield* InstructionState.importWS(db, { sessionID, baseline: "old", baselineSeq: 7, throughSeq: 12, snapshot: { [key]: { value: "current" } } })
  yield* InstructionState.apply(db, sessionID, 13, {})
  expect(yield* InstructionState.initial(db, sessionID, sources)).toBe("old")
  const forkID = SessionSchema.ID.make("ses_ws_fork")
  yield* db.insert(SessionTable).values({ id: forkID, project_id: Project.ID.global, slug: "fork", directory: "/project", version: "test" }).run().pipe(Effect.orDie)
  const current = yield* InstructionState.current(db, sessionID)
  if (current === undefined) throw new Error("Missing imported current values")
  yield* InstructionState.initialize(db, forkID, 0, current)
  expect(yield* InstructionState.initial(db, forkID, sources)).toBe("current")
  yield* InstructionState.advanceEpoch(db, sessionID, 14)
  expect(yield* InstructionState.initial(db, sessionID, sources)).toBe("current")
}))

it.effect("preserves unmapped values and uses saved removal rendering for absent sources", () => Effect.gen(function* () {
  const db = yield* setup
  yield* InstructionState.importWS(db, { sessionID, baseline: "old", baselineSeq: 7, throughSeq: 12, snapshot: {
    [key]: { value: "current" },
    "plugin/retired": { value: [1], removed: "retired" },
    "plugin/unknown": { value: [] },
  } })
  const observation = yield* InstructionState.observe(db, sources, sessionID)
  expect(observation.delta).toEqual({ "plugin/retired": "removed" })
  expect(observation.current["plugin/unknown"]).toBe(Instructions.hash([]))
  expect(yield* InstructionState.preview(db, sessionID, sources, yield* Instructions.read(sources))).toEqual({ initial: "old", update: "retired" })
  yield* InstructionState.apply(db, sessionID, 13, observation.delta)
  expect(yield* InstructionState.preview(db, sessionID, sources, yield* Instructions.read(sources))).toEqual({ initial: "old", update: "" })
}))

it.effect("keeps known-empty snapshot values distinct from unavailable observations", () => Effect.gen(function* () {
  const db = yield* setup
  const emptyKey = Instructions.Key.make("plugin/empty")
  yield* InstructionState.importWS(db, { sessionID, baseline: "", baselineSeq: 0, throughSeq: 0, snapshot: { [emptyKey]: { value: [] } } })
  const empty = Instructions.make({ key: emptyKey, codec: Schema.toCodecJson(Schema.Array(Schema.String)), read: Effect.succeed([]), render: { initial: JSON.stringify, changed: (_previous, current) => JSON.stringify(current) } })
  expect((yield* InstructionState.observe(db, empty, sessionID)).delta).toEqual({})
  expect(yield* InstructionState.preview(db, sessionID, empty, [{ key: emptyKey, value: Instructions.unavailable }])).toEqual({ initial: "", update: "" })
  expect(yield* InstructionState.current(db, sessionID)).toEqual({ [emptyKey]: Instructions.hash([]) })
}))
