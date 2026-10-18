import { expect } from "bun:test"
import { Effect } from "effect"
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
const sessionID = SessionSchema.ID.make("ses_ws_capture")
const key = Instructions.Key.make("core/date")
const setup = Effect.gen(function* () {
  const database = yield* Database.Service
  const db = database.db
  yield* db.insert(ProjectTable).values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] }).run().pipe(Effect.orDie)
  yield* db.insert(SessionTable).values({ id: sessionID, project_id: Project.ID.global, slug: "capture", directory: "/project", version: "test" }).run().pipe(Effect.orDie)
  return db
})

for (const baseline of ["imported baseline", ""]) {
  for (const throughSeq of [-1, 0, 12]) {
    it.effect(`captures current hashes and source cursor ${throughSeq} with baseline ${JSON.stringify(baseline)}`, () => Effect.gen(function* () {
      const db = yield* setup
      yield* InstructionState.importWS(db, {
        sessionID, baseline, baselineSeq: -1, throughSeq,
        snapshot: { [key]: { value: "current", removed: "removed" } },
      })
      const captured = yield* db.transaction(() => InstructionState.captureFork(db, sessionID)).pipe(Effect.orDie)
      expect(captured).toEqual({ instructions: { [key]: Instructions.hash("current") }, instructionSourceSeq: throughSeq })
      expect((yield* db.select().from(InstructionStateTable).get().pipe(Effect.orDie))?.initial_values).toEqual({})
      yield* InstructionState.apply(db, sessionID, 13, { [key]: Instructions.hash("updated") })
      expect(yield* InstructionState.captureFork(db, sessionID)).toEqual({ instructions: { [key]: Instructions.hash("updated") }, instructionSourceSeq: 13 })
      yield* InstructionState.advanceEpoch(db, sessionID, 14)
      expect(yield* InstructionState.captureFork(db, sessionID)).toEqual({ instructions: { [key]: Instructions.hash("updated") } })
      expect(captured).toEqual({ instructions: { [key]: Instructions.hash("current") }, instructionSourceSeq: throughSeq })
    }))
  }
}

it.effect("omits the cursor when native state adopts the latest hashes", () => Effect.gen(function* () {
  const db = yield* setup
  yield* InstructionState.initialize(db, sessionID, 0, { [key]: Instructions.hash("initial") })
  yield* InstructionState.apply(db, sessionID, 3, { [key]: Instructions.hash("current") })
  expect(yield* InstructionState.captureFork(db, sessionID)).toEqual({ instructions: { [key]: Instructions.hash("current") } })
}))

it.effect("omits the cursor and values when no instruction state exists", () => Effect.gen(function* () {
  const db = yield* setup
  expect(yield* InstructionState.captureFork(db, sessionID)).toEqual({ instructions: undefined })
}))
