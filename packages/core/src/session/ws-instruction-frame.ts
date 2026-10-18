export * as WsInstructionFrame from "./ws-instruction-frame.js"

import { and, asc, eq, gt, inArray, lte, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { isDeepStrictEqual } from "node:util"
import type { Database } from "../database/database.js"
import { DatabaseWsArchive } from "../database/ws-archive.js"
import { WsMessageCodec } from "../database/ws-message-codec.js"
import { EventTable, WsImportRootTable } from "../event/sql.js"
import { Instructions } from "../instructions/index.js"
import { SessionEvent } from "./event.js"
import type { SessionSchema } from "./schema.js"
import { SessionWsImport } from "./ws-import.js"

type Frame = {
  readonly baseline: string
  readonly values: Instructions.Values
  readonly removals: Readonly<Record<string, string>>
}

export function resolve(
  db: Database.Interface["db"],
  input: { readonly sessionID: SessionSchema.ID; readonly throughSeq: number; readonly values: Instructions.Values },
): Effect.Effect<Frame> {
  return Effect.gen(function* () {
    const root = yield* db.select().from(WsImportRootTable)
      .where(eq(WsImportRootTable.aggregate_id, input.sessionID)).get().pipe(Effect.orDie)
    const rows = yield* db.select().from(EventTable).where(and(
      eq(EventTable.aggregate_id, input.sessionID),
      lte(EventTable.seq, input.throughSeq),
      root ? gt(EventTable.seq, root.through_seq) : undefined,
      inArray(EventTable.type, ["session.forked.2", "session.instructions.updated.2", "session.compaction.ended.1", "session.revert.committed.1"]),
    )).orderBy(asc(EventTable.seq)).all().pipe(Effect.orDie)
    if (rows.some((row) => row.type === "session.compaction.ended.1" || row.type === "session.revert.committed.1"))
      return yield* Effect.die(new Error("Frozen instruction capture crosses a native epoch retirement"))
    const base: Frame = yield* Effect.gen(function* () {
      if (root) {
        if (input.throughSeq < root.through_seq)
          return yield* Effect.die(new Error("Instruction capture precedes the WS import snapshot"))
        const source = yield* SessionWsImport.history(db, input.sessionID).pipe(Effect.orDie)
        const instructions = source.instructions
        if (!instructions) return yield* Effect.die(new Error("Missing archived instruction epoch"))
        const table = DatabaseWsArchive.sourceTable(source.archive, "session_message")
        if (!table) return yield* Effect.die(new Error("Missing archived session message projection"))
        const notices = yield* db.all<{ message: string }>(sql`
          SELECT json_set(data, '$.id', id, '$.type', type) AS message
          FROM ${DatabaseWsArchive.identifier(table)}
          WHERE session_id = ${input.sessionID} AND type = 'system'
            AND seq > ${instructions.baselineSeq} AND seq <= ${root.through_seq}
          ORDER BY seq`).pipe(Effect.orDie)
        const messages = yield* Effect.forEach(notices, (notice) =>
          Schema.decodeUnknownEffect(Schema.fromJsonString(WsMessageCodec.Message.cases.system))(notice.message),
        ).pipe(Effect.orDie)
        const entries = Object.entries(instructions.snapshot)
        return {
          baseline: [instructions.baseline, ...messages.map((message) => message.text)].filter((text) => text.length > 0).join("\n\n"),
          values: Object.fromEntries(entries.map(([key, entry]) => [key, Instructions.hash(entry.value)])),
          removals: Object.fromEntries(entries.flatMap(([key, entry]) => entry.removed === undefined ? [] : [[key, entry.removed]])),
        }
      }
      const ancestor = rows.find((row) => row.type === "session.forked.2")
      if (!ancestor) return yield* Effect.die(new Error("Missing retained frozen fork ancestry"))
      const fork = yield* Schema.decodeUnknownEffect(SessionEvent.Forked.data)(ancestor.data).pipe(Effect.orDie)
      if (fork.instructionSourceSeq === undefined || fork.instructions === undefined)
        return yield* Effect.die(new Error("Fork ancestry has no frozen instruction capture"))
      return yield* resolve(db, { sessionID: fork.parentID, throughSeq: fork.instructionSourceSeq, values: fork.instructions })
    })
    const frame = { ...base }
    for (const row of rows) {
      if (row.type !== "session.instructions.updated.2") continue
      const update = yield* Schema.decodeUnknownEffect(SessionEvent.InstructionsUpdated.data)(row.data).pipe(Effect.orDie)
      frame.values = Instructions.applyHashDelta(frame.values, update.delta)
      frame.removals = Object.fromEntries(Object.entries(frame.removals).filter(([key]) => !Object.hasOwn(update.delta, key)))
      if (update.text) frame.baseline = [frame.baseline, update.text].filter((text) => text.length > 0).join("\n\n")
    }
    if (!isDeepStrictEqual(frame.values, input.values))
      return yield* Effect.die(new Error("Reconstructed instruction hashes differ from fork capture"))
    return frame
  })
}
