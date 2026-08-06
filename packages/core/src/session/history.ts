import { and, asc, desc, eq, gt, gte, ne, or, sql } from "drizzle-orm"
import { Effect } from "effect"
import { Database } from "../database/database"
import { SessionMessage } from "./message"
import { decodeChild, hydrateSelection } from "./message-storage"
import { SessionSchema } from "./schema"
import { SessionContextEpochTable, SessionMessagePartTable, SessionMessageTable } from "./sql"

type DatabaseService = Database.Interface["db"]

export const latestCompaction = Effect.fnUntraced(function* (db: DatabaseService, sessionID: SessionSchema.ID) {
  return yield* db
    .select({ seq: SessionMessageTable.seq })
    .from(SessionMessageTable)
    .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "compaction")))
    .orderBy(desc(SessionMessageTable.seq))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
})

const messageRows = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  compaction: { readonly seq: number } | undefined,
  baselineSeq?: number,
) {
  return yield* hydrateSelection({
    db,
    where: and(
      eq(SessionMessageTable.session_id, sessionID),
      compaction
        ? or(
            gte(SessionMessageTable.seq, compaction.seq),
            baselineSeq === undefined
              ? undefined
              : and(eq(SessionMessageTable.type, "system"), gt(SessionMessageTable.seq, baselineSeq)),
          )
        : undefined,
      baselineSeq === undefined
        ? undefined
        : or(ne(SessionMessageTable.type, "system"), gt(SessionMessageTable.seq, baselineSeq)),
    ),
    order: "asc",
  })
})

export const load = Effect.fn("SessionHistory.load")(function* (db: DatabaseService, sessionID: SessionSchema.ID) {
  const [epoch, compaction] = yield* Effect.all(
    [
      db
        .select({ baselineSeq: SessionContextEpochTable.baseline_seq })
        .from(SessionContextEpochTable)
        .where(eq(SessionContextEpochTable.session_id, sessionID))
        .get()
        .pipe(Effect.orDie),
      latestCompaction(db, sessionID),
    ],
    { concurrency: "unbounded" },
  )
  return (yield* messageRows(db, sessionID, compaction, epoch?.baselineSeq)).map((row) => row.message)
})

export const interruptedTools = Effect.fn("SessionHistory.interruptedTools")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  const compaction = yield* latestCompaction(db, sessionID)
  const rows = yield* db
    .select({
      sessionID: SessionMessageTable.session_id,
      messageID: SessionMessageTable.id,
      position: SessionMessagePartTable.position,
      id: SessionMessagePartTable.id,
      type: SessionMessagePartTable.type,
      dataText: sql<string>`${SessionMessagePartTable.data}`,
    })
    .from(SessionMessageTable)
    .innerJoin(SessionMessagePartTable, eq(SessionMessagePartTable.message_id, SessionMessageTable.id))
    .where(
      and(
        eq(SessionMessageTable.session_id, sessionID),
        eq(SessionMessageTable.type, "assistant"),
        eq(SessionMessagePartTable.type, "tool"),
        compaction ? gte(SessionMessageTable.seq, compaction.seq) : undefined,
      ),
    )
    .orderBy(asc(SessionMessageTable.seq), asc(SessionMessagePartTable.position))
    .all()
    .pipe(Effect.orDie)
  return (yield* Effect.forEach(rows, (row) => decodeChild(row).pipe(Effect.map((tool) => ({ row, tool }))))).flatMap(
    ({ row, tool }) =>
      tool.type === "tool" && (tool.state.status === "pending" || tool.state.status === "running")
        ? [{ assistantMessageID: row.messageID, tool }]
        : [],
  )
})

export const loadForRunner = Effect.fn("SessionHistory.loadForRunner")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  baselineSeq: number,
) {
  return (yield* entriesForRunner(db, sessionID, baselineSeq)).map((entry) => entry.message)
})

export const entriesForRunner = Effect.fn("SessionHistory.entriesForRunner")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  baselineSeq: number,
) {
  const rows = yield* messageRows(db, sessionID, yield* latestCompaction(db, sessionID), baselineSeq)
  return rows.map((row) => ({ seq: row.seq, message: row.message }))
})

export * as SessionHistory from "./history"
