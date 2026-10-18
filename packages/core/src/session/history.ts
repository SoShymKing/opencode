import { and, asc, desc, eq, gt, gte, inArray, isNotNull, ne, or, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { SessionError } from "@opencode/schema/session-error"
import { AttachmentQuarantine } from "./attachment-quarantine.js"
import { Database } from "../database/database.js"
import { MessageDecodeError } from "./error.js"
import { SessionMessage } from "./message.js"
import { decodeChild, hydrateSelection } from "./message-storage.js"
import { SessionSchema } from "./schema.js"
import { Instructions } from "../instructions/index.js"
import { InstructionState } from "./instruction-state.js"
import { SessionProviderContext } from "./provider-context.js"
import { InstructionStateTable, SessionMessagePartTable, SessionMessageTable } from "./sql.js"

type DatabaseService = Database.Interface["db"]

export const hydrateMessages = Effect.fnUntraced(function* (input: Parameters<typeof hydrateSelection>[0]) {
  const entries = yield* hydrateSelection(input)
  yield* Effect.forEach(entries, (entry) =>
    SessionProviderContext.isCheckpoint(entry.message)
      ? SessionProviderContext.validate(entry.message.providerContext).pipe(Effect.mapError(() =>
          new MessageDecodeError({ sessionID: entry.sessionID, messageID: entry.message.id }),
        ))
      : Effect.void,
    { discard: true },
  )
  return entries
})

export const latestUserID = Effect.fnUntraced(function* (db: DatabaseService, sessionID: SessionSchema.ID) {
  const row = yield* db
    .select({ id: SessionMessageTable.id })
    .from(SessionMessageTable)
    .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "user")))
    .orderBy(desc(SessionMessageTable.seq))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
  return row ? SessionMessage.ID.make(row.id) : undefined
})

/**
 * Which completed compactions bound a history read. Local summaries always do. Native
 * windows do for model-neutral readers (`latest`), never for the original transcript
 * (`local`), and only when the target model can replay them (a provenance).
 */
export type Boundary = "latest" | "local" | SessionProviderContext.Provenance

const replayable = (message: SessionMessage.Info, boundary: Boundary) =>
  !SessionProviderContext.isCheckpoint(message) ||
  boundary === "latest" ||
  (boundary !== "local" && SessionProviderContext.compatible(message.providerContext.provenance, boundary))

export const latestCompaction = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  boundary: Boundary,
) {
  return yield* db
    .select({ seq: SessionMessageTable.seq })
    .from(SessionMessageTable)
    .where(
      and(
        eq(SessionMessageTable.session_id, sessionID),
        eq(SessionMessageTable.type, "compaction"),
        sql`json_extract(${SessionMessageTable.data}, '$.status') = 'completed'`,
        boundary === "latest"
          ? undefined
          : or(
              sql`json_extract(${SessionMessageTable.data}, '$.providerContext') is null`,
              boundary === "local"
                ? undefined
                : and(
                    ...Object.entries(boundary).map(
                      ([key, value]) =>
                        sql`json_extract(${SessionMessageTable.data}, ${`$.providerContext.provenance.${key}`}) = ${value}`,
                    ),
                  ),
            ),
      ),
    )
    .orderBy(desc(SessionMessageTable.seq))
    .limit(1)
    .get()
    .pipe(Effect.orDie)
})

export const hydrateRows = Effect.fnUntraced(function* (
  db: DatabaseService,
  rows: readonly (typeof SessionMessageTable.$inferSelect)[],
) {
  if (rows.length === 0) return []
  const entries = yield* hydrateMessages({
    db,
    where: inArray(SessionMessageTable.id, rows.map((row) => row.id)),
  })
  const messages = new Map(entries.map((entry) => [entry.message.id, entry]))
  return yield* Effect.forEach(rows, (row) => {
    const entry = messages.get(row.id)
    return entry?.sessionID === row.session_id
      ? Effect.succeed(entry.message)
      : Effect.fail(new MessageDecodeError({ sessionID: row.session_id, messageID: row.id }))
  })
})

export const decodeMessageRow = Effect.fnUntraced(function* (
  db: DatabaseService,
  row: typeof SessionMessageTable.$inferSelect,
) {
  const messages = yield* hydrateRows(db, [row])
  const message = messages[0]
  if (!message) return yield* new MessageDecodeError({ sessionID: row.session_id, messageID: row.id })
  return message
})

export const interruptedTools = Effect.fn("SessionHistory.interruptedTools")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  const compaction = yield* latestCompaction(db, sessionID, "latest")
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
        sql`json_extract(${SessionMessagePartTable.data}, '$.state.status') in ('streaming', 'running')`,
        compaction ? gte(SessionMessageTable.seq, compaction.seq) : undefined,
      ),
    )
    .orderBy(asc(SessionMessageTable.seq), asc(SessionMessagePartTable.position))
    .all()
    .pipe(Effect.orDie)
  return (yield* Effect.forEach(rows, (row) => decodeChild(row).pipe(Effect.map((tool) => ({ row, tool }))))).flatMap(
    ({ row, tool }) =>
      tool.type === "tool" && (tool.state.status === "streaming" || tool.state.status === "running")
        ? [{ assistantMessageID: row.messageID, tool }]
        : [],
  )
})

const messageEntries = Effect.fnUntraced(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  boundary: Boundary,
) {
  const compaction = yield* latestCompaction(db, sessionID, boundary)
  const imported = yield* db
    .select({ epochStart: InstructionStateTable.epoch_start })
    .from(InstructionStateTable)
    .where(and(eq(InstructionStateTable.session_id, sessionID), isNotNull(InstructionStateTable.frozen_baseline)))
    .get()
    .pipe(Effect.orDie)
  const entries = yield* hydrateMessages({
    db,
    where: and(
      eq(SessionMessageTable.session_id, sessionID),
      imported
        ? or(
            and(eq(SessionMessageTable.type, "system"), gt(SessionMessageTable.seq, imported.epochStart)),
            and(ne(SessionMessageTable.type, "system"), compaction ? gte(SessionMessageTable.seq, compaction.seq) : undefined),
          )
        : compaction ? gte(SessionMessageTable.seq, compaction.seq) : undefined,
    ),
    order: "asc",
  })
  // Re-expansion may cross a native checkpoint whose completion already advanced the instruction
  // epoch: the baseline supersedes the chronological updates before it. Forks seed their baseline
  // at sequence 0 but retain parent sequences, so the copied checkpoint still retires them.
  const native = entries.findLast((entry) => SessionProviderContext.isCheckpoint(entry.message))
  // Skipped native checkpoints are not textual summaries. Their original transcript remains available.
  return entries.filter(
    (entry) =>
      !(entry.message.type === "system" && !imported && native && entry.seq < native.seq) && replayable(entry.message, boundary),
  )
})

export const load = Effect.fn("SessionHistory.load")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  boundary: Boundary,
) {
  return (yield* messageEntries(db, sessionID, boundary)).map((entry) => entry.message)
})

export const quarantinedEntries = Effect.fn("SessionHistory.quarantinedEntries")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  boundary: Boundary,
) {
  return yield* db.transaction(() => Effect.gen(function* () {
    const facts = yield* db.select({
      id: SessionMessageTable.id,
      error: sql<string>`json_extract(${SessionMessageTable.data}, '$.error')`,
    }).from(SessionMessageTable).where(and(
      eq(SessionMessageTable.session_id, sessionID),
      inArray(SessionMessageTable.type, ["assistant", "compaction"]),
      sql`json_extract(${SessionMessageTable.data}, '$.error.type') = 'provider.invalid-file'`,
      sql`json_extract(${SessionMessageTable.data}, '$.error.rejectedAttachments') is not null`,
    )).all().pipe(Effect.orDie)
    const errors = yield* Effect.forEach(facts, (fact) =>
      Schema.decodeUnknownEffect(Schema.fromJsonString(SessionError.Error))(fact.error).pipe(
        Effect.mapError(() => new MessageDecodeError({ sessionID, messageID: fact.id })),
      ),
    )
    const rejectedAttachments = AttachmentQuarantine.unique(errors.flatMap((error) => error.rejectedAttachments ?? []))
    if (rejectedAttachments.length > 0) {
      const targets = yield* hydrateMessages({ db, where: and(
        eq(SessionMessageTable.session_id, sessionID),
        inArray(SessionMessageTable.id, rejectedAttachments.map((ref) => ref.messageID)),
      ) })
      const originals = new Map(targets.map((target) => [target.message.id, target.message]))
      if (rejectedAttachments.some((ref) => !AttachmentQuarantine.exists(ref, originals)))
        return yield* Effect.die(new AttachmentQuarantine.Unavailable({ message: "Rejected attachment originals are unavailable for safe replay" }))
    }
    return {
      entries: yield* messageEntries(db, sessionID, rejectedAttachments.length > 0 ? "local" : boundary),
      rejectedAttachments,
    }
  })).pipe(Effect.orDie)
})

export const entriesForRunner = Effect.fn("SessionHistory.entriesForRunner")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  instructions: Instructions.List,
  boundary: Boundary,
) {
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const history = yield* quarantinedEntries(db, sessionID, boundary)
        return {
          initial: yield* InstructionState.initial(db, sessionID, instructions),
          ...history,
        }
      }),
    )
    .pipe(Effect.orDie)
})

export const preview = Effect.fn("SessionHistory.preview")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
  instructions: Instructions.List,
  boundary: Boundary,
) {
  const observed = yield* Instructions.read(instructions)
  return yield* db
    .transaction(() =>
      Effect.gen(function* () {
        const history = yield* quarantinedEntries(db, sessionID, boundary)
        const messages = history.entries
        // An active assistant may contain an unresolved tool call, so only preview the settled prefix.
        const unsettled = messages.findIndex(
          (entry) => entry.message.type === "assistant" && entry.message.time.completed === undefined,
        )
        const settled = unsettled === -1 ? messages : messages.slice(0, unsettled)
        const assembled = yield* InstructionState.preview(db, sessionID, instructions, observed)
        return {
          initial: assembled.initial,
          messages: settled.map((entry) => entry.message),
          rejectedAttachments: history.rejectedAttachments,
          instructionUpdate: assembled.update,
        }
      }),
    )
    .pipe(Effect.catch((error) => (error instanceof Instructions.InitializationBlocked ? error : Effect.die(error))))
})

/** Returns the session's first user message. */
export const firstUserMessage = Effect.fn("SessionHistory.firstUserMessage")(function* (
  db: DatabaseService,
  sessionID: SessionSchema.ID,
) {
  const rows = yield* hydrateMessages({
    db,
    where: and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "user")),
    order: "asc",
    limit: 1,
  }).pipe(Effect.orElseSucceed(() => []))
  const message = rows[0]?.message
  return message?.type === "user" ? message : undefined
})

export * as SessionHistory from "./history.js"
