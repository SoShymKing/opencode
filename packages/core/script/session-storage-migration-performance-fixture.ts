import { sql } from "drizzle-orm"
import { isDeepStrictEqual } from "node:util"
import { DateTime, Effect, Schema } from "effect"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import type { Database } from "@opencode-ai/core/database/database"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { BenchmarkContractError, measuredSamples, processCount, pragmas, warmupSamples } from "./session-projector-performance-fixture"

export { BenchmarkContractError, pragmas }
export const migrationRows = [0, 100, 1_000, 10_000] as const
export const partDensities = [0, 3, 12, 32] as const
export const predecessorMigrationID = "v01_baseline"
export type MigrationRows = (typeof migrationRows)[number]
export type MigrationDatabase = EffectDrizzleSqlite.EffectSQLiteDatabase

export const benchmarkOption = (argv: readonly string[], name: string) => {
  const index = argv.indexOf(name)
  return index < 0 ? undefined : argv[index + 1]
}

export function requireBenchmarkArguments(argv: readonly string[]) {
  if (Number(benchmarkOption(argv, "--processes")) !== processCount || Number(benchmarkOption(argv, "--warmups")) !== warmupSamples || Number(benchmarkOption(argv, "--samples")) !== measuredSamples) throw new BenchmarkContractError("Required arguments: --processes 5 --warmups 5 --samples 30")
  return { output: benchmarkOption(argv, "--output"), worker: Number(benchmarkOption(argv, "--worker")) }
}

export async function runBenchmarkWorkers<A>(input: { readonly script: string; readonly extra: readonly string[]; readonly decode: (value: unknown) => A }) {
  const raw: string[] = [],
    processes: A[] = []
  for (let index = 1; index <= processCount; index++) {
    const signal = AbortSignal.timeout(600_000)
    const child = Bun.spawn([process.execPath, "run", input.script, "--processes", "5", "--warmups", "5", "--samples", "30", "--worker", String(index), ...input.extra], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", signal })
    const [exit, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
    if (signal.aborted || child.signalCode !== null) throw new BenchmarkContractError(`Worker ${index} was terminated by ${child.signalCode ?? "timeout"}`)
    const encoded = stdout
      .split(/\r?\n/)
      .find((line) => line.startsWith("RESULT "))
      ?.slice(7)
    if (!encoded) throw new BenchmarkContractError(`Worker ${index} produced no result: ${stderr}`)
    const value = input.decode(Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(encoded))
    if (typeof value !== "object" || value === null || !("failed" in value) || typeof value.failed !== "boolean") throw new BenchmarkContractError(`Worker ${index} result has no strict failed state`)
    if ((exit === 0) === value.failed) throw new BenchmarkContractError(`Worker ${index} exit ${exit} disagrees with failed:${value.failed}`)
    raw.push(encoded)
    processes.push(value)
  }
  return { raw, processes }
}

const decodePlans = Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ id: Schema.Number, parent: Schema.Number, notused: Schema.Number, detail: Schema.String })))
type QueryPlan = { readonly statement: string }
export const queryPlansMatchStatements = (statements: readonly string[], plans: readonly QueryPlan[]) => statements.length === plans.length && statements.every((statement, index) => plans[index]?.statement === statement)

function representativeBindings(statement: string, size: number) {
  return statement
    .split("?")
    .slice(0, -1)
    .map((prefix) => {
      if (/limit\s*$/i.test(prefix)) return size
      if (/"seq"\s*(?:=|>|>=|<|<=)\s*$/.test(prefix)) return 0
      if (/"type"\s*(?:=|<>)\s*$/.test(prefix)) return "assistant"
      if (/"message_id"\s*=\s*$/.test(prefix) || /"id"\s*=\s*$/.test(prefix)) return "msg_plan"
      if (/"session_id"\s*=\s*$/.test(prefix)) return "ses_plan"
      return "benchmark"
    })
}

export async function readQueryPlans(db: Database.Interface["db"], workload: string, size: number, statements: readonly string[]) {
  const plans = []
  for (const [index, statement] of statements.entries()) {
    const bindings = representativeBindings(statement, size)
    let binding = 0
    const explained = statement.replaceAll("?", () => {
      const value = bindings[binding++]
      return typeof value === "number" ? String(value) : `'${value?.replaceAll("'", "''")}'`
    })
    plans.push({ name: `${workload}/${size}/${index}`, statement, bindings, details: decodePlans(await Effect.runPromise(db.all(sql.raw(`EXPLAIN QUERY PLAN ${explained}`)).pipe(Effect.orDie))) })
  }
  return plans
}

const created = DateTime.makeUnsafe(0)
const model = { id: ModelV2.ID.make("storage-migration"), providerID: ProviderV2.ID.make("benchmark") }
const encode = Schema.encodeSync(SessionMessage.Assistant)

function encodedAssistant(parts: number) {
  const message = SessionMessage.Assistant.make({
    id: SessionMessage.ID.make(`msg_template_${parts}`),
    type: "assistant",
    agent: "build",
    model,
    content: Array.from({ length: parts }, (_, index): SessionMessage.AssistantContent => {
      if (index % 3 === 0) return SessionMessage.AssistantText.make({ type: "text", id: `text-${index}`, text: `part-${index}` })
      if (index % 3 === 1)
        return SessionMessage.AssistantReasoning.make({
          type: "reasoning",
          id: `reasoning-${index}`,
          text: `why-${index}`,
        })
      return SessionMessage.AssistantTool.make({
        type: "tool",
        id: `tool-${index}`,
        name: "read",
        state: SessionMessage.ToolStateCompleted.make({ status: "completed", input: {}, structured: {}, content: [] }),
        time: { created, completed: created },
      })
    }),
    time: { created },
  })
  const { id: _id, type: _type, ...data } = encode(message)
  return JSON.stringify(data)
}

const encodedDensities = partDensities.map(encodedAssistant)
const decodeJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)
const decodeData = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))
const ParentRow = Schema.Struct({ id: Schema.String, seq: Schema.Number, data: Schema.String })
const ChildRow = Schema.Struct({
  message_id: Schema.String,
  position: Schema.Number,
  id: Schema.String,
  type: Schema.String,
  data: Schema.String,
})
type ChildRow = typeof ChildRow.Type
const expectedDensities = encodedDensities.map((data) => decodeData(decodeJson(data)))
const candidates = migrations.slice(migrations.findIndex((migration) => migration.id === predecessorMigrationID) + 1)
const baselineAdapter = {
  id: "benchmark_session_part_baseline",
  up: () => Effect.void,
} satisfies DatabaseMigration.Migration

export function benchmarkMigrations() {
  return candidates.length === 0 ? [baselineAdapter] : candidates
}

export function setupPredecessor(db: MigrationDatabase, rows: MigrationRows, malformed = false) {
  return Effect.gen(function* () {
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA synchronous = NORMAL")
    yield* db.run("PRAGMA wal_autocheckpoint = 0")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run("PRAGMA busy_timeout = 5000")
    yield* db.run(sql`CREATE TABLE session (id TEXT PRIMARY KEY NOT NULL)`)
    yield* db.run(sql`CREATE TABLE session_message (
      id TEXT PRIMARY KEY NOT NULL,
      session_id TEXT NOT NULL REFERENCES session(id) ON DELETE CASCADE,
      type TEXT NOT NULL,
      seq INTEGER NOT NULL,
      time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL,
      data TEXT NOT NULL
    )`)
    yield* db.run(sql`CREATE UNIQUE INDEX session_message_session_seq_idx ON session_message(session_id, seq)`)
    yield* db.run(sql`CREATE INDEX session_message_session_type_seq_idx ON session_message(session_id, type, seq)`)
    yield* db.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
    if (rows === 0) return
    yield* db.run(sql`INSERT INTO session(id) VALUES ('ses_migration_benchmark')`)
    yield* db.run(sql`WITH RECURSIVE seed(value) AS (
      SELECT 0 UNION ALL SELECT value + 1 FROM seed WHERE value + 1 < ${rows}
    ) INSERT INTO session_message(id, session_id, type, seq, time_created, time_updated, data)
      SELECT 'msg_migration_' || value, 'ses_migration_benchmark', 'assistant', value, value, value,
        CASE value % 4
          WHEN 0 THEN ${encodedDensities[0]}
          WHEN 1 THEN ${encodedDensities[1]}
          WHEN 2 THEN ${encodedDensities[2]}
          ELSE ${encodedDensities[3]}
        END
      FROM seed`)
    if (malformed) yield* db.run(sql`UPDATE session_message SET data = ${"{}"} WHERE seq = 0`)
  }).pipe(Effect.orDie)
}

export function applyBenchmarkMigration(db: MigrationDatabase) {
  return DatabaseMigration.applyOnly(db, benchmarkMigrations()).pipe(Effect.orDie)
}

export function growthInputs(rows: MigrationRows) {
  const counts = partDensities.map((_, density) => Math.floor((rows + 3 - density) / 4))
  const totalParts = counts.reduce((sum, count, index) => sum + count * (partDensities[index] ?? 0), 0)
  return {
    assistants: rows,
    densitySeed: 0,
    partDensities,
    totalParts,
    averageParts: rows === 0 ? 0 : totalParts / rows,
  }
}

export function reconstructAssistant(envelope: Record<string, unknown>, children: readonly ChildRow[]) {
  const content = children.map((child, position) => {
    if (child.position !== position) throw new BenchmarkContractError(`Child position ${child.position} does not match ${position}`)
    return { ...decodeData(decodeJson(child.data)), id: child.id, type: child.type }
  })
  return { ...envelope, content }
}

export function validateMigration(db: MigrationDatabase, rows: MigrationRows) {
  return Effect.gen(function* () {
    const journal = yield* db.all<{ id: string }>(sql`SELECT id FROM migration ORDER BY id`)
    const parents = Schema.decodeUnknownSync(Schema.Array(ParentRow))(yield* db.all(sql`SELECT id, seq, data FROM session_message ORDER BY seq`))
    if (journal.length !== benchmarkMigrations().length || parents.length !== rows) return yield* Effect.die(new BenchmarkContractError(`Migration validation failed for ${rows} assistants`))
    const childTable = yield* db.get<{ readonly name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_message_part'`)
    const children = childTable ? Schema.decodeUnknownSync(Schema.Array(ChildRow))(yield* db.all(sql`SELECT message_id, position, id, type, data FROM session_message_part ORDER BY message_id, position`)) : []
    if (childTable && children.length !== 0) return yield* Effect.die(new BenchmarkContractError(`Expected no migrated children, received ${children.length}`))
    for (const parent of parents) {
      const envelope = decodeData(decodeJson(parent.data))
      if (!isDeepStrictEqual(envelope, expectedDensities[parent.seq % partDensities.length])) return yield* Effect.die(new BenchmarkContractError(`Assistant ${parent.id} changed`))
    }
    return {
      journal: journal.map((item) => item.id),
      parents: parents.length,
      children: children.length,
      childOrder: true,
      reconstruction: true,
    }
  })
}
