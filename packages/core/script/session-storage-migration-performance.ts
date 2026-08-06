#!/usr/bin/env bun

import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { sql } from "drizzle-orm"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Effect, Schema } from "effect"
import { centralSummary, removeTemporary, summarizeSamples } from "./session-projector-performance-fixture"
import { applyBenchmarkMigration, BenchmarkContractError, growthInputs, migrationRows, pragmas, requireBenchmarkArguments, runBenchmarkWorkers, setupPredecessor, validateMigration, type MigrationRows } from "./session-storage-migration-performance-fixture"

export type MigrationRunnerOptions = {
  readonly process: number
  readonly warmups: 5
  readonly samples: 30
  readonly malformedRow?: boolean
}

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
const decodeVersion = Schema.decodeUnknownSync(Schema.Struct({ version: Schema.String }))
const decodePage = Schema.decodeUnknownSync(Schema.Struct({ page_count: Schema.Number }))
const decodeFree = Schema.decodeUnknownSync(Schema.Struct({ freelist_count: Schema.Number }))
const decodeCount = Schema.decodeUnknownSync(Schema.Struct({ count: Schema.Number }))
const decodeProcessBoundary = Schema.decodeUnknownSync(
  Schema.Struct({
    process: Schema.Number,
    failed: Schema.Boolean,
    buckets: Schema.Array(
      Schema.Struct({
        rows: Schema.Number,
        failed: Schema.Boolean,
        samplesNs: Schema.Array(Schema.Number),
        medianNs: Schema.optional(Schema.Number),
        p95Ns: Schema.optional(Schema.Number),
      }),
    ),
  }),
)
const bytes = async (file: string) => ((await Bun.file(file).exists()) ? Bun.file(file).size : 0)

async function useDatabase<A, E>(prefix: string, run: (db: Effect.Success<typeof makeDb>, file: string) => Effect.Effect<A, E>) {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), prefix))
  const file = path.join(temporary, "benchmark.sqlite")
  try {
    return await Effect.runPromise(
      Effect.gen(function* () {
        return yield* run(yield* makeDb, file)
      }).pipe(Effect.provide(SqliteClient.layer({ filename: file })), Effect.scoped),
    )
  } finally {
    await removeTemporary(temporary)
  }
}

async function sample(rows: MigrationRows, malformed: boolean) {
  return useDatabase("opencode-session-storage-migration-", (db) =>
    Effect.gen(function* () {
      yield* setupPredecessor(db, rows, malformed)
      yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)").pipe(Effect.orDie)
      const start = Bun.nanoseconds()
      yield* applyBenchmarkMigration(db)
      const durationNs = Bun.nanoseconds() - start
      if (malformed) return yield* Effect.die(new BenchmarkContractError("Malformed predecessor row was accepted"))
      yield* validateMigration(db, rows)
      return {
        durationNs,
        sqlite: decodeVersion(yield* db.get(sql`SELECT sqlite_version() AS version`)).version,
      }
    }),
  )
}

export async function runMigrationBucket(options: MigrationRunnerOptions, rows: MigrationRows) {
  try {
    for (let index = 0; index < options.warmups; index++) await sample(rows, options.malformedRow === true)
    const samples = []
    let sqliteVersion = ""
    const rssSamplesBytes: number[] = [process.memoryUsage().rss]
    const sampler = setInterval(() => rssSamplesBytes.push(process.memoryUsage().rss), 10)
    try {
      for (let index = 0; index < options.samples; index++) {
        const measured = await sample(rows, options.malformedRow === true)
        samples.push(measured.durationNs)
        sqliteVersion = measured.sqlite
      }
    } finally {
      clearInterval(sampler)
      rssSamplesBytes.push(process.memoryUsage().rss)
    }
    return {
      rows,
      failed: false as const,
      samplesNs: samples,
      ...summarizeSamples(samples),
      peakRssBytes: Math.max(...rssSamplesBytes),
      rssIntervalMs: 10,
      rssSamplesBytes,
      validation: {
        status: "PASS",
        samplesValidated: samples.length,
        transactionAndJournal: true,
        rssSampled: rssSamplesBytes.length >= 2,
      },
      growthInputs: growthInputs(rows),
      sqliteVersion,
    }
  } catch (error) {
    return {
      rows,
      failed: true as const,
      samplesNs: [] as const,
      error: error instanceof Error ? error.message : String(error),
    }
  }
}

async function accounting() {
  return useDatabase("opencode-session-storage-accounting-", (db, file) =>
    Effect.gen(function* () {
      yield* setupPredecessor(db, 10_000)
      yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)").pipe(Effect.orDie)
      const dbBytesBefore = yield* Effect.promise(() => bytes(file))
      const walBytesBefore = yield* Effect.promise(() => bytes(`${file}-wal`))
      yield* applyBenchmarkMigration(db)
      yield* validateMigration(db, 10_000)
      const dbBytesBeforeCheckpoint = yield* Effect.promise(() => bytes(file))
      const walBytesBeforeCheckpoint = yield* Effect.promise(() => bytes(`${file}-wal`))
      yield* db.run("PRAGMA wal_checkpoint(PASSIVE)").pipe(Effect.orDie)
      const dbBytesAfterCheckpoint = yield* Effect.promise(() => bytes(file))
      const walBytesAfterCheckpoint = yield* Effect.promise(() => bytes(`${file}-wal`))
      const page = decodePage(yield* db.get(sql`PRAGMA page_count`).pipe(Effect.orDie))
      const free = decodeFree(yield* db.get(sql`PRAGMA freelist_count`).pipe(Effect.orDie))
      const partsTable = yield* db
        .get<{
          name: string
        }>(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_message_part'`)
        .pipe(Effect.orDie)
      const parts = partsTable ? decodeCount(yield* db.get(sql`SELECT count(*) AS count FROM session_message_part`).pipe(Effect.orDie)).count : 0
      return {
        dbBytesBefore,
        walBytesBefore,
        dbBytesBeforeCheckpoint,
        walBytesBeforeCheckpoint,
        walDelta: walBytesBeforeCheckpoint - walBytesBefore,
        dbBytesAfterCheckpoint,
        walBytesAfterCheckpoint,
        storageTotal: dbBytesAfterCheckpoint + walBytesAfterCheckpoint,
        pageCount: page.page_count,
        freelistCount: free.freelist_count,
        rowCounts: { assistants: 10_000, parts },
        growthInputs: growthInputs(10_000),
      }
    }),
  )
}

export async function runMigrationProcess(options: MigrationRunnerOptions) {
  const buckets: Array<Awaited<ReturnType<typeof runMigrationBucket>>> = []
  for (const rows of options.malformedRow ? ([100] as const) : migrationRows) {
    const bucket = await runMigrationBucket(options, rows)
    buckets.push(bucket)
    if (bucket.failed) break
  }
  const failed = buckets.some((bucket) => bucket.failed)
  const successful = buckets.find((bucket) => !bucket.failed)
  return {
    process: options.process,
    failed,
    runtime: {
      bun: Bun.version,
      sqlite: successful && !successful.failed ? successful.sqliteVersion : undefined,
      platform: process.platform,
      architecture: process.arch,
    },
    pragmas,
    buckets,
    ...(failed ? {} : { storage: await accounting() }),
  }
}

async function main() {
  const argv = process.argv.slice(2)
  const options = requireBenchmarkArguments(argv)
  const malformedRow = argv.includes("--inject-malformed-row")
  if (options.worker) {
    const result = await runMigrationProcess({
      process: options.worker,
      warmups: 5,
      samples: 30,
      malformedRow,
    })
    console.log(`RESULT ${JSON.stringify(result)}`)
    return result.failed ? 1 : 0
  }
  if (!options.output) throw new BenchmarkContractError("--output is required")
  const { raw, processes } = await runBenchmarkWorkers({
    script: import.meta.path,
    extra: malformedRow ? ["--inject-malformed-row"] : [],
    decode: decodeProcessBoundary,
  })
  const failed = processes.some((item) => item.failed)
  const aggregates = failed
    ? []
    : migrationRows.map((rows) => ({
        rows,
        ...centralSummary(
          processes.map((item) => {
            const bucket = item.buckets.find((candidate) => candidate.rows === rows)
            if (!bucket || bucket.failed || bucket.medianNs === undefined || bucket.p95Ns === undefined) throw new BenchmarkContractError("Missing successful migration bucket")
            return { medianNs: bucket.medianNs, p95Ns: bucket.p95Ns }
          }),
        ),
      }))
  const header = JSON.stringify({
    schemaVersion: 1,
    benchmark: "Session storage migration",
    config: { processes: 5, warmups: 5, samples: 30 },
    pragmas,
    failed,
    validation: { status: failed ? "FAIL" : "PASS", transactionAndJournal: !failed },
    aggregates,
  })
  const artifact = `${header.slice(0, -1)},"processes":[${raw.join(",")}]}`
  await Bun.write(options.output, `${artifact}\n`)
  console.log(artifact)
  return failed ? 1 : 0
}

if (import.meta.main)
  main().then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(error instanceof Error ? error.message : error)
      process.exit(1)
    },
  )
