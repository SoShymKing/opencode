import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { sql } from "drizzle-orm"
import { Cause, Effect, Exit, Layer, Schema } from "effect"
import { EffectCache } from "drizzle-orm/cache/core/cache-effect"
import { EffectLogger } from "drizzle-orm/effect-core"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Database } from "@opencode-ai/core/database/database"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { layer as sqliteLayer } from "../src/database/sqlite.bun"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { ProjectV2 } from "@opencode-ai/core/project"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionStore } from "@opencode-ai/core/session/store"
import { centralSummary, removeTemporary, summarizeSamples } from "./session-projector-performance-fixture"
import { readQueryPlans, requireBenchmarkArguments, runBenchmarkWorkers } from "./session-storage-migration-performance-fixture"
import { BenchmarkContractError, executeRead, pragmas, readWorkloads, setupReadFixture, validateRead, type ReadRunnerOptions, type ReadWorkload } from "./session-storage-read-performance-fixture"

const decodeCount = Schema.decodeUnknownSync(Schema.Struct({ count: Schema.Number })),
  decodePage = Schema.decodeUnknownSync(Schema.Struct({ page_count: Schema.Number }))
const decodeFree = Schema.decodeUnknownSync(Schema.Struct({ freelist_count: Schema.Number })),
  decodeVersion = Schema.decodeUnknownSync(Schema.Struct({ version: Schema.String }))
const decodeProcessBoundary = Schema.decodeUnknownSync(Schema.Struct({ process: Schema.Number, failed: Schema.Boolean, buckets: Schema.Array(Schema.Struct({ workload: Schema.String, size: Schema.Number, failed: Schema.Boolean, samplesNs: Schema.Array(Schema.Number), medianNs: Schema.optional(Schema.Number), p95Ns: Schema.optional(Schema.Number) })) }))
const bytes = async (file: string) => ((await Bun.file(file).exists()) ? Bun.file(file).size : 0)
type QueryCounter = { active: boolean; readonly statements: string[] }

function countedDatabaseLayer(filename: string, counter: QueryCounter) {
  const logger = Layer.succeed(
    EffectLogger,
    EffectLogger.of({
      logQuery: (query) =>
        Effect.sync(() => {
          if (counter.active) counter.statements.push(query)
        }),
    }),
  )
  return Layer.effect(
    Database.Service,
    Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.make()
      yield* Effect.forEach(["PRAGMA journal_mode = WAL", "PRAGMA synchronous = NORMAL", "PRAGMA busy_timeout = 5000", "PRAGMA cache_size = -64000", "PRAGMA foreign_keys = ON"], (query) => db.run(query), { discard: true })
      yield* DatabaseMigration.apply(db, filename)
      return Database.Service.of({ db })
    }),
  ).pipe(Layer.provide(sqliteLayer({ filename })), Layer.provide(EffectCache.Default), Layer.provide(logger))
}

const projects = Layer.succeed(
  ProjectV2.Service,
  ProjectV2.Service.of({
    resolve: (directory) => Effect.succeed({ id: ProjectV2.ID.global, directory }),
    directories: () => Effect.succeed([]),
    commit: () => Effect.void,
  }),
)

export async function runReadBucket(input: ReadRunnerOptions & { readonly workload: ReadWorkload }) {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-session-storage-read-"))
  const databasePath = path.join(temporary, "benchmark.sqlite")
  const counter: QueryCounter = { active: false, statements: [] }
  const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node, SessionProjector.node, SessionStore.node, SessionV2.node]), [
    [Database.node, countedDatabaseLayer(databasePath, counter)],
    [ProjectV2.node, projects],
    [SessionExecution.node, SessionExecution.noopLayer],
  ])
  const program = Effect.gen(function* () {
    const database = yield* Database.Service
    const sessions = yield* SessionV2.Service
    yield* database.db.run("PRAGMA wal_autocheckpoint = 0").pipe(Effect.orDie)
    const fixture = yield* setupReadFixture(database.db, input.workload, `p${input.process}`, input.malformedRow)
    for (let index = 0; index < input.warmups; index++) validateRead(yield* executeRead(database.db, sessions, fixture, input.workload, input.invalidCursor), fixture, input.workload)
    yield* database.db.run("PRAGMA wal_checkpoint(TRUNCATE)").pipe(Effect.orDie)
    const dbBytesBefore = yield* Effect.promise(() => bytes(databasePath))
    const walBytesBefore = yield* Effect.promise(() => bytes(`${databasePath}-wal`))
    const samplesNs: number[] = [],
      queryCounts: number[] = []
    let queryStatements: readonly string[] = []
    let peakRssBytes = process.memoryUsage().rss
    const sampler = setInterval(() => {
      peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss)
    }, 10)
    try {
      for (let index = 0; index < input.samples; index++) {
        const queryStart = counter.statements.length
        counter.active = true
        const start = Bun.nanoseconds()
        const result = yield* executeRead(database.db, sessions, fixture, input.workload, input.invalidCursor)
        const duration = Bun.nanoseconds() - start
        counter.active = false
        validateRead(result, fixture, input.workload)
        samplesNs.push(duration)
        const statements = counter.statements.slice(queryStart).filter((query) => /\bsession_message(?:_part)?\b/.test(query))
        queryCounts.push(statements.length)
        if (index === 0) queryStatements = statements
      }
      const dbBytesBeforeCheckpoint = yield* Effect.promise(() => bytes(databasePath)),
        walBytesBeforeCheckpoint = yield* Effect.promise(() => bytes(`${databasePath}-wal`))
      yield* database.db.run("PRAGMA wal_checkpoint(PASSIVE)").pipe(Effect.orDie)
      const dbBytesAfterCheckpoint = yield* Effect.promise(() => bytes(databasePath)),
        walBytesAfterCheckpoint = yield* Effect.promise(() => bytes(`${databasePath}-wal`))
      peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss)
      const page = decodePage(yield* database.db.get(sql`PRAGMA page_count`).pipe(Effect.orDie))
      const free = decodeFree(yield* database.db.get(sql`PRAGMA freelist_count`).pipe(Effect.orDie))
      const rows = decodeCount(yield* database.db.get(sql`SELECT count(*) AS count FROM session_message`).pipe(Effect.orDie))
      const partsTable = yield* database.db
        .get<{
          readonly name: string
        }>(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_message_part'`)
        .pipe(Effect.orDie)
      const sqliteVersion = decodeVersion(yield* database.db.get(sql`SELECT sqlite_version() AS version`).pipe(Effect.orDie)).version
      const plans = yield* Effect.promise(() => readQueryPlans(database.db, input.workload.name, input.workload.size, queryStatements))
      const queryCount = queryCounts[0]
      if (queryCount === undefined || !queryCounts.every((count) => count === queryCount)) return yield* Effect.die(new BenchmarkContractError(`${input.workload.name}/${input.workload.size} query count changed between samples`))
      return {
        workload: input.workload.name,
        size: input.workload.size,
        failed: false as const,
        samplesNs,
        ...summarizeSamples(samplesNs),
        queryCount,
        queryCounts,
        queryStatements,
        queryPlans: plans,
        validation: {
          status: "PASS" as const,
          samplesValidated: samplesNs.length,
          payload: true as const,
          order: true as const,
        },
        storage: {
          dbBytesBefore,
          walBytesBefore,
          dbBytesBeforeCheckpoint,
          walBytesBeforeCheckpoint,
          walDelta: walBytesBeforeCheckpoint - walBytesBefore,
          dbBytesAfterCheckpoint,
          walBytesAfterCheckpoint,
          pageCount: page.page_count,
          freelistCount: free.freelist_count,
        },
        rowCounts: { messages: rows.count },
        peakRssBytes,
        sqliteVersion,
        fullAssistantDecode: input.workload.name === "interrupted-tools" && !partsTable,
      }
    } finally {
      counter.active = false
      clearInterval(sampler)
    }
  })
  try {
    const result = await Effect.runPromise(program.pipe(Effect.exit, Effect.scoped, Effect.provide(layer)))
    if (Exit.isSuccess(result)) return result.value
    return {
      workload: input.workload.name,
      size: input.workload.size,
      failed: true as const,
      samplesNs: [] as const,
      error: Cause.pretty(result.cause),
    }
  } finally {
    await removeTemporary(temporary)
  }
}

export async function runReadProcess(options: ReadRunnerOptions) {
  const buckets: Array<Awaited<ReturnType<typeof runReadBucket>>> = []
  for (const workload of readWorkloads) {
    const bucket = await runReadBucket({ ...options, workload })
    buckets.push(bucket)
    if (bucket.failed) break
  }
  const successful = buckets.find((bucket) => !bucket.failed)
  return {
    process: options.process,
    failed: buckets.some((bucket) => bucket.failed),
    runtime: {
      bun: Bun.version,
      sqlite: successful && !successful.failed ? successful.sqliteVersion : undefined,
      platform: process.platform,
      architecture: process.arch,
    },
    pragmas,
    buckets,
  }
}

async function main() {
  const argv = process.argv.slice(2)
  const options = requireBenchmarkArguments(argv)
  const invalidCursor = argv.includes("--inject-invalid-cursor"),
    malformedRow = argv.includes("--inject-malformed-row")
  if (options.worker) {
    const result = await runReadProcess({
      process: options.worker,
      warmups: 5,
      samples: 30,
      invalidCursor,
      malformedRow,
    })
    console.log(`RESULT ${JSON.stringify(result)}`)
    return result.failed ? 1 : 0
  }
  if (!options.output) throw new BenchmarkContractError("--output is required")
  const { raw, processes } = await runBenchmarkWorkers({
    script: import.meta.path,
    extra: [...(invalidCursor ? ["--inject-invalid-cursor"] : []), ...(malformedRow ? ["--inject-malformed-row"] : [])],
    decode: decodeProcessBoundary,
  })
  const failed = processes.some((item) => item.failed)
  const aggregates = failed
    ? []
    : readWorkloads.map((workload) => ({
        ...workload,
        ...centralSummary(
          processes.map((item) => {
            const bucket = item.buckets.find((candidate) => candidate.workload === workload.name && candidate.size === workload.size)
            if (!bucket || bucket.failed || bucket.medianNs === undefined || bucket.p95Ns === undefined) throw new BenchmarkContractError("Missing successful read bucket")
            return { medianNs: bucket.medianNs, p95Ns: bucket.p95Ns }
          }),
        ),
      }))
  const header = JSON.stringify({
    schemaVersion: 1,
    benchmark: "Session storage exact reads",
    config: { processes: 5, warmups: 5, samples: 30 },
    pragmas,
    failed,
    validation: { status: failed ? "FAIL" : "PASS", payload: !failed, order: !failed },
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
