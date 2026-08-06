#!/usr/bin/env bun

import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { isDeepStrictEqual } from "node:util"
import { asc, eq, sql } from "drizzle-orm"
import { Cause, Effect, Exit, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventTable } from "@opencode-ai/core/event/sql"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionMessageTable } from "@opencode-ai/core/session/sql"
import {
  BenchmarkContractError,
  assistantRow,
  centralSummary,
  encodeAssistant,
  makeFixture,
  measuredSamples,
  pragmas,
  processCount,
  publishUpdate,
  readAssistant,
  removeTemporary,
  setupFixture,
  setupProject,
  siblingCounts,
  summarizeSamples,
  warmupSamples,
  workloadEventSequences,
  workloadNames,
  type DatabaseService,
  type Fixture,
  type SiblingCount,
  type WorkloadName,
} from "./session-projector-performance-fixture"

type BucketSuccess = {
  readonly workload: WorkloadName
  readonly siblings: SiblingCount
  readonly failed: false
  readonly eventSequence: readonly string[]
  readonly samplesNs: readonly number[]
  readonly medianNs: number
  readonly p95Ns: number
  readonly rss: { readonly intervalMs: 10; readonly samples: readonly number[]; readonly peakRssBytes: number }
  readonly validation: { readonly status: "PASS"; readonly samplesValidated: number; readonly finalDecodedAssistant: true; readonly replayEquality: true }
  readonly storage: {
    readonly dbBytesBefore: number
    readonly walBytesBefore: number
    readonly dbBytesBeforeCheckpoint: number
    readonly walBytesBeforeCheckpoint: number
    readonly walDelta: number
    readonly dbBytesAfterCheckpoint: number
    readonly walBytesAfterCheckpoint: number
    readonly pageCount: number
    readonly freelistCount: number
  }
  readonly actualWriteProxies: { readonly walBytes: number; readonly databaseBytes: number; readonly durableEvents: number }
  readonly finalDecode: { readonly equal: true; readonly encodedBytes: number }
  readonly replayEquality: true
  readonly sqliteVersion: string
}
type BucketFailure = { readonly workload: WorkloadName; readonly siblings: SiblingCount; readonly failed: true; readonly samplesNs: readonly []; readonly error: string }
type ProcessResult = {
  readonly process: number
  readonly failed: boolean
  readonly runtime: { readonly bun: string; readonly sqlite?: string; readonly platform: NodeJS.Platform; readonly architecture: string }
  readonly pragmas: typeof pragmas
  readonly buckets: readonly (BucketSuccess | BucketFailure)[]
}
export type WriteRunnerOptions = { readonly process: number; readonly warmups: 5; readonly samples: 30; readonly mismatchExpected?: boolean }

class BenchmarkValidationError extends Error {
  constructor(readonly workload: WorkloadName, readonly siblings: SiblingCount, readonly stage: string) {
    super(`${workload}/${siblings} failed ${stage} validation`)
  }
}

const fileBytes = async (file: string) => {
  const handle = Bun.file(file)
  return (await handle.exists()) ? handle.size : 0
}

const decodeEventData = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.Unknown))
const decodePageCount = Schema.decodeUnknownSync(Schema.Struct({ page_count: Schema.Number }))
const decodeFreelistCount = Schema.decodeUnknownSync(Schema.Struct({ freelist_count: Schema.Number }))
const decodeSqliteVersion = Schema.decodeUnknownSync(Schema.Struct({ sqliteVersion: Schema.String }))

function storedEvents(db: DatabaseService, fixture: Fixture) {
  return db
    .select()
    .from(EventTable)
    .where(eq(EventTable.aggregate_id, fixture.sessionID))
    .orderBy(asc(EventTable.seq))
    .all()
    .pipe(
      Effect.orDie,
      Effect.map((events) =>
        events.map((event) => ({ id: event.id, type: event.type, seq: event.seq, aggregateID: event.aggregate_id, data: decodeEventData(event.data) })),
      ),
    )
}

function validateFinal(db: DatabaseService, fixture: Fixture) {
  return Effect.gen(function* () {
    if (!isDeepStrictEqual(yield* readAssistant(db, fixture), fixture.expected))
      return yield* Effect.fail(new BenchmarkValidationError(fixture.workload, fixture.siblings, "payload"))
    const stored = yield* storedEvents(db, fixture)
    if (!isDeepStrictEqual(stored.map((event) => event.type.replace(/\.\d+$/, "")), workloadEventSequences[fixture.workload]))
      return yield* Effect.fail(new BenchmarkValidationError(fixture.workload, fixture.siblings, "event sequence"))
    return stored
  })
}

function replay(db: DatabaseService, events: EventV2.Interface, fixture: Fixture, stored: readonly EventV2.SerializedEvent[]) {
  return Effect.gen(function* () {
    yield* events.remove(fixture.sessionID)
    const initial = assistantRow(fixture)
    yield* db
      .update(SessionMessageTable)
      .set({ type: initial.type, time_created: initial.time_created, data: initial.data })
      .where(eq(SessionMessageTable.id, fixture.assistantMessageID))
      .run()
      .pipe(Effect.orDie)
    yield* events.replayAll([...stored])
    if (!isDeepStrictEqual(yield* readAssistant(db, fixture), fixture.expected))
      return yield* Effect.fail(new BenchmarkValidationError(fixture.workload, fixture.siblings, "replay"))
  })
}

async function runBucket(input: WriteRunnerOptions & { readonly workload: WorkloadName; readonly siblings: SiblingCount }): Promise<BucketSuccess | BucketFailure> {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-session-projector-performance-"))
  const databasePath = path.join(temporary, "benchmark.sqlite")
  const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node, SessionProjector.node]), [[Database.node, Database.layerFromPath(databasePath)]])
  const program = Effect.gen(function* () {
    const database = yield* Database.Service
    const events = yield* EventV2.Service
    yield* database.db.run("PRAGMA wal_autocheckpoint = 0").pipe(Effect.orDie)
    yield* setupProject(database.db)
    const warmups = Array.from({ length: input.warmups }, (_, index) => makeFixture({ siblings: input.siblings, workload: input.workload, label: `p${input.process}_warmup_${index}`, mismatchExpected: input.mismatchExpected }))
    const measured = Array.from({ length: input.samples }, (_, index) => makeFixture({ siblings: input.siblings, workload: input.workload, label: `p${input.process}_sample_${index}`, mismatchExpected: input.mismatchExpected }))
    yield* Effect.forEach([...warmups, ...measured], (fixture) => setupFixture(database.db, fixture), { discard: true })
    yield* Effect.forEach(warmups, (fixture) => Effect.gen(function* () { yield* publishUpdate(events, fixture); const stored = yield* validateFinal(database.db, fixture); yield* replay(database.db, events, fixture, stored) }), { discard: true })
    yield* database.db.run("PRAGMA wal_checkpoint(TRUNCATE)").pipe(Effect.orDie)
    const dbBytesBefore = yield* Effect.promise(() => fileBytes(databasePath))
    const walBytesBefore = yield* Effect.promise(() => fileBytes(`${databasePath}-wal`))
    const rssSamples = [process.memoryUsage().rss]
    const rssTimer = yield* Effect.acquireRelease(
      Effect.sync(() => setInterval(() => rssSamples.push(process.memoryUsage().rss), 10)),
      (timer) => Effect.sync(() => clearInterval(timer)),
    )
    const pending: Array<{ readonly fixture: Fixture; readonly durationNs: number; readonly stored: readonly EventV2.SerializedEvent[] }> = []
    for (const fixture of measured) {
      const start = Bun.nanoseconds()
      yield* publishUpdate(events, fixture)
      const durationNs = Bun.nanoseconds() - start
      pending.push({ fixture, durationNs, stored: yield* validateFinal(database.db, fixture) })
    }
    const dbBytesBeforeCheckpoint = yield* Effect.promise(() => fileBytes(databasePath))
    const walBytesBeforeCheckpoint = yield* Effect.promise(() => fileBytes(`${databasePath}-wal`))
    yield* database.db.run("PRAGMA wal_checkpoint(PASSIVE)").pipe(Effect.orDie)
    const dbBytesAfterCheckpoint = yield* Effect.promise(() => fileBytes(databasePath))
    const walBytesAfterCheckpoint = yield* Effect.promise(() => fileBytes(`${databasePath}-wal`))
    const page = decodePageCount(yield* database.db.get(sql`PRAGMA page_count`).pipe(Effect.orDie))
    const free = decodeFreelistCount(yield* database.db.get(sql`PRAGMA freelist_count`).pipe(Effect.orDie))
    const version = decodeSqliteVersion(yield* database.db.get(sql`SELECT sqlite_version() AS sqliteVersion`).pipe(Effect.orDie))
    if (!page || !free || !version) return yield* Effect.die(new BenchmarkContractError("SQLite accounting query returned no row"))
    yield* Effect.sync(() => { rssSamples.push(process.memoryUsage().rss); clearInterval(rssTimer) })
    yield* Effect.forEach(pending, (item) => replay(database.db, events, item.fixture, item.stored), { discard: true })
    const samplesNs = pending.map((item) => item.durationNs)
    const summary = summarizeSamples(samplesNs)
    const walDelta = walBytesBeforeCheckpoint - walBytesBefore
    return {
      workload: input.workload, siblings: input.siblings, failed: false as const,
      eventSequence: workloadEventSequences[input.workload], samplesNs, ...summary,
      rss: { intervalMs: 10 as const, samples: rssSamples, peakRssBytes: Math.max(...rssSamples) },
      validation: { status: "PASS" as const, samplesValidated: samplesNs.length, finalDecodedAssistant: true as const, replayEquality: true as const },
      storage: { dbBytesBefore, walBytesBefore, dbBytesBeforeCheckpoint, walBytesBeforeCheckpoint, walDelta, dbBytesAfterCheckpoint, walBytesAfterCheckpoint, pageCount: page.page_count, freelistCount: free.freelist_count },
      actualWriteProxies: { walBytes: walDelta, databaseBytes: dbBytesBeforeCheckpoint - dbBytesBefore, durableEvents: input.samples * workloadEventSequences[input.workload].length },
      finalDecode: { equal: true as const, encodedBytes: new TextEncoder().encode(JSON.stringify(encodeAssistant(measured[measured.length - 1]?.expected))).byteLength },
      replayEquality: true as const, sqliteVersion: version.sqliteVersion,
    }
  })
  try {
    const result = await Effect.runPromise(program.pipe(Effect.exit, Effect.scoped, Effect.provide(layer)))
    if (Exit.isSuccess(result)) return result.value
    return { workload: input.workload, siblings: input.siblings, failed: true, samplesNs: [], error: Cause.pretty(result.cause) }
  } finally {
    await removeTemporary(temporary)
  }
}

export async function runWriteProcess(options: WriteRunnerOptions): Promise<ProcessResult> {
  const buckets: Array<BucketSuccess | BucketFailure> = []
  for (const workload of workloadNames) for (const siblings of siblingCounts) {
    const bucket = await runBucket({ ...options, workload, siblings })
    buckets.push(bucket)
    if (bucket.failed) break
  }
  const sqlite = buckets.find((bucket): bucket is BucketSuccess => !bucket.failed)?.sqliteVersion
  return { process: options.process, failed: buckets.some((bucket) => bucket.failed), runtime: { bun: Bun.version, sqlite, platform: process.platform, architecture: process.arch }, pragmas, buckets }
}

type CliOptions = { readonly processes: 5; readonly warmups: 5; readonly samples: 30; readonly output?: string; readonly worker?: number; readonly mismatchExpected: boolean }
function option(argv: readonly string[], name: string) {
  const index = argv.indexOf(name)
  return index < 0 ? undefined : argv[index + 1]
}
function parseArguments(argv: readonly string[]): CliOptions {
  const processes = Number(option(argv, "--processes"))
  const warmups = Number(option(argv, "--warmups"))
  const samples = Number(option(argv, "--samples"))
  if (processes !== processCount || warmups !== warmupSamples || samples !== measuredSamples)
    throw new BenchmarkContractError(`Required arguments: --processes ${processCount} --warmups ${warmupSamples} --samples ${measuredSamples}`)
  const workerValue = option(argv, "--worker")
  const worker = workerValue === undefined ? undefined : Number(workerValue)
  if (worker !== undefined && (!Number.isInteger(worker) || worker < 1 || worker > processCount)) throw new BenchmarkContractError("Invalid worker index")
  return { processes, warmups, samples, output: option(argv, "--output"), worker, mismatchExpected: argv.includes("--inject-mismatched-expected") }
}

async function worker(options: CliOptions & { readonly worker: number }) {
  const result = await runWriteProcess({ process: options.worker, warmups: options.warmups, samples: options.samples, mismatchExpected: options.mismatchExpected })
  console.log(`RESULT ${JSON.stringify(result)}`)
  for (const bucket of result.buckets) if (!bucket.failed) console.log(`SUMMARY\t${bucket.workload}\t${bucket.siblings}\t${bucket.medianNs}\t${bucket.p95Ns}`)
  return result.failed ? 1 : 0
}

async function main() {
  const options = parseArguments(process.argv.slice(2))
  if (options.worker !== undefined) return worker({ ...options, worker: options.worker })
  if (!options.output) throw new BenchmarkContractError("--output is required")
  const raw: string[] = []
  const summaries = new Map<string, Array<{ medianNs: number; p95Ns: number }>>()
  let failed = false
  for (let index = 1; index <= options.processes; index++) {
    const child = Bun.spawn([process.execPath, "run", import.meta.path, "--processes", "5", "--warmups", "5", "--samples", "30", "--worker", String(index), ...(options.mismatchExpected ? ["--inject-mismatched-expected"] : [])], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe" })
    const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
    const result = stdout.split(/\r?\n/).find((line) => line.startsWith("RESULT "))?.slice(7)
    if (!result) throw new BenchmarkContractError(`Worker ${index} produced no result: ${stderr}`)
    raw.push(result)
    failed ||= exitCode !== 0
    for (const line of stdout.split(/\r?\n/).filter((item) => item.startsWith("SUMMARY\t"))) {
      const [, workload, siblings, median, p95] = line.split("\t")
      if (!workload || !siblings || !median || !p95) throw new BenchmarkContractError(`Malformed worker summary: ${line}`)
      const key = `${workload}/${siblings}`
      summaries.set(key, [...(summaries.get(key) ?? []), { medianNs: Number(median), p95Ns: Number(p95) }])
    }
  }
  const aggregates = failed ? [] : workloadNames.flatMap((workload) => siblingCounts.map((siblings) => ({ workload, siblings, ...centralSummary(summaries.get(`${workload}/${siblings}`) ?? []) })))
  const header = JSON.stringify({ schemaVersion: 1, benchmark: "SessionProjector targeted JSON writes", runtime: { bun: Bun.version, platform: process.platform, architecture: process.arch }, config: { processes: options.processes, warmups: options.warmups, samples: options.samples }, pragmas, workloadEventSequences, aggregates, validation: { status: failed ? "FAIL" : "PASS", finalDecodedAssistant: !failed, replayEquality: !failed }, failed })
  const artifact = `${header.slice(0, -1)},"processes":[${raw.join(",")}]}`
  await Bun.write(options.output, `${artifact}\n`)
  console.log(artifact)
  return failed ? 1 : 0
}

if (import.meta.main) main().then((exitCode) => process.exit(exitCode), (error: unknown) => { console.error(error instanceof Error ? error.message : error); process.exit(1) })
