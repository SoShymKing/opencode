#!/usr/bin/env bun

import path from "node:path"
import { createHash } from "node:crypto"
import { Schema } from "effect"
import { runWriteProcess } from "./session-projector-performance"
import { BenchmarkContractError } from "./session-projector-performance-fixture"
import { runMigrationProcess } from "./session-storage-migration-performance"
import { runReadProcess } from "./session-storage-read-performance"

export const fixedOrder = ["B1", "C1", "B2", "C2", "B3", "C3", "B4", "C4", "B5", "C5"] as const
export const harnessFiles = ["packages/core/script/session-projector-performance-fixture.ts", "packages/core/script/session-projector-performance.ts", "packages/core/script/session-storage-benchmark-compare.ts", "packages/core/script/session-storage-benchmark-slot.ts", "packages/core/script/session-storage-evidence-check.ts", "packages/core/script/session-storage-migration-performance-fixture.ts", "packages/core/script/session-storage-migration-performance.ts", "packages/core/script/session-storage-read-performance-fixture.ts", "packages/core/script/session-storage-read-performance.ts", "packages/core/test/session-projector-performance-fixture.test.ts", "packages/core/test/session-storage-performance-fixture.test.ts"] as const
export const BenchmarkSlot = Schema.Literals(fixedOrder)
export const BenchmarkManifest = Schema.Struct({
  baselineWorktree: Schema.String,
  candidateWorktree: Schema.String,
  baselineCommit: Schema.String,
  candidateCommit: Schema.String,
  evidenceRoot: Schema.String,
  harnessFiles: Schema.Array(Schema.String),
  harnessSha256: Schema.String,
  fixedOrder: Schema.Array(Schema.String),
})
const Bucket = Schema.Struct({
  failed: Schema.Boolean,
  samplesNs: Schema.Array(Schema.Number),
  medianNs: Schema.optional(Schema.Number),
  p95Ns: Schema.optional(Schema.Number),
  peakRssBytes: Schema.optional(Schema.Number),
  validation: Schema.Struct({ status: Schema.String, samplesValidated: Schema.Number }),
})
const Storage = Schema.Struct({
  dbBytesBefore: Schema.Number,
  walBytesBefore: Schema.Number,
  dbBytesBeforeCheckpoint: Schema.Number,
  walBytesBeforeCheckpoint: Schema.Number,
  walDelta: Schema.Number,
  dbBytesAfterCheckpoint: Schema.Number,
  walBytesAfterCheckpoint: Schema.Number,
  pageCount: Schema.Number,
  freelistCount: Schema.Number,
})
const QueryPlan = Schema.Struct({
  name: Schema.String,
  statement: Schema.String,
  bindings: Schema.Array(Schema.Union([Schema.String, Schema.Number])),
  details: Schema.Array(Schema.Struct({ detail: Schema.String })),
})
export const BenchmarkSlotArtifact = Schema.Struct({
  schemaVersion: Schema.Number,
  slot: Schema.String,
  revision: Schema.String,
  commit: Schema.String,
  harnessSha256: Schema.String,
  runtime: Schema.Struct({ bun: Schema.String, platform: Schema.String, architecture: Schema.String }),
  write: Schema.Struct({
    failed: Schema.Boolean,
    buckets: Schema.Array(
      Schema.Struct({
        ...Bucket.fields,
        workload: Schema.String,
        siblings: Schema.Number,
        rss: Schema.Struct({ intervalMs: Schema.Number, samples: Schema.Array(Schema.Number), peakRssBytes: Schema.Number }),
        storage: Storage,
      }),
    ),
  }),
  read: Schema.Struct({
    failed: Schema.Boolean,
    buckets: Schema.Array(
      Schema.Struct({
        ...Bucket.fields,
        workload: Schema.String,
        size: Schema.Number,
        queryCount: Schema.Number,
        queryCounts: Schema.Array(Schema.Number),
        queryStatements: Schema.Array(Schema.String),
        queryPlans: Schema.Array(QueryPlan),
        fullAssistantDecode: Schema.Boolean,
        storage: Storage,
      }),
    ),
  }),
  migration: Schema.Struct({
    failed: Schema.Boolean,
    buckets: Schema.Array(
      Schema.Struct({
        ...Bucket.fields,
        rows: Schema.Number,
        rssIntervalMs: Schema.Number,
        rssSamplesBytes: Schema.Array(Schema.Number),
      }),
    ),
    storage: Schema.optional(
      Schema.Struct({
        dbBytesBefore: Schema.Number,
        walBytesBefore: Schema.Number,
        dbBytesBeforeCheckpoint: Schema.Number,
        walBytesBeforeCheckpoint: Schema.Number,
        walDelta: Schema.Number,
        dbBytesAfterCheckpoint: Schema.Number,
        walBytesAfterCheckpoint: Schema.Number,
        storageTotal: Schema.Number,
        pageCount: Schema.Number,
        freelistCount: Schema.Number,
      }),
    ),
  }),
})
export type ParsedBenchmarkSlot = typeof BenchmarkSlotArtifact.Type

export async function harnessHash(root: string) {
  const digest = createHash("sha256")
  for (const file of harnessFiles) {
    const hash = createHash("sha256")
      .update(new Uint8Array(await Bun.file(path.join(root, file)).arrayBuffer()))
      .digest("hex")
    digest.update(file).update("\0").update(hash).update("\n")
  }
  return digest.digest("hex")
}

export const ratio = (candidate: number, baseline: number) => candidate / baseline
export const regression = (candidate: number, baseline: number) => ratio(candidate, baseline) - 1
export function median5(values: readonly number[]) {
  const value = values.toSorted((left, right) => left - right)[2]
  if (value === undefined) throw new BenchmarkContractError("Expected five values")
  return value
}
export function findCentral(items: readonly Record<string, unknown>[], fields: Record<string, string | number>) {
  const item = items.find((candidate) => Object.entries(fields).every(([key, value]) => candidate[key] === value))
  if (!item || typeof item.centralMedianNs !== "number" || typeof item.centralP95Ns !== "number") throw new BenchmarkContractError(`Missing aggregate ${JSON.stringify(fields)}`)
  return { median: item.centralMedianNs, p95: item.centralP95Ns }
}

const option = (argv: readonly string[], name: string) => {
  const index = argv.indexOf(name)
  return index < 0 ? undefined : argv[index + 1]
}

export async function runBoundedCommand(cwd: string, args: readonly string[]) {
  const child = Bun.spawn([...args], { cwd, stdout: "pipe", stderr: "pipe", signal: AbortSignal.timeout(30_000) })
  const [exit, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
  if (exit !== 0) throw new BenchmarkContractError(`${args.join(" ")} failed: ${stderr}`)
  return stdout.trim()
}

export async function runSlot(input: { readonly manifest: string; readonly slot: typeof BenchmarkSlot.Type; readonly output: string }) {
  const manifest = Schema.decodeUnknownSync(BenchmarkManifest)(Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(await Bun.file(input.manifest).text()))
  if (manifest.fixedOrder.join(",") !== "B1,C1,B2,C2,B3,C3,B4,C4,B5,C5") throw new BenchmarkContractError("Benchmark manifest order is not frozen")
  const baseline = input.slot.startsWith("B")
  const expected = path.resolve(baseline ? manifest.baselineWorktree : manifest.candidateWorktree, "packages/core")
  if (path.resolve(process.cwd()) !== expected) throw new BenchmarkContractError(`Slot ${input.slot} must run from ${expected}`)
  const processIndex = Number(input.slot.slice(1))
  const write = await runWriteProcess({ process: processIndex, warmups: 5, samples: 30 })
  const read = await runReadProcess({ process: processIndex, warmups: 5, samples: 30 })
  const migration = await runMigrationProcess({ process: processIndex, warmups: 5, samples: 30 })
  const artifact = {
    schemaVersion: 1,
    slot: input.slot,
    revision: baseline ? "baseline" : "candidate",
    commit: baseline ? manifest.baselineCommit : manifest.candidateCommit,
    harnessSha256: manifest.harnessSha256,
    runtime: { bun: Bun.version, platform: process.platform, architecture: process.arch },
    write,
    read,
    migration,
  }
  await Bun.write(input.output, `${JSON.stringify(artifact)}\n`)
  return { artifact, exitCode: write.failed || read.failed || migration.failed ? 1 : 0 }
}

async function main() {
  const argv = process.argv.slice(2)
  const manifest = option(argv, "--manifest")
  const output = option(argv, "--output")
  const slot = Schema.decodeUnknownSync(BenchmarkSlot)(option(argv, "--slot"))
  if (!manifest || !output) throw new BenchmarkContractError("--manifest, --slot, and --output are required")
  return (await runSlot({ manifest: path.resolve(manifest), slot, output: path.resolve(output) })).exitCode
}

if (import.meta.main)
  main().then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(error instanceof Error ? error.message : error)
      process.exit(1)
    },
  )
