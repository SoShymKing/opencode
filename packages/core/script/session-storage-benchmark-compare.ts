#!/usr/bin/env bun

import path from "node:path"
import { createHash } from "node:crypto"
import { Schema } from "effect"
import { BenchmarkContractError, centralSummary } from "./session-projector-performance-fixture"
import { BenchmarkManifest, BenchmarkSlotArtifact, findCentral, fixedOrder, harnessFiles, harnessHash, median5, ratio, regression, runBoundedCommand, type ParsedBenchmarkSlot } from "./session-storage-benchmark-slot"
import { queryPlansMatchStatements } from "./session-storage-migration-performance-fixture"
import { readWorkloads } from "./session-storage-read-performance-fixture"

const option = (argv: readonly string[], name: string) => {
  const index = argv.indexOf(name)
  return index < 0 ? undefined : argv[index + 1]
}

function aggregate(slots: readonly ParsedBenchmarkSlot[], section: "write" | "read" | "migration") {
  const first = slots[0]?.[section].buckets
  if (!first) throw new BenchmarkContractError(`Missing ${section} buckets`)
  return first.map((bucket, index) => {
    const samples = slots
      .map((slot) => slot[section].buckets[index])
      .map((item) => {
        if (!item || item.failed || item.samplesNs.length !== 30 || item.medianNs === undefined || item.p95Ns === undefined) throw new BenchmarkContractError(`Invalid ${section} bucket ${index}`)
        return { medianNs: item.medianNs, p95Ns: item.p95Ns }
      })
    return { ...bucket, samplesNs: undefined, ...centralSummary(samples) }
  })
}

type PlannedBucket = { readonly workload: string; readonly size: number; readonly queryStatements: readonly string[]; readonly queryPlans: readonly { readonly name: string; readonly statement: string; readonly details: readonly { readonly detail: string }[] }[] }
export function candidateQueryPlansIndexed(buckets: readonly PlannedBucket[]) {
  return buckets.every(
    (bucket) =>
      queryPlansMatchStatements(bucket.queryStatements, bucket.queryPlans) &&
      bucket.queryPlans.every((plan, index) => {
        if (plan.name !== `${bucket.workload}/${bucket.size}/${index}` || plan.details.length === 0) return false
        const table = plan.statement.includes('"session_message_part"') ? "session_message_part" : "session_message"
        const details = plan.details.map((item) => item.detail.toUpperCase())
        return details.some((detail) => detail.includes(`SEARCH ${table.toUpperCase()} USING `) && (detail.includes("INDEX") || detail.includes("PRIMARY KEY"))) && details.every((detail) => !detail.includes("TEMP B-TREE") && !/SCAN SESSION_MESSAGE(?:_PART)?/.test(detail))
      }),
  )
}

type StorageEvidence = { readonly dbBytesBefore: number; readonly walBytesBefore: number; readonly dbBytesBeforeCheckpoint: number; readonly walBytesBeforeCheckpoint: number; readonly walDelta: number; readonly dbBytesAfterCheckpoint: number; readonly walBytesAfterCheckpoint: number; readonly pageCount: number; readonly freelistCount: number }
type EvidenceSlot = {
  readonly slot: string
  readonly write: { readonly buckets: readonly { readonly workload: string; readonly siblings: number; readonly storage: StorageEvidence; readonly rss: { readonly intervalMs: number; readonly samples: readonly number[]; readonly peakRssBytes: number } }[] }
  readonly read: { readonly buckets: readonly { readonly workload: string; readonly size: number; readonly storage: StorageEvidence; readonly peakRssBytes?: number }[] }
  readonly migration: { readonly storage?: StorageEvidence & { readonly storageTotal?: number }; readonly buckets: readonly { readonly rows: number; readonly rssIntervalMs: number; readonly rssSamplesBytes: readonly number[]; readonly peakRssBytes?: number }[] }
}
export function comparisonEvidence(slots: readonly EvidenceSlot[]) {
  if (slots.length !== fixedOrder.length || !slots.every((slot, index) => slot.slot === fixedOrder[index] && slot.write.buckets.length === 10 && slot.read.buckets.length === readWorkloads.length && slot.migration.storage !== undefined)) throw new BenchmarkContractError("Comparison checkpoint cardinality differs")
  for (const slot of slots) for (const bucket of slot.write.buckets) if (bucket.rss.intervalMs !== 10 || bucket.rss.samples.length === 0 || bucket.rss.peakRssBytes !== Math.max(...bucket.rss.samples)) throw new BenchmarkContractError(`Write RSS differs in ${slot.slot}`)
  return {
    checkpoint: Object.fromEntries(
      slots.map((slot) => [
        slot.slot,
        {
          write: slot.write.buckets.map((bucket) => ({ workload: bucket.workload, siblings: bucket.siblings, ...bucket.storage })),
          read: slot.read.buckets.map((bucket) => ({ workload: bucket.workload, size: bucket.size, ...bucket.storage })),
          migration: slot.migration.storage,
        },
      ]),
    ),
    rss: Object.fromEntries(
      slots.map((slot) => [
        slot.slot,
        {
          write: slot.write.buckets.map((bucket) => ({ workload: bucket.workload, siblings: bucket.siblings, ...bucket.rss })),
          read: slot.read.buckets.map((bucket) => ({ workload: bucket.workload, size: bucket.size, peakRssBytes: bucket.peakRssBytes })),
          migration: slot.migration.buckets.map((bucket) => ({ rows: bucket.rows, intervalMs: bucket.rssIntervalMs, samples: bucket.rssSamplesBytes, peakRssBytes: bucket.peakRssBytes })),
        },
      ]),
    ),
  }
}

export function recomputeComparison(manifest: typeof BenchmarkManifest.Type, slots: readonly ParsedBenchmarkSlot[], slotFileHashes: Readonly<Record<string, string>>) {
  const baseline = slots.filter((slot) => slot.revision === "baseline"),
    candidate = slots.filter((slot) => slot.revision === "candidate")
  const baselineWrite = aggregate(baseline, "write"),
    candidateWrite = aggregate(candidate, "write")
  const baselineRead = aggregate(baseline, "read"),
    candidateRead = aggregate(candidate, "read")
  const baselineMigration = aggregate(baseline, "migration"),
    candidateMigration = aggregate(candidate, "migration")
  const toolB = findCentral(baselineWrite, { workload: "tool", siblings: 512 }),
    toolC = findCentral(candidateWrite, { workload: "tool", siblings: 512 })
  const textB = findCentral(baselineWrite, { workload: "text", siblings: 512 }),
    textC = findCentral(candidateWrite, { workload: "text", siblings: 512 })
  const reasoningB = findCentral(baselineWrite, { workload: "reasoning", siblings: 512 }),
    reasoningC = findCentral(candidateWrite, { workload: "reasoning", siblings: 512 })
  const envelopeB = findCentral(baselineWrite, { workload: "envelope-only", siblings: 512 }),
    envelopeC = findCentral(candidateWrite, { workload: "envelope-only", siblings: 512 })
  const tool2048 = findCentral(candidateWrite, { workload: "tool", siblings: 2048 })
  const readRegression = readWorkloads.every((workload) => {
    const base = findCentral(baselineRead, { workload: workload.name, size: workload.size }),
      next = findCentral(candidateRead, { workload: workload.name, size: workload.size })
    return regression(next.median, base.median) <= 0.1 && regression(next.p95, base.p95) <= 0.1
  })
  const perSlotMigrationRatios = candidate.map((slot) => {
    const low = slot.migration.buckets.find((bucket) => bucket.rows === 1_000),
      high = slot.migration.buckets.find((bucket) => bucket.rows === 10_000)
    if (!low?.medianNs || !high?.medianNs || !low.peakRssBytes || !high.peakRssBytes) throw new BenchmarkContractError(`Missing migration growth bucket in ${slot.slot}`)
    return {
      slot: slot.slot,
      durationGrowth: ratio(high.medianNs, low.medianNs),
      rssGrowth: ratio(high.peakRssBytes, low.peakRssBytes),
    }
  })
  const migrationDurationGrowth = median5(perSlotMigrationRatios.map((item) => item.durationGrowth)),
    migrationRssGrowth = median5(perSlotMigrationRatios.map((item) => item.rssGrowth))
  const storageRatio = ratio(median5(candidate.map((slot) => slot.migration.storage?.storageTotal ?? 0)), median5(baseline.map((slot) => slot.migration.storage?.storageTotal ?? 0)))
  const evidence = comparisonEvidence(slots)
  const gates = {
    correctness: slots.every((slot) => !slot.write.failed && !slot.read.failed && !slot.migration.failed),
    sameHost: slots.every((slot) => slot.runtime.platform === slots[0]?.runtime.platform && slot.runtime.architecture === slots[0]?.runtime.architecture),
    writeRss: true,
    storagePhases: slots.every((slot) => {
      const storage = [...slot.write.buckets.map((bucket) => bucket.storage), ...slot.read.buckets.map((bucket) => bucket.storage)]
      const migration = slot.migration.storage
      return storage.every((value) => value.walDelta === value.walBytesBeforeCheckpoint - value.walBytesBefore) && !!migration && migration.walDelta === migration.walBytesBeforeCheckpoint - migration.walBytesBefore && migration.storageTotal === migration.dbBytesAfterCheckpoint + migration.walBytesAfterCheckpoint
    }),
    queryCount: candidate.every((slot) => slot.read.buckets.filter((bucket) => bucket.workload === "page" || bucket.workload === "single-message").every((bucket) => bucket.queryCount === 2)),
    queryPlans: candidate.every((slot) => candidateQueryPlansIndexed(slot.read.buckets)),
    interrupted: candidate.every((slot) => slot.read.buckets.filter((bucket) => bucket.workload === "interrupted-tools").every((bucket) => !bucket.fullAssistantDecode)),
    toolMedianImprovement: 1 - ratio(toolC.median, toolB.median) >= 0.2,
    toolP95Regression: regression(toolC.p95, toolB.p95) <= 0,
    textImprovement: textC.median < textB.median && textC.p95 < textB.p95,
    reasoningImprovement: reasoningC.median < reasoningB.median && reasoningC.p95 < reasoningB.p95,
    siblingGrowth: ratio(tool2048.median, toolC.median) <= 1.5,
    envelopeRegression: regression(envelopeC.median, envelopeB.median) <= 0.1 && regression(envelopeC.p95, envelopeB.p95) <= 0.1,
    readRegression,
    migrationDurationGrowth: migrationDurationGrowth <= 15,
    migrationRssGrowth: migrationRssGrowth <= 15,
    storageRatio: storageRatio <= 2,
  }
  const result = Object.values(gates).every(Boolean) ? "PASS" : "FAIL"
  const comparison = {
    schemaVersion: 1,
    result,
    fixedOrder,
    slotFileHashes,
    pins: {
      baselineCommit: manifest.baselineCommit,
      candidateCommit: manifest.candidateCommit,
      baselineWorktree: manifest.baselineWorktree,
      candidateWorktree: manifest.candidateWorktree,
    },
    harnessSha256: manifest.harnessSha256,
    formulas: {
      median30: "(x15 + x16) / 2",
      p95NearestRank30: "x29",
      central5: "sorted item 3",
      improvement: "(baselineCentral - candidateCentral) / baselineCentral",
      regression: "(candidateCentral - baselineCentral) / baselineCentral",
      siblingGrowth: "candidateCentralMedian2048 / candidateCentralMedian512",
    },
    metrics: { migrationDurationGrowth, migrationRssGrowth, storageRatio },
    perSlotMigrationRatios,
    checkpoint: evidence.checkpoint,
    rss: evidence.rss,
    gates,
  }
  return {
    comparison,
    baselineWrite,
    candidateWrite,
    baselineRead,
    candidateRead,
    baselineMigration,
    candidateMigration,
    baseline,
    candidate,
  }
}

export async function compare(manifestPath: string) {
  const manifest = Schema.decodeUnknownSync(BenchmarkManifest)(Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(await Bun.file(manifestPath).text()))
  if (!path.isAbsolute(manifest.baselineWorktree) || !path.isAbsolute(manifest.candidateWorktree) || !path.isAbsolute(manifest.evidenceRoot)) throw new BenchmarkContractError("Manifest paths must be absolute")
  if (manifest.fixedOrder.join(",") !== fixedOrder.join(",") || manifest.harnessFiles.join("\n") !== harnessFiles.join("\n")) throw new BenchmarkContractError("Frozen order or harness list mismatch")
  for (const [root, commit] of [
    [manifest.baselineWorktree, manifest.baselineCommit],
    [manifest.candidateWorktree, manifest.candidateCommit],
  ] as const) {
    if ((await runBoundedCommand(root, ["git", "rev-parse", "HEAD"])) !== commit) throw new BenchmarkContractError(`Worktree is not pinned to ${commit}`)
    if (await runBoundedCommand(root, ["git", "status", "--porcelain"])) throw new BenchmarkContractError(`Dirty benchmark worktree: ${root}`)
    if ((await harnessHash(root)) !== manifest.harnessSha256) throw new BenchmarkContractError(`Harness hash mismatch: ${root}`)
  }
  const raw = path.join(manifest.evidenceRoot, "raw")
  const slots: ParsedBenchmarkSlot[] = []
  const slotFileHashes: Record<string, string> = {}
  for (const slot of fixedOrder) {
    const root = slot.startsWith("B") ? manifest.baselineWorktree : manifest.candidateWorktree
    const output = path.join(raw, `${slot}.json`)
    const child = Bun.spawn([process.execPath, "run", "script/session-storage-benchmark-slot.ts", "--manifest", manifestPath, "--slot", slot, "--output", output], { cwd: path.join(root, "packages/core"), stdout: "pipe", stderr: "pipe", signal: AbortSignal.timeout(1_800_000) })
    const [exit, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()])
    if (exit !== 0) throw new BenchmarkContractError(`Slot ${slot} failed: ${stderr}`)
    const slotBytes = new Uint8Array(await Bun.file(output).arrayBuffer())
    slotFileHashes[slot] = createHash("sha256").update(slotBytes).digest("hex")
    slots.push(Schema.decodeUnknownSync(BenchmarkSlotArtifact)(Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(new TextDecoder().decode(slotBytes))))
  }
  const { comparison, baselineWrite, candidateWrite, baselineRead, candidateRead, baselineMigration, candidateMigration, baseline, candidate } = recomputeComparison(manifest, slots, slotFileHashes)
  await Bun.write(path.join(manifest.evidenceRoot, "baseline-write.json"), `${JSON.stringify({ aggregates: baselineWrite })}\n`)
  await Bun.write(path.join(manifest.evidenceRoot, "candidate-write.json"), `${JSON.stringify({ aggregates: candidateWrite })}\n`)
  await Bun.write(path.join(manifest.evidenceRoot, "baseline-read-migration.json"), `${JSON.stringify({ read: baselineRead, migration: baselineMigration })}\n`)
  await Bun.write(path.join(manifest.evidenceRoot, "candidate-read-migration.json"), `${JSON.stringify({ read: candidateRead, migration: candidateMigration })}\n`)
  await Bun.write(path.join(manifest.evidenceRoot, "query-plans.json"), `${JSON.stringify({ baseline: baseline.flatMap((slot) => slot.read.buckets.flatMap((bucket) => bucket.queryPlans ?? [])), candidate: candidate.flatMap((slot) => slot.read.buckets.flatMap((bucket) => bucket.queryPlans ?? [])) })}\n`)
  await Bun.write(path.join(manifest.evidenceRoot, "comparison.json"), `${JSON.stringify(comparison)}\n`)
  return comparison.result === "PASS" ? 0 : 1
}

if (import.meta.main) {
  const manifest = option(process.argv.slice(2), "--manifest")
  if (!manifest) throw new BenchmarkContractError("--manifest is required")
  compare(path.resolve(manifest)).then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(error instanceof Error ? error.message : error)
      process.exit(1)
    },
  )
}
