#!/usr/bin/env bun

import fs from "node:fs/promises"
import path from "node:path"
import { createHash } from "node:crypto"
import { isDeepStrictEqual } from "node:util"
import { Schema } from "effect"
import { BenchmarkContractError, siblingCounts, workloadNames } from "./session-projector-performance-fixture"
import { recomputeComparison } from "./session-storage-benchmark-compare"
import { BenchmarkManifest, BenchmarkSlotArtifact, fixedOrder, harnessFiles, harnessHash, runBoundedCommand, type ParsedBenchmarkSlot } from "./session-storage-benchmark-slot"
import { migrationRows, queryPlansMatchStatements } from "./session-storage-migration-performance-fixture"
import { readWorkloads } from "./session-storage-read-performance-fixture"

const Phase = Schema.Literals(["final", "performance"])
const Status = Schema.Literals(["PASS", "FAIL"])
type Check = { readonly id: string; readonly status: typeof Status.Type; readonly detail: string }
const pass = (id: string, detail: string): Check => ({ id, status: "PASS", detail })
const fail = (id: string, detail: string): Check => ({ id, status: "FAIL", detail })
const readJson = async (file: string) => Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(await Bun.file(file).text())
const option = (argv: readonly string[], name: string) => {
  const index = argv.indexOf(name)
  return index < 0 ? undefined : argv[index + 1]
}
const exact = (actual: readonly string[], expected: readonly string[]) => actual.length === expected.length && actual.every((value, index) => value === expected[index])

function bucketKeys(slot: ParsedBenchmarkSlot) {
  return {
    write: slot.write.buckets.map((bucket) => `${bucket.workload}/${bucket.siblings}`),
    read: slot.read.buckets.map((bucket) => `${bucket.workload}/${bucket.size}`),
    migration: slot.migration.buckets.map((bucket) => String(bucket.rows)),
  }
}

export function validateSlotEvidence(value: unknown) {
  const slot = Schema.decodeUnknownSync(BenchmarkSlotArtifact)(value)
  const keys = bucketKeys(slot)
  const expectedWrite = workloadNames.flatMap((workload) => siblingCounts.map((siblings) => `${workload}/${siblings}`))
  const expectedRead = readWorkloads.map((workload) => `${workload.name}/${workload.size}`)
  if (!exact(keys.write, expectedWrite) || !exact(keys.read, expectedRead) || !exact(keys.migration, migrationRows.map(String))) throw new BenchmarkContractError(`Slot ${slot.slot} bucket set differs`)
  for (const section of [slot.write, slot.read, slot.migration]) if (section.failed || section.buckets.some((bucket) => bucket.failed || bucket.samplesNs.length !== 30 || bucket.medianNs === undefined || bucket.p95Ns === undefined || bucket.validation.status !== "PASS" || bucket.validation.samplesValidated !== 30)) throw new BenchmarkContractError(`Slot ${slot.slot} has failed or incomplete samples`)
  for (const bucket of slot.write.buckets) if (bucket.rss.intervalMs !== 10 || bucket.rss.samples.length === 0 || bucket.rss.peakRssBytes !== Math.max(...bucket.rss.samples) || bucket.storage.walDelta !== bucket.storage.walBytesBeforeCheckpoint - bucket.storage.walBytesBefore) throw new BenchmarkContractError(`Slot ${slot.slot} write evidence differs`)
  for (const bucket of slot.read.buckets) {
    if (bucket.queryCounts.length !== 30 || bucket.queryCounts.some((count) => count !== bucket.queryCount) || bucket.queryStatements.length !== bucket.queryCount || !queryPlansMatchStatements(bucket.queryStatements, bucket.queryPlans)) throw new BenchmarkContractError(`Slot ${slot.slot} read query evidence differs`)
    if (bucket.storage.walDelta !== bucket.storage.walBytesBeforeCheckpoint - bucket.storage.walBytesBefore) throw new BenchmarkContractError(`Slot ${slot.slot} read storage phase differs`)
  }
  for (const bucket of slot.migration.buckets) if (bucket.rssIntervalMs !== 10 || bucket.rssSamplesBytes.length < 2 || bucket.peakRssBytes !== Math.max(...bucket.rssSamplesBytes)) throw new BenchmarkContractError(`Slot ${slot.slot} migration RSS differs`)
  const storage = slot.migration.storage
  if (!storage || storage.walDelta !== storage.walBytesBeforeCheckpoint - storage.walBytesBefore || storage.storageTotal !== storage.dbBytesAfterCheckpoint + storage.walBytesAfterCheckpoint) throw new BenchmarkContractError(`Slot ${slot.slot} migration storage differs`)
  return slot
}

async function authenticateWorktrees(manifest: typeof BenchmarkManifest.Type) {
  for (const [root, commit] of [
    [manifest.baselineWorktree, manifest.baselineCommit],
    [manifest.candidateWorktree, manifest.candidateCommit],
  ] as const) {
    if (!path.isAbsolute(root) || !/^[0-9a-f]{40}$/i.test(commit)) throw new BenchmarkContractError("Manifest worktree or commit pin is invalid")
    if ((await runBoundedCommand(root, ["git", "rev-parse", "HEAD"])) !== commit) throw new BenchmarkContractError(`Worktree is not pinned to ${commit}`)
    if (await runBoundedCommand(root, ["git", "status", "--porcelain"])) throw new BenchmarkContractError(`Dirty benchmark worktree: ${root}`)
    if ((await harnessHash(root)) !== manifest.harnessSha256) throw new BenchmarkContractError(`Harness hash mismatch: ${root}`)
  }
}

async function performance(root: string) {
  const checks: Check[] = []
  const manifestFile = path.join(root, "benchmark-manifest.json")
  if (!(await Bun.file(manifestFile).exists())) return [fail("performance.manifest.exists", "benchmark-manifest.json is missing")]
  let manifest: typeof BenchmarkManifest.Type
  try {
    manifest = Schema.decodeUnknownSync(BenchmarkManifest)(await readJson(manifestFile))
  } catch (error) {
    return [fail("performance.manifest.schema", error instanceof Error ? error.message : String(error))]
  }
  checks.push(exact(manifest.fixedOrder, fixedOrder) ? pass("performance.order", "fixed B/C order") : fail("performance.order", "slot order differs"))
  checks.push(exact(manifest.harnessFiles, harnessFiles) ? pass("performance.harness.files", "frozen harness list") : fail("performance.harness.files", "harness list differs"))
  checks.push(path.resolve(manifest.evidenceRoot) === path.resolve(root) ? pass("performance.root", "evidence root pinned") : fail("performance.root", "evidence root differs"))
  try {
    await authenticateWorktrees(manifest)
    checks.push(pass("performance.pins", "commits, worktrees, cleanliness, and harness hash authenticated"))
  } catch (error) {
    checks.push(fail("performance.pins", error instanceof Error ? error.message : String(error)))
  }
  const raw = path.join(root, "raw")
  const entries = (await fs.readdir(raw).catch(() => [])).toSorted()
  const expectedFiles = fixedOrder.map((slot) => `${slot}.json`).toSorted()
  checks.push(exact(entries, expectedFiles) ? pass("performance.raw.files", "exact ten raw files") : fail("performance.raw.files", "raw file set differs"))
  const slots: ParsedBenchmarkSlot[] = [],
    slotFileHashes: Record<string, string> = {}
  for (const expected of fixedOrder) {
    const file = path.join(raw, `${expected}.json`)
    if (!(await Bun.file(file).exists())) continue
    try {
      const bytes = new Uint8Array(await Bun.file(file).arrayBuffer())
      const slot = validateSlotEvidence(Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(new TextDecoder().decode(bytes)))
      const baseline = expected.startsWith("B")
      if (slot.slot !== expected || slot.revision !== (baseline ? "baseline" : "candidate") || slot.commit !== (baseline ? manifest.baselineCommit : manifest.candidateCommit) || slot.harnessSha256 !== manifest.harnessSha256) throw new BenchmarkContractError(`Slot ${expected} revision pin differs`)
      slotFileHashes[expected] = createHash("sha256").update(bytes).digest("hex")
      slots.push(slot)
      checks.push(pass(`performance.slot.${expected}`, "schema, buckets, revision, queries, storage, and RSS authenticated"))
    } catch (error) {
      checks.push(fail(`performance.slot.${expected}`, error instanceof Error ? error.message : String(error)))
    }
  }
  const comparisonFile = path.join(root, "comparison.json")
  if (slots.length !== fixedOrder.length || !(await Bun.file(comparisonFile).exists())) checks.push(fail("performance.comparison", "complete raw slots or comparison missing"))
  else {
    const recomputed = recomputeComparison(manifest, slots, slotFileHashes).comparison
    const comparison = await readJson(comparisonFile)
    checks.push(isDeepStrictEqual(comparison, recomputed) && recomputed.result === "PASS" ? pass("performance.comparison", "hashes, formulas, metrics, ratios, checkpoint, RSS, gates, and result recomputed") : fail("performance.comparison", "comparison differs from raw recomputation"))
  }
  return checks
}

export const receiptPath = (root: string, task: number) => path.join(root, `../task-${task}-session-part-normalization.json`)
async function final(root: string) {
  const checks = await performance(root)
  const receipts = await Promise.all(Array.from({ length: 8 }, (_, index) => Bun.file(receiptPath(root, index + 1)).exists()))
  checks.push(receipts.every(Boolean) ? pass("final.receipts", "all task receipts exist") : fail("final.receipts", "task receipt missing"))
  const changeFile = path.join(root, "change-manifest.json")
  if (!(await Bun.file(changeFile).exists())) checks.push(fail("final.change-manifest", "change-manifest.json is missing"))
  else {
    const change = Schema.decodeUnknownSync(
      Schema.Struct({
        planStartCommit: Schema.String,
        baselineCommit: Schema.String,
        candidateCommit: Schema.String,
        cleanupCommit: Schema.String,
      }),
    )(await readJson(changeFile))
    checks.push(new Set(Object.values(change)).size === 4 ? pass("final.commits", "four commit boundaries are distinct") : fail("final.commits", "commit boundaries are incomplete"))
  }
  return checks
}

export async function checkEvidence(input: { readonly root: string; readonly phase: typeof Phase.Type; readonly injectFailingGate?: boolean }) {
  const checks = input.phase === "performance" ? await performance(input.root) : await final(input.root)
  if (input.injectFailingGate) checks.push(fail("injected.hard-gate", "isolated synthetic failure"))
  const ids = checks.map((check) => check.id)
  if (new Set(ids).size !== ids.length) throw new BenchmarkContractError("Evidence check IDs must be unique")
  return {
    schemaVersion: 1,
    phase: input.phase,
    result: checks.every((check) => check.status === "PASS") ? ("PASS" as const) : ("FAIL" as const),
    checks,
  }
}

async function main() {
  const argv = process.argv.slice(2),
    root = option(argv, "--root")
  if (!root) throw new BenchmarkContractError("--root is required")
  const evidence = await checkEvidence({
    root: path.resolve(root),
    phase: Schema.decodeUnknownSync(Phase)(option(argv, "--phase") ?? "performance"),
    injectFailingGate: argv.includes("--inject-failing-gate"),
  })
  console.log(JSON.stringify(evidence))
  return evidence.result === "PASS" ? 0 : 1
}

if (import.meta.main)
  main().then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(error instanceof Error ? error.message : error)
      process.exit(1)
    },
  )
