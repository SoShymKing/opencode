import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { describe, expect, test } from "bun:test"
import { DateTime, Effect, Schema } from "effect"
import { Database } from "@opencode/core/database/database"
import { SessionStore } from "@opencode/core/session/store"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Global } from "@opencode/util/global"
import { EffectDrizzleSqlite } from "../src/database/drizzle.js"
import { sqliteLayer } from "../src/database/sqlite.bun.js"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Agent } from "@opencode/schema/agent"
import { BenchmarkContractError, measuredSamples, removeTemporary, summarizeSamples } from "../script/session-projector-performance-fixture"
import { fixedOrder, harnessFiles } from "../script/session-storage-benchmark-slot"
import { candidateQueryPlansIndexed, comparisonEvidence, recomputeComparison } from "../script/session-storage-benchmark-compare"
import { checkEvidence } from "../script/session-storage-evidence-check"
import { applyBenchmarkMigration, benchmarkMigrations, growthInputs, migrationRows, normalizationMigrationID, partDensities, queryPlansMatchStatements, runBenchmarkWorkers, setupPredecessor, validateMigration } from "../script/session-storage-migration-performance-fixture"
import { executeRead, readWorkloads, setupReadFixture, validateRead } from "../script/session-storage-read-performance-fixture"

const FailedArtifact = Schema.Struct({
  failed: Schema.Literal(true),
  validation: Schema.Struct({ status: Schema.Literal("FAIL") }),
  aggregates: Schema.Array(Schema.Unknown),
  processes: Schema.Array(
    Schema.Struct({
      failed: Schema.Literal(true),
      buckets: Schema.NonEmptyArray(Schema.Struct({ failed: Schema.Literal(true), samplesNs: Schema.Array(Schema.Number), error: Schema.String })),
    }),
  ),
})

async function negative(script: string, flag: string) {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-storage-negative-"))
  const output = path.join(temporary, "negative.json")
  try {
    const child = Bun.spawn([process.execPath, "run", script, "--processes", "5", "--warmups", "5", "--samples", "30", "--output", output, flag], {
      cwd: path.resolve(import.meta.dir, ".."),
      stdout: "pipe",
      stderr: "pipe",
      signal: AbortSignal.timeout(120_000),
    })
    const [exit, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()])
    expect(exit, stderr).not.toBe(0)
    const artifact = Schema.decodeUnknownSync(Schema.fromJsonString(FailedArtifact))(await Bun.file(output).text())
    expect(artifact.processes).toHaveLength(5)
    expect(artifact.aggregates).toHaveLength(0)
    expect(artifact.processes.flatMap((item) => item.buckets).every((bucket) => bucket.samplesNs.length === 0)).toBe(true)
    const reason = script.includes("migration") ? "Cannot normalize assistant message" : flag === "--inject-invalid-cursor" ? "payload or order mismatch" : "MessageDecodeError"
    expect(artifact.processes.flatMap((item) => item.buckets).every((bucket) => bucket.error.includes(reason))).toBe(true)
  } finally {
    await removeTemporary(temporary)
  }
}

async function fabricatedEvidence(root: string) {
  await fs.mkdir(path.join(root, "raw"), { recursive: true })
  await Bun.write(
    path.join(root, "benchmark-manifest.json"),
    JSON.stringify({
      storageContract: "native2-session-parts",
      baselineStatus: "available",
      baselineCommit: "baseline",
      candidateCommit: "candidate",
      baselineWorktree: "baseline",
      candidateWorktree: "candidate",
      evidenceRoot: root,
      harnessFiles,
      harnessSha256: "fake",
      fixedOrder,
    }),
  )
  for (const slot of fixedOrder)
    await Bun.write(
      path.join(root, "raw", `${slot}.json`),
      JSON.stringify({
        slot,
        harnessSha256: "fake",
        rss: { intervalMs: 10 },
        write: { failed: false, buckets: [] },
        read: { failed: false, buckets: [] },
        migration: { failed: false, buckets: [] },
      }),
    )
  await Bun.write(path.join(root, "comparison.json"), JSON.stringify({ result: "PASS", fixedOrder, harnessSha256: "fake", gates: { fabricated: true } }))
}

describe("session storage performance fixture", () => {
  test("rejects comparison when a native baseline is not available", () => {
    expect(() => recomputeComparison({ storageContract: "native2-session-parts", baselineStatus: "not-available", baselineWorktree: "", candidateWorktree: "", baselineCommit: "", candidateCommit: "", evidenceRoot: "", harnessFiles, harnessSha256: "", fixedOrder }, [], {})).toThrow("Native comparison baseline is not available")
  })

  test("reads native normalized fixtures through production readers", async () => {
    await Effect.runPromise(Effect.gen(function* () {
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      for (const workload of readWorkloads.filter((item, index, all) => all.findIndex((other) => other.name === item.name) === index)) {
        const fixture = yield* setupReadFixture(database.db, workload, workload.name)
        const result = yield* executeRead(database.db, store, fixture, workload)
        expect(validateRead(result, fixture, workload)).toBe(true)
        if (workload.name === "runner-context" && result && "entries" in result) {
          expect(() => validateRead({
            ...result,
            rejectedAttachments: [{ source: "user", messageID: SessionMessage.ID.make("msg_unexpected_attachment"), fileIndex: 0 }],
          }, fixture, workload)).toThrow("runner-context/200 payload or order mismatch")
        }
      }
    }).pipe(Effect.provide(LayerNode.compile(LayerNode.group([Database.node, SessionStore.node]))), Effect.scoped))
  })

  test("runs only native normalization and reconstructs migrated children", async () => {
    expect(benchmarkMigrations().map((migration) => migration.id)).toEqual([normalizationMigrationID])
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* setupPredecessor(db, 100)
      yield* applyBenchmarkMigration(db)
      const result = yield* validateMigration(db, 100)
      expect(result.parents).toBe(100)
      expect(result.children).toBe(1175)
    }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provideService(Global.Service, Global.make()), Effect.scoped))
  })

  test("freezes every read and migration bucket", () => {
    expect(readWorkloads).toEqual([
      { name: "page", size: 50 },
      { name: "page", size: 200 },
      { name: "single-message", size: 32 },
      { name: "single-message", size: 128 },
      { name: "single-message", size: 512 },
      { name: "single-message", size: 2048 },
      { name: "runner-context", size: 200 },
      { name: "interrupted-tools", size: 32 },
      { name: "interrupted-tools", size: 128 },
      { name: "interrupted-tools", size: 512 },
      { name: "interrupted-tools", size: 2048 },
      { name: "revert-envelope-scan", size: 200 },
    ])
    expect(migrationRows).toEqual([0, 100, 1_000, 10_000])
    expect(partDensities).toEqual([0, 3, 12, 32])
    expect(growthInputs(10_000)).toEqual({
      assistants: 10_000,
      densitySeed: 0,
      partDensities,
      totalParts: 117_500,
      averageParts: 11.75,
    })
  })

  test("locks exact sample formulas and frozen comparison order", () => {
    const samples = Array.from({ length: measuredSamples }, (_, index) => index + 1)
    expect(summarizeSamples(samples)).toEqual({ medianNs: 15.5, p95Ns: 29 })
    expect(fixedOrder).toEqual(["B1", "C1", "B2", "C2", "B3", "C3", "B4", "C4", "B5", "C5"])
    expect(harnessFiles).toHaveLength(11)
    expect(harnessFiles.join("\n")).toBe([...harnessFiles].toSorted().join("\n"))
  })

  test("rejects an invalid cursor without a successful timing", async () => {
    await negative("script/session-storage-read-performance.ts", "--inject-invalid-cursor")
  }, 120_000)

  test("rejects a malformed read row without a successful timing", async () => {
    await negative("script/session-storage-read-performance.ts", "--inject-malformed-row")
  }, 120_000)

  test("rejects a malformed migration row without a successful timing", async () => {
    await negative("script/session-storage-migration-performance.ts", "--inject-malformed-row")
  }, 120_000)

  test("injects an isolated evidence failure without modifying evidence", async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-evidence-negative-"))
    const sentinel = path.join(temporary, "sentinel.json")
    try {
      await Bun.write(sentinel, '{"authoritative":true}\n')
      const before = Bun.hash(await Bun.file(sentinel).arrayBuffer())
      const child = Bun.spawn([process.execPath, "run", "script/session-storage-evidence-check.ts", "--root", temporary, "--phase", "performance", "--inject-failing-gate"], {
        cwd: path.resolve(import.meta.dir, ".."),
        stdout: "pipe",
        stderr: "pipe",
        signal: AbortSignal.timeout(30_000),
      })
      expect(await child.exited).not.toBe(0)
      expect(Bun.hash(await Bun.file(sentinel).arrayBuffer())).toBe(before)
    } finally {
      await removeTemporary(temporary)
    }
  })

  test("rejects fabricated evidence with empty buckets and unpinned revisions", async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-evidence-fabricated-"))
    try {
      await fabricatedEvidence(temporary)
      expect((await checkEvidence({ root: temporary, phase: "performance" })).result).toBe("FAIL")
    } finally {
      await removeTemporary(temporary)
    }
  })

  test("resolves final task receipts beside the stable evidence root", async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-evidence-receipts-"))
    const root = path.join(temporary, "session-part-normalization")
    try {
      await fabricatedEvidence(root)
      for (let index = 1; index <= 8; index++) await Bun.write(path.join(temporary, `task-${index}-session-part-normalization.json`), "{}")
      await Bun.write(path.join(root, "change-manifest.json"), JSON.stringify({ planStartCommit: "a", baselineCommit: "b", candidateCommit: "c", cleanupCommit: "d" }))
      const evidence = await checkEvidence({ root, phase: "final" })
      expect(evidence.checks.find((check) => check.id === "final.receipts")?.status).toBe("PASS")
    } finally {
      await removeTemporary(temporary)
    }
  })

  test("rejects wrong interrupted-tool and revert payloads", () => {
    const sessionID = Session.ID.make("ses_wrong_payload")
    const assistant = SessionMessage.Assistant.make({
      id: SessionMessage.ID.make("msg_wrong_payload"),
      type: "assistant",
      agent: Agent.ID.make("build"),
      model: { id: Model.ID.make("model"), providerID: Provider.ID.make("provider") },
      content: [],
      time: { created: DateTime.makeUnsafe(0) },
    })
    const fixture = { sessionID, messages: [assistant], boundaryID: assistant.id }
    expect(() => validateRead([1, 2, 3, 4], fixture, { name: "interrupted-tools", size: 32 })).toThrow()
    expect(() =>
      validateRead(
        Array.from({ length: 200 }, () => "wrong"),
        fixture,
        { name: "revert-envelope-scan", size: 200 },
      ),
    ).toThrow()
  })

  test("rejects candidate query scans and plans detached from statements", () => {
    const statement = 'select * from "session_message" where "session_id" = ?'
    const scan = { name: "page/50/0", statement, bindings: ["ses_plan"], details: [{ detail: "SCAN session_message" }] }
    expect(queryPlansMatchStatements([statement], [{ ...scan, statement: "select 1" }])).toBe(false)
    expect(candidateQueryPlansIndexed([{ workload: "page", size: 50, queryStatements: [statement], queryPlans: [scan] }])).toBe(false)
  })

  test("records every slot write/read checkpoint and per-write-bucket RSS", () => {
    const storage = { dbBytesBefore: 1, walBytesBefore: 2, dbBytesBeforeCheckpoint: 3, walBytesBeforeCheckpoint: 5, walDelta: 3, dbBytesAfterCheckpoint: 4, walBytesAfterCheckpoint: 0, pageCount: 1, freelistCount: 0 }
    const write = Array.from({ length: 10 }, (_, index) => ({ workload: `write-${index}`, siblings: 512, storage, rss: { intervalMs: 10, samples: [10, 20], peakRssBytes: 20 } }))
    const read = Array.from({ length: 12 }, (_, index) => ({ workload: `read-${index}`, size: index, storage, peakRssBytes: 30 }))
    const slots = fixedOrder.map((slot) => ({ slot, write: { buckets: write }, read: { buckets: read }, migration: { storage, buckets: [] } }))
    const evidence = comparisonEvidence(slots)
    expect(Object.values(evidence.checkpoint).every((item) => item.write.length === 10 && item.read.length === 12 && item.migration !== undefined)).toBe(true)
    expect(Object.values(evidence.rss).every((item) => item.write.length === 10 && item.write.every((bucket) => bucket.peakRssBytes === Math.max(...bucket.samples)))).toBe(true)
  })

  test("resolves final receipts without workspace evidence", async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-evidence-isolated-"))
    const root = path.join(temporary, "session-part-normalization")
    try {
      await fabricatedEvidence(root)
      for (let task = 1; task <= 8; task++) await Bun.write(path.join(temporary, `task-${task}-session-part-normalization.json`), JSON.stringify({ schemaVersion: 1, task, name: "session-part-normalization", result: "PASS" }))
      await Bun.write(path.join(root, "change-manifest.json"), JSON.stringify({ planStartCommit: "a", baselineCommit: "b", candidateCommit: "c", cleanupCommit: "d" }))
      const evidence = await checkEvidence({ root, phase: "final" })
      expect(evidence.checks.find((check) => check.id === "final.receipts")?.status).toBe("PASS")
    } finally {
      await removeTemporary(temporary)
    }
  })

  test("rejects a nonzero worker that claims success", async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-worker-contract-"))
    const worker = path.join(temporary, "worker.ts")
    try {
      await Bun.write(worker, "console.log('RESULT {\"failed\":false}')\nprocess.exit(1)\n")
      await expect(runBenchmarkWorkers({ script: worker, extra: [], decode: Schema.decodeUnknownSync(Schema.Struct({ failed: Schema.Boolean })) })).rejects.toBeInstanceOf(BenchmarkContractError)
    } finally {
      await removeTemporary(temporary)
    }
  })

  test("enforces matching worker exit and failed states", async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-worker-truth-"))
    const decode = Schema.decodeUnknownSync(Schema.Struct({ failed: Schema.Boolean }))
    try {
      for (const item of [
        { failed: false, exit: 0, rejected: false },
        { failed: true, exit: 0, rejected: true },
        { failed: true, exit: 1, rejected: false },
      ] as const) {
        const worker = path.join(temporary, `worker-${item.failed}-${item.exit}.ts`)
        await Bun.write(worker, `console.log('RESULT ${JSON.stringify({ failed: item.failed })}')\nprocess.exit(${item.exit})\n`)
        const result = runBenchmarkWorkers({ script: worker, extra: [], decode })
        if (item.rejected) await expect(result).rejects.toBeInstanceOf(BenchmarkContractError)
        else expect((await result).processes).toHaveLength(5)
      }
    } finally {
      await removeTemporary(temporary)
    }
  })
})
