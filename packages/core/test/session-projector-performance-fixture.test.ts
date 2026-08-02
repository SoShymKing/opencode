import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import {
  centralSummary,
  measuredSamples,
  processCount,
  removeTemporary,
  siblingCounts,
  summarizeSamples,
  workloadEventSequences,
  workloadNames,
} from "../script/session-projector-performance-fixture"

const Rss = Schema.Struct({
  intervalMs: Schema.Literal(10),
  samples: Schema.NonEmptyArray(Schema.Number),
  peakRssBytes: Schema.Number,
}).pipe(Schema.check(Schema.makeFilter((rss) => rss.peakRssBytes === Math.max(...rss.samples))))
const SuccessfulBucket = Schema.Struct({
  workload: Schema.Literals(workloadNames),
  siblings: Schema.Literals(siblingCounts),
  failed: Schema.Literal(false),
  samplesNs: Schema.Array(Schema.Number),
  medianNs: Schema.Number,
  p95Ns: Schema.Number,
  rss: Rss,
  validation: Schema.Struct({ status: Schema.Literal("PASS") }),
})
const NormalRun = Schema.Struct({
  failed: Schema.Literal(false),
  validation: Schema.Struct({ status: Schema.Literal("PASS") }),
  processes: Schema.Array(
    Schema.Struct({ process: Schema.Number, failed: Schema.Literal(false), buckets: Schema.Array(SuccessfulBucket) }),
  ),
  aggregates: Schema.Array(
    Schema.Struct({
      workload: Schema.Literals(workloadNames),
      siblings: Schema.Literals(siblingCounts),
      centralMedianNs: Schema.Number,
      centralP95Ns: Schema.Number,
    }),
  ),
})
const FailedBucket = Schema.Struct({
  failed: Schema.Literal(true),
  samplesNs: Schema.Array(Schema.Number),
})
const FailedRun = Schema.Struct({
  failed: Schema.Literal(true),
  validation: Schema.Struct({ status: Schema.Literal("FAIL") }),
  aggregates: Schema.Array(Schema.Unknown),
  processes: Schema.Array(
    Schema.Struct({
      failed: Schema.Literal(true),
      buckets: Schema.NonEmptyArray(FailedBucket),
    }),
  ),
})

describe("session projector performance fixture", () => {
  test("defines every targeted write workload at 512 and 2048 siblings", () => {
    expect(siblingCounts).toEqual([512, 2048])
    expect(workloadNames).toEqual(["tool", "text", "reasoning", "envelope-only", "duplicate-id-latest-match"])
    expect(Object.keys(workloadEventSequences)).toEqual([...workloadNames])
    expect(workloadEventSequences.tool).toHaveLength(5)
    expect(workloadEventSequences.text).toHaveLength(2)
    expect(workloadEventSequences.reasoning).toHaveLength(2)
    expect(workloadEventSequences["envelope-only"]).toHaveLength(1)
    expect(workloadEventSequences["duplicate-id-latest-match"]).toHaveLength(1)
  })

  test("locks the bounded normal output contract and aggregate formulas", () => {
    const processes = Array.from({ length: processCount }, (_, processIndex) => ({
      process: processIndex + 1,
      failed: false as const,
      buckets: workloadNames.flatMap((workload, workloadIndex) =>
        siblingCounts.map((siblings, siblingIndex) => {
          const samplesNs = Array.from(
            { length: measuredSamples },
            (_, sampleIndex) => workloadIndex * 10_000 + siblingIndex * 1_000 + processIndex * 100 + sampleIndex + 1,
          )
          const rssStart = processIndex * 100_000 + workloadIndex * 10_000 + siblingIndex * 1_000
          return {
            workload,
            siblings,
            failed: false as const,
            samplesNs,
            ...summarizeSamples(samplesNs),
            rss: { intervalMs: 10 as const, samples: [rssStart + 1, rssStart + 2], peakRssBytes: rssStart + 2 },
            validation: { status: "PASS" as const },
          }
        }),
      ),
    }))
    const aggregates = workloadNames.flatMap((workload) =>
      siblingCounts.map((siblings) => ({
        workload,
        siblings,
        ...centralSummary(
          processes.flatMap((process) =>
            process.buckets
              .filter((bucket) => bucket.workload === workload && bucket.siblings === siblings)
              .map((bucket) => ({ medianNs: bucket.medianNs, p95Ns: bucket.p95Ns })),
          ),
        ),
      })),
    )
    const result = Schema.decodeUnknownSync(NormalRun)({
      failed: false,
      validation: { status: "PASS" },
      processes,
      aggregates,
    })

    expect(result.processes).toHaveLength(5)
    expect(result.processes.every((process) => process.buckets.length === 10)).toBe(true)
    expect(result.processes.every((process) => process.buckets.every((bucket) => bucket.samplesNs.length === 30))).toBe(
      true,
    )
    expect(result.aggregates).toHaveLength(10)
    const rss = result.processes.flatMap((process) => process.buckets.map((bucket) => bucket.rss))
    expect(rss).toHaveLength(50)
    expect(rss.every((sample) => sample.intervalMs === 10 && sample.peakRssBytes === Math.max(...sample.samples))).toBe(true)
    expect(new Set(rss.map((sample) => sample.samples[0])).size).toBe(50)
    const tool512 = result.processes.map((process) =>
      process.buckets.find((bucket) => bucket.workload === "tool" && bucket.siblings === 512),
    )
    expect(tool512.map((bucket) => bucket?.medianNs)).toEqual([15.5, 115.5, 215.5, 315.5, 415.5])
    expect(tool512.map((bucket) => bucket?.p95Ns)).toEqual([29, 129, 229, 329, 429])
    expect(result.aggregates.find((aggregate) => aggregate.workload === "tool" && aggregate.siblings === 512)).toEqual({
      workload: "tool",
      siblings: 512,
      centralMedianNs: 215.5,
      centralP95Ns: 229,
    })
  })

  test("rejects missing, empty, or inconsistent bucket RSS evidence", () => {
    const decode = Schema.decodeUnknownSync(Rss)
    expect(() => decode({ intervalMs: 10, samples: [1], peakRssBytes: 0 })).toThrow()
    expect(() => decode({ intervalMs: 10, samples: [], peakRssBytes: 0 })).toThrow()
    expect(() => decode({ intervalMs: 10, samples: [1] })).toThrow()
  })

  test("fails a mismatched expected assistant without recording a successful timing", async () => {
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-projector-negative-"))
    const output = path.join(temporary, "negative.json")
    try {
      const child = Bun.spawn(
        [
          process.execPath,
          "run",
          "script/session-projector-performance.ts",
          "--processes",
          "5",
          "--warmups",
          "5",
          "--samples",
          "30",
          "--output",
          output,
          "--inject-mismatched-expected",
        ],
        { cwd: path.resolve(import.meta.dir, ".."), stdout: "pipe", stderr: "pipe" },
      )
      const exitCode = await child.exited
      const stderr = await new Response(child.stderr).text()
      expect(exitCode, stderr).not.toBe(0)
      const json = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)(await Bun.file(output).text())
      const result = Schema.decodeUnknownSync(FailedRun)(json)
      expect(result.processes).toHaveLength(5)
      expect(result.validation.status).toBe("FAIL")
      expect(result.aggregates).toHaveLength(0)
      const failedBuckets = result.processes.flatMap((item) => item.buckets)
      expect(failedBuckets.length).toBeGreaterThan(0)
      expect(failedBuckets.every((bucket) => bucket.failed && bucket.samplesNs.length === 0)).toBe(true)
    } finally {
      await removeTemporary(temporary)
    }
  }, 120_000)
})
