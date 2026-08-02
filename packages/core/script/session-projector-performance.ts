#!/usr/bin/env bun

import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { isDeepStrictEqual } from "node:util"
import { asc, eq } from "drizzle-orm"
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventTable } from "@opencode-ai/core/event/sql"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionMessageTable } from "@opencode-ai/core/session/sql"
import {
  assistantRow,
  baselineEndToEndNs,
  baselineP95Ns,
  bucketSizes,
  encodeAssistant,
  eventSequence,
  makeFixture,
  measuredSamples,
  publishUpdate,
  readAssistant,
  removeTemporary,
  setupFixture,
  setupProject,
  type DatabaseService,
  type Fixture,
  type PartCount,
} from "./session-projector-performance-fixture"

function account(db: DatabaseService, events: EventV2.Interface, fixture: Fixture) {
  return Effect.gen(function* () {
    const rowCharacters: number[] = []
    const observe = readAssistant(db, fixture).pipe(
      Effect.tap((assistant) =>
        Effect.sync(() => rowCharacters.push(JSON.stringify(encodeAssistant(assistant)).length)),
      ),
      Effect.asVoid,
    )
    yield* publishUpdate(events, fixture, observe)
    const final = yield* readAssistant(db, fixture)
    if (!isDeepStrictEqual(final, fixture.expected))
      return yield* Effect.die("Projected assistant differs from expected state")
    const stored = yield* db
      .select()
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, fixture.sessionID))
      .orderBy(asc(EventTable.seq))
      .all()
      .pipe(Effect.orDie)
    if (stored.length !== eventSequence.length) return yield* Effect.die("Unexpected projected event count")
    yield* events.remove(fixture.sessionID)
    const initial = assistantRow(fixture)
    yield* db
      .update(SessionMessageTable)
      .set({ type: initial.type, time_created: initial.time_created, data: initial.data })
      .where(eq(SessionMessageTable.id, fixture.assistantMessageID))
      .run()
      .pipe(Effect.orDie)
    yield* events.replayAll(
      stored.map((event) => ({
        id: event.id,
        type: event.type,
        seq: event.seq,
        aggregateID: event.aggregate_id,
        data: event.data,
      })),
    )
    if (!isDeepStrictEqual(yield* readAssistant(db, fixture), fixture.expected))
      return yield* Effect.die("Replayed assistant differs from expected state")
    return {
      finalRowJsonCharacters: JSON.stringify(encodeAssistant(final)).length,
      projectedEventCount: stored.length,
      estimatedFullRowEncodedCharactersWritten: rowCharacters.reduce((total, characters) => total + characters, 0),
    }
  })
}

const benchmark = Effect.gen(function* () {
  const { db } = yield* Database.Service
  const events = yield* EventV2.Service
  yield* setupProject(db)
  const warmups = bucketSizes.map((partCount) => makeFixture(partCount, "warmup"))
  const measured = Array.from({ length: measuredSamples }, (_, sample) =>
    bucketSizes.map((partCount) => makeFixture(partCount, `sample_${sample}`)),
  )
  const accounting = bucketSizes.map((partCount) => makeFixture(partCount, "accounting"))
  yield* Effect.forEach([...warmups, ...measured.flat(), ...accounting], (fixture) => setupFixture(db, fixture), {
    discard: true,
  })
  yield* Effect.forEach(warmups, (fixture) => publishUpdate(events, fixture), { discard: true })
  const samples: Record<PartCount, number[]> = { 32: [], 128: [], 512: [] }
  for (const [iteration, fixtures] of measured.entries()) {
    for (const fixture of iteration % 2 === 0 ? fixtures : fixtures.toReversed()) {
      const start = Bun.nanoseconds()
      yield* publishUpdate(events, fixture)
      samples[fixture.partCount].push(Bun.nanoseconds() - start)
    }
  }
  const accountingResults = yield* Effect.forEach(accounting, (fixture) => account(db, events, fixture))
  const buckets = bucketSizes.map((partCount, index) => {
    const sorted = samples[partCount].toSorted((left, right) => left - right)
    return {
      parts: partCount,
      samplesNs: samples[partCount],
      medianNs: sorted[4] ?? 0,
      p95Ns: sorted[8] ?? 0,
      ...accountingResults[index],
    }
  })
  const bucket128 = buckets.find((bucket) => bucket.parts === 128)
  const bucket512 = buckets.find((bucket) => bucket.parts === 512)
  if (!bucket128 || !bucket512) return yield* Effect.die("Required benchmark buckets are missing")
  const medianGrowth = bucket512.medianNs / bucket128.medianNs
  const encodedGrowth =
    bucket512.estimatedFullRowEncodedCharactersWritten / bucket128.estimatedFullRowEncodedCharactersWritten
  const scaleMaterial = medianGrowth >= 3 || encodedGrowth >= 3
  const absoluteMaterial =
    bucket512.medianNs >= 2_000_000 ||
    bucket512.estimatedFullRowEncodedCharactersWritten >= bucket512.finalRowJsonCharacters * 2
  return {
    benchmark: "SessionProjector durable assistant tool lifecycle",
    runtime: { bun: Bun.version, platform: process.platform, arch: process.arch },
    timer: "Bun.nanoseconds",
    baseline: { medianNs: baselineEndToEndNs, p95Ns: baselineP95Ns },
    warmupsPerBucket: 1,
    measuredSamplesPerBucket: measuredSamples,
    bucketOrder: "alternating forward/reverse",
    fixtureParts: "repeating text/reasoning/completed-tool",
    eventSequence,
    buckets,
    scaling: { from128To512: { partGrowth: 4, medianGrowth, encodedCharacterGrowth: encodedGrowth } },
    materiality: {
      scale: "medianGrowth >= 3 OR encodedCharacterGrowth >= 3",
      absolute: "median512 >= 2ms OR encodedCharacters512 >= 2x finalRowCharacters512",
      scaleMaterial,
      absoluteMaterial,
      established: scaleMaterial && absoluteMaterial,
    },
    verification: { finalDecodedAssistant: true, replayedAssistant: true },
  }
})

const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "opencode-session-projector-performance-"))
try {
  const layer = AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node, SessionProjector.node]), [
    [Database.node, Database.layerFromPath(path.join(temporary, "benchmark.sqlite"))],
  ])
  console.log(JSON.stringify(await Effect.runPromise(benchmark.pipe(Effect.scoped, Effect.provide(layer)))))
} finally {
  await removeTemporary(temporary)
}
