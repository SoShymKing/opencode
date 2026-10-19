import { expect, test } from "bun:test"
import { randomUUID } from "node:crypto"
import { mkdir, rm } from "node:fs/promises"
import { basename, join } from "node:path"
import { Effect } from "effect"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseLegacyV01 } from "../src/database/legacy-v01"
import { DatabaseReplacement } from "../src/database/replacement"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { WsNative } from "../src/database/ws-native"
import { setupWs } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

async function measuredReplacement() {
  const signature = "function validateStage(filename: string, version: 1 | 2) {"
  const source = await Bun.file(new URL("../src/database/replacement.ts", import.meta.url)).text()
  expect(source).toContain(signature)
  const directory = new URL("../.cache/", import.meta.url)
  await mkdir(directory, { recursive: true })
  const adapter = new URL(`replacement-validation-${randomUUID()}.ts`, directory)
  await Bun.write(adapter, `export const observations: { stages: string[]; afterStage?: (filename: string) => Promise<void>; afterRename?: (filename: string) => Promise<void> } = { stages: [] }\n` + source
    .replace('export * as DatabaseReplacement from "./replacement"', "")
    .replaceAll(/"\.\/([^"\n]+)"/g, '"../src/database/$1"')
    .replace(signature, `${signature}\n  observations.stages.push(filename)`)
    .replace("yield* validateStage(staged, marker.version)", "yield* validateStage(staged, marker.version)\n    yield* Effect.promise(() => observations.afterStage?.(staged) ?? Promise.resolve())")
    .replace("yield* DatabaseArtifact.replace(staged, target)", "yield* DatabaseArtifact.replace(staged, target)\n      yield* Effect.promise(() => observations.afterRename?.(target) ?? Promise.resolve())"))
  try {
    const loaded: {
      activate: typeof DatabaseReplacement.activate
      observations: { stages: string[]; afterStage?: (filename: string) => Promise<void>; afterRename?: (filename: string) => Promise<void> }
    } = await import(adapter.href)
    return loaded
  } finally {
    await rm(adapter)
  }
}

async function fixture(directory: string, version: 1 | 2, phase: "prepared" | "backup_ready" | "postrename") {
  const target = join(directory, "target.db")
  const staged = join(directory, "staged.db")
  const backup = join(directory, "backup.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory, lineage: "v02" })
    yield* DatabaseArtifact.snapshot(db, staged)
    if (phase !== "prepared") yield* DatabaseArtifact.snapshot(db, backup)
  }).pipe(Effect.provide(sqliteLayer({ filename: target })), Effect.scoped))
  if (version === 2) await DatabaseLegacyV01.useDatabase(staged, false, WsNative.convert).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)
  const artifact = { name: basename(staged), ...(await DatabaseArtifact.digest(staged).pipe(Effect.runPromise)) }
  const preimage = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
  const base = version === 1
    ? { format: "opencode.database.replacement", version, staged: artifact } as const
    : { format: "opencode.database.replacement", version, stageFormat: "native", staged: artifact, targetPreimage: preimage } as const
  const marker = phase === "prepared" ? { ...base, phase } as const : {
    ...base, phase: "backup_ready", backup: { name: basename(backup), ...(await DatabaseArtifact.digest(backup).pipe(Effect.runPromise)) },
  } as const
  await DatabaseArtifact.write(`${target}.replacement.json`, JSON.stringify(marker)).pipe(Effect.runPromise)
  if (phase === "postrename") await DatabaseArtifact.replace(staged, target).pipe(Effect.runPromise)
  return { target, staged, backup, artifact, preimage, marker }
}

for (const version of [1, 2] as const) for (const phase of ["prepared", "backup_ready", "postrename"] as const) {
  test(`activation validates v${version} semantics once in ${phase}`, async () => {
    await using temp = await tmpdir()
    const setup = await fixture(temp.path, version, phase)
    const measured = await measuredReplacement()
    await measured.activate(setup.target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)
    expect(measured.observations.stages).toEqual([phase === "postrename" ? setup.target : setup.staged])
    expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual({ sha256: setup.artifact.sha256, bytes: setup.artifact.bytes })
    expect(await Bun.file(`${setup.target}.replacement.json`).exists()).toBe(false)
  })
}

for (const fault of ["staged", "staged-semantics", "backup", "target-hash", "target-semantics"] as const) {
  test(`activation rejects corrupt ${fault} without consuming its recovery marker`, async () => {
    await using temp = await tmpdir()
    const setup = await fixture(temp.path, 2, fault.startsWith("staged") ? "prepared" : fault === "backup" ? "backup_ready" : "postrename")
    const damaged = fault.startsWith("staged") ? setup.staged : fault === "backup" ? setup.backup : setup.target
    await DatabaseLegacyV01.useDatabase(damaged, false, (db) => db.run("UPDATE session_message SET data = '{}' ")).pipe(Effect.runPromise)
    if (fault === "backup" || fault.endsWith("semantics")) {
      const artifact = { name: basename(fault === "backup" ? setup.backup : setup.staged), ...(await DatabaseArtifact.digest(damaged).pipe(Effect.runPromise)) }
      await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify({ ...setup.marker, [fault === "backup" ? "backup" : "staged"]: artifact })).pipe(Effect.runPromise)
    }
    const before = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
    const measured = await measuredReplacement()
    await expect(measured.activate(setup.target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).rejects.toThrow()
    expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(before)
    expect(await Bun.file(`${setup.target}.replacement.json`).exists()).toBe(true)
    if (fault === "target-semantics") expect(measured.observations.stages).toEqual([setup.target])
    if (fault === "staged-semantics") expect(measured.observations.stages).toEqual([setup.staged])
  })
}

test("same-call semantic proof cannot bypass a changed staged digest", async () => {
  await using temp = await tmpdir()
  const setup = await fixture(temp.path, 2, "prepared")
  const measured = await measuredReplacement()
  measured.observations.afterStage = (filename) => DatabaseLegacyV01.useDatabase(filename, false, (db) => db.run("UPDATE session_v2 SET title = 'changed after validation' ")).pipe(Effect.asVoid, Effect.runPromise)
  await expect(measured.activate(setup.target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).rejects.toThrow("replacement artifact is corrupt")
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(setup.preimage)
  expect(await Bun.file(`${setup.target}.replacement.json`).exists()).toBe(true)
})

test("same-call semantic proof cannot bypass the final target digest", async () => {
  await using temp = await tmpdir()
  const setup = await fixture(temp.path, 2, "prepared")
  const measured = await measuredReplacement()
  measured.observations.afterRename = (filename) => DatabaseLegacyV01.useDatabase(filename, false, (db) => db.run("UPDATE session_v2 SET title = 'changed after rename' ")).pipe(Effect.asVoid, Effect.runPromise)
  await expect(measured.activate(setup.target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).rejects.toThrow("replacement artifact is corrupt")
  const marker = await DatabaseReplacement.readMarker(`${setup.target}.replacement.json`).pipe(Effect.runPromise)
  if (marker.phase !== "backup_ready" || !marker.backup) throw new Error("Missing recovery backup")
  await DatabaseReplacement.validate(join(temp.path, marker.backup.name), marker.backup).pipe(Effect.runPromise)
  expect(measured.observations.stages).toEqual([setup.staged])
})

test("a staged sidecar cannot contribute to a same-call semantic proof", async () => {
  await using temp = await tmpdir()
  const setup = await fixture(temp.path, 2, "prepared")
  await DatabaseArtifact.write(`${setup.staged}-wal`, "").pipe(Effect.runPromise)
  const measured = await measuredReplacement()
  await expect(measured.activate(setup.target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).rejects.toThrow("database sidecar is unsafe")
  expect(measured.observations.stages).toEqual([])
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(setup.preimage)
})
