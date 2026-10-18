import { expect, test } from "bun:test"
import { basename, join } from "node:path"
import { readdir, rename, symlink } from "node:fs/promises"
import { mkdirSync, readFileSync, renameSync, watch, writeFileSync } from "node:fs"
import { Effect, Schema } from "effect"
import { Database } from "../src/database/database"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseLegacyV01 } from "../src/database/legacy-v01"
import { DatabaseMigration } from "../src/database/migration"
import { DatabaseReplacement } from "../src/database/replacement"
import { WsNativeFile } from "../src/database/ws-native-file"
import { createStartupDatabase } from "./fixture/database-startup"
import { useDatabase } from "./fixture/database-migration"
import { setupWs, sessionID } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

function open(target: string) {
  return Effect.gen(function* () {
    const service = yield* Database.Service
    return yield* service.db.all<{ id: string }>("SELECT id FROM session_v2")
  }).pipe(Effect.provide(Database.layerFromPath(target)), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
}

async function fixture(directory: string, native: boolean) {
  const target = join(directory, "target.db")
  const staged = join(directory, "stage.db")
  await useDatabase(target, (db) => setupWs(db, { directory, lineage: "v02" }))
  await useDatabase(staged, (db) => Effect.gen(function* () {
    yield* setupWs(db, { directory, lineage: "v02" })
    if (native) yield* DatabaseMigration.apply(db)
  }))
  const artifact = { name: basename(staged), ...(await DatabaseArtifact.digest(staged).pipe(Effect.runPromise)) }
  const marker = native
    ? { format: "opencode.database.replacement", version: 2, stageFormat: "native", phase: "prepared", staged: artifact,
      targetPreimage: await DatabaseArtifact.digest(target).pipe(Effect.runPromise) } as const
    : { format: "opencode.database.replacement", version: 1, phase: "prepared", staged: artifact } as const
  await DatabaseArtifact.write(`${target}.replacement.json`, JSON.stringify(marker)).pipe(Effect.runPromise)
  return { target, staged, marker }
}

test.each(["prepared", "backup_ready", "postrename"])("native %s marker resumes exactly once", async (phase) => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, true)
  if (phase !== "prepared") {
    const backup = join(tmp.path, "backup.db")
    await useDatabase(setup.target, (db) => DatabaseArtifact.snapshot(db, backup))
    await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify({
      ...setup.marker, phase: "backup_ready",
      backup: { name: basename(backup), ...(await DatabaseArtifact.digest(backup).pipe(Effect.runPromise)) },
    })).pipe(Effect.runPromise)
  }
  if (phase === "postrename") await rename(setup.staged, setup.target)
  expect(await open(setup.target)).toEqual([{ id: sessionID }])
  expect(await open(setup.target)).toEqual([{ id: sessionID }])
  expect(await Bun.file(`${setup.target}.replacement.json`).exists()).toBe(false)
  expect((await readdir(tmp.path)).filter((name) => name.includes("backup"))).toHaveLength(1)
})

test.each(["prepared", "backup_ready", "postrename"])("old WS %s marker promotes through a different stage", async (phase) => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  if (phase !== "prepared") {
    const backup = join(tmp.path, "backup.db")
    await useDatabase(setup.target, (db) => DatabaseArtifact.snapshot(db, backup))
    await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify({
      ...setup.marker, phase: "backup_ready",
      backup: { name: basename(backup), ...(await DatabaseArtifact.digest(backup).pipe(Effect.runPromise)) },
    })).pipe(Effect.runPromise)
  }
  if (phase === "postrename") await rename(setup.staged, setup.target)
  expect(await open(setup.target)).toEqual([{ id: sessionID }])
  expect(await Bun.file(setup.staged).exists()).toBe(false)
  expect((await readdir(tmp.path)).filter((name) => name.includes("backup"))).toHaveLength(1)
})

test.each(["prepared", "backup_ready"])("changed target refuses native %s activation and keeps marker and stage", async (phase) => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, true)
  if (phase === "backup_ready") {
    const backup = join(tmp.path, "backup.db")
    await useDatabase(setup.target, (db) => DatabaseArtifact.snapshot(db, backup))
    await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify({
      ...setup.marker, phase,
      backup: { name: basename(backup), ...(await DatabaseArtifact.digest(backup).pipe(Effect.runPromise)) },
    })).pipe(Effect.runPromise)
  }
  await useDatabase(setup.target, (db) => db.run("UPDATE session SET title = 'changed'"))
  const changed = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
  await expect(open(setup.target)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(changed)
  expect(await Bun.file(setup.staged).exists()).toBe(true)
  expect(await Bun.file(`${setup.target}.replacement.json`).exists()).toBe(true)
})

test.each(["UPDATE session SET title = 'changed'", "PRAGMA user_version = 7", "CREATE TABLE extra (id TEXT)"])("old backup_ready refuses changed source facts: %s", async (change) => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  const backup = join(tmp.path, "backup.db")
  await useDatabase(setup.target, (db) => DatabaseArtifact.snapshot(db, backup))
  const marker = { ...setup.marker, phase: "backup_ready",
    backup: { name: basename(backup), ...(await DatabaseArtifact.digest(backup).pipe(Effect.runPromise)) } } satisfies DatabaseLegacyV01.PendingReplacement
  await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify(marker)).pipe(Effect.runPromise)
  await useDatabase(setup.target, (db) => db.run(change))
  const before = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
  const staged = await DatabaseArtifact.digest(setup.staged).pipe(Effect.runPromise)
  await expect(open(setup.target)).rejects.toThrow("replacement target differs from its backup")
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(before)
  expect(await DatabaseArtifact.digest(setup.staged).pipe(Effect.runPromise)).toEqual(staged)
  expect(await DatabaseReplacement.readMarker(`${setup.target}.replacement.json`).pipe(Effect.runPromise)).toEqual(marker)
})

test("native marker with a valid digest but WS stage is refused before rename", async () => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  const before = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
  await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify({
    ...setup.marker, version: 2, stageFormat: "native", targetPreimage: before,
  })).pipe(Effect.runPromise)
  await expect(open(setup.target)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(before)
  expect(await Bun.file(setup.staged).exists()).toBe(true)
  expect(await Bun.file(`${setup.target}.replacement.json`).exists()).toBe(true)
  expect((await readdir(tmp.path)).filter((name) => name.includes("backup"))).toEqual([])
})

test.each(["missing", "corrupt"])("old %s stage leaves target and marker unchanged", async (kind) => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  const before = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
  if (kind === "missing") await DatabaseArtifact.remove(setup.staged).pipe(Effect.runPromise)
  if (kind === "corrupt") await Bun.write(setup.staged, "corrupt")
  await expect(open(setup.target)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(before)
  expect(await DatabaseReplacement.readMarker(`${setup.target}.replacement.json`).pipe(Effect.runPromise)).toEqual(setup.marker)
})

test("standalone old WS activation remains usable", async () => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  await DatabaseReplacement.activate(setup.target).pipe(Effect.runPromise)
  await DatabaseLegacyV01.validateCurrentFile(setup.target).pipe(Effect.runPromise)
  expect(await Bun.file(`${setup.target}.replacement.json`).exists()).toBe(false)
})

test("native replacement accepts a native backup", async () => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, true)
  await useDatabase(setup.target, (db) => Effect.gen(function* () {
    yield* DatabaseMigration.apply(db)
    for (const index of Array.from({ length: 100 }, (_, index) => index)) yield* db.all(`SELECT ${index}`)
  }))
  await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify({
    ...setup.marker, targetPreimage: await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise),
  })).pipe(Effect.runPromise)
  expect(await open(setup.target)).toEqual([{ id: sessionID }])
  const backups = (await readdir(tmp.path)).filter((name) => name.includes("replacement-backup"))
  expect(backups).toHaveLength(1)
  await useDatabase(join(tmp.path, backups[0] ?? ""), (db) => Effect.gen(function* () {
    expect(yield* db.all("SELECT id FROM session_v2")).toEqual([{ id: sessionID }])
  }))
})

test.each(["prepared", "backup_ready", "postrename"])("old %s replacement into an absent target creates no backup", async (phase) => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  await DatabaseArtifact.remove(setup.target).pipe(Effect.runPromise)
  if (phase !== "prepared") await DatabaseArtifact.replaceWithText(`${setup.target}.replacement.json`, JSON.stringify({
    ...setup.marker, phase: "backup_ready",
  })).pipe(Effect.runPromise)
  if (phase === "postrename") await rename(setup.staged, setup.target)
  expect(await open(setup.target)).toEqual([{ id: sessionID }])
  expect((await readdir(tmp.path)).filter((name) => name.includes("backup"))).toEqual([])
})

test("invalid target keeps old WS stage and marker intact before promotion", async () => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  const stageBefore = await DatabaseArtifact.digest(setup.staged).pipe(Effect.runPromise)
  await useDatabase(setup.target, (db) => db.run("INSERT INTO migration VALUES ('unknown', 1)"))
  await expect(open(setup.target)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(setup.staged).pipe(Effect.runPromise)).toEqual(stageBefore)
  expect(await DatabaseReplacement.readMarker(`${setup.target}.replacement.json`).pipe(Effect.runPromise)).toEqual(setup.marker)
})

test("readonly native validation creates only an empty WAL after runtime closes", async () => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "native.db")
  await createStartupDatabase(target, true)
  await open(target)
  const before = { main: await DatabaseArtifact.digest(target).pipe(Effect.runPromise),
    wal: await DatabaseArtifact.entry(`${target}-wal`).pipe(Effect.runPromise) }
  await WsNativeFile.validateSourceFile(target).pipe(Effect.runPromise)
  const after = { main: await DatabaseArtifact.digest(target).pipe(Effect.runPromise),
    wal: await DatabaseArtifact.digest(`${target}-wal`).pipe(Effect.runPromise) }
  expect(after.main).toEqual(before.main)
  expect(before.wal.kind).toBe("missing")
  expect(after.wal.bytes).toBe(0)
})

test("legacy promotion replaces a closed native WAL target and retires its old stage", async () => {
  await using tmp = await tmpdir()
  const current = join(tmp.path, "current.db")
  const legacy = join(tmp.path, "legacy.db")
  const target = join(tmp.path, "native.db")
  await createStartupDatabase(current)
  await createStartupDatabase(target, true)
  await open(target)
  await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
  const staged = await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)
  expect(await open(target)).toEqual([{ id: sessionID }])
  expect(await Bun.file(staged.staged).exists()).toBe(false)
  expect(await Bun.file(legacy).exists()).toBe(true)
})

test.each(["publication", "activation"])("failed %s preserves the old stage and target", async (failure) => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  const markerPath = `${setup.target}.replacement.json`
  const before = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
  const oldStage = await DatabaseArtifact.digest(setup.staged).pipe(Effect.runPromise)
  let interrupted = false
  const watcher = watch(tmp.path, (_, name) => {
    if (interrupted) return
    if (failure === "publication" && name?.includes(".native-") && name.endsWith(".db")) {
      interrupted = true
      renameSync(markerPath, join(tmp.path, "original-marker.json"))
      mkdirSync(markerPath)
    }
    if (failure === "activation" && name === basename(markerPath)) {
      const marker = Schema.decodeUnknownSync(Schema.fromJsonString(DatabaseLegacyV01.PendingReplacement))(readFileSync(markerPath, "utf8"))
      if (marker.version !== 2) return
      interrupted = true
      writeFileSync(`${join(tmp.path, marker.staged.name)}-wal`, "uncommitted")
    }
  })
  try {
    await expect(open(setup.target)).rejects.toThrow()
    expect(interrupted).toBe(true)
    expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(before)
    expect(await DatabaseArtifact.digest(setup.staged).pipe(Effect.runPromise)).toEqual(oldStage)
    if (failure === "activation") {
      const marker = await DatabaseReplacement.readMarker(markerPath).pipe(Effect.runPromise)
      expect(marker.version).toBe(2)
      expect(await Bun.file(join(tmp.path, marker.staged.name)).exists()).toBe(true)
    }
  } finally {
    watcher.close()
  }
})

test("empty WAL symlink is still refused before promotion", async () => {
  await using tmp = await tmpdir()
  const setup = await fixture(tmp.path, false)
  const before = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
  await Bun.write(join(tmp.path, "empty"), "")
  await symlink(join(tmp.path, "empty"), `${setup.target}-wal`)
  await expect(open(setup.target)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(before)
  expect(await Bun.file(setup.staged).exists()).toBe(true)
})
