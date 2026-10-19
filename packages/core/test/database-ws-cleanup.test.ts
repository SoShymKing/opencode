import { expect, test } from "bun:test"
import { randomUUID } from "node:crypto"
import { mkdir, readdir, rm, symlink } from "node:fs/promises"
import { basename, join } from "node:path"
import { Effect } from "effect"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseLegacyV01 } from "../src/database/legacy-v01"
import { DatabaseMigration } from "../src/database/migration"
import { DatabaseReplacement } from "../src/database/replacement"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { WsNativeFile } from "../src/database/ws-native-file"
import { WsNativeProfile } from "../src/database/ws-native-profile"
import { WsNativeStage } from "../src/database/ws-native-stage"
import { setupWs } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

async function measuredFiles() {
  const names = ["artifact", "replacement", "ws-native-stage", "ws-native-file"]
  const directory = new URL("../.cache/", import.meta.url)
  const token = randomUUID()
  const adapter = (name: string) => new URL(`cleanup-${token}-${name}.ts`, directory)
  const dispose = () => Promise.all(names.map((name) => rm(adapter(name), { force: true }))).then(() => {})
  await mkdir(directory, { recursive: true })
  try {
    for (const name of names) {
      const source = await Bun.file(new URL(`../src/database/${name}.ts`, import.meta.url)).text()
      const adapted = source.replaceAll(/"\.\/([^"\n]+)"/g, (_match, specifier: string) => {
        const dependency = specifier.replace(/\.js$/, "")
        return JSON.stringify(names.includes(dependency) ? `./${basename(adapter(dependency).pathname)}` : `../src/database/${specifier}`)
      })
      if (name === "artifact") expect(source).toContain("export function digest(filename: string) {")
      await Bun.write(adapter(name), name === "artifact" ? `export const reads: string[] = []\n` + adapted.replace(
        "export function digest(filename: string) {", "export function digest(filename: string) {\n  reads.push(filename)",
      ) : adapted)
    }
    const artifact: typeof DatabaseArtifact & { reads: string[] } = await import(adapter("artifact").href)
    const replacement: typeof DatabaseReplacement = await import(adapter("replacement").href)
    const stage: typeof WsNativeStage = await import(adapter("ws-native-stage").href)
    const file: typeof WsNativeFile = await import(adapter("ws-native-file").href)
    return { artifact, replacement, stage, file, [Symbol.asyncDispose]: dispose }
  } catch (error) {
    await dispose()
    throw error
  }
}

function source(filename: string, directory: string) {
  return Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory })
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped, Effect.runPromise)
}

async function activated(directory: string) {
  const target = join(directory, "source.db")
  await source(target, directory)
  const ready = await WsNativeStage.prepare(target, target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)
  await WsNativeStage.handoff(target).pipe(Effect.runPromise)
  await DatabaseArtifact.write(`${target}.replacement.json`, JSON.stringify({
    format: "opencode.database.replacement", version: 2, stageFormat: "native", phase: "prepared", staged: ready,
    targetPreimage: await DatabaseArtifact.digest(target).pipe(Effect.runPromise),
  })).pipe(Effect.runPromise)
  expect(await DatabaseReplacement.activate(target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).toBeUndefined()
  const marker = await WsNativeStage.read(target).pipe(Effect.runPromise)
  const backups = (await readdir(directory)).filter((name) => name.includes(".replacement-backup-"))
  expect(backups).toHaveLength(1)
  return { target, marker, snapshot: join(directory, marker.snapshot.name), backup: join(directory, backups[0]) }
}

test("cold file activation omits exactly the fresh-snapshot and cleanup-only digest reads", async () => {
  await using temp = await tmpdir()
  const target = join(temp.path, "source.db")
  await source(target, temp.path)
  await using measured = await measuredFiles()
  await measured.file.admit(target).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  expect(measured.artifact.reads.filter((name) => name === target)).toHaveLength(10)
  expect(measured.artifact.reads.filter((name) => name.includes(".ws-snapshot-"))).toHaveLength(2)
  expect(measured.artifact.reads.filter((name) => name.includes(".native-"))).toHaveLength(4)
  expect(measured.artifact.reads.filter((name) => name.includes(".replacement-backup-"))).toHaveLength(2)
  expect(measured.artifact.reads).toHaveLength(18)
})

test("fresh capture hashes its snapshot once but reopened capture still verifies it", async () => {
  await using temp = await tmpdir()
  const target = join(temp.path, "source.db")
  await source(target, temp.path)
  await using measured = await measuredFiles()
  const proof = await measured.stage.capture(target, target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)
  const snapshot = join(temp.path, proof.marker.snapshot.name)
  expect(measured.artifact.reads.filter((name) => name === snapshot)).toHaveLength(1)
  measured.artifact.reads.length = 0
  await measured.stage.capture(target, target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)
  expect(measured.artifact.reads.filter((name) => name === snapshot)).toHaveLength(1)
  await DatabaseLegacyV01.useDatabase(snapshot, false, (db) => db.run("UPDATE session SET title = 'changed snapshot' ")).pipe(Effect.runPromise)
  await expect(measured.stage.capture(target, target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).rejects.toThrow("replacement artifact is corrupt")
  await expect(measured.stage.prepare(target, target, proof).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).rejects.toThrow("replacement artifact is corrupt")
})

test("valid activated target can discard a corrupt owned snapshot without rehashing it", async () => {
  await using temp = await tmpdir()
  const setup = await activated(temp.path)
  const backup = await DatabaseArtifact.digest(setup.backup).pipe(Effect.runPromise)
  const facts = () => DatabaseLegacyV01.useDatabase(setup.target, true, (db) => db.all("SELECT * FROM session_message ORDER BY id")).pipe(Effect.runPromise)
  const before = await facts()
  await Bun.write(setup.snapshot, "corrupt derived snapshot")
  await using measured = await measuredFiles()
  await measured.stage.retire(setup.target).pipe(Effect.runPromise)
  expect(measured.artifact.reads).toEqual([setup.target])
  expect(await facts()).toEqual(before)
  expect(await DatabaseArtifact.digest(setup.backup).pipe(Effect.runPromise)).toEqual(backup)
  expect(await Bun.file(setup.snapshot).exists()).toBe(false)
  expect(await Bun.file(`${setup.target}.ws-stage.json`).exists()).toBe(false)
})

for (const fault of ["path", "symlink", "journal", "target"]) test(`direct cleanup rejects unsafe ${fault}`, async () => {
  await using temp = await tmpdir()
  const setup = await activated(temp.path)
  if (fault === "path") await DatabaseArtifact.replaceWithText(`${setup.target}.ws-stage.json`, JSON.stringify({
    ...setup.marker, snapshot: { ...setup.marker.snapshot, name: "../outside.db" },
  })).pipe(Effect.runPromise)
  if (fault === "symlink") {
    const outside = join(temp.path, "outside")
    await mkdir(outside)
    await rm(setup.snapshot)
    await symlink(outside, setup.snapshot, "junction")
  }
  if (fault === "journal") await DatabaseArtifact.write(`${setup.snapshot}-journal`, "").pipe(Effect.runPromise)
  if (fault === "target") await DatabaseLegacyV01.useDatabase(setup.target, false, (db) => db.run("UPDATE session_v2 SET title = 'different target' ")).pipe(Effect.runPromise)
  const before = await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)
  const backup = await DatabaseArtifact.digest(setup.backup).pipe(Effect.runPromise)
  await expect(WsNativeStage.retire(setup.target).pipe(Effect.runPromise)).rejects.toThrow()
  if (fault === "target") await expect(WsNativeStage.activateAndRetire(setup.target).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)).rejects.toThrow("replacement artifact is corrupt")
  expect(await DatabaseArtifact.digest(setup.target).pipe(Effect.runPromise)).toEqual(before)
  expect(await DatabaseArtifact.digest(setup.backup).pipe(Effect.runPromise)).toEqual(backup)
  expect(await Bun.file(`${setup.target}.ws-stage.json`).exists()).toBe(true)
})

for (const completed of [false, true]) test(`profile cleanup needs completion proof but no artifact hashes: completed=${completed}`, async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  const owner = join(temp.path, "profile")
  await source(filename, temp.path)
  const before = await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)
  const ready = await WsNativeStage.prepare(filename, owner).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)
  const marker = await WsNativeStage.read(owner).pipe(Effect.runPromise)
  await using measured = await measuredFiles()
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* DatabaseMigration.apply(db)
    if (completed) yield* WsNativeProfile.merge(db, join(temp.path, ready.name), `migration.ws-profile.${marker.snapshot.sha256}`)
    const facts = yield* db.all("SELECT * FROM session_message ORDER BY id")
    yield* Effect.promise(() => Bun.write(join(temp.path, marker.snapshot.name), "corrupt snapshot"))
    yield* Effect.promise(() => Bun.write(join(temp.path, marker.work), "corrupt work"))
    expect((yield* measured.stage.retireProfile(db, owner).pipe(Effect.exit))._tag).toBe(completed ? "Success" : "Failure")
    expect(yield* db.all("SELECT * FROM session_message ORDER BY id")).toEqual(facts)
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
  expect(measured.artifact.reads).toEqual([])
  expect(await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)).toEqual(before)
  expect(await Bun.file(join(temp.path, marker.work)).exists()).toBe(!completed)
  expect(await Bun.file(join(temp.path, marker.snapshot.name)).exists()).toBe(!completed)
})

for (const artifact of ["snapshot", "work"] as const) test(`completed profile cleanup still rejects a ${artifact} journal`, async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  const owner = join(temp.path, "profile")
  await source(filename, temp.path)
  const ready = await WsNativeStage.prepare(filename, owner).pipe(Effect.provide(tempGlobalLayer), Effect.runPromise)
  const marker = await WsNativeStage.read(owner).pipe(Effect.runPromise)
  const guarded = join(temp.path, artifact === "work" ? marker.work : marker.snapshot.name)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* DatabaseMigration.apply(db)
    yield* WsNativeProfile.merge(db, join(temp.path, ready.name), `migration.ws-profile.${marker.snapshot.sha256}`)
    const facts = yield* db.all("SELECT * FROM session_message ORDER BY id")
    yield* DatabaseArtifact.write(`${guarded}-journal`, "")
    expect((yield* WsNativeStage.retireProfile(db, owner).pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* db.all("SELECT * FROM session_message ORDER BY id")).toEqual(facts)
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
  expect(await Bun.file(guarded).exists()).toBe(true)
})
