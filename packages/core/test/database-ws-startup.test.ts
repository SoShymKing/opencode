import { expect, test } from "bun:test"
import { watch } from "node:fs"
import { readdir } from "node:fs/promises"
import { join } from "node:path"
import { Effect } from "effect"
import { Database } from "../src/database/database"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseLease } from "../src/database/lease"
import { DatabaseMigration } from "../src/database/migration"
import previousV2Migration from "../src/database/migration/20260804233008_loose_psylocke"
import { useDatabase } from "./fixture/database-migration"
import { setupWs, setupPreviousNative, sessionID } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

function open(target: string) {
  return Effect.gen(function* () {
    const service = yield* Database.Service
    return yield* service.db.all<{ id: string }>("SELECT id FROM session_v2")
  }).pipe(Effect.provide(Database.layerFromPath(target)), Effect.provide(tempGlobalLayer), Effect.scoped)
}

test("fresh native startup creates no replacement backup", async () => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "fresh.db")
  expect(await open(target).pipe(Effect.runPromise)).toEqual([])
  expect((await readdir(tmp.path)).filter((name) => name.includes("backup") || name.includes("replacement"))).toEqual([])
})

test("native startup shares the lifetime lease without an exclusive upgrade", async () => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "native.db")
  await Effect.gen(function* () {
    yield* Database.Service
    expect((yield* Effect.exit(DatabaseLease.exclusive(target).pipe(Effect.scoped)))._tag).toBe("Failure")
    expect(yield* open(target)).toEqual([])
  }).pipe(Effect.provide(Database.layerFromPath(target)), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  await DatabaseLease.exclusive(target).pipe(Effect.scoped, Effect.runPromise)
})

test.each(["ws", "previous-native"])("file startup converts %s once and retains a source backup", async (source) => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "source.db")
  await useDatabase(target, (db) => source === "ws"
    ? setupWs(db, { directory: tmp.path })
    : setupPreviousNative(db, { directory: tmp.path }))
  expect(await open(target).pipe(Effect.runPromise)).toEqual([{ id: sessionID }])
  expect(await open(target).pipe(Effect.runPromise)).toEqual([{ id: sessionID }])
  const backups = (await readdir(tmp.path)).filter((name) => name.includes("replacement-backup"))
  expect(backups).toHaveLength(1)
  await useDatabase(join(tmp.path, backups[0] ?? ""), (db) => Effect.gen(function* () {
    expect(yield* db.all("SELECT id FROM session")).toEqual([{ id: sessionID }])
  }))
  expect(await Bun.file(`${target}.replacement.json`).exists()).toBe(false)
})

test("held shared lease refuses upgrade before publishing artifacts", async () => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "source.db")
  await useDatabase(target, (db) => setupWs(db, { directory: tmp.path }))
  const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
  await Effect.gen(function* () {
    yield* DatabaseLease.shared(target)
    expect((yield* Effect.exit(open(target)))._tag).toBe("Failure")
  }).pipe(Effect.scoped, Effect.runPromise)
  expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
  expect((await readdir(tmp.path)).filter((name) => name.includes("native-") || name.includes("replacement"))).toEqual([])
})

test("unknown source remains byte unchanged", async () => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "unknown.db")
  await useDatabase(target, (db) => db.run("CREATE TABLE mystery (id TEXT)"))
  const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
  await expect(open(target).pipe(Effect.runPromise)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
})

test("incomplete WS source remains byte unchanged", async () => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "incomplete.db")
  await useDatabase(target, (db) => Effect.gen(function* () {
    yield* setupWs(db, { directory: tmp.path })
    yield* db.run("DROP TABLE session_input")
  }))
  const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
  await expect(open(target).pipe(Effect.runPromise)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
  expect((await readdir(tmp.path)).filter((name) => name.includes("native-") || name.includes("replacement"))).toEqual([])
})

test("in-memory apply dispatches the converter before strict native preflight", async () => {
  await using tmp = await tmpdir()
  await useDatabase(":memory:", (db) => Effect.gen(function* () {
    yield* setupWs(db, { directory: tmp.path })
    expect((yield* Effect.exit(DatabaseMigration.preflight(db)))._tag).toBe("Failure")
    yield* DatabaseMigration.apply(db)
    expect(yield* db.all("SELECT id FROM session_v2")).toEqual([{ id: sessionID }])
  }))
})

test("injected client converts WS before native rejection without file activation", async () => {
  await using tmp = await tmpdir()
  await useDatabase(":memory:", (db) => Effect.gen(function* () {
    yield* setupWs(db, { directory: tmp.path })
    const service = yield* Database.Service.pipe(Effect.provide(Database.layerFromClient))
    expect(yield* service.db.all("SELECT id FROM session_v2")).toEqual([{ id: sessionID }])
  }))
  expect(await readdir(tmp.path)).toEqual([])
})

test.each(["DELETE", "WAL"])("source commit after snapshot in %s mode refuses a stale stage", async (journal) => {
  const { Database } = await import("bun:sqlite")
  await using tmp = await tmpdir()
  const target = join(tmp.path, "source.db")
  await useDatabase(target, (db) => setupWs(db, { directory: tmp.path }))
  using writer = new Database(target)
  writer.run(`PRAGMA journal_mode = ${journal}`)
  writer.run("PRAGMA wal_autocheckpoint = 0")
  const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
  let changed = false
  const watcher = watch(tmp.path, (_, name) => {
    if (changed || !name?.startsWith("source.db.native-") || !name.endsWith(".db")) return
    changed = true
    writer.run("UPDATE session SET title = 'new commit'")
  })
  try {
    await expect(open(target).pipe(Effect.runPromise)).rejects.toThrow("database source changed during conversion")
    expect(changed).toBe(true)
    expect(writer.query("SELECT title FROM session").all()).toEqual([{ title: "new commit" }])
    if (journal === "WAL") expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
    expect(await Bun.file(`${target}.replacement.json`).exists()).toBe(false)
    expect((await readdir(tmp.path)).some((name) => name.startsWith("source.db.native-") && name.endsWith(".db"))).toBe(true)
  } finally {
    watcher.close()
  }
})

test("busy external WAL writer refuses activation and retains the stage", async () => {
  const { Database } = await import("bun:sqlite")
  await using tmp = await tmpdir()
  const target = join(tmp.path, "source.db")
  await useDatabase(target, (db) => setupWs(db, { directory: tmp.path }))
  using writer = new Database(target)
  writer.run("PRAGMA journal_mode = WAL")
  writer.run("BEGIN IMMEDIATE")
  writer.run("UPDATE session SET title = 'uncommitted'")
  try {
    await expect(open(target).pipe(Effect.runPromise)).rejects.toThrow()
    expect(writer.query("SELECT title FROM session").all()).toEqual([{ title: "uncommitted" }])
    expect(await Bun.file(`${target}.replacement.json`).exists()).toBe(false)
    expect((await readdir(tmp.path)).some((name) => name.includes(".native-"))).toBe(true)
  } finally {
    writer.run("ROLLBACK")
  }
})

test("canonical previous-native adapter satisfies listed migrations without resets", async () => {
  await using tmp = await tmpdir()
  await useDatabase(":memory:", (db) => Effect.gen(function* () {
    yield* setupPreviousNative(db, { directory: tmp.path })
    expect((yield* Effect.exit(DatabaseMigration.applyOnly(db, [{ ...previousV2Migration }])))._tag).toBe("Failure")
    yield* DatabaseMigration.applyOnly(db, [previousV2Migration])
    expect(yield* db.all("SELECT id FROM session_v2")).toEqual([{ id: sessionID }])
  }))
})

test.each(["supported", "unknown"])("startup handles committed WAL for %s source without losing writes", async (source) => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "wal.db")
  await useDatabase(target, (db) => source === "supported"
    ? setupWs(db, { directory: tmp.path })
    : db.run("CREATE TABLE mystery (value TEXT)"))
  const child = Bun.spawn([process.execPath, "-e", `
    const { Database } = require('bun:sqlite');
    const db = new Database(process.argv[1]);
    db.run('PRAGMA journal_mode = WAL');
    db.run('PRAGMA wal_autocheckpoint = 0');
    db.run(process.argv[2]);
    console.log('ready');
    setInterval(() => {}, 1000);
  `, target, source === "supported" ? "UPDATE session SET title = 'WAL title'" : "INSERT INTO mystery VALUES ('WAL title')"], { stdout: "pipe", stderr: "pipe" })
  try {
    const reader = child.stdout.getReader()
    const ready = await reader.read()
    expect(new TextDecoder().decode(ready.value)).toContain("ready")
    reader.releaseLock()
  } finally {
    child.kill()
    await child.exited
  }
  expect(await Bun.file(`${target}-wal`).exists()).toBe(true)
  if (source === "unknown") {
    const main = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
    const wal = await DatabaseArtifact.digest(`${target}-wal`).pipe(Effect.runPromise)
    await expect(open(target).pipe(Effect.runPromise)).rejects.toThrow()
    expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(main)
    expect(await DatabaseArtifact.digest(`${target}-wal`).pipe(Effect.runPromise)).toEqual(wal)
    return
  }
  await open(target).pipe(Effect.runPromise)
  await useDatabase(target, (db) => Effect.gen(function* () {
    expect(yield* db.all("SELECT title FROM session_v2")).toEqual([{ title: "WAL title" }])
  }))
})
