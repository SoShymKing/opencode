import { describe, expect, test } from "bun:test"
import { Database as SqliteDatabase } from "bun:sqlite"
import { readdir, rename, rm, symlink } from "node:fs/promises"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { DatabaseArtifact } from "@opencode-ai/core/database/artifact"
import { DatabaseLegacyV01 } from "@opencode-ai/core/database/legacy-v01"
import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { createCurrentDatabase } from "./fixture/database-transfer"
import { tmpdir } from "./fixture/tmpdir"

describe("Database pending replacement", () => {
  test("activates a prepared stage before opening SQLite and retains the old target backup", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const legacy = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    using old = new SqliteDatabase(target)
    old.query("UPDATE session SET title = 'old target'").run()
    old.run("PRAGMA wal_checkpoint(TRUNCATE)")
    old.close()
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
    const staged = await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)

    const title = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* (yield* Database.Service).db.get<{ readonly title: string }>(sql`SELECT title FROM session`)
      }).pipe(Effect.provide(Database.layerFromPath(target)), Effect.scoped),
    )

    expect(title).toEqual({ title: "Session" })
    expect(await Bun.file(staged.marker).exists()).toBe(false)
    expect(await Bun.file(staged.staged).exists()).toBe(false)
    const backups = (await readdir(tmp.path)).filter((name) =>
      name.startsWith(`${path.basename(target)}.replacement-backup-`),
    )
    expect(backups).toHaveLength(1)
    using backup = new SqliteDatabase(path.join(tmp.path, backups[0] ?? ""), { readonly: true })
    expect(backup.query<{ readonly title: string }, []>("SELECT title FROM session").get()).toEqual({
      title: "old target",
    })
  })

  test("resumes from backup_ready without creating another backup", async () => {
    await using tmp = await tmpdir()
    const fixture = await stagedFixture(tmp.path)
    const backup = `${fixture.target}.replacement-backup-interrupted.db`
    await DatabaseLegacyV01.snapshotCurrentFile(fixture.target, backup).pipe(Effect.runPromise)
    const marker = await preparedMarker(fixture.marker)
    await Bun.write(
      fixture.marker,
      JSON.stringify({
        ...marker,
        phase: "backup_ready",
        backup: { name: path.basename(backup), ...(await DatabaseArtifact.digest(backup).pipe(Effect.runPromise)) },
      }),
    )

    expect(await openTitle(fixture.target)).toEqual({ title: "Session" })
    expect(await Bun.file(backup).exists()).toBe(true)
    expect(await Bun.file(fixture.marker).exists()).toBe(false)
    expect((await readdir(tmp.path)).filter((name) => name.includes("replacement-backup-"))).toEqual([
      path.basename(backup),
    ])
  })

  test("recovers when the stage rename completed before marker removal", async () => {
    await using tmp = await tmpdir()
    const fixture = await stagedFixture(tmp.path)
    const backup = `${fixture.target}.replacement-backup-interrupted.db`
    await DatabaseLegacyV01.snapshotCurrentFile(fixture.target, backup).pipe(Effect.runPromise)
    const marker = await preparedMarker(fixture.marker)
    await Bun.write(
      fixture.marker,
      JSON.stringify({
        ...marker,
        phase: "backup_ready",
        backup: { name: path.basename(backup), ...(await DatabaseArtifact.digest(backup).pipe(Effect.runPromise)) },
      }),
    )
    await rename(fixture.staged, fixture.target)

    expect(await openTitle(fixture.target)).toEqual({ title: "Session" })
    expect(await Bun.file(fixture.marker).exists()).toBe(false)
    expect(await Bun.file(backup).exists()).toBe(true)
  })

  test.each(["missing", "corrupt"])("fails closed when the prepared stage is %s", async (state) => {
    await using tmp = await tmpdir()
    const fixture = await stagedFixture(tmp.path)
    if (state === "missing") await DatabaseArtifact.discard(fixture.staged).pipe(Effect.runPromise)
    if (state === "corrupt") await Bun.write(fixture.staged, "corrupt")

    await expect(openTitle(fixture.target)).rejects.toThrow()

    expect(await Bun.file(fixture.marker).exists()).toBe(true)
    using target = new SqliteDatabase(fixture.target, { readonly: true })
    expect(target.query<{ readonly title: string }, []>("SELECT title FROM session").get()).toEqual({
      title: "old target",
    })
  })

  test("rejects unsafe marker basenames before touching the target", async () => {
    await using tmp = await tmpdir()
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(target)
    using old = new SqliteDatabase(target)
    old.query("UPDATE session SET title = 'old target'").run()
    old.close()
    await Bun.write(
      `${target}.replacement.json`,
      JSON.stringify({
        format: "opencode.database.replacement",
        version: 1,
        phase: "prepared",
        staged: { name: "../outside.db", sha256: "0".repeat(64), bytes: 0 },
      }),
    )

    await expect(openTitle(target)).rejects.toThrow()

    using unchanged = new SqliteDatabase(target, { readonly: true })
    expect(unchanged.query<{ readonly title: string }, []>("SELECT title FROM session").get()).toEqual({
      title: "old target",
    })
  })

  test("releases SQLite statements before replacing an existing Windows database", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const legacy = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await Effect.runPromise(
      Effect.gen(function* () {
        const db = (yield* Database.Service).db
        yield* Effect.forEach(
          Array.from({ length: 100 }, (_, index) => index),
          (index) => db.run(`SELECT ${index}`),
          {
            discard: true,
          },
        )
      }).pipe(Effect.provide(Database.layerFromPath(target)), Effect.scoped),
    )
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
    await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)

    expect(await openTitle(target)).toEqual({ title: "Session" })
  })

  test("fails immediately while another process holds the target database lease", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const legacy = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
    const child = spawnDatabase(target)
    try {
      await ready(child)
      const fixture = await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)
      await expect(openTitle(target)).rejects.toThrow()
      expect(await Bun.file(fixture.marker).exists()).toBe(true)
      expect((await readdir(tmp.path)).filter((name) => name.includes("replacement-backup-"))).toEqual([])
      child.stdin.end()
      expect(await child.exited).toBe(0)
      expect(await openTitle(target)).toEqual({ title: "Session" })
    } finally {
      child.kill()
      await child.exited
    }
  })

  test("rejects staged and backup symlink artifacts", async () => {
    await using tmp = await tmpdir()
    const stagedFixtureValue = await stagedFixture(tmp.path)
    const stagedTarget = `${stagedFixtureValue.staged}.real`
    await rename(stagedFixtureValue.staged, stagedTarget)
    await symlink(stagedTarget, stagedFixtureValue.staged)

    await expect(openTitle(stagedFixtureValue.target)).rejects.toThrow("unsafe")

    await rm(stagedFixtureValue.marker)
    await using backupTmp = await tmpdir()
    const backupFixture = await stagedFixture(backupTmp.path)
    const backup = `${backupFixture.target}.replacement-backup.db`
    await DatabaseLegacyV01.snapshotCurrentFile(backupFixture.target, backup).pipe(Effect.runPromise)
    const marker = await preparedMarker(backupFixture.marker)
    const backupReal = `${backup}.real`
    await rename(backup, backupReal)
    await symlink(backupReal, backup)
    await Bun.write(
      backupFixture.marker,
      JSON.stringify({
        ...marker,
        phase: "backup_ready",
        backup: { name: path.basename(backup), ...(await DatabaseArtifact.digest(backupReal).pipe(Effect.runPromise)) },
      }),
    )

    await expect(openTitle(backupFixture.target)).rejects.toThrow("unsafe")
  })
})

const decodeJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)

async function stagedFixture(root: string) {
  const current = path.join(root, "current.sqlite")
  const legacy = path.join(root, "legacy.sqlite")
  const target = path.join(root, "target.sqlite")
  await createCurrentDatabase(current)
  await createCurrentDatabase(target)
  using old = new SqliteDatabase(target)
  old.query("UPDATE session SET title = 'old target'").run()
  old.run("PRAGMA wal_checkpoint(TRUNCATE)")
  old.close()
  await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
  return { target, ...(await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)) }
}

async function preparedMarker(filename: string) {
  const marker = Schema.decodeUnknownSync(DatabaseLegacyV01.PendingReplacement, { onExcessProperty: "error" })(
    decodeJson(await Bun.file(filename).text()),
  )
  if (marker.phase !== "prepared") throw new Error("Expected prepared replacement marker")
  return marker
}

function openTitle(target: string) {
  return Effect.runPromise(
    Effect.gen(function* () {
      return yield* (yield* Database.Service).db.get<{ readonly title: string }>(sql`SELECT title FROM session`)
    }).pipe(Effect.provide(Database.layerFromPath(target)), Effect.scoped),
  )
}

function spawnDatabase(target: string) {
  return Bun.spawn(
    [process.execPath, "run", path.join(import.meta.dir, "fixture", "database-lease-worker.ts"), "database", target],
    { cwd: process.cwd(), stdin: "pipe", stdout: "pipe", stderr: "pipe" },
  )
}

async function ready(child: ReturnType<typeof spawnDatabase>) {
  const reader = child.stdout.getReader()
  const decoder = new TextDecoder()
  let output = ""
  try {
    for (;;) {
      const chunk = await reader.read()
      if (chunk.done) throw new Error(`worker exited before readiness: ${await new Response(child.stderr).text()}`)
      output += decoder.decode(chunk.value, { stream: true })
      if (output.includes("READY\n")) return
    }
  } finally {
    reader.releaseLock()
  }
}
