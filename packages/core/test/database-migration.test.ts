import { describe, expect, test } from "bun:test"
import { readdir } from "node:fs/promises"
import path from "node:path"
import { Database } from "@opencode-ai/core/database/database"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { migrations } from "@opencode-ai/core/database/migration.gen"
import sessionMessagePartMigration from "@opencode-ai/core/database/migration/v02_session_message_part"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import {
  apply,
  backupPaths,
  inspectBackup,
  messageBytes,
  setupExisting,
  useDatabase,
} from "./fixture/database-migration"
import { tmpdir } from "./fixture/tmpdir"

const canonicalIDs = ["v01_baseline", "v02_session_message_part"]

describe("DatabaseMigration", () => {
  test("uses the sequential registry and bootstraps a fresh file without a backup", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "fresh.sqlite")

    const journal = await Effect.runPromise(
      Effect.gen(function* () {
        const db = (yield* Database.Service).db
        expect(yield* db.get(sql`SELECT name FROM sqlite_master WHERE name = 'session_message_part'`)).toEqual({
          name: "session_message_part",
        })
        return yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`)
      }).pipe(Effect.provide(Database.layerFromPath(filename)), Effect.scoped),
    )

    expect(migrations.map((migration) => migration.id)).toEqual(canonicalIDs)
    expect(journal.map((row) => row.id)).toEqual(canonicalIDs)
    expect(await backupPaths(filename)).toEqual([])
  })

  test("backs up committed WAL state before v02 and creates no backup on re-run", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "pending.sqlite")

    const source = await useDatabase(filename, (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, { migrationIDs: ["v01_baseline"] })
        yield* DatabaseMigration.apply(db, filename)
        return {
          journal: yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`),
          dataBytes: yield* db.get<{ readonly bytes: string }>(
            sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message WHERE id = 'message'`,
          ),
          parts: yield* db.get<{ readonly count: number }>(sql`SELECT count(*) AS count FROM session_message_part`),
        }
      }),
    )
    const backups = await backupPaths(filename)
    const backup = backups[0]
    if (!backup) throw new Error("Expected migration backup")

    expect(backups).toHaveLength(1)
    expect(inspectBackup(backup)).toEqual({
      quickCheck: "ok",
      journal: [{ id: "v01_baseline" }],
      partTable: null,
      dataBytes: messageBytes,
    })
    expect(source).toEqual({
      journal: canonicalIDs.map((id) => ({ id })),
      dataBytes: { bytes: messageBytes },
      parts: { count: 0 },
    })
    expect((await readdir(tmp.path)).filter((name) => name.includes(".backup-"))).toEqual([path.basename(backup)])

    await apply(filename)
    expect(await backupPaths(filename)).toEqual(backups)
  })

  test("serializes fresh initialization across independent processes", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "concurrent.sqlite")
    const worker = path.join(import.meta.dir, "fixture", "database-migration-worker.ts")
    const children = Array.from({ length: 8 }, () =>
      Bun.spawn([process.execPath, "run", worker, filename], {
        cwd: process.cwd(),
        stdout: "ignore",
        stderr: "pipe",
      }),
    )

    const results = await Promise.all(
      children.map(async (child) => {
        const [exit, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()])
        return { exit, stderr }
      }),
    )

    expect(results).toEqual(Array.from({ length: children.length }, () => ({ exit: 0, stderr: "" })))
    expect(
      await useDatabase(filename, (db) =>
        db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`),
      ),
    ).toEqual(canonicalIDs.map((id) => ({ id })))
    expect(await backupPaths(filename)).toEqual([])
  }, 30_000)

  test("maps complete union legacy evidence with the metadata alias and runs v02 once", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "legacy.sqlite")
    const substituted = DatabaseMigration.legacyBaselineMigrationIDs.map((id) =>
      id === DatabaseMigration.legacySessionMetadataMigrationID ? DatabaseMigration.legacySessionMetadataAlias : id,
    )
    const split = Math.floor(substituted.length / 2)

    const state = await useDatabase(filename, (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, {
          migrationIDs: substituted.slice(0, split),
          drizzleIDs: substituted.slice(split),
        })
        yield* DatabaseMigration.apply(db, filename)
        return {
          journal: yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`),
          drizzle: yield* db.all<{ readonly name: string }>(sql`SELECT name FROM __drizzle_migrations ORDER BY id`),
          parts: yield* db.get<{ readonly count: number }>(sql`SELECT count(*) AS count FROM session_message_part`),
        }
      }),
    )

    expect(state.journal.slice(0, split).map((row) => row.id)).toEqual(substituted.slice(0, split))
    expect(state.journal.slice(-2).map((row) => row.id)).toEqual(canonicalIDs)
    expect(state.drizzle.map((row) => row.name)).toEqual(substituted.slice(split))
    expect(state.parts).toEqual({ count: 0 })
  })

  test("maps the old part marker without replay and preserves every legacy row", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "legacy-complete.sqlite")
    const legacy = [...DatabaseMigration.legacyBaselineMigrationIDs, DatabaseMigration.legacyPartMigrationID]

    const journal = await useDatabase(filename, (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, { migrationIDs: legacy, partComplete: true })
        yield* DatabaseMigration.apply(db, filename)
        return yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`)
      }),
    )

    expect(journal.map((row) => row.id)).toEqual([...legacy, ...canonicalIDs])
    expect(await backupPaths(filename)).toHaveLength(1)
  })

  test("rejects partial legacy history before journal or schema mutation", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "partial.sqlite")
    const partial = DatabaseMigration.legacyBaselineMigrationIDs.slice(0, 3)

    await useDatabase(filename, (db) => setupExisting(db, { migrationIDs: partial }))
    await expect(apply(filename)).rejects.toThrow("Unsafe legacy migration history")

    const state = await useDatabase(filename, (db) =>
      Effect.gen(function* () {
        return {
          journal: yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`),
          part: yield* db.get(sql`SELECT name FROM sqlite_master WHERE name = 'session_message_part'`),
        }
      }),
    )
    expect(state).toEqual({ journal: partial.map((id) => ({ id })), part: undefined })
    expect(await backupPaths(filename)).toEqual([])
  })

  test("rejects v02 evidence without baseline proof", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "part-only.sqlite")

    await useDatabase(filename, (db) =>
      setupExisting(db, { migrationIDs: [DatabaseMigration.legacyPartMigrationID], partComplete: true }),
    )

    await expect(apply(filename)).rejects.toThrow("Unsafe legacy migration history")
    expect(await backupPaths(filename)).toEqual([])
  })

  test("migrates an existing in-memory database without a filesystem backup", async () => {
    const journal = await useDatabase(":memory:", (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, { migrationIDs: ["v01_baseline"] })
        yield* DatabaseMigration.apply(db, ":memory:")
        return yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`)
      }),
    )

    expect(journal.map((row) => row.id)).toEqual(canonicalIDs)
  })

  test("retains the backup and rolls back the v02 marker when DDL fails", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "failure.sqlite")

    await useDatabase(filename, (db) => setupExisting(db, { migrationIDs: ["v01_baseline"], partComplete: true }))
    await expect(apply(filename)).rejects.toThrow()

    const backups = await backupPaths(filename)
    expect(backups).toHaveLength(1)
    expect(inspectBackup(backups[0] ?? "").quickCheck).toBe("ok")
    expect(
      await useDatabase(filename, (db) =>
        db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`),
      ),
    ).toEqual([{ id: "v01_baseline" }])
  })

  test("v02 is DDL-only and leaves existing message bytes untouched", async () => {
    await useDatabase(":memory:", (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, { migrationIDs: ["v01_baseline"] })
        yield* db.transaction((tx) => sessionMessagePartMigration.up(tx))

        expect(
          yield* db.get(sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message WHERE id = 'message'`),
        ).toEqual({ bytes: messageBytes })
        expect(yield* db.get(sql`SELECT count(*) AS count FROM session_message_part`)).toEqual({ count: 0 })
      }),
    )
  })
})
