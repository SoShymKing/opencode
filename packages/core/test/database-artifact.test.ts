import { describe, expect, test } from "bun:test"
import path from "node:path"
import { mkdir } from "node:fs/promises"
import { DatabaseArtifact } from "@opencode/core/database/artifact"
import { DatabaseLegacyV01 } from "@opencode/core/database/legacy-v01"
import { Effect } from "effect"
import { tmpdir } from "./fixture/tmpdir"
import { Database } from "bun:sqlite"
import { EffectDrizzleSqlite } from "../src/database/drizzle.js"
import { sqliteLayer } from "../src/database/sqlite.bun.js"

describe("DatabaseArtifact", () => {
  test("snapshot includes committed WAL rows and publishes a standalone database", async () => {
    await using directory = await tmpdir()
    const destination = path.join(directory.path, "backup.sqlite")
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* db.run("PRAGMA journal_mode = WAL")
      yield* db.run("PRAGMA wal_autocheckpoint = 0")
      yield* db.run("CREATE TABLE history (data TEXT)")
      yield* db.run("INSERT INTO history VALUES ('committed-wal')")
      yield* DatabaseArtifact.snapshot(db, destination)
    }).pipe(Effect.provide(sqliteLayer({ filename: path.join(directory.path, "source.sqlite") })), Effect.scoped))
    using backup = new Database(destination, { readonly: true })
    expect(backup.query("SELECT data FROM history").all()).toEqual([{ data: "committed-wal" }])
    expect(backup.query("PRAGMA quick_check").get()).toEqual({ quick_check: "ok" })
  })

  test("reports marker publication as invisible when the destination link was not created", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "source")
    const destination = path.join(tmp.path, "destination")
    await Bun.write(source, "source")
    await Bun.write(destination, "destination")

    const error = await DatabaseArtifact.publish(source, destination).pipe(Effect.flip, Effect.runPromise)

    expect(error.destinationVisible).toBe(false)
    expect(await Bun.file(source).exists()).toBe(true)
    expect(await Bun.file(destination).text()).toBe("destination")
  })

  test("reports marker publication as visible when cleanup fails after the destination link", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "non-empty-source")
    const destination = path.join(tmp.path, "destination")
    await mkdir(source)
    await Bun.write(path.join(source, "child"), "source")
    await Bun.write(destination, "published")

    const error = await DatabaseArtifact.completePublication(source, destination).pipe(Effect.flip, Effect.runPromise)

    expect(error.destinationVisible).toBe(true)
    expect(await Bun.file(destination).text()).toBe("published")
  })

  test("retains the stage unless marker absence is proven", async () => {
    await using tmp = await tmpdir()
    const marker = path.join(tmp.path, "marker")
    const retained = path.join(tmp.path, "retained")
    const discarded = path.join(tmp.path, "discarded")
    const cause = new DatabaseArtifact.Error("publish", { cause: new Error("failure"), destinationVisible: true })
    await Bun.write(marker, "marker")
    await Bun.write(retained, "stage")
    await Bun.write(discarded, "stage")

    await DatabaseLegacyV01.handleMarkerPublicationFailure(marker, retained, cause).pipe(Effect.flip, Effect.runPromise)
    await DatabaseLegacyV01.handleMarkerPublicationFailure(
      path.join(tmp.path, "missing-marker"),
      discarded,
      cause,
    ).pipe(Effect.flip, Effect.runPromise)

    expect(await Bun.file(retained).exists()).toBe(true)
    expect(await Bun.file(discarded).exists()).toBe(false)
  })
})
