import { expect, test } from "bun:test"
import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { stat } from "node:fs/promises"
import { basename, join } from "node:path"
import { sql } from "drizzle-orm"
import { Effect, Exit } from "effect"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseLegacyV01 } from "../src/database/legacy-v01"
import { WsNativeFile } from "../src/database/ws-native-file"
import { WsNativeStage } from "../src/database/ws-native-stage"
import { useDatabase, type MigrationDatabase } from "./fixture/database-migration"
import { setupWs } from "./fixture/database-ws"
import { tmpdir } from "./fixture/tmpdir"

function checkpoint(db: MigrationDatabase, source: string) {
  return Effect.gen(function* () {
    const before = yield* WsNativeStage.identity(source)
    expect(before.evidence.wal).not.toBeNull()
    expect(yield* db.all("PRAGMA wal_checkpoint(TRUNCATE)")).toEqual([{ busy: 0, log: 0, checkpointed: 0 }])
    const after = yield* WsNativeStage.identity(source)
    expect(after.evidence.main).not.toEqual(before.evidence.main)
    expect(after.evidence.wal).toBeNull()
    expect(after.fingerprint).not.toBe(before.fingerprint)
    return after
  })
}

for (const phase of ["snapshot", "ready", "converting"] as const) {
  test(`reuses saved ${phase} pins and progress after a logical-no-op WAL checkpoint`, async () => {
    await using temp = await tmpdir()
    const source = join(temp.path, "source.db")
    await useDatabase(source, (db) => Effect.gen(function* () {
      yield* db.run("PRAGMA journal_mode = WAL")
      yield* db.run("PRAGMA wal_autocheckpoint = 0")
      yield* setupWs(db, { directory: temp.path })
      yield* WsNativeStage.capture(source, source)
      if (phase !== "snapshot") yield* WsNativeStage.prepare(source, source)
      const saved = yield* WsNativeStage.read(source)
      const snapshot = join(temp.path, saved.snapshot.name)
      const work = join(temp.path, saved.work)
      if (phase === "converting") {
        yield* DatabaseLegacyV01.useDatabase(work, false, (copy) => copy.run(
          "CREATE TRIGGER refuse_replay BEFORE INSERT ON session_message BEGIN SELECT RAISE(ABORT, 'replayed committed prefix'); END",
        ))
        yield* DatabaseArtifact.replaceWithText(`${source}.ws-stage.json`, JSON.stringify({ ...saved, phase, ready: undefined }))
      }
      const marker = yield* WsNativeStage.read(source)
      const markerText = yield* Effect.promise(() => Bun.file(`${source}.ws-stage.json`).text())
      const snapshotDigest = yield* DatabaseArtifact.digest(snapshot)
      const progress = phase === "snapshot" ? undefined : yield* DatabaseLegacyV01.useDatabase(work, true, (copy) => copy.all("SELECT * FROM _ws_stage"))
      const workDigest = phase === "snapshot" ? undefined : yield* DatabaseArtifact.digest(work)
      const workModified = phase === "snapshot" ? undefined : yield* Effect.promise(() => stat(work).then((info) => info.mtimeMs))
      const current = yield* checkpoint(db, source)
      yield* WsNativeFile.requireSameSource(source, snapshot)
      const retry = yield* WsNativeStage.capture(source, source).pipe(Effect.exit)
      expect(yield* Effect.promise(() => Bun.file(`${source}.ws-stage.json`).text())).toBe(markerText)
      expect(yield* DatabaseArtifact.digest(snapshot)).toEqual(snapshotDigest)
      if (workDigest) {
        assert(progress !== undefined)
        expect(yield* DatabaseArtifact.digest(work)).toEqual(workDigest)
        expect(yield* DatabaseLegacyV01.useDatabase(work, true, (copy) => copy.all("SELECT * FROM _ws_stage"))).toEqual(progress)
      }
      if (Exit.isFailure(retry)) return yield* Effect.failCause(retry.cause)
      expect(retry.value.before).toEqual(current)
      expect(retry.value.marker).toEqual(marker)
      const ready = yield* WsNativeStage.prepare(source, source, retry.value)
      const completed = yield* WsNativeStage.read(source)
      expect(completed.token).toBe(marker.token)
      expect(completed.snapshot).toEqual(marker.snapshot)
      expect(completed.work).toBe(marker.work)
      expect(completed.phase).toBe("ready")
      expect(yield* DatabaseArtifact.digest(work)).toEqual({ sha256: ready.sha256, bytes: ready.bytes })
      if (phase === "ready") {
        assert(saved.ready !== undefined && workModified !== undefined)
        expect(ready).toEqual(saved.ready)
        expect(yield* Effect.promise(() => stat(work).then((info) => info.mtimeMs))).toBe(workModified)
      }
      if (progress) expect(yield* DatabaseLegacyV01.useDatabase(work, true, (copy) => copy.all("SELECT * FROM _ws_stage"))).toEqual(progress)
      expect(yield* WsNativeStage.identity(source)).toEqual(current)
    }))
  })
}

const mutations = {
  row: "UPDATE session SET title = 'changed after checkpoint'",
  insert: "INSERT INTO data_migration (name, time_completed) VALUES ('checkpoint_insert', 3)",
  delete: "DELETE FROM data_migration WHERE name = 'fixture-data'",
  schema: "CREATE TABLE later_schema (value TEXT)",
  pragma: "PRAGMA user_version = 42",
  application: "PRAGMA application_id = 42",
} as const

for (const [kind, mutation] of Object.entries(mutations)) {
  test(`rejects a real ${kind} change after checkpoint without altering saved ready artifacts`, async () => {
    await using temp = await tmpdir()
    const source = join(temp.path, "source.db")
    await useDatabase(source, (db) => Effect.gen(function* () {
      yield* db.run("PRAGMA journal_mode = WAL")
      yield* db.run("PRAGMA wal_autocheckpoint = 0")
      yield* setupWs(db, { directory: temp.path })
      yield* WsNativeStage.prepare(source, source)
      const marker = yield* WsNativeStage.read(source)
      const snapshot = join(temp.path, marker.snapshot.name)
      const work = join(temp.path, marker.work)
      const pins = yield* Effect.all([DatabaseArtifact.digest(snapshot), DatabaseArtifact.digest(work)])
      const markerText = yield* Effect.promise(() => Bun.file(`${source}.ws-stage.json`).text())
      yield* checkpoint(db, source)
      yield* WsNativeFile.requireSameSource(source, snapshot)
      yield* db.run(mutation)
      expect((yield* WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.flip)).message).toBe("replacement target differs from its backup")
      const current = yield* WsNativeStage.identity(source)
      expect((yield* WsNativeStage.prepare(source, source).pipe(Effect.exit))._tag).toBe("Failure")
      expect(yield* Effect.promise(() => Bun.file(`${source}.ws-stage.json`).text())).toBe(markerText)
      expect(yield* Effect.all([DatabaseArtifact.digest(snapshot), DatabaseArtifact.digest(work)])).toEqual(pins)
      expect(yield* WsNativeStage.identity(source)).toEqual(current)
    }))
  })
}

for (const kind of ["unchanged", "row", "insert", "delete", "schema", "pragma", "application"] as const) {
  test(`old empty-sequence snapshot after checkpoint: ${kind}`, async () => {
    await using temp = await tmpdir()
    const source = join(temp.path, "source.db")
    const token = randomUUID()
    const snapshot = `${source}.ws-snapshot-${token}.db`
    const faithful = join(temp.path, "faithful.db")
    await useDatabase(source, (db) => Effect.gen(function* () {
      yield* db.run("PRAGMA journal_mode = WAL")
      yield* db.run("PRAGMA wal_autocheckpoint = 0")
      yield* setupWs(db, { directory: temp.path })
      yield* db.run("CREATE TABLE discarded (id INTEGER PRIMARY KEY AUTOINCREMENT)")
      yield* db.run("DROP TABLE discarded")
      yield* db.run(sql`VACUUM INTO ${snapshot}`)
      yield* DatabaseArtifact.snapshot(db, faithful)
      expect(yield* DatabaseLegacyV01.useDatabase(snapshot, true, (copy) => copy.get("SELECT 1 FROM sqlite_schema WHERE name = 'sqlite_sequence'"))).toBeUndefined()
      const digest = yield* DatabaseArtifact.digest(snapshot)
      const before = yield* WsNativeStage.identity(source)
      const marker = {
        format: "opencode.database.ws-stage", version: 1, token,
        source: { name: basename(source), evidence: before.evidence },
        snapshot: { name: basename(snapshot), ...digest }, work: `${basename(source)}.native-${token}.db`, phase: "snapshot",
      } satisfies Effect.Success<ReturnType<typeof WsNativeStage.read>>
      const markerText = `${JSON.stringify(marker)}\n`
      yield* DatabaseArtifact.write(`${source}.ws-stage.json`, markerText)
      const current = yield* checkpoint(db, source)
      yield* WsNativeFile.requireSameSource(source, faithful)
      expect((yield* WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.exit))._tag).toBe("Failure")
      if (kind !== "unchanged") {
        yield* db.run(mutations[kind])
        expect((yield* WsNativeFile.requireSameSource(source, faithful).pipe(Effect.exit))._tag).toBe("Failure")
      }
      const retry = yield* WsNativeStage.capture(source, source).pipe(Effect.exit)
      expect(yield* DatabaseArtifact.digest(snapshot)).toEqual(digest)
      expect(yield* DatabaseArtifact.exists(join(temp.path, marker.work))).toBe(false)
      if (kind !== "unchanged") {
        expect(retry._tag).toBe("Failure")
        expect(yield* Effect.promise(() => Bun.file(`${source}.ws-stage.json`).text())).toBe(markerText)
        return
      }
      if (Exit.isFailure(retry)) {
        expect(yield* Effect.promise(() => Bun.file(`${source}.ws-stage.json`).text())).toBe(markerText)
        return yield* Effect.failCause(retry.cause)
      }
      expect(retry.value.before).toEqual(current)
      expect(retry.value.marker.token).not.toBe(token)
      expect(retry.value.marker.source).toEqual(marker.source)
      expect(retry.value.marker.phase).toBe("snapshot")
      const repaired = join(temp.path, retry.value.marker.snapshot.name)
      expect(yield* DatabaseArtifact.digest(repaired)).toEqual({ sha256: retry.value.marker.snapshot.sha256, bytes: retry.value.marker.snapshot.bytes })
      yield* WsNativeFile.requireSameSource(source, repaired)
      expect(yield* DatabaseLegacyV01.useDatabase(repaired, true, (copy) => copy.all("SELECT * FROM sqlite_sequence"))).toEqual([])
      expect((yield* WsNativeStage.capture(source, source)).marker).toEqual(retry.value.marker)
      expect(yield* WsNativeStage.identity(source)).toEqual(current)
    }))
  })
}
