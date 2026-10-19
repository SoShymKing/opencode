import { expect, spyOn, test } from "bun:test"
import { randomUUID } from "node:crypto"
import { readdir } from "node:fs/promises"
import { basename, join } from "node:path"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { DatabaseArtifact } from "../src/database/artifact"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { DatabaseLegacyV01 } from "../src/database/legacy-v01"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { WsNativeFile } from "../src/database/ws-native-file"
import { WsNativeStage } from "../src/database/ws-native-stage"
import { tmpdir } from "./fixture/tmpdir"

for (const scenario of ["recover", "publication-before", "publication-after", "row", "schema", "pragma", "source", "work", "-wal", "-shm", "-journal", "converting", "ready", "handoff"]) {
  test(`persisted orphan sequence snapshot: ${scenario}`, async () => {
    await using temp = await tmpdir()
    const source = join(temp.path, "source.db")
    const token = randomUUID()
    const snapshot = `${source}.ws-snapshot-${token}.db`
    const work = `${basename(source)}.native-${token}.db`
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* db.run("PRAGMA journal_mode = WAL")
      yield* db.run("CREATE TABLE facts (id TEXT PRIMARY KEY, value TEXT)")
      yield* db.run("INSERT INTO facts (rowid, id, value) VALUES (2, 'one', 'retained'), (7, 'two', 'retained')")
      yield* db.run("CREATE TABLE _snapshot_sequence (value TEXT)")
      yield* db.run("CREATE TABLE discarded (id INTEGER PRIMARY KEY AUTOINCREMENT)")
      yield* db.run("DROP TABLE discarded")
      yield* db.run(sql`VACUUM INTO ${snapshot}`)
      const mutation = {
        row: "UPDATE facts SET value = 'changed' WHERE id = 'one'",
        schema: "CREATE TABLE extra (value TEXT)",
        pragma: "PRAGMA user_version = 42",
      }[scenario]
      if (mutation) yield* DatabaseLegacyV01.useDatabase(snapshot, false, (copy) => copy.run(mutation))
      const digest = yield* DatabaseArtifact.digest(snapshot)
      const before = yield* WsNativeStage.identity(source)
      expect(before.evidence.wal).not.toBeNull()
      const phase = scenario === "converting" || scenario === "ready" || scenario === "handoff" ? scenario : "snapshot"
      const marker = {
        format: "opencode.database.ws-stage", version: 1, token,
        source: { name: basename(source), evidence: before.evidence },
        snapshot: { name: basename(snapshot), ...digest }, work, phase,
        ...(phase === "ready" || phase === "handoff" ? { ready: { name: work, ...digest } } : {}),
      } satisfies Effect.Success<ReturnType<typeof WsNativeStage.read>>
      const text = `${JSON.stringify(marker)}\n`
      yield* DatabaseArtifact.write(`${source}.ws-stage.json`, text)
      if (scenario === "source") yield* db.run("UPDATE facts SET value = 'changed' WHERE id = 'one'")
      if (scenario === "work" || scenario.startsWith("-"))
        yield* DatabaseArtifact.write(join(temp.path, work + (scenario === "work" ? "" : scenario)), "retained evidence")
      const candidates: { name: string; digest: DatabaseArtifact.Digest }[] = []
      if (scenario === "publication-before" || scenario === "publication-after") {
        const replace = DatabaseArtifact.replaceWithText
        const fault = new DatabaseArtifact.Error("replace", { cause: new Error("synthetic marker publication failure") })
        const injection = spyOn(DatabaseArtifact, "replaceWithText").mockImplementation((filename, data) => {
          if (filename !== `${source}.ws-stage.json`) return replace(filename, data)
          return Effect.gen(function* () {
            const names = yield* Effect.promise(() => readdir(temp.path))
            for (const name of names.filter((name) => name.startsWith(`${basename(source)}.ws-snapshot-`) && name.endsWith(".db") && name !== basename(snapshot)))
              candidates.push({ name, digest: yield* DatabaseArtifact.digest(join(temp.path, name)) })
            if (scenario === "publication-after") yield* replace(filename, data)
            return yield* Effect.fail(fault)
          })
        })
        try {
          expect(yield* WsNativeStage.capture(source, source).pipe(Effect.flip)).toBe(fault)
        } finally {
          injection.mockRestore()
        }
        expect(candidates).toHaveLength(1)
        expect(yield* DatabaseArtifact.digest(snapshot)).toEqual(digest)
        const visible = yield* WsNativeStage.read(source)
        expect(yield* DatabaseArtifact.digest(join(temp.path, visible.snapshot.name)))
          .toEqual({ sha256: visible.snapshot.sha256, bytes: visible.snapshot.bytes })
        if (scenario === "publication-before") expect(visible).toEqual(marker)
        if (scenario === "publication-after") {
          expect(visible.snapshot.name).toBe(candidates[0]?.name)
          yield* WsNativeFile.requireSameSource(source, join(temp.path, visible.snapshot.name))
        }
        for (const candidate of candidates) {
          expect(yield* DatabaseArtifact.digest(join(temp.path, candidate.name))).toEqual(candidate.digest)
          yield* WsNativeFile.requireSameSource(source, join(temp.path, candidate.name))
        }
      }
      if (scenario !== "recover" && !scenario.startsWith("publication-")) {
        expect((yield* WsNativeStage.capture(source, source).pipe(Effect.exit))._tag).toBe("Failure")
        expect(yield* Effect.promise(() => Bun.file(`${source}.ws-stage.json`).text())).toBe(text)
        expect(yield* DatabaseArtifact.digest(snapshot)).toEqual(digest)
        return
      }
      const captured = yield* WsNativeStage.capture(source, source)
      expect(captured.marker.token).not.toBe(token)
      expect(captured.marker.source).toEqual(marker.source)
      expect(captured.marker.phase).toBe("snapshot")
      expect(captured.marker.work).toBe(`${basename(source)}.native-${captured.marker.token}.db`)
      expect(yield* WsNativeStage.read(source)).toEqual(captured.marker)
      expect(yield* DatabaseArtifact.digest(snapshot)).toEqual(digest)
      const restored = join(temp.path, captured.marker.snapshot.name)
      expect(yield* DatabaseArtifact.digest(restored)).toEqual({ sha256: captured.marker.snapshot.sha256, bytes: captured.marker.snapshot.bytes })
      yield* WsNativeFile.requireSameSource(source, restored)
      yield* DatabaseLegacyV01.useDatabase(restored, true, (copy) => Effect.gen(function* () {
        expect(yield* copy.all("SELECT * FROM sqlite_sequence")).toEqual([])
        expect(yield* copy.all("SELECT rowid FROM facts ORDER BY rowid")).toEqual([{ rowid: 2 }, { rowid: 7 }])
      }))
      expect(yield* WsNativeStage.identity(source)).toEqual(before)
      for (const candidate of candidates)
        expect(yield* DatabaseArtifact.digest(join(temp.path, candidate.name))).toEqual(candidate.digest)
      if (scenario === "publication-before") expect(captured.marker.snapshot.name).not.toBe(candidates[0]?.name)
      if (scenario === "publication-after") expect(captured.marker.snapshot.name).toBe(candidates[0]?.name)
    }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
  })
}
