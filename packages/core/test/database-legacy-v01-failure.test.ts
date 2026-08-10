import { describe, expect, test } from "bun:test"
import { Database as SqliteDatabase } from "bun:sqlite"
import { readdir, symlink } from "node:fs/promises"
import path from "node:path"
import { DatabaseArtifact } from "@opencode-ai/core/database/artifact"
import { DatabaseLegacyV01 } from "@opencode-ai/core/database/legacy-v01"
import { Effect, Schema } from "effect"
import { assistantContent, assistantEnvelope, createCurrentDatabase } from "./fixture/database-transfer"
import { tmpdir } from "./fixture/tmpdir"

const decodeJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)

describe("DatabaseLegacyV01 failures", () => {
  test.each([
    [
      "ambiguous assistant storage",
      (database: SqliteDatabase) =>
        database
          .query("UPDATE session_message SET data = ? WHERE id = 'msg_assistant'")
          .run(JSON.stringify({ ...assistantEnvelope, content: assistantContent })),
    ],
    [
      "gapped assistant positions",
      (database: SqliteDatabase) =>
        database
          .query("UPDATE session_message_part SET position = 9 WHERE message_id = 'msg_assistant' AND position = 3")
          .run(),
    ],
    [
      "non-assistant child storage",
      (database: SqliteDatabase) =>
        database
          .query(
            "INSERT INTO session_message_part (message_id, position, id, type, data) VALUES ('msg_user', 0, 'bad', 'text', '{}')",
          )
          .run(),
    ],
    [
      "malformed assistant child",
      (database: SqliteDatabase) =>
        database
          .query(
            "UPDATE session_message_part SET data = '{\"text\":1}' WHERE message_id = 'msg_assistant' AND position = 0",
          )
          .run(),
    ],
  ])("fails closed for %s and leaves the source untouched", async (_name, corrupt) => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    await createCurrentDatabase(source)
    using database = new SqliteDatabase(source)
    corrupt(database)
    database.run("PRAGMA wal_checkpoint(TRUNCATE)")
    database.close()
    const before = await DatabaseArtifact.digest(source).pipe(Effect.runPromise)

    await expect(
      DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise),
    ).rejects.toBeInstanceOf(DatabaseLegacyV01.DatabaseLegacyV01Error)

    expect(await DatabaseArtifact.digest(source).pipe(Effect.runPromise)).toEqual(before)
    expect(await Bun.file(destination).exists()).toBe(false)
    expect(await Bun.file(`${destination}.manifest.json`).exists()).toBe(false)
  })

  test("preserves a valid parent-only assistant row", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    await createCurrentDatabase(source)
    using current = new SqliteDatabase(source)
    current.query("DELETE FROM session_message_part WHERE message_id = 'msg_assistant'").run()
    current
      .query("UPDATE session_message SET data = ? WHERE id = 'msg_assistant'")
      .run(JSON.stringify({ ...assistantEnvelope, content: assistantContent }))
    current.close()

    await DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise)

    using legacy = new SqliteDatabase(destination, { readonly: true })
    expect(
      decodeJson(
        legacy.query<{ readonly data: string }, []>("SELECT data FROM session_message WHERE id = 'msg_assistant'").get()
          ?.data ?? "",
      ),
    ).toEqual({ ...assistantEnvelope, content: assistantContent })
  })

  test("never overwrites an existing export destination", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    await createCurrentDatabase(source)
    await Bun.write(destination, "sentinel")

    await expect(DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise)).rejects.toThrow(
      "already exists",
    )

    expect(await Bun.file(destination).text()).toBe("sentinel")
    expect(await Bun.file(`${destination}.manifest.json`).exists()).toBe(false)
  })

  test.each([
    ["checksum", (manifest: DatabaseLegacyV01.Manifest) => ({ ...manifest, sha256: "0".repeat(64) })],
    ["size", (manifest: DatabaseLegacyV01.Manifest) => ({ ...manifest, bytes: manifest.bytes + 1 })],
  ])("rejects a manifest with the wrong %s without touching the target", async (_name, mutate) => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const source = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: source }).pipe(Effect.runPromise)
    const manifestPath = `${source}.manifest.json`
    const manifest = Schema.decodeUnknownSync(DatabaseLegacyV01.Manifest)(
      decodeJson(await Bun.file(manifestPath).text()),
    )
    await Bun.write(manifestPath, JSON.stringify(mutate(manifest)))
    const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)

    await expect(DatabaseLegacyV01.stageImport({ source, target }).pipe(Effect.runPromise)).rejects.toThrow(
      "does not match",
    )

    expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
    expect(await Bun.file(`${target}.replacement.json`).exists()).toBe(false)
  })

  test("blocks staging when a pending marker exists", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const source = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: source }).pipe(Effect.runPromise)
    await Bun.write(`${target}.replacement.json`, "sentinel")
    const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)

    await expect(DatabaseLegacyV01.stageImport({ source, target }).pipe(Effect.runPromise)).rejects.toThrow(
      "pending database replacement",
    )

    expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
    expect(await Bun.file(`${target}.replacement.json`).text()).toBe("sentinel")
  })

  test("rejects malformed legacy rows after manifest verification and cleans the owned stage", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const source = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: source }).pipe(Effect.runPromise)
    using legacy = new SqliteDatabase(source)
    legacy
      .query("UPDATE session_message SET data = ? WHERE id = 'msg_assistant'")
      .run(JSON.stringify({ ...assistantEnvelope, content: [{ type: "text", id: "bad", text: 1 }] }))
    legacy.run("PRAGMA wal_checkpoint(TRUNCATE)")
    legacy.close()
    const sourceDigest = await DatabaseArtifact.digest(source).pipe(Effect.runPromise)
    await Bun.write(
      `${source}.manifest.json`,
      JSON.stringify({ format: "opencode.database", version: "legacy-v01", ...sourceDigest }),
    )
    const targetBefore = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)

    await expect(DatabaseLegacyV01.stageImport({ source, target }).pipe(Effect.runPromise)).rejects.toThrow(
      "repository schema",
    )

    expect(await DatabaseArtifact.digest(source).pipe(Effect.runPromise)).toEqual(sourceDigest)
    expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(targetBefore)
    expect(
      (await readdir(tmp.path)).filter((name) => name.startsWith(`${path.basename(target)}.replacement-`)),
    ).toEqual([])
  })

  test("rejects duplicate structural JSON properties", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    await createCurrentDatabase(source)
    using database = new SqliteDatabase(source)
    database.run("UPDATE session_message SET data = ? WHERE id = 'msg_assistant'", [
      '{"agent":"build","model":{"id":"test-model","providerID":"test-provider"},"time":{"created":1},"content":[],"content":[]}',
    ])
    database.run("DELETE FROM session_message_part WHERE message_id = 'msg_assistant'")
    database.run("PRAGMA wal_checkpoint(TRUNCATE)")
    database.close()

    await expect(DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise)).rejects.toThrow(
      "malformed JSON",
    )
    expect(await Bun.file(destination).exists()).toBe(false)
  })

  test.each([
    ["trailing comma", '{"text":"preserve me","files":[],"agents":[],"time":{"created":1},}'],
    ["comment", '{"text":"preserve me",/* unsafe */"files":[],"agents":[],"time":{"created":1}}'],
  ])("rejects non-strict JSON with a %s", async (_name, raw) => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    await createCurrentDatabase(source)
    using database = new SqliteDatabase(source)
    database.run("UPDATE session_message SET data = ? WHERE id = 'msg_user'", [raw])
    database.close()

    await expect(DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise)).rejects.toThrow(
      "malformed JSON",
    )
  })

  test("rejects unknown current migration IDs before export", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    await createCurrentDatabase(source)
    using database = new SqliteDatabase(source)
    database.run("INSERT INTO migration (id, time_completed) VALUES ('future_migration', 1)")
    database.run("PRAGMA wal_checkpoint(TRUNCATE)")
    database.close()
    const before = await DatabaseArtifact.digest(source).pipe(Effect.runPromise)

    await expect(DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise)).rejects.toThrow(
      "unknown migration",
    )

    expect(await DatabaseArtifact.digest(source).pipe(Effect.runPromise)).toEqual(before)
    expect(await Bun.file(destination).exists()).toBe(false)
  })

  test("rejects source and destination SQLite sidecars", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const source = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    const destination = path.join(tmp.path, "export.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: source }).pipe(Effect.runPromise)
    await Bun.write(`${source}-wal`, "unauthenticated")

    await expect(DatabaseLegacyV01.stageImport({ source, target }).pipe(Effect.runPromise)).rejects.toThrow("sidecar")

    await Bun.write(`${destination}-shm`, "unauthenticated")
    await expect(
      DatabaseLegacyV01.exportDatabase({ source: current, destination }).pipe(Effect.runPromise),
    ).rejects.toThrow("sidecar")
    expect(await Bun.file(target).exists()).toBe(true)
  })

  test("treats a dangling marker symlink as an unsafe pending replacement", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const source = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: source }).pipe(Effect.runPromise)
    await symlink(path.join(tmp.path, "missing"), `${target}.replacement.json`)

    await expect(DatabaseLegacyV01.stageImport({ source, target }).pipe(Effect.runPromise)).rejects.toThrow(
      "pending database replacement",
    )
  })
})
