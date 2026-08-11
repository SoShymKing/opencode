import { describe, expect, test } from "bun:test"
import { Database as SqliteDatabase } from "bun:sqlite"
import path from "node:path"
import { symlink } from "node:fs/promises"
import { DatabaseLease } from "@opencode-ai/core/database/lease"
import { DatabaseLegacyV01 } from "@opencode-ai/core/database/legacy-v01"
import { DatabaseReplacement } from "@opencode-ai/core/database/replacement"
import { Effect } from "effect"
import { createCurrentDatabase } from "./fixture/database-transfer"
import { tmpdir } from "./fixture/tmpdir"

const worker = path.join(import.meta.dir, "fixture", "database-lease-worker.ts")

describe("DatabaseLease", () => {
  test("opens normal databases in WAL mode", async () => {
    await using tmp = await tmpdir()
    const target = path.join(tmp.path, "target.sqlite")

    await createCurrentDatabase(target)

    using database = new SqliteDatabase(target, { readonly: true })
    expect(database.query<{ readonly journal_mode: string }, []>("PRAGMA journal_mode").get()).toEqual({
      journal_mode: "wal",
    })
  })

  test("holds concurrent shared leases and releases them when processes exit", async () => {
    await using tmp = await tmpdir()
    const target = path.join(tmp.path, "target.sqlite")
    const first = spawn("shared", target)
    const second = spawn("shared", target)
    await Promise.all([ready(first), ready(second)])

    await expect(Effect.runPromise(DatabaseLease.exclusive(target).pipe(Effect.scoped))).rejects.toBeInstanceOf(
      DatabaseLease.DatabaseLeaseError,
    )

    first.kill()
    second.kill()
    await Promise.all([first.exited, second.exited])
    await Effect.runPromise(DatabaseLease.exclusive(target).pipe(Effect.scoped))
  })

  test("a file-backed database layer holds a shared lease for its whole scope", async () => {
    await using tmp = await tmpdir()
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(target)
    const child = spawn("database", target)
    await ready(child)

    await expect(Effect.runPromise(DatabaseLease.exclusive(target).pipe(Effect.scoped))).rejects.toBeInstanceOf(
      DatabaseLease.DatabaseLeaseError,
    )

    child.stdin.end()
    expect(await child.exited).toBe(0)
    await Effect.runPromise(DatabaseLease.exclusive(target).pipe(Effect.scoped))
  })

  test("blocks replacement until an abruptly exiting database process releases its shared lease", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const legacy = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    {
      using old = new SqliteDatabase(target)
      old.query("UPDATE session SET title = 'old target'").run()
      old.run("PRAGMA wal_checkpoint(TRUNCATE)")
    }
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
    const child = spawn("database", target)
    await ready(child)
    const staged = await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)

    await expect(DatabaseReplacement.activate(target).pipe(Effect.runPromise)).rejects.toBeInstanceOf(
      DatabaseLease.DatabaseLeaseError,
    )
    expect(await Bun.file(staged.marker).exists()).toBe(true)

    child.kill()
    await child.exited
    await DatabaseReplacement.activate(target).pipe(Effect.runPromise)
    expect(await Bun.file(staged.marker).exists()).toBe(false)
    expect(await Bun.file(`${target}.lease.sqlite`).exists()).toBe(true)
    using replaced = new SqliteDatabase(target, { readonly: true })
    expect(replaced.query<{ readonly title: string }, []>("SELECT title FROM session").get()).toEqual({
      title: "Session",
    })
  })

  test("rejects an unsafe stable lease entry", async () => {
    await using tmp = await tmpdir()
    const target = path.join(tmp.path, "target.sqlite")
    await Bun.write(path.join(tmp.path, "other"), "")
    await symlink(path.join(tmp.path, "other"), `${target}.lease.sqlite`)

    await expect(Effect.runPromise(DatabaseLease.shared(target).pipe(Effect.scoped))).rejects.toBeInstanceOf(
      DatabaseLease.DatabaseLeaseError,
    )
  })
})

function spawn(mode: "shared" | "exclusive" | "database", target: string) {
  return Bun.spawn([process.execPath, "run", worker, mode, target], {
    cwd: process.cwd(),
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  })
}

async function ready(child: ReturnType<typeof spawn>) {
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
