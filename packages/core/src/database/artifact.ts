export * as DatabaseArtifact from "./artifact"

import { createHash, randomUUID } from "node:crypto"
import { createReadStream } from "node:fs"
import { link, lstat, open, rename, rm, writeFile } from "node:fs/promises"
import { dirname } from "node:path"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { sql } from "drizzle-orm"
import { Effect, Exit } from "effect"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase

export type Digest = {
  readonly sha256: string
  readonly bytes: number
}

export type Entry = { readonly kind: "missing" } | { readonly kind: "regular" } | { readonly kind: "unsafe" }

export class Error extends globalThis.Error {
  override readonly name = "DatabaseArtifactError"

  constructor(
    readonly operation:
      | "close"
      | "hash"
      | "open"
      | "publish"
      | "remove"
      | "replace"
      | "snapshot"
      | "stat"
      | "sync"
      | "sync-directory"
      | "write",
    options: ErrorOptions & { readonly destinationVisible?: boolean },
  ) {
    super(`Failed to ${operation} database artifact`, options)
    this.destinationVisible = options.destinationVisible
  }

  readonly destinationVisible: boolean | undefined
}

export function snapshot(db: Database, destination: string) {
  const partial = `${destination}.${process.pid}-${randomUUID()}.partial`
  return db.run(sql`VACUUM INTO ${partial}`).pipe(
    Effect.mapError((cause) => new Error("snapshot", { cause })),
    Effect.andThen(syncFile(partial)),
    Effect.andThen(publish(partial, destination)),
    Effect.onExit((exit) => (Exit.isFailure(exit) ? removeOwned(partial) : Effect.void)),
  )
}

export function digest(filename: string) {
  return operation("hash", async () => {
    const hash = createHash("sha256")
    let bytes = 0
    for await (const chunk of createReadStream(filename)) {
      hash.update(chunk)
      bytes += chunk.length
    }
    return { sha256: hash.digest("hex"), bytes } satisfies Digest
  })
}

export function publish(source: string, destination: string) {
  return operation("publish", () => link(source, destination)).pipe(
    Effect.mapError((cause) => new Error("publish", { cause, destinationVisible: false })),
    Effect.andThen(completePublication(source, destination)),
  )
}

export function completePublication(source: string, destination: string) {
  return syncDirectory(destination).pipe(
    Effect.andThen(removeOwned(source)),
    Effect.andThen(syncDirectory(destination)),
    Effect.mapError((cause) => new Error("publish", { cause, destinationVisible: true })),
  )
}

export function write(destination: string, data: string) {
  const partial = `${destination}.${process.pid}-${randomUUID()}.partial`
  return operation("write", () => writeFile(partial, data, { encoding: "utf8", flag: "wx" })).pipe(
    Effect.andThen(syncFile(partial)),
    Effect.andThen(publish(partial, destination)),
    Effect.onExit((exit) => (Exit.isFailure(exit) ? removeOwned(partial) : Effect.void)),
  )
}

export function replaceWithText(destination: string, data: string) {
  const partial = `${destination}.${process.pid}-${randomUUID()}.partial`
  return operation("write", () => writeFile(partial, data, { encoding: "utf8", flag: "wx" })).pipe(
    Effect.andThen(syncFile(partial)),
    Effect.andThen(replace(partial, destination)),
    Effect.onExit((exit) => (Exit.isFailure(exit) ? removeOwned(partial) : Effect.void)),
  )
}

export function replace(source: string, destination: string) {
  return operation("replace", () => rename(source, destination)).pipe(Effect.andThen(syncDirectory(destination)))
}

export function remove(filename: string) {
  return operation("remove", () => rm(filename)).pipe(Effect.andThen(syncDirectory(filename)))
}

export function discard(filename: string) {
  return operation("remove", () => rm(filename, { force: true })).pipe(Effect.andThen(syncDirectory(filename)))
}

export function exists(filename: string) {
  return entry(filename).pipe(Effect.map((value) => value.kind !== "missing"))
}

export function entry(filename: string) {
  return operation("stat", () => lstat(filename)).pipe(
    Effect.map((info): Entry => (info.isFile() && !info.isSymbolicLink() ? { kind: "regular" } : { kind: "unsafe" })),
    Effect.catch((cause) =>
      cause.cause instanceof globalThis.Error && "code" in cause.cause && cause.cause.code === "ENOENT"
        ? Effect.succeed({ kind: "missing" } as const)
        : Effect.fail(cause),
    ),
  )
}

export function requireRegularFile(filename: string) {
  return entry(filename).pipe(
    Effect.flatMap((value) =>
      value.kind === "regular"
        ? Effect.void
        : Effect.fail(new Error("stat", { cause: new globalThis.Error(`Unsafe database artifact: ${filename}`) })),
    ),
  )
}

export function requireAbsent(filename: string) {
  return entry(filename).pipe(
    Effect.flatMap((value) =>
      value.kind === "missing"
        ? Effect.void
        : Effect.fail(new Error("stat", { cause: new globalThis.Error(`Database artifact exists: ${filename}`) })),
    ),
  )
}

export function syncFile(filename: string) {
  return Effect.acquireUseRelease(
    operation("open", () => open(filename, "r+")),
    (handle) => operation("sync", () => handle.sync()),
    (handle) => operation("close", () => handle.close()),
  )
}

export function syncDirectory(filename: string) {
  // Node and Bun expose no supported Windows parent-directory metadata flush.
  if (process.platform === "win32") return Effect.void
  return Effect.acquireUseRelease(
    operation("open", () => open(dirname(filename), "r")),
    (handle) => operation("sync-directory", () => handle.sync()),
    (handle) => operation("close", () => handle.close()),
  )
}

function removeOwned(filename: string) {
  return operation("remove", () => rm(filename, { force: true }))
}

function operation<A>(name: Error["operation"], run: () => Promise<A>) {
  return Effect.tryPromise({
    try: run,
    catch: (cause) => new Error(name, { cause }),
  })
}
