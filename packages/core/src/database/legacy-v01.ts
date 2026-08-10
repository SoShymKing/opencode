export * as DatabaseLegacyV01 from "./legacy-v01"

import { randomUUID } from "node:crypto"
import { lstat, readFile } from "node:fs/promises"
import { basename, dirname, resolve } from "node:path"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Effect, Exit, Schema } from "effect"
import { DatabaseArtifact } from "./artifact"
import { DatabaseLease } from "./lease"
import { downgrade, upgrade, validateCurrent } from "./legacy-v01-conversion"

export const Manifest = Schema.Struct({
  format: Schema.Literal("opencode.database"),
  version: Schema.Literal("legacy-v01"),
  sha256: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  bytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
})
export interface Manifest extends Schema.Schema.Type<typeof Manifest> {}

const SafeBasename = Schema.String.check(
  Schema.isPattern(/^(?!\.{1,2}$)(?!.*[. ]$)(?!.*[\\/:*?"<>|\u0000-\u001f])[^\\/]+$/),
)
const Artifact = Schema.Struct({
  name: SafeBasename,
  sha256: Manifest.fields.sha256,
  bytes: Manifest.fields.bytes,
})
const ReplacementBase = {
  format: Schema.Literal("opencode.database.replacement"),
  version: Schema.Literal(1),
  staged: Artifact,
}
export const PendingReplacement = Schema.Union([
  Schema.Struct({ ...ReplacementBase, phase: Schema.Literal("prepared") }),
  Schema.Struct({ ...ReplacementBase, phase: Schema.Literal("backup_ready"), backup: Schema.optional(Artifact) }),
]).pipe(Schema.toTaggedUnion("phase"))
export type PendingReplacement = Schema.Schema.Type<typeof PendingReplacement>

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()

export class DatabaseLegacyV01Error extends globalThis.Error {
  override readonly name = "DatabaseLegacyV01Error"
}

export function exportDatabase(input: { readonly source: string; readonly destination: string }) {
  const destination = resolve(input.destination)
  const sidecar = `${destination}.manifest.json`
  const partial = `${destination}.${process.pid}-${randomUUID()}.partial`
  const manifestPartial = `${sidecar}.${process.pid}-${randomUUID()}.partial`
  return Effect.gen(function* () {
    const source = resolve(input.source)
    yield* validatePaths(source, destination)
    yield* DatabaseArtifact.requireAbsent(destination).pipe(
      Effect.mapError((cause) => new DatabaseLegacyV01Error("export destination already exists", { cause })),
    )
    yield* DatabaseArtifact.requireAbsent(sidecar).pipe(
      Effect.mapError((cause) => new DatabaseLegacyV01Error("export manifest already exists", { cause })),
    )
    yield* requireNoSidecars(destination)
    yield* Effect.gen(function* () {
      yield* DatabaseLease.shared(source)
      yield* useDatabase(source, false, (db) =>
        Effect.gen(function* () {
          yield* validateCurrent(db)
          yield* DatabaseArtifact.snapshot(db, partial)
        }),
      )
    }).pipe(Effect.scoped)
    yield* useDatabase(partial, false, (db) =>
      Effect.gen(function* () {
        yield* downgrade(db)
        yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)")
      }),
    )
    yield* DatabaseArtifact.syncFile(partial)
    const digest = yield* DatabaseArtifact.digest(partial)
    const manifest = { format: "opencode.database", version: "legacy-v01", ...digest } satisfies Manifest
    yield* DatabaseArtifact.write(manifestPartial, `${JSON.stringify(manifest)}\n`)
    yield* DatabaseArtifact.requireAbsent(destination)
    yield* DatabaseArtifact.requireAbsent(sidecar)
    yield* requireNoSidecars(destination)
    yield* DatabaseArtifact.publish(partial, destination)
    yield* DatabaseArtifact.publish(manifestPartial, sidecar).pipe(
      Effect.tapError(() => DatabaseArtifact.discard(destination)),
    )
    return yield* Effect.void
  }).pipe(
    Effect.onExit((exit) =>
      Exit.isFailure(exit)
        ? Effect.all([DatabaseArtifact.discard(partial), DatabaseArtifact.discard(manifestPartial)]).pipe(Effect.asVoid)
        : Effect.void,
    ),
  )
}

export function stageImport(input: { readonly source: string; readonly target: string }) {
  const source = resolve(input.source)
  const target = resolve(input.target)
  const marker = `${target}.replacement.json`
  const staged = `${target}.replacement-${process.pid}-${randomUUID()}.db`
  return Effect.gen(function* () {
    yield* validatePaths(source, target)
    yield* DatabaseArtifact.requireAbsent(marker).pipe(
      Effect.mapError((cause) => new DatabaseLegacyV01Error("pending database replacement already exists", { cause })),
    )
    yield* requireNoSidecars(source)
    const manifest = yield* readManifest(`${source}.manifest.json`)
    const sourceDigest = yield* DatabaseArtifact.digest(source)
    if (manifest.sha256 !== sourceDigest.sha256 || manifest.bytes !== sourceDigest.bytes)
      return yield* failure("legacy manifest does not match database")
    const stagedDigest = yield* Effect.gen(function* () {
      yield* useDatabase(source, true, (db) => DatabaseArtifact.snapshot(db, staged))
      yield* requireUnchanged(source, sourceDigest)
      yield* useDatabase(staged, false, (db) =>
        Effect.gen(function* () {
          yield* upgrade(db)
          yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)")
        }),
      )
      yield* requireNoSidecars(staged)
      yield* DatabaseArtifact.syncFile(staged)
      return yield* DatabaseArtifact.digest(staged)
    }).pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? DatabaseArtifact.discard(staged) : Effect.void)))
    const replacement = {
      format: "opencode.database.replacement",
      version: 1,
      phase: "prepared",
      staged: { name: basename(staged), ...stagedDigest },
    } satisfies PendingReplacement
    yield* DatabaseArtifact.write(marker, `${JSON.stringify(replacement)}\n`).pipe(
      Effect.catch((cause) => handleMarkerPublicationFailure(marker, staged, cause)),
    )
    return { marker, staged }
  })
}

export function handleMarkerPublicationFailure(marker: string, staged: string, cause: DatabaseArtifact.Error) {
  return DatabaseArtifact.entry(marker).pipe(
    Effect.flatMap((entry) => (entry.kind === "missing" ? DatabaseArtifact.discard(staged) : Effect.void)),
    Effect.catch(() => Effect.void),
    Effect.andThen(Effect.fail(cause)),
  )
}

export function validateCurrentFile(filename: string) {
  return useDatabase(filename, true, validateCurrent)
}

export function snapshotCurrentFile(source: string, destination: string) {
  return useDatabase(source, false, (db) =>
    Effect.gen(function* () {
      yield* validateCurrent(db)
      yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)")
      yield* DatabaseArtifact.snapshot(db, destination)
    }),
  ).pipe(Effect.andThen(requireNoSidecars(source)))
}

function useDatabase<A, E, R>(
  filename: string,
  readonly: boolean,
  use: (db: Effect.Success<typeof makeDatabase>) => Effect.Effect<A, E, R>,
) {
  return Effect.gen(function* () {
    return yield* use(yield* makeDatabase)
  }).pipe(Effect.provide(sqliteLayer({ filename, readonly, readwrite: !readonly, create: false })), Effect.scoped)
}

function validatePaths(source: string, destination: string) {
  return Effect.gen(function* () {
    if (source === resolve(":memory:") || destination === resolve(":memory:"))
      return yield* failure("memory databases are unsupported")
    if (process.platform === "win32" ? source.toLowerCase() === destination.toLowerCase() : source === destination)
      return yield* failure("source and destination must differ")
    const sourceStat = yield* fileStat(source)
    if (!sourceStat.isFile() || sourceStat.isSymbolicLink())
      return yield* failure("source must be a regular database file")
    const parent = yield* fileStat(dirname(destination))
    if (!parent.isDirectory()) return yield* failure("destination directory does not exist")
    if (yield* DatabaseArtifact.exists(destination)) {
      const destinationStat = yield* fileStat(destination)
      if (!destinationStat.isFile() || destinationStat.isSymbolicLink())
        return yield* failure("destination must be a regular database file")
    }
    return yield* Effect.void
  })
}

function fileStat(filename: string) {
  return Effect.tryPromise({
    try: () => lstat(filename),
    catch: (cause) => new DatabaseLegacyV01Error(`unsafe database path: ${filename}`, { cause }),
  })
}

function readManifest(filename: string) {
  return Effect.gen(function* () {
    const info = yield* fileStat(filename)
    if (!info.isFile() || info.isSymbolicLink()) return yield* failure("legacy manifest is unsafe")
    const text = yield* Effect.tryPromise({
      try: () => readFile(filename, "utf8"),
      catch: (cause) => new DatabaseLegacyV01Error("legacy manifest is missing", { cause }),
    })
    const json = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(text)
    return yield* Schema.decodeUnknownEffect(Manifest)(json, { onExcessProperty: "error" })
  }).pipe(
    Effect.mapError((cause) =>
      cause instanceof DatabaseLegacyV01Error
        ? cause
        : new DatabaseLegacyV01Error("legacy manifest is invalid", { cause }),
    ),
  )
}

function requireNoSidecars(filename: string) {
  return Effect.all([
    DatabaseArtifact.requireAbsent(`${filename}-wal`),
    DatabaseArtifact.requireAbsent(`${filename}-shm`),
  ]).pipe(
    Effect.mapError((cause) => new DatabaseLegacyV01Error(`database sidecar exists: ${filename}`, { cause })),
    Effect.asVoid,
  )
}

function requireUnchanged(filename: string, expected: DatabaseArtifact.Digest) {
  return Effect.gen(function* () {
    yield* requireNoSidecars(filename)
    const actual = yield* DatabaseArtifact.digest(filename)
    if (actual.sha256 !== expected.sha256 || actual.bytes !== expected.bytes)
      return yield* failure("database source changed during snapshot")
  })
}

function failure(message: string) {
  return Effect.fail(new DatabaseLegacyV01Error(message))
}
