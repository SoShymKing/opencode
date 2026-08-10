export * as DatabaseReplacement from "./replacement"

import { randomUUID } from "node:crypto"
import { lstat, readFile } from "node:fs/promises"
import { basename, dirname, join } from "node:path"
import { Effect, Schema } from "effect"
import { DatabaseArtifact } from "./artifact"
import { DatabaseLease } from "./lease"
import { DatabaseLegacyV01 } from "./legacy-v01"

export function activate(target: string) {
  if (target === ":memory:") return Effect.void
  const markerPath = `${target}.replacement.json`
  return pending(target).pipe(
    Effect.flatMap((exists) =>
      exists
        ? Effect.gen(function* () {
            yield* DatabaseLease.exclusive(target)
            if (!(yield* pending(target))) return
            const marker = yield* readMarker(markerPath)
            if (marker.phase === "prepared") return yield* prepare(target, markerPath, marker)
            return yield* replace(target, markerPath, marker)
          }).pipe(Effect.scoped)
        : Effect.void,
    ),
  )
}

export function pending(target: string) {
  return DatabaseArtifact.entry(`${target}.replacement.json`).pipe(Effect.map((entry) => entry.kind !== "missing"))
}

function prepare(
  target: string,
  markerPath: string,
  marker: Extract<DatabaseLegacyV01.PendingReplacement, { readonly phase: "prepared" }>,
) {
  return Effect.gen(function* () {
    const staged = join(dirname(target), marker.staged.name)
    yield* validate(staged, marker.staged)
    const backup = yield* DatabaseArtifact.exists(target)
      ? Effect.gen(function* () {
          const filename = `${target}.replacement-backup-${Date.now()}-${process.pid}-${randomUUID()}.db`
          yield* DatabaseLegacyV01.snapshotCurrentFile(target, filename)
          return { name: basename(filename), ...(yield* DatabaseArtifact.digest(filename)) }
        })
      : Effect.succeed(undefined)
    const next = {
      format: marker.format,
      version: marker.version,
      phase: "backup_ready",
      staged: marker.staged,
      ...(backup ? { backup } : {}),
    } satisfies DatabaseLegacyV01.PendingReplacement
    yield* DatabaseArtifact.replaceWithText(markerPath, `${JSON.stringify(next)}\n`)
    yield* replace(target, markerPath, next)
  })
}

function replace(
  target: string,
  markerPath: string,
  marker: Extract<DatabaseLegacyV01.PendingReplacement, { readonly phase: "backup_ready" }>,
) {
  return Effect.gen(function* () {
    if (marker.backup) {
      const backup = join(dirname(target), marker.backup.name)
      yield* validate(backup, marker.backup)
      yield* requireNoSidecars(backup)
      yield* DatabaseLegacyV01.validateCurrentFile(backup)
    }
    const staged = join(dirname(target), marker.staged.name)
    if (yield* DatabaseArtifact.exists(staged)) {
      yield* validate(staged, marker.staged)
      yield* requireNoSidecars(staged)
      const targetExists = yield* DatabaseArtifact.exists(target)
      if (marker.backup && !targetExists) return yield* failure("replacement target disappeared after backup")
      if (!marker.backup && targetExists) return yield* failure("replacement target appeared without a backup")
      yield* requireNoSidecars(target)
      yield* DatabaseArtifact.replace(staged, target)
    }
    yield* requireNoSidecars(target)
    yield* validate(target, marker.staged)
    yield* DatabaseLegacyV01.validateCurrentFile(target)
    yield* DatabaseArtifact.remove(markerPath)
    return yield* Effect.void
  })
}

function validate(filename: string, expected: { readonly sha256: string; readonly bytes: number }) {
  return Effect.gen(function* () {
    const entry = yield* DatabaseArtifact.entry(filename)
    if (entry.kind === "missing") return yield* failure(`replacement artifact is missing: ${basename(filename)}`)
    if (entry.kind === "unsafe") return yield* failure(`replacement artifact is unsafe: ${basename(filename)}`)
    const actual = yield* DatabaseArtifact.digest(filename)
    if (actual.sha256 !== expected.sha256 || actual.bytes !== expected.bytes)
      return yield* failure(`replacement artifact is corrupt: ${basename(filename)}`)
    return yield* Effect.void
  })
}

function requireNoSidecars(filename: string) {
  return Effect.all([
    DatabaseArtifact.requireAbsent(`${filename}-wal`),
    DatabaseArtifact.requireAbsent(`${filename}-shm`),
  ]).pipe(
    Effect.mapError((cause) => new DatabaseLegacyV01.DatabaseLegacyV01Error("database sidecar is unsafe", { cause })),
    Effect.asVoid,
  )
}

function readMarker(filename: string) {
  return Effect.gen(function* () {
    const info = yield* Effect.tryPromise({
      try: () => lstat(filename),
      catch: (cause) =>
        new DatabaseLegacyV01.DatabaseLegacyV01Error("pending replacement marker is unreadable", { cause }),
    })
    if (!info.isFile() || info.isSymbolicLink()) return yield* failure("pending replacement marker is unsafe")
    const text = yield* Effect.tryPromise({
      try: () => readFile(filename, "utf8"),
      catch: (cause) =>
        new DatabaseLegacyV01.DatabaseLegacyV01Error("pending replacement marker is unreadable", { cause }),
    })
    const json = yield* Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(text)
    return yield* Schema.decodeUnknownEffect(DatabaseLegacyV01.PendingReplacement)(json, {
      onExcessProperty: "error",
    })
  }).pipe(
    Effect.mapError((cause) =>
      cause instanceof DatabaseLegacyV01.DatabaseLegacyV01Error
        ? cause
        : new DatabaseLegacyV01.DatabaseLegacyV01Error("pending replacement marker is invalid", { cause }),
    ),
  )
}

function failure(message: string) {
  return Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error(message))
}
