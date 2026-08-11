export * as DatabaseLease from "./lease"

import { open } from "node:fs/promises"
import { lease as platformLease } from "#sqlite"
import { Effect } from "effect"
import { DatabaseArtifact } from "./artifact"

export type Mode = "shared" | "exclusive"

export class DatabaseLeaseError extends globalThis.Error {
  override readonly name = "DatabaseLeaseError"

  constructor(
    readonly target: string,
    options: ErrorOptions,
  ) {
    super(`Database is in use: ${target}`, options)
  }
}

export function shared(target: string) {
  return acquire(target, "shared")
}

export function exclusive(target: string) {
  return acquire(target, "exclusive")
}

function acquire(target: string, mode: Mode) {
  const filename = `${target}.lease.sqlite`
  return Effect.gen(function* () {
    const entry = yield* DatabaseArtifact.entry(filename)
    if (entry.kind === "missing") yield* create(filename)
    yield* DatabaseArtifact.requireRegularFile(filename)
    yield* platformLease({ filename, mode })
  }).pipe(Effect.mapError((cause) => new DatabaseLeaseError(target, { cause })))
}

function create(filename: string) {
  return Effect.acquireUseRelease(
    Effect.tryPromise({
      try: () => open(filename, "wx"),
      catch: (cause) => cause,
    }),
    () => Effect.void,
    (handle) => Effect.promise(() => handle.close()),
  ).pipe(
    Effect.catch((cause) =>
      cause instanceof globalThis.Error && "code" in cause && cause.code === "EEXIST"
        ? Effect.void
        : Effect.fail(cause),
    ),
  )
}
