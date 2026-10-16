import { Database, type SQLQueryBindings } from "bun:sqlite"
import { Context, Effect, Layer } from "effect"
import { Reactivity } from "effect/reactivity"
import { SqlClient } from "effect/sql"
import { classifySqliteError, SqlError } from "effect/sql/SqlError"
import { Sqlite } from "./sqlite.js"

const TypeId = "~@opencode/core/database/SqliteBun" as const

export const supportsTuningPragmas = true

// Foreign keys default OFF and can be toggled per connection.
export const supportsForeignKeyToggle = true

interface Config extends Sqlite.ClientConfig {
  readonly filename: string
  readonly readonly?: boolean
  readonly create?: boolean
  readonly readwrite?: boolean
  readonly disableWAL?: boolean
}

type LeaseConfig = {
  readonly filename: string
  readonly mode: "shared" | "exclusive"
}

const make = (options: Config) =>
  Effect.gen(function* () {
    const native = (yield* Sqlite.Native) as Database

    const run = (query: string, params: ReadonlyArray<unknown> = []) =>
      Effect.withFiber<Array<Record<string, unknown>>, SqlError>((fiber) => {
        using statement = native.query<Record<string, unknown>, SQLQueryBindings[]>(query)
        if (!("safeIntegers" in statement) || typeof statement.safeIntegers !== "function")
          throw new Error("Bun SQLite safeIntegers is required")
        statement.safeIntegers(Context.get(fiber.context, SqlClient.SafeIntegers))
        try {
          return Effect.succeed(statement.all(...(params as SQLQueryBindings[])) ?? [])
        } catch (cause) {
          if (!(cause instanceof globalThis.Error)) throw cause
          return Effect.fail(
            new SqlError({
              reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" }),
            }),
          )
        }
      })

    const runValues = (query: string, params: ReadonlyArray<unknown> = []) =>
      Effect.withFiber<Array<unknown[]>, SqlError>((fiber) => {
        using statement = native.query<unknown, SQLQueryBindings[]>(query)
        if (!("safeIntegers" in statement) || typeof statement.safeIntegers !== "function")
          throw new Error("Bun SQLite safeIntegers is required")
        statement.safeIntegers(Context.get(fiber.context, SqlClient.SafeIntegers))
        try {
          return Effect.succeed(statement.values(...(params as SQLQueryBindings[])) ?? [])
        } catch (cause) {
          if (!(cause instanceof globalThis.Error)) throw cause
          return Effect.fail(
            new SqlError({
              reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" }),
            }),
          )
        }
      })

    const connection = Sqlite.makeConnection(run, runValues, {
      export: Effect.try({
        try: () => native.serialize(),
        catch: (cause) =>
          new SqlError({
            reason: classifySqliteError(cause, { message: "Failed to export database", operation: "export" }),
          }),
      }),
      loadExtension: (path: string) =>
        Effect.try({
          try: () => native.loadExtension(path),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, { message: "Failed to load extension", operation: "loadExtension" }),
            }),
        }),
    })

    return yield* Sqlite.makeClient(options, connection, TypeId, (acquirer) => ({
      export: Effect.flatMap(acquirer, (_) => _.export),
      loadExtension: (path: string) => Effect.flatMap(acquirer, (_) => _.loadExtension(path)),
    }))
  })

const nativeLayer = (config: Config) =>
  Layer.effect(
    Sqlite.Native,
    Effect.gen(function* () {
      const native = new Database(config.filename, {
        readonly: config.readonly,
        readwrite: config.readwrite ?? true,
        create: config.create ?? true,
      })
      yield* Effect.addFinalizer(() => Effect.sync(() => native.close()))
      native.run("PRAGMA busy_timeout = 5000;")
      return native
    }),
  )

const clientLayer = (config: Config) => Layer.effect(SqlClient.SqlClient, make(config))

export const sqliteLayer = (config: Config) => {
  const native = nativeLayer(config)
  return Layer.merge(native, clientLayer(config).pipe(Layer.provide(native))).pipe(Layer.provide(Reactivity.layer))
}

export const lease = (config: LeaseConfig) =>
  Effect.acquireRelease(
    Effect.try({
      try: () => {
        const native = new Database(config.filename, { create: true })
        try {
          native.run("PRAGMA busy_timeout = 0")
          const journal = native.query<{ readonly journal_mode: string }, []>("PRAGMA journal_mode = DELETE").get()
          if (journal?.journal_mode !== "delete") throw new globalThis.Error("Lease database is not in DELETE mode")
          native.run(config.mode === "shared" ? "BEGIN DEFERRED" : "BEGIN EXCLUSIVE")
          if (config.mode === "shared") native.query("SELECT count(*) FROM sqlite_schema").get()
          return native
        } catch (cause) {
          native.close()
          throw cause
        }
      },
      catch: (cause) => cause,
    }),
    (native) =>
      Effect.sync(() => {
        try {
          native.run("ROLLBACK")
        } finally {
          native.close()
        }
      }),
  ).pipe(Effect.asVoid)
