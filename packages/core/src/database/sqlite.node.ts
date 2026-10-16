import { DatabaseSync, type SQLInputValue } from "node:sqlite"
import { Context, Effect, Layer } from "effect"
import { Reactivity } from "effect/reactivity"
import { SqlClient } from "effect/sql"
import { classifySqliteError, SqlError } from "effect/sql/SqlError"
import { Sqlite } from "./sqlite.js"

const TypeId = "~@opencode/core/database/SqliteNode" as const

export const supportsTuningPragmas = true

// Foreign keys default OFF and can be toggled per connection.
export const supportsForeignKeyToggle = true

interface Config extends Sqlite.ClientConfig {
  readonly filename: string
  readonly readonly?: boolean
  readonly create?: boolean
  readonly readwrite?: boolean
  readonly timeout?: number
  readonly allowExtension?: boolean
}

type LeaseConfig = {
  readonly filename: string
  readonly mode: "shared" | "exclusive"
}

const make = (options: Config) =>
  Effect.gen(function* () {
    const native = (yield* Sqlite.Native) as DatabaseSync

    const run = (query: string, params: ReadonlyArray<unknown> = []) =>
      Effect.withFiber<Array<Record<string, unknown>>, SqlError>((fiber) => {
        const statement = native.prepare(query)
        statement.setReadBigInts(Context.get(fiber.context, SqlClient.SafeIntegers))
        try {
          return Effect.succeed(statement.all(...(params as SQLInputValue[])))
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
      Effect.withFiber<ReadonlyArray<ReadonlyArray<unknown>>, SqlError>((fiber) => {
        const statement = native.prepare(query)
        statement.setReadBigInts(Context.get(fiber.context, SqlClient.SafeIntegers))
        statement.setReturnArrays(true)
        try {
          const rows: unknown = statement.all(...(params as SQLInputValue[]))
          if (!Array.isArray(rows) || !rows.every(Array.isArray))
            throw new globalThis.Error("Invalid SQLite row result")
          return Effect.succeed(rows)
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
      loadExtension: (path: string) => Effect.flatMap(acquirer, (_) => _.loadExtension(path)),
    }))
  })

const nativeLayer = (config: Config) =>
  Layer.effect(
    Sqlite.Native,
    Effect.gen(function* () {
      const native = new DatabaseSync(config.filename, {
        readOnly: config.readonly,
        timeout: config.timeout ?? 5000,
        allowExtension: config.allowExtension,
        enableForeignKeyConstraints: true,
        open: true,
      })
      yield* Effect.addFinalizer(() => Effect.sync(() => native.close()))
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
        const native = new DatabaseSync(config.filename, { open: true, timeout: 0 })
        try {
          native.exec("PRAGMA busy_timeout = 0")
          const journal = native.prepare("PRAGMA journal_mode = DELETE").get()
          if (journal?.journal_mode !== "delete") throw new globalThis.Error("Lease database is not in DELETE mode")
          native.exec(config.mode === "shared" ? "BEGIN DEFERRED" : "BEGIN EXCLUSIVE")
          if (config.mode === "shared") native.prepare("SELECT count(*) FROM sqlite_schema").get()
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
          native.exec("ROLLBACK")
        } finally {
          native.close()
        }
      }),
  ).pipe(Effect.asVoid)
