import { afterEach, describe, expect } from "bun:test"
import { Effect, Layer, Queue, Schema, Stream } from "effect"
import { EventPaths } from "../../src/server/routes/instance/httpapi/groups/event"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"
import { EventBuffer } from "../../src/server/routes/instance/httpapi/handlers/event-buffer"
import { EventV2 } from "@opencode-ai/core/event"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { WorkspaceID } from "@opencode-ai/schema/workspace-id"
import { GlobalBus } from "../../src/bus/global"
import { eventResponse } from "../../src/server/routes/instance/httpapi/handlers/event"
import { globalEventResponse } from "../../src/server/routes/instance/httpapi/handlers/global"

const EventData = Schema.Struct({
  id: Schema.optional(Schema.String),
  type: Schema.String,
  properties: Schema.Record(Schema.String, Schema.Any),
})

const readEvent = (reader: Queue.Dequeue<Uint8Array>) =>
  Effect.gen(function* () {
    const value = yield* Queue.take(reader).pipe(
      Effect.timeoutOrElse({
        duration: "5 seconds",
        orElse: () => Effect.fail(new Error("timed out waiting for event")),
      }),
    )
    return Schema.decodeUnknownSync(EventData)(JSON.parse(new TextDecoder().decode(value).replace(/^data: /, "")))
  })

const openEventStream = (directory: string) =>
  Effect.gen(function* () {
    const response = yield* requestInDirectory(EventPaths.event, directory)
    const reader = yield* Queue.unbounded<Uint8Array>()
    yield* response.stream.pipe(
      Stream.runForEach((value) => Queue.offer(reader, value)),
      Effect.forkScoped,
    )
    return { response, reader }
  })

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

const it = testEffect(httpApiLayer)

const bufferTest = testEffect(Layer.empty)

describe("legacy event buffer", () => {
  bufferTest.effect("preserves all 256 events at capacity", () =>
    Effect.gen(function* () {
      const buffer = yield* EventBuffer.make<number>()
      const values = Array.from({ length: 256 }, (_, index) => index)
      values.forEach(buffer.offer)
      expect(yield* buffer.stream.pipe(Stream.take(256), Stream.runCollect)).toEqual(values)
    }),
  )

  bufferTest.effect("fails immediately without draining after 257 queued events", () =>
    Effect.gen(function* () {
      const buffer = yield* EventBuffer.make<number>()
      Array.from({ length: 257 }, (_, index) => buffer.offer(index))
      const received: number[] = []
      const error = yield* buffer.stream.pipe(
        Stream.take(1),
        Stream.runForEach((value) => Effect.sync(() => received.push(value))),
        Effect.flip,
        Effect.timeout("1 second"),
      )
      expect(error).toBeInstanceOf(EventV2.SubscriberOverflowError)
      expect(error).toMatchObject({ capacity: 256 })
      expect(received).toEqual([])
    }),
  )

  bufferTest.effect("rejects connected emission after overflow", () =>
    Effect.gen(function* () {
      const buffer = yield* EventBuffer.make<number>()
      Array.from({ length: 257 }, (_, index) => buffer.offer(index))
      expect(yield* buffer.check.pipe(Effect.flip)).toBeInstanceOf(EventV2.SubscriberOverflowError)
    }),
  )

  bufferTest.live("global overflow fails before connected and removes its listener on scope end", () =>
    Effect.gen(function* () {
      const listeners = GlobalBus.listenerCount("event")
      yield* Effect.gen(function* () {
        const response = yield* globalEventResponse()
        expect(GlobalBus.listenerCount("event")).toBe(listeners + 1)
        Array.from({ length: 257 }, (_, index) =>
          GlobalBus.emit("event", {
            directory: "global",
            payload: index % 2 === 0
              ? { type: "session.updated", properties: {} }
              : { type: "sync", syncEvent: { id: EventV2.ID.create() } },
          }),
        )
        if (response.body._tag !== "Stream") throw new Error("expected stream body")
        const received: Uint8Array[] = []
        const error = yield* response.body.stream.pipe(
          Stream.take(1),
          Stream.runForEach((value) => Effect.sync(() => received.push(value))),
          Effect.flip,
        )
        expect(error).toBeInstanceOf(EventV2.SubscriberOverflowError)
        expect(received).toEqual([])
      }).pipe(Effect.scoped)
      expect(GlobalBus.listenerCount("event")).toBe(listeners)
    }),
  )

  bufferTest.instance(
    "instance disposal shares the 256 event cap and fails before connected",
    () => Effect.gen(function* () {
      const { directory } = yield* TestInstance
      const response = yield* eventResponse({
        listen: (listener) => Effect.gen(function* () {
          yield* Effect.forEach(Array.from({ length: 256 }), () =>
            listener({
              id: EventV2.ID.create(),
              type: "test.target",
              data: {},
              location: { directory: AbsolutePath.make(directory) },
            }),
          )
          return Effect.void
        }),
      })
      GlobalBus.emit("event", { directory, payload: { type: "server.instance.disposed", properties: {} } })
      if (response.body._tag !== "Stream") throw new Error("expected stream body")
      expect(yield* response.body.stream.pipe(Stream.take(1), Stream.runCollect, Effect.flip)).toBeInstanceOf(
        EventV2.SubscriberOverflowError,
      )
    }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  bufferTest.instance(
    "ignores unrelated directory and workspace bursts before queueing and removes instance listeners",
    () => Effect.gen(function* () {
      const { directory } = yield* TestInstance
      const listeners = new Set<EventV2.Subscriber>()
      const globalListeners = GlobalBus.listenerCount("event")
      yield* Effect.gen(function* () {
        const response = yield* eventResponse({
          listen: (listener) => Effect.sync(() => {
            listeners.add(listener)
            return Effect.sync(() => {
              listeners.delete(listener)
            })
          }),
        })
        expect(listeners.size).toBe(1)
        yield* Effect.forEach(Array.from({ length: 514 }, (_, index) => index), (index) =>
          Effect.forEach(listeners, (listener) =>
            listener({
              id: EventV2.ID.create(),
              type: "test.unrelated",
              data: {},
              location: index % 2 === 0
                ? { directory: AbsolutePath.make(`${directory}/other`) }
                : { directory: AbsolutePath.make(directory), workspaceID: WorkspaceID.create() },
            }),
          ),
        )
        yield* Effect.forEach(listeners, (listener) =>
          listener({
            id: EventV2.ID.create(),
            type: "test.target",
            data: { delivered: true },
            location: { directory: AbsolutePath.make(directory) },
          }),
        )
        GlobalBus.emit("event", { directory, payload: { type: "server.instance.disposed", properties: {} } })
        if (response.body._tag !== "Stream") throw new Error("expected stream body")
        const chunks = yield* response.body.stream.pipe(Stream.runCollect)
        const text = chunks.map((chunk) => new TextDecoder().decode(chunk)).join("")
        expect(text).toContain('"type":"server.connected"')
        expect(text).toContain('"type":"test.target"')
        expect(text).toContain('"type":"server.instance.disposed"')
        expect(text).not.toContain("test.unrelated")
      }).pipe(Effect.scoped)
      expect(listeners.size).toBe(0)
      expect(GlobalBus.listenerCount("event")).toBe(globalListeners)
    }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})

describe("event HttpApi", () => {
  it.instance(
    "serves event stream",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { response, reader } = yield* openEventStream(directory)

        expect(response.status).toBe(200)
        expect(response.headers["content-type"]).toContain("text/event-stream")
        expect(response.headers["cache-control"]).toBe("no-cache, no-transform")
        expect(response.headers["x-accel-buffering"]).toBe("no")
        expect(response.headers["x-content-type-options"]).toBe("nosniff")
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "keeps the event stream open after the initial event",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { reader } = yield* openEventStream(directory)
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })

        // If no second event arrives within 250ms, the stream is still open.
        const status = yield* Queue.take(reader).pipe(
          Effect.as("event" as const),
          Effect.timeoutOrElse({ duration: "250 millis", orElse: () => Effect.succeed("open" as const) }),
        )
        expect(status).toBe("open")
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "delivers instance events after the initial event",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { reader } = yield* openEventStream(directory)
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })

        const created = yield* requestInDirectory("/session", directory, { method: "POST" })
        expect(created.status).toBe(200)
        expect(yield* readEvent(reader)).toMatchObject({ type: "session.created" })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})
