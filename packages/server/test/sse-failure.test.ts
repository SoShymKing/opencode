import { expect } from "bun:test"
import { OpenCode } from "@opencode/client/effect"
import { ClientApi } from "@opencode/protocol/client"
import { Authorization } from "@opencode/protocol/middleware/authorization"
import { Event } from "@opencode/schema/event"
import { Session } from "@opencode/schema/session"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { EventGroup } from "@opencode/protocol/groups/event"
import { Cause, Effect, Exit, Layer, Stream } from "effect"
import { HttpClient, HttpClientResponse, HttpEffect, HttpRouter, HttpServer, HttpServerRequest } from "effect/http"
import { HttpApi, HttpApiBuilder, HttpApiGroup } from "effect/http-api"
import { it } from "../../core/test/lib/effect"
import { schemaErrorLayer } from "../src/middleware/schema-error"
import { EventFeed } from "../src/event-feed"
import { EventHandler } from "../src/handlers/event"

const eventApp = (feed: EventFeed.Interface) => HttpRouter.toHttpEffect(
  HttpApiBuilder.layer(HttpApi.make("server").add(EventGroup)).pipe(
    Layer.provide(EventHandler.pipe(
      Layer.provide(Layer.succeed(Authorization, Authorization.of((effect) => effect))),
      Layer.provide(schemaErrorLayer),
    )),
    Layer.provide(Layer.succeed(EventFeed.Service, feed)),
    Layer.provide(HttpServer.layerServices),
  ),
)

async function readData(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<string> {
  const chunk = await reader.read()
  expect(chunk.done).toBe(false)
  const text = new TextDecoder().decode(chunk.value)
  if (text === ": heartbeat\n\n") return readData(reader)
  return text
}

it.live("subscribes before the response boundary and releases on body cancellation", () =>
  Effect.gen(function* () {
    let publish: Bus.Subscriber = () => Effect.die("feed not started")
    let encodes = 0
    let closed = false
    const feed = yield* EventFeed.make((subscriber) => Effect.sync(() => {
      publish = subscriber
      return Effect.void
    }), { encode: (event) => { encodes++; return EventFeed.frame(event) } })
    const payload = { id: Event.ID.create(), created: 1, type: Agent.Event.Updated.type, data: {} }
    const app = yield* eventApp(feed)
    const context = yield* Effect.context<never>()
    const serve = app.pipe(
      Effect.tap(() => Effect.addFinalizer(() => Effect.sync(() => { closed = true }))),
      HttpEffect.withPreResponseHandler((_request, response) => Effect.gen(function* () {
        yield* publish(payload)
        return response
      })),
      HttpEffect.toWebHandlerWith(context),
    )
    const response = yield* Effect.promise(() => serve(new Request("http://localhost/api/event")))
    expect(encodes).toBe(1)
    expect(response.status).toBe(200)
    expect(response.headers.get("content-type")).toContain("text/event-stream")
    expect(response.headers.get("cache-control")).toBe("no-cache, no-transform")
    expect(response.headers.get("x-accel-buffering")).toBe("no")
    expect(response.headers.get("x-content-type-options")).toBe("nosniff")
    expect(closed).toBe(false)
    if (!response.body) throw new Error("expected response body")
    const reader = response.body.getReader()
    expect(yield* Effect.promise(() => readData(reader))).toContain('"type":"server.connected"')
    expect(yield* Effect.promise(() => readData(reader))).toBe(EventFeed.frame(payload))
    yield* publish(payload)
    expect(yield* Effect.promise(() => readData(reader))).toBe(EventFeed.frame(payload))
    yield* Effect.promise(() => reader.cancel())
    expect(closed).toBe(true)
    yield* publish(payload)
    expect(encodes).toBe(2)
  }),
)

it.live("fails overflow before connected and releases the HTTP request scope", () =>
  Effect.gen(function* () {
    let publish: Bus.Subscriber = () => Effect.die("feed not started")
    let encodes = 0
    let closed = false
    const feed = yield* EventFeed.make((subscriber) => Effect.sync(() => {
      publish = subscriber
      return Effect.void
    }), { capacity: 1, encode: (event) => { encodes++; return EventFeed.frame(event) } })
    const app = yield* eventApp(feed)
    const payload = { id: Event.ID.create(), created: 1, type: Agent.Event.Updated.type, data: {} }
    const received: string[] = []
    yield* HttpEffect.toHandled(app, (_request, response) => Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.sync(() => { closed = true }))
      yield* publish(payload)
      yield* publish(payload)
      expect(encodes).toBe(2)
      if (response.body._tag !== "Stream") throw new Error("expected stream")
      const exit = yield* response.body.stream.pipe(
        Stream.decodeText,
        Stream.runForEach((frame) => Effect.sync(() => { received.push(frame) })),
        Effect.exit,
      )
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(EventFeed.SubscriberOverflowError)
      expect(received.filter((frame) => frame.startsWith("data:"))).toEqual([])
    })).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, HttpServerRequest.fromWeb(new Request("http://localhost/api/event"))))
    expect(closed).toBe(true)
    yield* publish(payload)
    expect(encodes).toBe(2)
  }),
)

it.live("keeps the subscriber through body completion and removes the feed observer at scope end", () =>
  Effect.gen(function* () {
    let observers = 0
    yield* Effect.gen(function* () {
      let publish: Bus.Subscriber = () => Effect.die("feed not started")
      let encodes = 0
      const feed = yield* EventFeed.make((subscriber) => Effect.sync(() => {
        observers++
        publish = subscriber
        return Effect.sync(() => { observers-- })
      }), { encode: (event) => { encodes++; return EventFeed.frame(event) } })
      const app = yield* eventApp(feed)
      const payload = { id: Event.ID.create(), created: 1, type: Agent.Event.Updated.type, data: {} }
      yield* HttpEffect.toHandled(app, (_request, response) => Effect.gen(function* () {
        expect(observers).toBe(1)
        yield* publish(payload)
        expect(encodes).toBe(1)
        if (response.body._tag !== "Stream") throw new Error("expected stream")
        const frames = yield* response.body.stream.pipe(
          Stream.decodeText,
          Stream.filter((frame) => frame.startsWith("data:")),
          Stream.take(2),
          Stream.runCollect,
        )
        expect(frames[0]).toContain('"type":"server.connected"')
        expect(frames[1]).toBe(EventFeed.frame(payload))
      })).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, HttpServerRequest.fromWeb(new Request("http://localhost/api/event"))))
      yield* publish(payload)
      expect(encodes).toBe(1)
      expect(observers).toBe(1)
    }).pipe(Effect.scoped)
    expect(observers).toBe(0)
  }),
)

it.live("propagates encoding failure through the web body and stops heartbeat delivery", () =>
  Effect.gen(function* () {
    let publish: Bus.Subscriber = () => Effect.die("feed not started")
    let encodes = 0
    let closed = false
    const feed = yield* EventFeed.make((subscriber) => Effect.sync(() => {
      publish = subscriber
      return Effect.void
    }), { encode: () => { encodes++; throw new Error("invalid event") } })
    const app = yield* eventApp(feed)
    const context = yield* Effect.context<never>()
    const serve = app.pipe(
      Effect.tap(() => Effect.addFinalizer(() => Effect.sync(() => { closed = true }))),
      HttpEffect.toWebHandlerWith(context),
    )
    const response = yield* Effect.promise(() => serve(new Request("http://localhost/api/event")))
    if (!response.body) throw new Error("expected response body")
    const reader = response.body.getReader()
    expect(yield* Effect.promise(() => readData(reader))).toContain('"type":"server.connected"')
    const payload = { id: Event.ID.create(), created: 1, type: Agent.Event.Updated.type, data: {} }
    yield* publish(payload)
    const exit = yield* Effect.tryPromise(() => readData(reader)).pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toMatchObject({ cause: { _tag: "EventFeed.EncodingError" } })
    expect(closed).toBe(true)
    yield* publish(payload)
    expect(encodes).toBe(1)
  }),
)

// Serve the real protocol declaration for the session log stream, so the server encoder and the
// generated Effect client agree on the reserved SSE failure event for this Effect version.
const LogApi = HttpApi.make("log").add(
  HttpApiGroup.make("server.session").add(ClientApi.groups["server.session"].endpoints["session.log"]),
)
const synced = { type: "log.synced" as const, aggregateID: "ses_test", seq: Event.Seq.make(1) }

const handler = HttpRouter.toWebHandler(
  HttpApiBuilder.layer(LogApi).pipe(
    Layer.provide(
      HttpApiBuilder.group(LogApi, "server.session", (handlers) =>
        handlers.handle("session.log", () =>
          Effect.succeed(Stream.make(synced).pipe(Stream.concat(Stream.die(new Error("log reader failed"))))),
        ),
      ),
    ),
    Layer.provide(schemaErrorLayer),
    Layer.provide(
      Layer.succeed(
        Authorization,
        Authorization.of((effect) => effect),
      ),
    ),
    Layer.provide(HttpServer.layerServices),
  ),
).handler

const fetchLog = (request: Request) => Effect.promise(() => handler(request))

it.live("encodes a mid-stream session log failure as the reserved SSE failure event", () =>
  Effect.gen(function* () {
    const response = yield* fetchLog(new Request("http://opencode.local/api/experimental/session/ses_test/log"))
    const body = yield* Effect.promise(() => response.text())
    expect(response.headers.get("content-type")).toContain("text/event-stream")
    expect(body).toContain("event: effect/http-api/stream/failure")
  }),
)

it.live("the generated Effect client fails the session log stream with the server's defect", () =>
  Effect.gen(function* () {
    const httpClient = HttpClient.make((request) =>
      fetchLog(new Request(request.url, { method: request.method })).pipe(
        Effect.map((response) => HttpClientResponse.fromWeb(request, response)),
      ),
    )
    const received: unknown[] = []
    const exit = yield* Effect.gen(function* () {
      const client = yield* OpenCode.make({ baseUrl: "http://opencode.local" })
      return yield* client.session
        .log({ sessionID: Session.ID.make("ses_test") })
        .pipe(Stream.runForEach((item) => Effect.sync(() => received.push(item))))
    }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.exit)

    expect(received).toEqual([synced])
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true)
    if (Exit.isSuccess(exit)) return
    expect(Cause.squash(exit.cause)).toMatchObject({ message: "log reader failed" })
  }),
)
