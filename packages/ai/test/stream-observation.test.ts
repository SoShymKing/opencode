import { expect } from "bun:test"
import { Effect, Stream } from "effect"
import { LLM, LLMEvent } from "../src/index.js"
import { OpenAI } from "../src/providers.js"
import { LLMClient, WebSocketTransport } from "../src/route.js"
import { fixedResponse } from "./lib/http.js"
import { sseEvents } from "./lib/sse.js"
import { it } from "./lib/effect.js"

const request = LLM.request({
  model: OpenAI.configure({ apiKey: "test", baseURL: "https://provider.test/v1" }).responses("test"),
  prompt: "Hello",
})
const completed = '{"type":"response.completed","response":{"id":"test"}}'

it.effect("counts eleven provider records without reasoning summaries across reused and concurrent subscriptions", () =>
  Effect.gen(function* () {
    const body = sseEvents(
      { type: "response.created", response: { id: "resp_count" } },
      { type: "response.in_progress", response: { id: "resp_count" } },
      { type: "response.output_item.added", item: { type: "reasoning", id: "rs_count" } },
      { type: "response.output_item.done", item: { type: "reasoning", id: "rs_count" } },
      { type: "response.output_item.added", item: { type: "message", id: "msg_count" } },
      { type: "response.content_part.added", item_id: "msg_count" },
      { type: "response.output_text.delta", item_id: "msg_count", delta: "Hello" },
      { type: "response.output_text.done", item_id: "msg_count", text: "Hello" },
      { type: "response.content_part.done", item_id: "msg_count" },
      { type: "response.output_item.done", item: { type: "message", id: "msg_count" } },
      { type: "response.completed", response: { id: "resp_count" } },
    )
    const expected = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    const counts: Array<number | undefined> = []
    const stream = LLMClient.stream(request, {
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    })
    const events = yield* stream.pipe(Stream.runCollect, Effect.provide(fixedResponse(body)))
    expect(counts).toEqual(expected)
    expect(events.filter(LLMEvent.is.reasoningDelta)).toEqual([])
    expect(events.filter(LLMEvent.is.textDelta).map((event) => event.text)).toEqual(["Hello"])
    expect(events.filter(LLMEvent.is.finish)).toHaveLength(1)
    yield* stream.pipe(Stream.runDrain, Effect.provide(fixedResponse(body)))
    expect(counts).toEqual([...expected, ...expected])
    const independent: Array<number | undefined> = []
    const concurrent = yield* Effect.all([
      stream.pipe(Stream.runCollect),
      LLMClient.stream(request, {
        onStreamEventCount: (count) => Effect.sync(() => { independent.push(count) }),
      }).pipe(Stream.runCollect),
    ], { concurrency: "unbounded" }).pipe(Effect.provide(fixedResponse(body)))
    expect(counts.slice(24)).toEqual(expected)
    expect(independent).toEqual(expected)
    expect(concurrent).toEqual([events, events])
    const unobserved = yield* LLMClient.stream(request).pipe(Stream.runCollect, Effect.provide(fixedResponse(body)))
    expect(unobserved).toEqual(events)
  }),
)

it.effect("reports zero before an HTTP request fails", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    yield* LLMClient.stream(request, {
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(Stream.runDrain, Effect.provide(fixedResponse('{"error":{"code":"invalid_file"}}', { status: 400 })), Effect.flip)
    expect(counts).toEqual([0])
  }),
)

it.effect("counts WebSocket completion once and closes the connection", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    let closed = false
    yield* LLMClient.stream(request, {
      webSocket: WebSocketTransport.makeDirect({
        open: () => Effect.succeed({
          sendText: () => Effect.void,
          messages: Stream.make('{"type":"response.created","response":{"id":"test"}}', completed),
          close: Effect.sync(() => { closed = true }),
        }),
      }),
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(Stream.runDrain, Effect.provide(fixedResponse(sseEvents(completed))))
    expect(counts).toEqual([0, 1, 2])
    expect(closed).toBe(true)
  }),
)

it.effect("reports unsupported observation instead of counting semantic output", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    const route = request.model.route.with({ transport: { ...request.model.route.transport, rawFrames: false } })
    yield* LLMClient.stream(LLM.request({ model: route.model({ id: "test" }), prompt: "Hello" }), {
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(Stream.runDrain, Effect.provide(fixedResponse(sseEvents(completed))))
    expect(counts).toEqual([undefined])
  }),
)

it.effect("makes the final absolute count available before the semantic finish event", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    yield* LLMClient.stream(request, {
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(
      Stream.tap((event) => Effect.sync(() => {
        if (event.type === "finish") expect(counts).toEqual([0, 1, 2])
      })),
      Stream.runDrain,
      Effect.provide(fixedResponse(sseEvents(
        { type: "response.output_text.delta", item_id: "test", delta: "hello" }, completed,
      ))),
    )
    expect(counts).toEqual([0, 1, 2])
  }),
)

it.effect("counts HTTP fallback frames once when a channel uses its fallback", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    yield* LLMClient.stream(request, {
      webSocket: { execute: (exchange) => Effect.succeed({ frames: exchange.fallback(), complete: Effect.void }) },
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(Stream.runDrain, Effect.provide(fixedResponse(sseEvents(completed))))
    expect(counts).toEqual([0, 1])
  }),
)

it.effect("counts raw SSE frames once before decoding and resets on reuse", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    const stream = LLMClient.stream(request, {
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(Stream.runDrain, Effect.provide(fixedResponse(
      ': heartbeat\n\ndata:\n\ndata: null\n\ndata: : keepalive\n\n' + sseEvents(completed, "[DONE]"),
    )))
    yield* stream
    yield* stream
    expect(counts).toEqual([0, 1, 0, 1])
  }),
)

it.effect("counts a malformed SSE event before semantic decoding fails", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    yield* LLMClient.stream(request, {
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(Stream.runDrain, Effect.provide(fixedResponse(sseEvents("{broken"))), Effect.flip)
    expect(counts).toEqual([0, 1])
  }),
)

it.effect("counts WebSocket provider failures before the channel decoder rejects them", () =>
  Effect.gen(function* () {
    const counts: Array<number | undefined> = []
    const webSocket = WebSocketTransport.makeDirect({
      open: () => Effect.succeed({
        sendText: () => Effect.void,
        messages: Stream.make('{"type":"error","code":"invalid_file","message":"bad file"}'),
        close: Effect.void,
      }),
    })
    yield* LLMClient.stream(request, {
      webSocket,
      onStreamEventCount: (count) => Effect.sync(() => { counts.push(count) }),
    }).pipe(Stream.runDrain, Effect.provide(fixedResponse(sseEvents(completed))), Effect.flip)
    expect(counts).toEqual([0, 1])
  }),
)
