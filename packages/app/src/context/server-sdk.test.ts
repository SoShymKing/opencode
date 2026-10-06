import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { EventManifest } from "@opencode-ai/schema/event-manifest"
import { adaptServerEvent, coalesceServerEvents, enqueueServerEvent, resumeStreamAfterPageShow } from "./server-sdk"
import type { NativeServerEvent } from "./server-sdk"
import type { Event } from "@opencode-ai/sdk/v2/client"

const native = Schema.decodeUnknownSync(Schema.toEncoded(Schema.Union(EventManifest.Definitions)))

describe("resumeStreamAfterPageShow", () => {
  test("restarts a stream only after a back-forward cache restore", () => {
    let starts = 0
    const start = () => starts++

    resumeStreamAfterPageShow({ persisted: false } as PageTransitionEvent, start)
    resumeStreamAfterPageShow({ persisted: true } as PageTransitionEvent, start)

    expect(starts).toBe(1)
  })
})

describe("adaptServerEvent", () => {
  test("preserves V2 events while adapting permission requests for existing consumers", () => {
    const current = {
      id: "evt_1",
      type: "permission.v2.asked",
      data: { id: "perm_1", sessionID: "ses_1", action: "read", resources: ["src/**"] },
    } satisfies NativeServerEvent

    expect(adaptServerEvent(native(current))).toMatchObject({
      type: "permission.asked",
      properties: { id: "perm_1", sessionID: "ses_1", permission: "read", patterns: ["src/**"] },
      current,
    })
  })

  test("preserves native question and permission reply behavior", () => {
    const permission = native({ id: "evt_reply", type: "permission.v2.replied", data: { requestID: "perm_1", sessionID: "ses_1", reply: "once" } })
    expect(adaptServerEvent(permission)).toMatchObject({ type: "permission.replied", properties: permission.data, current: permission })
    const question = native({ id: "evt_question", type: "question.v2.asked", data: { id: "question_1", sessionID: "ses_1", questions: [{ header: "Choice", question: "Choose", options: [{ label: "A", description: "Option" }] }] } })
    expect(adaptServerEvent(question)).toMatchObject({ type: "question.asked", properties: question.data, current: question })
  })

  test("preserves rich native status and count events across coalescing barriers", () => {
    const status = native({ id: "evt_status", type: "session.status", data: { sessionID: "ses_1", status: { type: "busy", activity: { userMessageID: "msg_user", model: "receiving", streamEventCount: 0, lastStreamEventAt: 5 } } } })
    const counted = native({ id: "evt_count", type: "session.next.step.stream.updated", data: { timestamp: 6, sessionID: "ses_1", assistantMessageID: "msg_assistant", streamEventCount: 11 } })
    const delta = native({ id: "evt_delta", type: "session.next.text.delta", data: { timestamp: 4, sessionID: "ses_1", assistantMessageID: "msg_assistant", textID: "text", delta: "x" } })
    expect(adaptServerEvent(status)).toMatchObject({ properties: status.data, current: status })
    expect(adaptServerEvent(counted)).toMatchObject({ properties: counted.data, current: counted })
    const events = [delta, status, delta, counted, delta].map((event) => ({ directory: "/repo", payload: adaptServerEvent(event) }))
    expect(coalesceServerEvents(events)).toEqual(events)
    const terminal = native({ id: "evt_terminal", type: "session.status", data: { sessionID: "ses_1", status: { type: "idle", terminal: { userMessageID: "msg_user", reason: "cancelled", message: "stopped" } } } })
    expect(adaptServerEvent(terminal)).toMatchObject({ properties: terminal.data, current: terminal })
  })
})

describe("coalesceServerEvents", () => {
  const delta = (value: string, field = "text", partID = "part") => ({
    directory: "/repo",
    payload: {
      type: "message.part.delta",
      properties: { messageID: "msg", partID, field, delta: value },
    } as Event,
  })

  test("merges adjacent deltas for the same field", () => {
    const first = delta("hello ")
    const second = delta("world")
    first.payload.id = "first"
    second.payload.id = "second"
    const result = coalesceServerEvents([first, second])

    expect(result).toHaveLength(1)
    expect(result[0]?.payload).toMatchObject({ id: "second", properties: { delta: "hello world" } })
  })

  test("merges adjacent current text deltas", () => {
    const current = (id: string, value: string) =>
      adaptServerEvent(native({
        id,
        type: "session.next.text.delta",
        location: { directory: "/repo" },
        data: { timestamp: 1, sessionID: "ses_1", assistantMessageID: "msg_1", textID: "text", delta: value },
      } satisfies NativeServerEvent))
    const result = coalesceServerEvents([
      { directory: "/repo", payload: current("evt_1", "hello ") },
      { directory: "/repo", payload: current("evt_2", "world") },
    ])

    expect(result).toHaveLength(1)
    expect(result[0]?.payload.current).toMatchObject({ id: "evt_2", data: { delta: "hello world" } })
  })

  test("preserves event boundaries and distinct fields", () => {
    const status = {
      directory: "/repo",
      payload: { type: "session.status", properties: { sessionID: "ses", status: { type: "idle" } } } as Event,
    }
    const result = coalesceServerEvents([delta("a"), delta("b", "metadata"), status, delta("c")])

    expect(result.map((event) => event.payload.type)).toEqual([
      "message.part.delta",
      "message.part.delta",
      "session.status",
      "message.part.delta",
    ])
  })

  test("isolates native content, message, session and full-value boundaries", () => {
    const delta = (messageID: string, textID: string, sessionID = "ses_1") => ({
      directory: "/repo", payload: adaptServerEvent(native({ id: "evt_delta", type: "session.next.text.delta", data: { timestamp: 1, sessionID, assistantMessageID: messageID, textID, delta: "x" } })),
    })
    const full = { directory: "/repo", payload: adaptServerEvent(native({ id: "evt_full", type: "session.next.text.ended", data: { timestamp: 2, sessionID: "ses_1", assistantMessageID: "msg_a", textID: "one", text: "full" } })) }
    const result = coalesceServerEvents([delta("msg_a", "one"), delta("msg_a", "two"), delta("msg_b", "two"), delta("msg_b", "two", "ses_other"), full, delta("msg_a", "one")])
    expect(result).toHaveLength(6)
    expect(result[4]?.payload.current).toMatchObject({ type: "session.next.text.ended", data: { text: "full" } })
  })

  test("does not confuse delimiters in native message and content identities", () => {
    const delta = (assistantMessageID: string, textID: string) => ({
      directory: "/repo",
      payload: adaptServerEvent(native({ id: "evt_delta", type: "session.next.text.delta", data: { timestamp: 1, sessionID: "ses_1", assistantMessageID, textID, delta: "x" } })),
    })
    expect(coalesceServerEvents([delta("msg_a", "b:c"), delta("msg_a:b", "c")])).toHaveLength(2)
  })

  test("merges only adjacent native delta kinds and their explicit identities", () => {
    const queue = (event: NativeServerEvent) => ({ directory: "/repo", payload: adaptServerEvent(native(event)) })
    const assistant = { timestamp: 1, sessionID: "ses_1", assistantMessageID: "msg_a" }
    const reasoning = { id: "evt_reason", type: "session.next.reasoning.delta", data: { ...assistant, reasoningID: "r", delta: "a" } } satisfies NativeServerEvent
    const input = { id: "evt_tool", type: "session.next.tool.input.delta", data: { ...assistant, callID: "call", delta: "b" } } satisfies NativeServerEvent
    const compact = { id: "evt_compact", type: "session.next.compaction.delta", data: { timestamp: 1, sessionID: "ses_1", messageID: "msg_compact", text: "c" } } satisfies NativeServerEvent
    const full = { id: "evt_input", type: "session.next.tool.input.ended", data: { ...assistant, callID: "call", text: "full" } } satisfies NativeServerEvent
    const result = coalesceServerEvents([
      queue(reasoning), queue(reasoning),
      queue(input), queue(input), queue(full), queue(input),
      queue(compact), queue(compact), queue({ ...compact, data: { ...compact.data, messageID: "msg_other" } }),
    ])
    expect(result.map((event) => event.payload.current?.type)).toEqual([
      "session.next.reasoning.delta", "session.next.tool.input.delta", "session.next.tool.input.ended",
      "session.next.tool.input.delta", "session.next.compaction.delta", "session.next.compaction.delta",
    ])
    expect(result[0]?.payload.current).toMatchObject({ data: { delta: "aa" } })
    expect(result[1]?.payload.current).toMatchObject({ data: { delta: "bb" } })
    expect(result[2]?.payload.current).toMatchObject({ data: { text: "full" } })
    expect(result[4]?.payload.current).toMatchObject({ data: { text: "cc" } })
  })

  test("preserves event ID order across interleaved deltas", () => {
    const first = delta("a")
    const other = delta("b", "text", "other")
    const last = delta("c")
    first.payload.id = "1"
    other.payload.id = "2"
    last.payload.id = "3"

    const result = coalesceServerEvents([first, other, last])

    expect(result.map((event) => event.payload.id)).toEqual(["1", "2", "3"])
  })
})

describe("enqueueServerEvent", () => {
  const partUpdated = (text: string) =>
    ({
      type: "message.part.updated",
      properties: {
        sessionID: "session",
        part: { id: "part", sessionID: "session", messageID: "message", type: "text", text },
      },
    }) as Event

  test("preserves part updates across message remove and re-add barriers", () => {
    const events: Array<{ directory: string; payload: Event }> = []
    const enqueue = (payload: Event) => enqueueServerEvent(events, { directory: "/repo", payload })

    enqueue(partUpdated("old"))
    enqueue({ type: "message.removed", properties: { sessionID: "session", messageID: "message" } } as Event)
    enqueue({
      type: "message.updated",
      properties: {
        sessionID: "session",
        info: {
          id: "message",
          sessionID: "session",
          role: "user",
          time: { created: 1 },
          agent: "build",
          model: { providerID: "provider", modelID: "model" },
        },
      },
    } as Event)
    enqueue(partUpdated("new"))

    expect(events.map((event) => event.payload.type)).toEqual([
      "message.part.updated",
      "message.removed",
      "message.updated",
      "message.part.updated",
    ])
  })

  test("preserves deltas after a replacement snapshot", () => {
    const events: Array<{ directory: string; payload: Event }> = []
    const enqueue = (payload: Event) => enqueueServerEvent(events, { directory: "/repo", payload })

    enqueue(partUpdated("a"))
    enqueue(partUpdated("ab"))
    enqueue({
      type: "message.part.delta",
      properties: { sessionID: "session", messageID: "message", partID: "part", field: "text", delta: "c" },
    } as Event)

    const result = coalesceServerEvents(events)
    expect(result.map((event) => event.payload.type)).toEqual(["message.part.updated", "message.part.delta"])
    expect(result[0]?.payload).toMatchObject({ properties: { part: { text: "ab" } } })
    expect(result[1]?.payload).toMatchObject({ properties: { delta: "c" } })
  })

  test("preserves updates after session deletion", () => {
    const events: Array<{ directory: string; payload: Event }> = []
    const enqueue = (payload: Event) => enqueueServerEvent(events, { directory: "/repo", payload })

    enqueue(partUpdated("old"))
    enqueue({
      type: "session.deleted",
      properties: { sessionID: "session", info: { id: "session" } },
    } as Event)
    enqueue(partUpdated("new"))

    expect(events.map((event) => event.payload.type)).toEqual([
      "message.part.updated",
      "session.deleted",
      "message.part.updated",
    ])
  })

  test("does not coalesce edge-triggered session statuses", () => {
    const events: Array<{ directory: string; payload: Event }> = []
    const enqueue = (status: "retry" | "busy") =>
      enqueueServerEvent(events, {
        directory: "/repo",
        payload: {
          type: "session.status",
          properties: {
            sessionID: "session",
            status: status === "retry" ? { type: "retry", attempt: 1, message: "retry", next: 1 } : { type: "busy" },
          },
        } as Event,
      })

    enqueue("retry")
    enqueue("busy")

    expect(events).toHaveLength(2)
  })
})
