import { expect, test } from "bun:test"
import {
  buildInitialStreamEvent,
  buildStreamDeltaEvents,
  createTimelineBenchmarkBackend,
} from "../timeline/session-timeline-benchmark.fixture"

test("reconnect REST history contains only delivered timeline events", () => {
  const backend = createTimelineBenchmarkBackend({ historyTurns: 1, eventBatch: 1 })
  const initial = buildInitialStreamEvent(2)
  const deltas = buildStreamDeltaEvents(2)
  const assistant = () => backend.pageMessages().items.find((item) => item.id === "msg_assistant_regression")
  const text = () => {
    const message = assistant()
    return message?.type === "assistant" ? message.content.find((part) => part.type === "text")?.text : undefined
  }
  backend.transport.enqueue([...initial, ...deltas])
  expect(text()).toBeUndefined()
  expect(backend.events()).toEqual([initial[0]])
  expect(text()).toBe("")
  expect(backend.events()).toEqual([initial[1]])
  expect(text()).toBe(
    "Streaming\n\n## Implementation plan\n\nStreaming **bold analysis\n\n```ts\nconst initial = true\n```",
  )
  expect(backend.transport.pendingCount()).toBe(2)
  backend.events()
  expect(text()).toEndWith(" continues across three")
  expect(text()).not.toContain("benchmark-complete")
  backend.events()
  expect(text()).toEndWith("<!-- stream-2 -->")
  expect(String(text()).match(/ continues across three/g)).toHaveLength(1)
  const deliveredText = text()
  backend.transport.enqueue({
    id: "evt_benchmark_ended",
    created: 1700000003000,
    type: "session.step.ended",
    durable: { aggregateID: "ses_timeline_state_regression", seq: 20, version: 1 },
    data: {
      sessionID: "ses_timeline_state_regression",
      assistantMessageID: "msg_assistant_regression",
      finish: "stop",
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      streamEventCount: 91,
    },
  })
  backend.events()
  expect(assistant()?.time).toEqual({ created: 1700000001000, completed: 1700000003000 })
  expect(assistant()).toMatchObject({ streamEventCount: 91 })
  expect(text()).toBe(deliveredText)
  backend.transport.enqueue(initial[0]!)
  backend.events()
  expect(text()).toBe(deliveredText)
  const fresh = createTimelineBenchmarkBackend({ historyTurns: 1, eventBatch: 1 }).pageMessages().items.at(-1)
  expect(fresh?.type === "assistant" && fresh.content).toHaveLength(1)
})
