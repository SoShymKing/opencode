import { expect, test } from "bun:test"
import {
  buildInitialStreamEvent,
  buildStreamDeltaEvents,
  createTimelineBenchmarkBackend,
  textPartID,
} from "../timeline/session-timeline-benchmark.fixture"

test("reconnect REST history contains only delivered timeline events", () => {
  const backend = createTimelineBenchmarkBackend({ historyTurns: 1, eventBatch: 1 })
  const initial = buildInitialStreamEvent(2)
  const deltas = buildStreamDeltaEvents(2)
  const assistant = () => backend.pageMessages().items.find((item) => item.info.id === "msg_assistant_regression")
  const text = () => assistant()?.parts.find((part) => part.id === textPartID)?.text
  backend.transport.enqueue([initial, ...deltas])
  expect(text()).toBeUndefined()
  expect(backend.events()).toEqual([initial])
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
    directory: initial.directory,
    payload: {
      type: "message.updated",
      properties: {
        info: {
          id: "msg_assistant_regression",
          role: "assistant",
          time: { created: 1700000001000, completed: 1700000003000 },
        },
      },
    },
  })
  backend.events()
  expect(assistant()?.info.time).toEqual({ created: 1700000001000, completed: 1700000003000 })
  expect(text()).toBe(deliveredText)
  expect(assistant()?.parts.map((part) => part.id)).toEqual(["prt_0001_edit", textPartID])
  backend.transport.enqueue({
    directory: initial.directory,
    payload: {
      type: "message.updated",
      properties: {
        info: { id: "msg_user_regression", role: "user", summary: { diffs: [{ file: "src/regression.ts" }] } },
      },
    },
  })
  backend.events()
  expect(backend.pageMessages().items.find((item) => item.info.id === "msg_user_regression")?.info.summary).toEqual({
    diffs: [{ file: "src/regression.ts" }],
  })
  backend.transport.enqueue(initial)
  backend.events()
  expect(text()).not.toContain("benchmark-complete")
  expect(assistant()?.parts.map((part) => part.id)).toEqual(["prt_0001_edit", textPartID])
  expect(
    createTimelineBenchmarkBackend({ historyTurns: 1, eventBatch: 1 }).pageMessages().items.at(-1)?.parts,
  ).toHaveLength(1)
})
