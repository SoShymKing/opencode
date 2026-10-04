/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { onMount } from "solid-js"
import type { AssistantMessage, Event, UserMessage } from "@opencode-ai/sdk/v2"
import type { JSX } from "@opentui/solid"
import { TuiConfigProvider } from "../../../src/config"
import { DataProvider, useData } from "../../../src/context/data"
import { ThemeProvider } from "../../../src/context/theme"
import { CurrentResponseThinking, ReasoningHeader } from "../../../src/routes/session"
import { tmpdir } from "../../fixture/fixture"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"
import { directory, mount, wait } from "../cmd/tui/sync-fixture"
import { json, type FetchHandler } from "../../fixture/tui-sdk"

const sessionID = "ses_thinking_stream"
const user = {
  id: "msg_user",
  sessionID,
  role: "user",
  agent: "build",
  model: { providerID: "test", modelID: "model" },
  time: { created: 1 },
} satisfies UserMessage

function assistant(streamEventCount?: number, id = "msg_assistant", created = 2): AssistantMessage {
  return {
    id,
    sessionID,
    role: "assistant",
    parentID: user.id,
    agent: "build",
    mode: "build",
    modelID: "model",
    providerID: "test",
    path: { cwd: directory, root: directory },
    time: { created },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    streamEventCount,
  }
}

async function fixture(
  state: string,
  view: () => JSX.Element = () => <CurrentResponseThinking sessionID={sessionID} />,
  mode: "hide" | "show" = "hide",
  fetch?: FetchHandler,
) {
  await Bun.write(`${state}/kv.json`, JSON.stringify({ animations_enabled: false, thinking_mode: mode }))
  const ready = Promise.withResolvers<ReturnType<typeof useData>>()
  function View() {
    const data = useData()
    onMount(() => ready.resolve(data))
    return view()
  }
  const setup = await mount(fetch, state, () => (
    <TuiConfigProvider config={createTuiResolvedConfig()}>
      <ThemeProvider mode="dark" source={{ discover: async () => ({}) }}>
        <DataProvider>
          <View />
        </DataProvider>
      </ThemeProvider>
    </TuiConfigProvider>
  ))
  const data = await ready.promise
  const emit = (payload: Event) => setup.emit({ directory, project: "proj_test", payload })
  const message = (info: UserMessage | AssistantMessage) =>
    emit({ id: `evt_${info.id}_${info.time.created}`, type: "message.updated", properties: { sessionID, info } })
  emit({ id: "evt_busy", type: "session.status", properties: { sessionID, status: { type: "busy" } } })
  message(user)
  await wait(() => setup.sync.data.message[sessionID]?.length === 1)
  return {
    ...setup,
    data,
    emit,
    message,
    async frame() {
      await setup.app.renderOnce()
      return setup.app.captureCharFrame()
    },
    [Symbol.dispose]() {
      setup.app.renderer.destroy()
    },
  }
}

test.each([0, 1, 11, undefined])("renders current count %s with an empty public summary", async (count) => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path)
  setup.message(assistant(count))
  await wait(() => setup.sync.data.message[sessionID]?.length === 2)
  const label =
    count === undefined ? "Thinking (streams unavailable)" : `Thinking (${count} ${count === 1 ? "stream" : "streams"})`
  expect(await setup.frame()).toContain(label)
  expect(setup.sync.data.part.msg_assistant).toBeUndefined()
})

test("native refresh keeps live assistant updates and pending users", async () => {
  await using tmp = await tmpdir()
  const snapshot = Promise.withResolvers<Response>()
  let requested = false
  using setup = await fixture(tmp.path, undefined, "hide", (url) => {
    if (url.pathname === `/api/session/${sessionID}/message`) {
      requested = true
      return snapshot.promise
    }
    return undefined
  })
  setup.emit({ id: "evt_step", type: "session.next.step.started", properties: {
    sessionID, assistantMessageID: "msg_assistant", timestamp: 2, agent: "build",
    model: { id: "model", providerID: "test" }, streamEventCount: 11,
  } })
  setup.emit({ id: "evt_prompt", type: "session.next.prompted", properties: {
    sessionID, messageID: "msg_native_pending", timestamp: 3, delivery: "queue", prompt: { text: "pending", files: [], agents: [] },
  } })
  await wait(() => setup.data.session.message.list(sessionID)?.length === 2)
  const refresh = setup.data.session.message.refresh(sessionID)
  await wait(() => requested)
  setup.emit({ id: "evt_stream", type: "session.next.step.stream.updated", properties: {
    sessionID, assistantMessageID: "msg_assistant", timestamp: 4, streamEventCount: 12,
  } })
  await wait(() => {
    const message = setup.data.session.message.list(sessionID)?.find((item) => item.id === "msg_assistant")
    return message?.type === "assistant" && message.streamEventCount === 12
  })
  snapshot.resolve(json({ data: [{ id: "msg_assistant", type: "assistant", agent: "build",
    model: { id: "model", providerID: "test" }, content: [{ type: "text", id: "text_restored", text: "restored" }], time: { created: 2 }, streamEventCount: 11 }] }))
  await refresh
  expect(setup.data.session.message.list(sessionID)?.map((message) => message.id)).toContain("msg_native_pending")
  const current = setup.data.session.message.list(sessionID)?.find((item) => item.id === "msg_assistant")
  expect(current?.type === "assistant" && current.streamEventCount).toBe(12)
  expect(current?.type === "assistant" && current.content).toContainEqual({ type: "text", id: "text_restored", text: "restored" })
})

test("live activity counts before an assistant exists and keeps its owner with a pending user", async () => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path)
  const status = (count?: number, owner = user.id) => setup.emit({
    id: "evt_activity", type: "session.status",
    properties: { sessionID, status: { type: "busy", activity: {
      userMessageID: owner, model: count === undefined ? "none" : "receiving", streamEventCount: count,
    } } },
  })
  status(11)
  await wait(() => setup.sync.data.session_status[sessionID]?.type === "busy" &&
    "activity" in setup.sync.data.session_status[sessionID])
  expect(await setup.frame()).toContain("Thinking (11 streams)")
  setup.message(assistant(99))
  setup.message({ ...user, id: "msg_pending", time: { created: 3 } })
  await wait(() => setup.sync.data.message[sessionID]?.length === 3)
  expect(await setup.frame()).toContain("Thinking (11 streams)")
  status(0, "msg_pending")
  await setup.app.renderOnce()
  await wait(() => {
    const value = setup.sync.data.session_status[sessionID]
    return value?.type === "busy" && value.activity?.streamEventCount === 0
  })
  expect(await setup.frame()).toContain("Thinking (0 streams)")
  status(undefined, "msg_pending")
  await wait(() => {
    const value = setup.sync.data.session_status[sessionID]
    return value?.type === "busy" && value.activity?.streamEventCount === undefined
  })
  expect(await setup.frame()).toContain("Thinking (streams unavailable)")
})

test("a pending user does not take the active owner's reasoning header", async () => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path)
  setup.message(assistant(99))
  setup.message({ ...user, id: "msg_pending", time: { created: 3 } })
  setup.emit({ id: "evt_activity", type: "session.status", properties: {
    sessionID, status: { type: "busy", activity: { userMessageID: user.id, model: "receiving", streamEventCount: 11 } },
  } })
  setup.emit({ id: "evt_reasoning", type: "message.part.updated", properties: {
    sessionID, time: 4, part: { id: "prt_owner", sessionID, messageID: "msg_assistant",
      type: "reasoning", text: "**Inspecting**", time: { start: 2 } },
  } })
  await wait(() => setup.sync.data.part.msg_assistant?.length === 1)
  expect(await setup.frame()).not.toContain("Thinking")
  setup.emit({ id: "evt_new_owner", type: "session.status", properties: {
    sessionID, status: { type: "busy", activity: { userMessageID: "msg_pending", model: "preparing", streamEventCount: 0 } },
  } })
  await wait(() => {
    const status = setup.sync.data.session_status[sessionID]
    return status?.type === "busy" && status.activity?.userMessageID === "msg_pending"
  })
  expect(await setup.frame()).toContain("Thinking (0 streams)")
})

test("keeps the empty-summary counter visible in show mode", async () => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path, undefined, "show")
  setup.message(assistant(11))
  await wait(() => setup.sync.data.message[sessionID]?.length === 2)
  expect(await setup.frame()).toContain("Thinking (11 streams)")
})

test.each([true, false] as const)("renders count-only native updates with legacy assistant=%s", async (legacy) => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path)
  if (legacy) setup.message(assistant())
  setup.emit({
    id: "evt_step",
    type: "session.next.step.started",
    properties: {
      sessionID,
      assistantMessageID: "msg_assistant",
      timestamp: 2,
      agent: "build",
      model: { id: "model", providerID: "test" },
      streamEventCount: 0,
    },
  })
  await wait(() => setup.data.session.message.list(sessionID)?.[0]?.type === "assistant")
  expect(await setup.frame()).toContain("Thinking (0 streams)")
  setup.emit({
    id: "evt_stream",
    type: "session.next.step.stream.updated",
    properties: { sessionID, assistantMessageID: "msg_assistant", timestamp: 3, streamEventCount: 11 },
  })
  await wait(() => {
    const current = setup.data.session.message.list(sessionID)?.[0]
    return current?.type === "assistant" && current.streamEventCount === 11
  })
  const frame = await setup.frame()
  expect(frame).toContain("Thinking (11 streams)")
  expect(setup.sync.data.part.msg_assistant).toBeUndefined()
  if (!legacy && process.env.OPENCODE_TUI_THINKING_FRAME) await Bun.write(process.env.OPENCODE_TUI_THINKING_FRAME, frame)
  if (!legacy) {
    setup.message({ ...user, id: "msg_new_user", time: { created: 3 } })
    await wait(() => setup.sync.data.message[sessionID]?.length === 2)
    expect(await setup.frame()).toContain("Thinking (0 streams)")
  }
  if (legacy) setup.message(assistant(0, "msg_next", 4))
  setup.emit({
    id: "evt_next_step",
    type: "session.next.step.started",
    properties: {
      sessionID,
      assistantMessageID: "msg_next",
      timestamp: 4,
      agent: "build",
      model: { id: "model", providerID: "test" },
      streamEventCount: 0,
    },
  })
  setup.emit({
    id: "evt_old_stream",
    type: "session.next.step.stream.updated",
    properties: { sessionID, assistantMessageID: "msg_assistant", timestamp: 5, streamEventCount: 99 },
  })
  await wait(() => setup.data.session.message.list(sessionID)?.[0]?.id === "msg_next")
  expect(await setup.frame()).toContain("Thinking (0 streams)")
  const previous = setup.data.session.message.list(sessionID)?.find((message) => message.id === "msg_assistant")
  expect(previous?.type === "assistant" && previous.streamEventCount).toBe(11)
  if (!legacy) expect(setup.sync.data.message[sessionID]?.map((message) => message.role)).toEqual(["user", "user"])
})

test("keeps completed and earlier user response counts out of the current label", async () => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path)
  setup.message({ ...assistant(11), time: { created: 2, completed: 3 } })
  await wait(() => setup.sync.data.message[sessionID]?.length === 2)
  expect(await setup.frame()).toContain("Thinking (0 streams)")
  setup.message(assistant(0, "msg_next", 4))
  await wait(() => setup.sync.data.message[sessionID]?.length === 3)
  const frame = await setup.frame()
  expect(frame).toContain("Thinking (0 streams)")
  expect(frame).not.toContain("11 streams")
  setup.message(assistant(11, "msg_next", 4))
  await wait(() => {
    const current = setup.sync.data.message[sessionID]?.[2]
    return current?.role === "assistant" && current.streamEventCount === 11
  })
  setup.message({ ...user, id: "msg_new_user", time: { created: 5 } })
  await wait(() => setup.sync.data.message[sessionID]?.length === 4)
  expect(await setup.frame()).toContain("Thinking (0 streams)")
})

test("updates the live empty-summary label from zero to eleven", async () => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path)
  setup.message(assistant(0))
  setup.emit({
    id: "evt_empty_reasoning",
    type: "message.part.updated",
    properties: {
      sessionID,
      time: 2,
      part: { id: "prt_empty", sessionID, messageID: "msg_assistant", type: "reasoning", text: "", time: { start: 2 } },
    },
  })
  await wait(() => setup.sync.data.part.msg_assistant?.length === 1)
  expect(await setup.frame()).toContain("Thinking (0 streams)")
  setup.message(assistant(11))
  await wait(() => {
    const current = setup.sync.data.message[sessionID]?.[1]
    return current?.role === "assistant" && current.streamEventCount === 11
  })
  expect(await setup.frame()).toContain("Thinking (11 streams)")
  expect(setup.sync.data.part.msg_assistant).toHaveLength(1)
})

test("does not add a response header beside a visible live reasoning header", async () => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path)
  setup.message(assistant(11))
  setup.emit({
    id: "evt_reasoning",
    type: "message.part.updated",
    properties: {
      sessionID,
      time: 2,
      part: {
        id: "prt_reasoning", sessionID, messageID: "msg_assistant", type: "reasoning",
        text: "**Inspecting**", time: { start: 2 },
      },
    },
  })
  await wait(() => setup.sync.data.part.msg_assistant?.length === 1)
  expect(await setup.frame()).not.toContain("Thinking")
})

test.each([false, true])("preserves title and completed copy when done=%s", async (done) => {
  await using tmp = await tmpdir()
  using setup = await fixture(tmp.path, () => (
    <ReasoningHeader toggleable={true} open={false} done={done} title="Inspecting" current={true} streamEventCount={11} />
  ))
  expect(await setup.frame()).toContain(done ? "+ Thought: Inspecting" : "Thinking (11 streams): Inspecting")
})
