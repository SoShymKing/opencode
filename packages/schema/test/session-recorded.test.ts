import { expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionMessage } from "../src/session-message.js"
import { Shell } from "../src/shell.js"
import { Prompt } from "../src/prompt.js"

const recording = {
  id: "msg_recorded",
  type: "shell-recorded",
  metadata: { source: "import" },
  callID: "call_recorded",
  command: "printf recorded",
  output: "recorded\n",
  time: { created: 1000, completed: 2000 },
} satisfies typeof SessionMessage.RecordedShell.Encoded

test("recorded shell round trips exact source facts through stored messages", () => {
  const decoded = Schema.decodeUnknownSync(SessionMessage.Info)(recording)
  expect(Schema.encodeSync(SessionMessage.Info)(decoded)).toEqual(recording)
  expect(Object.keys(decoded).sort()).toEqual(Object.keys(recording).sort())
})

test("recorded shell omits unknown completion and native job facts", () => {
  const source = { ...recording, time: { created: 1000 } }
  const decoded = Schema.decodeUnknownSync(SessionMessage.RecordedShell)(source)
  const encoded = Schema.encodeSync(SessionMessage.RecordedShell)({
    ...decoded,
    time: { ...decoded.time, completed: undefined },
  })
  expect(encoded).toEqual(source)
  expect(encoded.time).not.toHaveProperty("completed")
  for (const key of ["shellID", "pid", "exit", "status", "outcome", "job"]) {
    expect(decoded).not.toHaveProperty(key)
    expect(encoded).not.toHaveProperty(key)
  }
})

test("completed tool result round trips empty content", () => {
  const source = { status: "completed", input: {}, content: [] } satisfies typeof SessionMessage.ToolStateCompleted.Encoded
  const decoded = Schema.decodeUnknownSync(SessionMessage.ToolState)(source)
  expect(Schema.encodeSync(SessionMessage.ToolState)(decoded)).toEqual(source)
})

test("error tool result round trips empty or omitted content", () => {
  const source = {
    status: "error",
    input: {},
    error: { type: "tool.error", message: "Failed" },
  } satisfies typeof SessionMessage.ToolStateError.Encoded
  const decode = Schema.decodeUnknownSync(SessionMessage.ToolState)
  const encode = Schema.encodeSync(SessionMessage.ToolState)
  expect(encode(decode(source))).toEqual(source)
  expect(encode(decode({ ...source, content: [] }))).toEqual({ ...source, content: [] })
})

test("native message shell still requires shell identity, status and structured output", () => {
  const decode = Schema.decodeUnknownSync(SessionMessage.Shell)
  const source = {
    id: "msg_native",
    type: "shell",
    command: "printf native",
    time: { created: 1000 },
  } satisfies Pick<typeof SessionMessage.Shell.Encoded, "id" | "type" | "command" | "time">
  expect(() => decode(source)).toThrow()
  expect(() => decode({ ...source, status: "exited" })).toThrow()
  expect(() => decode({ ...source, shellID: "sh_native" })).toThrow()
  const native = { ...source, shellID: "sh_native", status: "exited" } satisfies typeof SessionMessage.Shell.Encoded
  expect(Schema.encodeSync(SessionMessage.Shell)(decode(native))).toEqual(native)
  expect(() => decode({ ...native, output: "native" })).toThrow()
  expect(() => decode(recording)).toThrow()
})

test("native shell info still requires native execution facts", () => {
  const decode = Schema.decodeUnknownSync(Shell.Info)
  const source = {
    id: "sh_native",
    status: "exited",
    command: "printf native",
    cwd: "/workspace",
    shell: "sh",
    file: "/output",
    metadata: {},
    time: { started: 1000 },
  } satisfies typeof Shell.Info.Encoded
  expect(Schema.encodeSync(Shell.Info)(decode(source))).toEqual(source)
  for (const key of ["id", "status", "cwd", "shell", "file", "metadata", "time"]) {
    expect(() => decode(Object.fromEntries(Object.entries(source).filter(([name]) => name !== key)))).toThrow()
  }
  expect(() => decode(recording)).toThrow()
})

const recordedFile = {
  uri: "file:///unavailable/report.txt",
  mime: "text/plain",
  name: "report.txt",
  description: "Historical reference",
  source: { start: 4, end: 15, text: "@report.txt" },
}

const recordedAssistant = {
  id: "msg_facts",
  type: "assistant",
  agent: "build",
  model: { providerID: "provider", id: "model" },
  time: { created: 1000 },
}

const recordedTool = {
  type: "tool",
  id: "call_facts",
  name: "historical",
  state: { status: "running", input: {}, metadata: { structured: { progress: 1 } } },
  time: { created: 1000 },
}

function roundTrip(input: unknown): unknown {
  return Schema.encodeSync(SessionMessage.Info)(Schema.decodeUnknownSync(SessionMessage.Info)(input))
}

test("stored user preserves unavailable URI references and source ranges without bytes", () => {
  const source = {
    id: "msg_files",
    type: "user",
    text: "See @report.txt",
    time: { created: 1000 },
    recordedFiles: [recordedFile, { uri: "https://example.invalid/missing", mime: "image/png" }],
  }
  expect(roundTrip(source)).toEqual(source)
  expect(() => Schema.decodeUnknownSync(Prompt)({ text: source.text, files: [recordedFile] })).toThrow()
})

test("recorded tool result preserves absent, null, false and empty values", () => {
  for (const recorded of [
    {},
    { result: null },
    { result: false },
    { result: "" },
    { result: [] },
    { result: {} },
    { content: [], attachments: [], outputPaths: [], providerMetadata: {}, providerResultMetadata: {} },
  ]) {
    const source = { ...recordedAssistant, content: [{ ...recordedTool, recorded }] }
    expect(roundTrip(source)).toEqual(source)
  }
  expect(Schema.encodeSync(SessionMessage.RecordedTool)({ result: undefined })).toEqual({})
})

test("recorded provider outer maps and progress facts round trip without wrapping", () => {
  const providerMetadata = { provider: { signature: "opaque", nested: { enabled: false } }, other: { value: null } }
  const source = {
    ...recordedAssistant,
    content: [
      { type: "reasoning", text: "reason", state: { native: true }, recordedProviderMetadata: providerMetadata },
      {
        ...recordedTool,
        providerState: { native: true },
        providerResultState: { nativeResult: true },
        recorded: {
          providerMetadata,
          providerResultMetadata: { provider: { result: { items: [false, null, ""] } } },
          content: [{ type: "text", text: "partial" }],
          attachments: [recordedFile],
          outputPaths: ["/unavailable/output"],
          pruned: 2000,
        },
      },
    ],
  }
  expect(roundTrip(source)).toEqual(source)
})

test("historical fields stay absent by default and omit explicit undefined", () => {
  const user = { id: "msg_plain", type: "user", text: "plain", time: { created: 1000 } }
  expect(roundTrip(user)).toEqual(user)
  expect(
    Schema.encodeSync(SessionMessage.User)({
      ...Schema.decodeUnknownSync(SessionMessage.User)(user),
      recordedFiles: undefined,
    }),
  ).toEqual(Schema.encodeSync(SessionMessage.User)(Schema.decodeUnknownSync(SessionMessage.User)(user)))
  const source = { ...recordedAssistant, content: [recordedTool, { type: "reasoning", text: "plain" }] }
  expect(roundTrip(source)).toEqual(source)
  expect(
    Schema.encodeSync(SessionMessage.Assistant)({
      ...Schema.decodeUnknownSync(SessionMessage.Assistant)(source),
      content: [
        { ...Schema.decodeUnknownSync(SessionMessage.AssistantTool)(recordedTool), recorded: undefined },
        { type: "reasoning", text: "plain", recordedProviderMetadata: undefined },
      ],
    }),
  ).toEqual(Schema.encodeSync(SessionMessage.Assistant)(Schema.decodeUnknownSync(SessionMessage.Assistant)(source)))
})

test("recorded provider metadata requires JSON object values", () => {
  const decode = Schema.decodeUnknownSync(SessionMessage.RecordedProviderMetadata)
  for (const value of [null, false, "", [], { invalid: undefined }, { invalid: () => "not JSON" }]) {
    expect(() => decode({ provider: value })).toThrow()
  }
})
