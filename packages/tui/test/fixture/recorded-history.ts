import type { SessionMessageAssistant, SessionMessageAssistantTool, SessionMessageRecordedShell, SessionMessageUser } from "@opencode/client"

export function recordedShell(completed?: number): SessionMessageRecordedShell {
  return {
    id: "msg_recorded",
    type: "shell-recorded",
    callID: "original-call-not-a-shell-id",
    command: "printf recorded",
    output: "\u001b[31mrecorded output\u001b[0m\r\nsecond line\u0007\u001b]0;bad title\u0007",
    time: { created: 1, ...(completed === undefined ? {} : { completed }) },
  }
}

export const recordedUser: SessionMessageUser = {
  id: "msg_recorded_user",
  type: "user",
  text: "",
  files: [],
  agents: [],
  recordedFiles: [{ uri: "not a URI:%zz\u001b[2J\u0007", mime: "image/png", name: "missing.png" }],
  time: { created: 2 },
}

export const recordedAssistant: SessionMessageAssistant = {
  id: "msg_recorded_tools",
  type: "assistant",
  agent: "build",
  model: { providerID: "demo", id: "demo-model" },
  time: { created: 3, completed: 4 },
  finish: "stop",
  content: [
    {
      type: "tool", id: "imported-bash", name: "bash",
      state: { status: "running", input: { command: "printf saved-command", nested: { exact: true } }, metadata: { shellID: "sh_must_not_read" } },
      recorded: { content: [{ type: "text", text: "\u001b[31msavedprogress\u001b[0m\u0007" }] },
      time: { created: 3 },
    },
    {
      type: "tool", id: "imported-read", name: "read",
      state: { status: "streaming", input: "{partial-read" },
      recorded: {}, time: { created: 3 },
    },
    {
      type: "tool", id: "imported-hosted", name: "hosted_custom",
      state: { status: "completed", input: { query: "saved-query" }, content: [{ type: "text", text: "saved-output" }] },
      recorded: { result: { exact: "hosted-raw", count: 0 }, content: [{ type: "file", uri: "not a URI:%zz", mime: "image/png", name: "absent.png" }] },
      time: { created: 3, completed: 4 },
    },
    {
      type: "tool", id: "imported-error", name: "execute",
      state: { status: "error", input: { code: "saved-code" }, error: { type: "unknown", message: "saved-error" }, content: [{ type: "text", text: "saved-partial" }] },
      recorded: { result: false }, time: { created: 3, completed: 4 },
    },
    {
      type: "tool", id: "imported-subagent", name: "subagent",
      state: { status: "running", input: { description: "saved-task" }, metadata: { sessionID: "ses_must_not_adopt", status: "running" } },
      recorded: {}, time: { created: 3 },
    },
    ...["shell", "read", "websearch", "execute"].map((name): SessionMessageAssistantTool => ({
      type: "tool", id: `imported-running-${name}`, name,
      state: { status: "running", input: { saved: name }, metadata: {} },
      recorded: {}, time: { created: 3 },
    })),
  ],
}
