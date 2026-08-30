import { describe, expect, test } from "bun:test"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { InvalidFile } from "../../src/session/invalid-file"
import { SessionID } from "../../src/session/schema"

const sessionID = SessionID.make("session")
const providerID = ProviderV2.ID.make("provider")
const modelID = ModelV2.ID.make("model")
type AssistantWithParts = { readonly info: SessionV1.Assistant; readonly parts: SessionV1.Part[] }

function messageID(value: string) {
  return SessionV1.MessageID.make(`msg_${value}`)
}

function partID(value: string) {
  return SessionV1.PartID.make(`prt_${value}`)
}

function user(input: {
  readonly id: string
  readonly created: number
  readonly files?: readonly { readonly id: string; readonly mime: string; readonly filename?: string }[]
}): SessionV1.WithParts {
  const id = messageID(input.id)
  return {
    info: {
      id,
      sessionID,
      role: "user",
      time: { created: input.created },
      agent: "build",
      model: { providerID, modelID },
    },
    parts: (input.files ?? []).map((file) => ({
      id: partID(file.id),
      sessionID,
      messageID: id,
      type: "file",
      mime: file.mime,
      url: `data:${file.mime};base64,secret-${file.id}`,
      ...(file.filename === undefined ? {} : { filename: file.filename }),
    })),
  }
}

function assistant(input: {
  readonly id: string
  readonly parentID: string
  readonly created: number
  readonly finished?: boolean
  readonly error?: SessionV1.Assistant["error"]
  readonly parts?: readonly SessionV1.Part[]
}): AssistantWithParts {
  const id = messageID(input.id)
  return {
    info: {
      id,
      sessionID,
      role: "assistant",
      time: { created: input.created },
      parentID: messageID(input.parentID),
      providerID,
      modelID,
      mode: "build",
      agent: "build",
      path: { cwd: "/workspace", root: "/workspace" },
      cost: 0,
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      ...(input.error === undefined ? {} : { error: input.error }),
    },
    parts: [
      ...(input.parts ??
        (input.finished
          ? [
              {
                id: partID(`${input.id}_finish`),
                sessionID,
                messageID: id,
                type: "step-finish" as const,
                reason: "stop",
                cost: 0,
                tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
              },
            ]
          : [])),
    ],
  }
}

function read(input: {
  readonly messageID: SessionV1.MessageID
  readonly fileID: SessionV1.PartID
  readonly mime?: string
}): SessionV1.ToolPart {
  return {
    id: partID("read"),
    sessionID,
    messageID: input.messageID,
    type: "tool",
    callID: "call-read",
    tool: "read",
    state: {
      status: "completed",
      input: { filePath: "/tmp/rejected.pdf" },
      output: "PDF read successfully",
      title: "Read",
      metadata: {},
      time: { start: 0, end: 1 },
      attachments: [
        {
          id: input.fileID,
          sessionID,
          messageID: input.messageID,
          type: "file",
          mime: input.mime ?? "application/pdf",
          url: "data:application/pdf;base64,JVBERg==",
        },
      ],
    },
  }
}

function rejected(files: readonly InvalidFile.Record[]): SessionV1.Assistant["error"] {
  return {
    name: "APIError",
    data: {
      message: "Provider rejected an attachment.",
      isRetryable: false,
      metadata: {
        providerErrorType: "invalid_request_error",
        providerErrorParam: "input",
        providerErrorCode: "invalid_file",
        [InvalidFile.metadataKey]: InvalidFile.encode(files),
      },
    },
  }
}

describe("session.invalid-file", () => {
  test("selects same-parent Read attachment before later concurrent assistant", () => {
    const parent = user({ id: "parent", created: 20 })
    const priorID = messageID("prior")
    const prior = assistant({
      id: "prior",
      parentID: "parent",
      created: 30,
      parts: [read({ messageID: priorID, fileID: partID("tool_pdf") })],
    })
    const concurrentID = messageID("concurrent")
    const concurrent = assistant({
      id: "concurrent",
      parentID: "other",
      created: 40,
      parts: [read({ messageID: concurrentID, fileID: partID("concurrent_pdf") })],
    })
    const failed = assistant({ id: "failed", parentID: "parent", created: 50 })

    expect(InvalidFile.select([parent, prior, concurrent, failed], failed.info)).toEqual([
      { partID: partID("tool_pdf"), messageID: priorID, mime: "application/pdf" },
    ])
  })

  test("selects latest ambiguous sendable batch from unordered history", () => {
    const boundary = assistant({ id: "boundary", parentID: "before", created: 10, finished: true })
    const older = user({ id: "older", created: 20, files: [{ id: "older", mime: "application/pdf" }] })
    const tiedFirst = user({ id: "tie_a", created: 30, files: [{ id: "tie_a", mime: "image/jpeg" }] })
    const tiedLast = user({
      id: "tie_z",
      created: 30,
      files: [
        { id: "pdf", mime: "application/pdf", filename: "report.pdf" },
        { id: "image", mime: "image/png" },
        { id: "text", mime: "text/plain" },
      ],
    })
    const parent = user({ id: "parent", created: 40 })
    const failed = assistant({ id: "failed", parentID: "parent", created: 50 })

    expect(InvalidFile.select([failed, tiedFirst, older, parent, boundary, tiedLast], failed.info)).toEqual([
      { partID: partID("pdf"), messageID: messageID("tie_z"), mime: "application/pdf", filename: "report.pdf" },
      { partID: partID("image"), messageID: messageID("tie_z"), mime: "image/png" },
    ])
  })

  test("decodes null filename without changing attachment identity", () => {
    expect(
      InvalidFile.decode(
        JSON.stringify({
          version: 1,
          files: [{ partID: partID("pdf"), messageID: messageID("user"), mime: "application/pdf", filename: null }],
        }),
      ),
    ).toEqual([{ partID: partID("pdf"), messageID: messageID("user"), mime: "application/pdf" }])
  })

  test("deduplicates repeated quarantine records idempotently", () => {
    const file = { partID: partID("pdf"), messageID: messageID("user"), mime: "application/pdf" }
    const messages = [
      assistant({ id: "first", parentID: "user", created: 2, error: rejected([file, file]) }),
      assistant({ id: "second", parentID: "user", created: 3, error: rejected([file]) }),
    ]

    const collected = InvalidFile.collect(messages)

    expect(collected).toEqual([file])
    expect(InvalidFile.merge(collected, collected)).toEqual(collected)
  })

  test("progresses past previously quarantined batch", () => {
    const earlier = user({ id: "earlier", created: 10, files: [{ id: "earlier", mime: "application/pdf" }] })
    const latest = user({ id: "latest", created: 20, files: [{ id: "latest", mime: "image/png" }] })
    const prior = assistant({
      id: "prior",
      parentID: "latest",
      created: 30,
      error: rejected([{ partID: partID("latest"), messageID: messageID("latest"), mime: "image/png" }]),
    })
    const parent = user({ id: "parent", created: 40 })
    const failed = assistant({ id: "failed", parentID: "parent", created: 50 })

    expect(InvalidFile.select([failed, latest, prior, earlier, parent], failed.info)).toEqual([
      { partID: partID("earlier"), messageID: messageID("earlier"), mime: "application/pdf" },
    ])
  })

  test("does not select another batch when failed assistant is already enriched", () => {
    const earlier = user({ id: "earlier", created: 10, files: [{ id: "earlier", mime: "application/pdf" }] })
    const latest = user({ id: "latest", created: 20, files: [{ id: "latest", mime: "image/png" }] })
    const parent = user({ id: "parent", created: 30 })
    const failed = assistant({
      id: "failed",
      parentID: "parent",
      created: 40,
      error: rejected([{ partID: partID("latest"), messageID: messageID("latest"), mime: "image/png" }]),
    })

    expect(InvalidFile.select([failed, latest, earlier, parent], failed.info)).toEqual([])
  })
})
