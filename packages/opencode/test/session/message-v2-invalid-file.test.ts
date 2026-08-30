import { describe, expect, test } from "bun:test"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { InvalidFile } from "@/session/invalid-file"
import { MessageV2 } from "@/session/message-v2"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { ProviderTest } from "../fake/provider"

const sessionID = SessionID.make("session")
const providerID = ProviderV2.ID.make("test-provider")
const modelID = ModelV2.ID.make("test-model")
const model = ProviderTest.model({ id: modelID, providerID })

function messageID(value: string) {
  return MessageID.make(`msg_${value}`)
}

function partID(value: string) {
  return PartID.make(`prt_${value}`)
}

function assistant(id: string, parentID: string, created: number): SessionV1.Assistant {
  return {
    id: messageID(id),
    sessionID,
    role: "assistant",
    time: { created },
    parentID: messageID(parentID),
    providerID,
    modelID,
    mode: "build",
    agent: "build",
    path: { cwd: "/workspace", root: "/workspace" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  }
}

describe("session.message-v2 rejected attachments", () => {
  test("omits quarantined Read tool-result attachment while preserving persisted history", async () => {
    const userID = messageID("user")
    const toolMessageID = messageID("tool")
    const toolFileID = partID("tool_pdf")
    const normal = `data:application/pdf;base64,${Buffer.from("%PDF-normal").toString("base64")}`
    const rejectedBase64 = Buffer.from("%PDF-rejected").toString("base64")
    const rejected = `data:application/pdf;base64,${rejectedBase64}`
    const input = [
      {
        info: {
          id: userID,
          sessionID,
          role: "user",
          time: { created: 1 },
          agent: "build",
          model: { providerID, modelID },
        },
        parts: [
          { id: partID("text"), sessionID, messageID: userID, type: "text", text: "keep text" },
          {
            id: partID("normal"),
            sessionID,
            messageID: userID,
            type: "file",
            mime: "application/pdf",
            filename: "normal.pdf",
            url: normal,
          },
        ],
      },
      {
        info: assistant("tool", "user", 2),
        parts: [
          {
            id: partID("read"),
            sessionID,
            messageID: toolMessageID,
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
                  id: toolFileID,
                  sessionID,
                  messageID: toolMessageID,
                  type: "file",
                  mime: "application/pdf",
                  url: rejected,
                },
              ],
            },
          },
        ],
      },
      {
        info: {
          ...assistant("rejected", "user", 3),
          error: {
            name: "APIError",
            data: {
              message: "Provider rejected an attachment.",
              isRetryable: false,
              metadata: {
                providerErrorType: "invalid_request_error",
                providerErrorParam: "input",
                providerErrorCode: "invalid_file",
                [InvalidFile.metadataKey]: InvalidFile.encode([
                  { partID: toolFileID, messageID: toolMessageID, mime: "application/pdf" },
                ]),
              },
            },
          },
        },
        parts: [],
      },
    ] satisfies SessionV1.WithParts[]
    const before = structuredClone(input)

    const result = await MessageV2.toModelMessages(input, model)

    expect(input).toEqual(before)
    expect(JSON.stringify(result)).not.toContain(rejectedBase64)
    expect(JSON.stringify(result)).toContain(normal)
  })
})
