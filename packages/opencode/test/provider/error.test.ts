import { describe, expect, test } from "bun:test"
import { ProviderError } from "@/provider/error"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { APICallError } from "ai"

const secret = "secret-attachment-marker"
const providerID = ProviderV2.ID.make("test-provider")

function apiCallError(data?: unknown, responseBody?: string, statusCode = 400) {
  return new APICallError({
    message: `Provider prose ${secret}`,
    url: `https://example.com/${secret}`,
    requestBodyValues: { file: secret },
    statusCode,
    responseHeaders: { "content-type": "application/json" },
    responseBody,
    isRetryable: statusCode === 429,
    data,
  })
}

describe("provider stream errors", () => {
  test("retries provider stream errors without a code", () => {
    const messages = [
      "The model is currently at capacity due to high demand. Please try again in a few minutes, or use a higher service tier for priority processing: https://docs.x.ai/developers/advanced-api-usage/priority-processing",
      "The model is temporarily unavailable.",
    ]

    for (const message of messages)
      expect(
        ProviderError.parseStreamError({
          type: "error",
          error: { message },
        }),
      ).toEqual({
        type: "api_error",
        message,
        isRetryable: true,
        responseBody: JSON.stringify({ type: "error", error: { message } }),
      })
  })
})

describe("provider API call errors", () => {
  const rejected = {
    error: {
      type: "invalid_request_error",
      param: "input",
      code: "invalid_file",
      message: secret,
      attachment: { mime: "application/pdf", data: secret, url: `https://example.com/${secret}` },
    },
  }

  test("normalizes rejected attachments from structured data without unsafe provider fields", () => {
    const result = ProviderError.parseAPICallError({
      providerID,
      error: apiCallError(rejected, JSON.stringify({ error: { code: "other" } })),
    })

    expect(result).toEqual({
      type: "rejected_attachment",
      message: "Provider rejected an attachment.",
      statusCode: 400,
      isRetryable: false,
      metadata: {
        providerErrorType: "invalid_request_error",
        providerErrorParam: "input",
        providerErrorCode: "invalid_file",
      },
    })
    expect(JSON.stringify(result)).not.toContain(secret)
  })

  test("falls back to rejected attachment tuple in response body", () => {
    const result = ProviderError.parseAPICallError({
      providerID,
      error: apiCallError({ error: { code: "other" } }, JSON.stringify(rejected)),
    })

    expect(result.type).toBe("rejected_attachment")
  })

  test.each([
    ["type", { type: "other", param: "input", code: "invalid_file" }],
    ["param", { type: "invalid_request_error", param: "other", code: "invalid_file" }],
    ["code", { type: "invalid_request_error", param: "input", code: "other" }],
  ])("preserves generic API error when %s differs", (_field, error) => {
    const result = ProviderError.parseAPICallError({ providerID, error: apiCallError({ error }) })

    expect(result.type).toBe("api_error")
  })

  test("preserves attachment-free invalid request errors", () => {
    const result = ProviderError.parseAPICallError({
      providerID,
      error: apiCallError({ error: { type: "invalid_request_error", code: "invalid_file" } }),
    })

    expect(result.type).toBe("api_error")
  })

  test.each([400, 401, 429])("preserves ordinary %d API errors", (statusCode) => {
    const error = apiCallError(
      { error: { type: "invalid_request_error", param: "input", code: "other" } },
      undefined,
      statusCode,
    )
    const result = ProviderError.parseAPICallError({ providerID, error })

    expect(result).toMatchObject({
      type: "api_error",
      statusCode,
      isRetryable: error.isRetryable,
      responseHeaders: error.responseHeaders,
      responseBody: error.responseBody,
      metadata: { url: error.url },
    })
  })
})
