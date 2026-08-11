import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { MessageV2 } from "@/session/message-v2"
import { PromptHistoryConversion } from "@/session/prompt-history-conversion"
import { ProviderTransform } from "@/provider/transform"
import type { Provider } from "@/provider/provider"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import {
  assistant,
  failure,
  historicalParts,
  mediaToolPart,
  messageID,
  model,
  stepParts,
  toolPart,
  turn,
  user,
} from "./prompt-history-conversion.fixtures"

function recorder(calls: Array<{ ids: string[]; model: string }>) {
  return (messages: SessionV1.WithParts[], selected: Provider.Model) =>
    MessageV2.toModelMessagesEffect(messages, selected).pipe(
      Effect.tap(() =>
        Effect.sync(() => calls.push({ ids: messages.map((item) => item.info.id), model: `${selected.providerID}/${selected.id}` })),
      ),
    )
}

function cold(messages: SessionV1.WithParts[], selected: Provider.Model) {
  return Effect.runPromise(MessageV2.toModelMessagesEffect(structuredClone(messages), selected))
}

function convert(
  cache: ReturnType<typeof PromptHistoryConversion.make>,
  messages: SessionV1.WithParts[],
  selected: Provider.Model,
  transformed = false,
) {
  return Effect.runPromise(cache.convert({ messages, model: selected, transformed }))
}

function contaminate(value: unknown) {
  if (typeof value !== "object" || value === null) return
  for (const [key, child] of Object.entries(value)) {
    if (key === "value") Reflect.set(value, key, "poisoned")
    contaminate(child)
  }
}

describe("PromptHistoryConversion", () => {
  test("reuses settled turns and promotes only whole completed extensions", async () => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const selected = model()
    const cache = PromptHistoryConversion.make(recorder(calls))
    const historical = user("two", "two", historicalParts("two"))
    const first = [
      ...turn("one"),
      historical,
      assistant("two", historical.info.id, { parts: [toolPart("two", "error")], summary: true }),
      user("three"),
    ]

    // When
    expect(await convert(cache, first, selected)).toEqual(await cold(first, selected))
    expect(await convert(cache, first, selected)).toEqual(await cold(first, selected))
    const promoted = [...first, assistant("three", messageID("user_three")), user("four")]
    expect(await convert(cache, promoted, selected)).toEqual(await cold(promoted, selected))
    expect(await convert(cache, promoted, selected)).toEqual(await cold(promoted, selected))

    // Then
    expect(calls.map((call) => call.ids)).toEqual([
      first.slice(0, 4).map((item) => item.info.id),
      [messageID("user_three")],
      [messageID("user_three")],
      [messageID("user_three"), messageID("assistant_three")],
      [messageID("user_four")],
      [messageID("user_four")],
    ])
  })

  test("keeps tool pairs and step-start splits inside whole assistant source cuts", async () => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const selected = model()
    const cache = PromptHistoryConversion.make(recorder(calls))
    const history = [
      ...turn("tool", { parts: [toolPart("tool", "completed")] }),
      ...turn("step", { parts: stepParts("step") }),
      user("tail"),
    ]

    // When
    const optimized = await convert(cache, history, selected)

    // Then
    expect(optimized).toEqual(await cold(history, selected))
    expect(optimized.map((item) => item.role)).toEqual(["user", "assistant", "tool", "user", "assistant", "assistant", "user"])
    expect(calls.map((call) => call.ids)).toEqual([
      history.slice(0, 4).map((item) => item.info.id),
      [messageID("user_tail")],
    ])
  })

  test.each([
    ["pending tool", [user("pending"), assistant("pending", messageID("user_pending"), { parts: [toolPart("pending", "pending")] })]],
    ["running tool", [user("running"), assistant("running", messageID("user_running"), { parts: [toolPart("running", "running")] })]],
    ["current assistant", [user("current"), assistant("current", messageID("user_current"), { finish: false })]],
    ["assistant failure", [user("failed"), assistant("failed", messageID("user_failed"), { error: failure })]],
    ["malformed turn", [user("orphan"), user("replacement"), assistant("replacement", messageID("user_replacement"))]],
    ["unsettled historical work", [user("history", "history", historicalParts("history"))]],
  ])("stops prefix growth at %s", async (_name, blocked) => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const selected = model()
    const cache = PromptHistoryConversion.make(recorder(calls))
    const stable = turn("stable")
    const history = [...stable, ...blocked, ...turn("later")]

    // When
    const optimized = await convert(cache, history, selected)

    // Then
    expect(optimized).toEqual(await cold(history, selected))
    expect(calls[0]?.ids).toEqual(stable.map((item) => item.info.id))
    expect(calls[1]?.ids).toEqual(history.slice(stable.length).map((item) => item.info.id))
  })

  test.each([
    ["content mutation", (source: SessionV1.WithParts[]) => {
      const changed = structuredClone(source)
      const part = changed[0]?.parts[0]
      if (part?.type === "text") part.text = "changed"
      return changed
    }],
    ["reorder", (source: SessionV1.WithParts[]) => [...source.slice(2), ...source.slice(0, 2)]],
    ["ID replacement", () => [...turn("replacement-one"), ...turn("replacement-two")]],
  ])("rebuilds on stable source %s", async (_name, change) => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const selected = model()
    const cache = PromptHistoryConversion.make(recorder(calls))
    const original = [...turn("original-one"), ...turn("original-two")]
    await convert(cache, original, selected)

    // When
    const changed = change(original)
    const optimized = await convert(cache, changed, selected)

    // Then
    expect(optimized).toEqual(await cold(changed, selected))
    expect(calls).toHaveLength(2)
    expect(calls[1]?.ids).toEqual(changed.map((item) => item.info.id))
  })

  test("rebuilds on model, provider, and explicit invalidation", async () => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const cache = PromptHistoryConversion.make(recorder(calls))
    const history = turn("identity")

    // When
    expect(await convert(cache, history, model())).toEqual(await cold(history, model()))
    expect(await convert(cache, history, model({ modelID: "other-model" }))).toEqual(
      await cold(history, model({ modelID: "other-model" })),
    )
    expect(await convert(cache, history, model({ providerID: "other-provider", modelID: "other-model" }))).toEqual(
      await cold(history, model({ providerID: "other-provider", modelID: "other-model" })),
    )
    cache.invalidate()
    const rebuilt = await convert(cache, history, model({ providerID: "other-provider", modelID: "other-model" }))

    // Then
    expect(rebuilt).toEqual(await cold(history, model({ providerID: "other-provider", modelID: "other-model" })))
    expect(calls.map((call) => call.model)).toEqual([
      "test/test-model",
      "test/other-model",
      "other-provider/other-model",
      "other-provider/other-model",
    ])
  })

  test.each([
    [
      "api.npm changes from Anthropic to Bedrock",
      model({ providerID: "shared", modelID: "same", apiNpm: "@ai-sdk/anthropic", apiID: "shared-api" }),
      model({ providerID: "shared", modelID: "same", apiNpm: "@ai-sdk/amazon-bedrock", apiID: "shared-api" }),
      "user,assistant,tool,user",
    ],
    [
      "api.id changes from Gemini 2 to Gemini 3",
      model({ providerID: "shared", modelID: "same", apiNpm: "@ai-sdk/google", apiID: "gemini-2-pro" }),
      model({ providerID: "shared", modelID: "same", apiNpm: "@ai-sdk/google", apiID: "gemini-3-pro" }),
      "user,assistant,tool",
    ],
  ])("rebuilds when same-ID model %s", async (_name, firstModel, secondModel, roles) => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const cache = PromptHistoryConversion.make(recorder(calls))
    const history = turn("media", { selectedModel: firstModel, parts: [mediaToolPart("media")] })
    await convert(cache, history, firstModel)

    // When
    const optimized = await convert(cache, history, secondModel)

    // Then
    expect(optimized).toEqual(await cold(history, secondModel))
    expect(optimized.map((item) => item.role).join(",")).toBe(roles)
    expect(calls).toHaveLength(2)
  })

  test("reuses same API identity across capability and option changes", async () => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const selected = model()
    const changed = { ...selected, capabilities: { ...selected.capabilities, reasoning: true }, options: { other: true } }
    const cache = PromptHistoryConversion.make(recorder(calls))
    const history = turn("unrelated-model-fields")
    await convert(cache, history, selected)

    // When
    const optimized = await convert(cache, history, changed)

    // Then
    expect(optimized).toEqual(await cold(history, changed))
    expect(calls).toHaveLength(1)
  })

  test("hook mode full-converts every call, resets cache, and avoids accumulation", async () => {
    // Given
    const calls: Array<{ ids: string[]; model: string }> = []
    const selected = model()
    const cache = PromptHistoryConversion.make(recorder(calls))
    const history = [...turn("hook"), user("tail")]
    await convert(cache, history, selected)

    // When
    const transformed = structuredClone(history)
    const oldest = transformed[0]?.parts[0]
    if (oldest?.type === "text") oldest.text = transformed.at(-1)?.info.id ?? "missing"
    const first = await convert(cache, transformed, selected, true)
    contaminate(first)
    const second = await convert(cache, transformed, selected, true)
    await convert(cache, history, selected)

    // Then
    expect(second).toEqual(await cold(transformed, selected))
    expect(calls.map((call) => call.ids)).toEqual([
      history.slice(0, 2).map((item) => item.info.id),
      [messageID("user_tail")],
      transformed.map((item) => item.info.id),
      transformed.map((item) => item.info.id),
      history.slice(0, 2).map((item) => item.info.id),
      [messageID("user_tail")],
    ])
  })

  test("retains pristine source and model payloads across deep caller and provider mutation", async () => {
    // Given
    const selected = model({ providerID: "anthropic" })
    const cache = PromptHistoryConversion.make()
    const history = [...turn("mutation", { selectedModel: selected, parts: [toolPart("mutation", "completed")] }), user("tail")]
    const source = structuredClone(history)

    // When
    const first = await convert(cache, history, selected)
    ProviderTransform.message(first, selected, {})
    contaminate(first)
    const second = await convert(cache, history, selected)

    // Then
    expect(history).toEqual(source)
    expect(second).toEqual(await cold(history, selected))
  })
})
