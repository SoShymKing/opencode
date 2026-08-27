import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { MessageV2 } from "@/session/message-v2"
import { PromptHistoryConversion } from "@/session/prompt-history-conversion"
import { ProviderTransform } from "@/provider/transform"
import type { Provider } from "@/provider/provider"
import type { SessionV1 } from "@opencode-ai/core/v1/session"
import {
  assistant,
  cloneLedger,
  contaminate,
  failure,
  historicalParts,
  mediaToolPart,
  messageID,
  model,
  recorder,
  stepParts,
  text,
  toolPart,
  turn,
  user,
} from "./prompt-history-conversion.fixtures"

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

describe("PromptHistoryConversion", () => {
  test("uses recoverable configured projection only for oversized settled tool text", async () => {
    // Given
    const selected = model({ apiNpm: "@ai-sdk/anthropic" })
    const belowOutput = "S".repeat(49 * 1024)
    const oversizedOutput = "O".repeat(60 * 1024)
    const tailOutput = "T".repeat(60 * 1024)
    const belowUser = user("below", "U".repeat(2_500))
    const belowTool = mediaToolPart("below")
    if (belowTool.state.status !== "completed") throw new Error("Expected completed below-limit tool")
    belowTool.state.output = belowOutput
    const oversizedUser = user("oversized")
    const oversizedTool = toolPart("oversized", "completed")
    if (oversizedTool.state.status !== "completed") throw new Error("Expected completed oversized tool")
    oversizedTool.state.output = oversizedOutput
    const tailUser = user("tail")
    const tailTool = toolPart("tail", "completed")
    if (tailTool.state.status !== "completed") throw new Error("Expected completed tail tool")
    tailTool.state.output = tailOutput
    let projectionCalls = 0
    const projectToolOutput = (output: string) =>
      Effect.sync(() => {
        projectionCalls++
        if (output.length <= 50 * 1024) return output
        return `${output.slice(0, 50 * 1024)}\nFull output saved to: history-output\nUse Grep or Read to inspect it.`
      })
    const history = [
      belowUser,
      assistant("below", belowUser.info.id, {
        parts: [text(messageID("assistant_below"), "prose", "A".repeat(2_500)), belowTool],
      }),
      oversizedUser,
      assistant("oversized", oversizedUser.info.id, { parts: [oversizedTool] }),
      tailUser,
      assistant("tail", tailUser.info.id, { parts: [tailTool], finish: false }),
    ]
    const source = structuredClone(history)
    const cache = PromptHistoryConversion.make(undefined, structuredClone, projectToolOutput)

    // When
    const output = JSON.stringify(await convert(cache, history, selected))
    await convert(cache, history, selected)
    await convert(cache, history, selected, true)

    // Then
    expect(output.includes(belowOutput)).toBe(true)
    expect(output.includes(oversizedOutput)).toBe(false)
    expect(output.includes("Full output saved to:")).toBe(true)
    expect(output.includes("U".repeat(2_500))).toBe(true)
    expect(output.includes("A".repeat(2_500))).toBe(true)
    expect(output.includes(tailOutput)).toBe(true)
    expect(output).toContain("document.pdf")
    expect(history).toEqual(source)
    expect(projectionCalls).toBe(2)
  })

  test("does not deep-clone large cached tool output on repeated warm conversions", async () => {
    // Given
    const ledger = cloneLedger()
    const selected = model()
    const prompt = user("warm")
    const tool = toolPart("warm", "completed")
    if (tool.state.status !== "completed") throw new Error("Expected completed tool")
    tool.state.output = "x".repeat(512 * 1024)
    const history = [prompt, assistant("warm", prompt.info.id, { parts: [tool] })]
    const cache = PromptHistoryConversion.make(undefined, ledger.clone)
    const expected = await convert(cache, history, selected)
    ledger.take()

    // When
    for (let index = 0; index < 50; index++) expect(await convert(cache, history, selected)).toEqual(expected)

    // Then
    expect(ledger.characters()).toBe(0)
    expect(ledger.take()).toEqual([])
  })

  test("clones and converts only a stable extension while preserving the old prefix", async () => {
    // Given
    const ledger = cloneLedger()
    const calls: Array<{ ids: string[]; model: string }> = []
    const cache = PromptHistoryConversion.make(recorder(calls), ledger.clone)
    const first = turn("first")
    await convert(cache, first, model())
    ledger.take()

    // When
    const extended = [...first, ...turn("second")]
    expect(await convert(cache, extended, model())).toEqual(await cold(extended, model()))

    // Then
    expect(calls.at(-1)?.ids).toEqual(turn("second").map((message) => message.info.id))
    expect(ledger.take()).toEqual([{ kind: "source", messages: 2, parts: 2 }])
  })

  test("returns transient tail output without cloning the tail or assembled output", async () => {
    // Given
    const ledger = cloneLedger()
    const cache = PromptHistoryConversion.make(undefined, ledger.clone)
    const stable = turn("stable")
    await convert(cache, stable, model())
    ledger.take()

    // When
    const history = [...stable, user("tail")]
    expect(await convert(cache, history, model())).toEqual(await cold(history, model()))

    // Then
    expect(ledger.take()).toEqual([])
  })

  test("transformed mode converts one private input and returns its fresh output directly", async () => {
    // Given
    const ledger = cloneLedger()
    const cache = PromptHistoryConversion.make(undefined, ledger.clone)
    const history = [...turn("transformed"), user("tail")]

    // When
    expect(await convert(cache, history, model(), true)).toEqual(await cold(history, model()))

    // Then
    expect(ledger.take()).toEqual([{ kind: "source", messages: 3, parts: 3 }])
  })

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
    const nested = history[1]?.parts[0]
    if (nested?.type === "tool") nested.metadata = { ...nested.metadata, binary: new Uint8Array([1, 2, 3]) }
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
