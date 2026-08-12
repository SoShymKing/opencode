import { isDeepStrictEqual } from "node:util"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import type { ModelMessage } from "ai"
import { Effect } from "effect"
import type { Provider } from "@/provider/provider"
import { MessageV2 } from "./message-v2"

type Converter = (
  messages: SessionV1.WithParts[],
  model: Provider.Model,
) => Effect.Effect<ModelMessage[]>

type Clone = <T>(value: T) => T

type Input = {
  readonly messages: SessionV1.WithParts[]
  readonly model: Provider.Model
  readonly transformed: boolean
}

type Cache = {
  readonly providerID: Provider.Model["providerID"]
  readonly modelID: Provider.Model["id"]
  readonly apiNpm: Provider.Model["api"]["npm"]
  readonly apiID: Provider.Model["api"]["id"]
  readonly source: SessionV1.WithParts[]
  readonly converted: ModelMessage[]
}

export function make(converter: Converter = MessageV2.toModelMessagesEffect, clone: Clone = structuredClone) {
  let cache: Cache | undefined

  const invalidate = () => {
    cache = undefined
  }

  const convert = Effect.fnUntraced(function* (input: Input) {
    if (input.transformed) {
      invalidate()
      return yield* converter(clone(input.messages), input.model)
    }

    const end = stablePrefixLength(input.messages)
    const stable = input.messages.slice(0, end)
    const previous = cacheMatches(cache, stable, input.model) ? cache : undefined
    const extension = previous ? stable.slice(previous.source.length) : stable
    const owned = extension.length === 0 ? [] : clone(extension)
    const converted = owned.length === 0 ? [] : yield* converter(owned, input.model)
    const next = {
      providerID: input.model.providerID,
      modelID: input.model.id,
      apiNpm: input.model.api.npm,
      apiID: input.model.api.id,
      source: previous ? [...previous.source, ...owned] : owned,
      converted: previous ? [...previous.converted, ...converted] : converted,
    } satisfies Cache
    cache = next
    const tail = input.messages.slice(end)
    const convertedTail = tail.length === 0 ? [] : yield* converter(clone(tail), input.model)
    return [...clone(next.converted), ...convertedTail]
  })

  return Object.freeze({ convert, invalidate })
}

function cacheMatches(cache: Cache | undefined, stable: SessionV1.WithParts[], model: Provider.Model): cache is Cache {
  if (
    !cache ||
    cache.providerID !== model.providerID ||
    cache.modelID !== model.id ||
    cache.apiNpm !== model.api.npm ||
    cache.apiID !== model.api.id
  )
    return false
  if (cache.source.length > stable.length) return false
  return cache.source.every((message, index) => isDeepStrictEqual(message, stable[index]))
}

function stablePrefixLength(messages: SessionV1.WithParts[]) {
  let end = 0
  for (let index = 0; index < messages.length; index += 2) {
    const user = messages[index]
    const assistant = messages[index + 1]
    if (user?.info.role !== "user" || assistant?.info.role !== "assistant") return end
    if (assistant.info.parentID !== user.info.id || !settled(assistant)) return end
    end = index + 2
  }
  return end
}

function settled(message: SessionV1.WithParts) {
  if (message.info.role !== "assistant") return false
  if (message.info.finish === undefined || message.info.error !== undefined) return false
  return message.parts.every(
    (part) => part.type !== "tool" || part.state.status === "completed" || part.state.status === "error",
  )
}

export * as PromptHistoryConversion from "./prompt-history-conversion"
