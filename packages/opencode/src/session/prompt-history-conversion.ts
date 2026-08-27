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
type ProjectToolOutput = (output: string) => Effect.Effect<string>

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

export function make(
  converter: Converter = MessageV2.toModelMessagesEffect,
  clone: Clone = structuredClone,
  projectToolOutput: ProjectToolOutput = (output) => Effect.succeed(output),
) {
  let cache: Cache | undefined
  const projections = new Map<string, { readonly source: string; readonly projected: string }>()

  const project = Effect.fnUntraced(function* (messages: SessionV1.WithParts[], count = messages.length) {
    for (const message of messages.slice(0, count)) {
      for (const part of message.parts) {
        if (part.type !== "tool" || part.state.status !== "completed") continue
        if (part.state.time.compacted || part.state.metadata.truncated === true) continue
        const source = part.state.output
        const cached = projections.get(part.id)
        const projected = cached?.source === source ? cached.projected : yield* projectToolOutput(source)
        projections.set(part.id, { source, projected })
        part.state.output = projected
      }
    }
    return messages
  })

  const invalidate = () => {
    cache = undefined
  }

  const convert = Effect.fnUntraced(function* (input: Input) {
    const end = stablePrefixLength(input.messages)
    if (input.transformed) {
      invalidate()
      const projected = yield* project(clone(input.messages), end)
      return yield* converter(projected, input.model)
    }

    const stable = input.messages.slice(0, end)
    const previous = cacheMatches(cache, stable, input.model) ? cache : undefined
    const extension = previous ? stable.slice(previous.source.length) : stable
    const owned = extension.length === 0 ? [] : clone(extension)
    const projected = owned.length === 0 ? [] : yield* project(copyMutable(owned))
    const converted = projected.length === 0 ? [] : yield* converter(projected, input.model)
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
    const convertedTail = tail.length === 0 ? [] : yield* converter(copyMutable(tail), input.model)
    return [...copyMutable(next.converted), ...convertedTail]
  })

  return Object.freeze({ convert, invalidate })
}

function copyMutable<T>(value: T): T
function copyMutable(value: unknown): unknown {
  return copyValue(value, new Map())
}

function copyValue(value: unknown, seen: Map<object, unknown>): unknown {
  if (typeof value !== "object" || value === null) return value
  const previous = seen.get(value)
  if (previous !== undefined) return previous
  if (value instanceof ArrayBuffer) return value.slice(0)
  if (ArrayBuffer.isView(value)) return structuredClone(value)
  if (value instanceof Date) return new Date(value)
  if (value instanceof URL) return new URL(value)
  if (Array.isArray(value)) {
    const result: unknown[] = []
    seen.set(value, result)
    result.push(...value.map((item) => copyValue(item, seen)))
    return result
  }
  if (value instanceof Map) {
    const result = new Map<unknown, unknown>()
    seen.set(value, result)
    for (const [key, item] of value) result.set(copyValue(key, seen), copyValue(item, seen))
    return result
  }
  if (value instanceof Set) {
    const result = new Set<unknown>()
    seen.set(value, result)
    for (const item of value) result.add(copyValue(item, seen))
    return result
  }
  const result: Record<string, unknown> = {}
  seen.set(value, result)
  for (const [key, item] of Object.entries(value)) result[key] = copyValue(item, seen)
  return result
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
