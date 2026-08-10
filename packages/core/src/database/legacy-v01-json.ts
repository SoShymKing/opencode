export * as DatabaseLegacyV01Json from "./legacy-v01-json"

import { getNodeValue, parseTree, type Node, type ParseError } from "jsonc-parser"

export type ObjectValue = {
  readonly source: string
  readonly node: Node
  readonly value: Record<string, unknown>
}

export class JsonSyntaxError extends globalThis.Error {
  override readonly name = "JsonSyntaxError"
}

export function object(source: string) {
  const errors: ParseError[] = []
  const node = parseTree(source, errors, { allowTrailingComma: false, disallowComments: true })
  if (!node || node.type !== "object" || errors.length > 0) throw new JsonSyntaxError("Expected a strict JSON object")
  return fromNode(source, node)
}

export function objects(value: ObjectValue, key: string) {
  const node = property(value, key)?.value
  if (!node) return
  if (node.type !== "array") throw new JsonSyntaxError(`Expected ${key} to be an array`)
  return (node.children ?? []).map((child) => {
    if (child.type !== "object") throw new JsonSyntaxError(`Expected ${key} items to be objects`)
    return fromNode(value.source, child)
  })
}

export function string(value: ObjectValue, key: string) {
  const node = property(value, key)?.value
  if (!node) return
  const decoded = getNodeValue(node)
  if (typeof decoded !== "string") throw new JsonSyntaxError(`Expected ${key} to be a string`)
  return decoded
}

export function has(value: ObjectValue, key: string) {
  return property(value, key) !== undefined
}

export function remove(value: ObjectValue, keys: ReadonlySet<string>) {
  return compose(
    properties(value)
      .filter((item) => !keys.has(item.key))
      .map((item) => raw(value, item.node)),
  )
}

export function add(value: ObjectValue, entries: ReadonlyArray<{ readonly key: string; readonly raw: string }>) {
  return compose([
    ...properties(value).map((item) => raw(value, item.node)),
    ...entries.map((entry) => `${JSON.stringify(entry.key)}:${entry.raw}`),
  ])
}

function fromNode(source: string, node: Node): ObjectValue {
  const decoded = getNodeValue(node)
  if (!isRecord(decoded)) throw new JsonSyntaxError("Expected a JSON object")
  const value = { source, node, value: decoded }
  const structural = new Set(["content", "id", "type"])
  const seen = new Set<string>()
  for (const item of properties(value)) {
    if (structural.has(item.key) && seen.has(item.key)) throw new JsonSyntaxError(`Duplicate ${item.key} property`)
    seen.add(item.key)
  }
  return value
}

function property(value: ObjectValue, key: string) {
  return properties(value).find((item) => item.key === key)
}

function properties(value: ObjectValue) {
  return (value.node.children ?? []).map((node) => {
    const children = node.children ?? []
    const key = children[0]
    const propertyValue = children[1]
    if (node.type !== "property" || !key || !propertyValue) throw new JsonSyntaxError("Malformed JSON property")
    const decoded = getNodeValue(key)
    if (typeof decoded !== "string") throw new JsonSyntaxError("Malformed JSON property name")
    return { key: decoded, value: propertyValue, node }
  })
}

function raw(value: ObjectValue, node: Node) {
  return value.source.slice(node.offset, node.offset + node.length)
}

function compose(properties: readonly string[]) {
  return `{${properties.join(",")}}`
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}
