import { Database } from "@opencode-ai/core/database/database"
import { EventV2, type Payload } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect, Exit, Option, Schema, Stream } from "effect"
import { MessageV2 } from "./message-v2"
import type { MessageID, PartID, SessionID } from "./schema"

const DELTA_CAP = 256

type State = {
  readonly source: readonly SessionV1.WithParts[]
  readonly sequence: number
  readonly disabled: boolean
}

type Range = {
  readonly after: number
  readonly target: number
}

type Dependencies = {
  readonly load?: (
    effect: Effect.Effect<SessionV1.WithParts[]>,
  ) => Effect.Effect<SessionV1.WithParts[]>
  readonly readRange?: (
    effect: Effect.Effect<Payload[]>,
    range: Range,
  ) => Effect.Effect<Payload[]>
  readonly invalidate?: () => void
}

type SourceIndex = {
  readonly messages: ReadonlyMap<MessageID, SessionV1.WithParts>
  readonly partOwners: ReadonlyMap<PartID, MessageID>
}

type Touched = {
  readonly messageIDs: readonly MessageID[]
  readonly partOwners: ReadonlyMap<PartID, MessageID>
}

const decodeMessageUpdated = Schema.decodeUnknownOption(SessionV1.Event.MessageUpdated.data)
const decodePartUpdated = Schema.decodeUnknownOption(SessionV1.Event.PartUpdated.data)
const decodeSessionUpdated = Schema.decodeUnknownOption(SessionV1.Event.Updated.data)

export const make = Effect.fn("PromptHistory.make")(function* (sessionID: SessionID, dependencies?: Dependencies) {
  const database = yield* Database.Service
  const events = yield* EventV2.Service
  const load = () =>
    dependencies?.load?.(
      MessageV2.filterCompactedEffect(sessionID).pipe(Effect.provideService(Database.Service, database)),
    ) ?? MessageV2.filterCompactedEffect(sessionID).pipe(Effect.provideService(Database.Service, database))
  const latestSequence = () => EventV2.latestSequence(database.db, sessionID)
  const readRange = Effect.fn("PromptHistory.readRange")((range: Range) => {
    const read = events
      .durable({ aggregateID: sessionID, after: range.after })
      .pipe(
        Stream.take(range.target - range.after),
        Stream.takeUntil((event) => (event.durable?.seq ?? -1) >= range.target),
        Stream.timeout("1 second"),
        Stream.runCollect,
      )
    return dependencies?.readRange?.(read, range) ?? read
  })
  const get = (messageID: MessageID) =>
    MessageV2.get({ sessionID, messageID }).pipe(Effect.provideService(Database.Service, database))
  let state: State | undefined

  const disable = Effect.fnUntraced(function* (current: State) {
    dependencies?.invalidate?.()
    const source = yield* load()
    state = { source, sequence: current.sequence, disabled: true }
    return clone(source)
  })

  const incremental = Effect.fnUntraced(function* (current: State, target: number) {
    const count = target - current.sequence
    if (count < 0 || count > DELTA_CAP) return yield* disable(current)
    if (count === 0) return clone(current.source)
    const range = yield* readRange({ after: current.sequence, target }).pipe(Effect.exit)
    if (Exit.isFailure(range) || !validRange(range.value, sessionID, { after: current.sequence, target })) {
      return yield* disable(current)
    }
    const touched = touchedMessages(range.value, sessionID, indexSource(current.source))
    if (!touched) return yield* disable(current)
    const hydrated = yield* Effect.forEach(touched.messageIDs, get).pipe(Effect.exit)
    if (Exit.isFailure(hydrated)) return yield* disable(current)
    const source = updateSource(current.source, hydrated.value, touched.partOwners)
    if (!source) return yield* disable(current)
    state = { source, sequence: target, disabled: false }
    return clone(source)
  })

  const refresh = Effect.fnUntraced(function* () {
    if (state?.disabled) {
      dependencies?.invalidate?.()
      const source = yield* load()
      state = { ...state, source }
      return clone(source)
    }
    if (state) return yield* incremental(state, yield* latestSequence())
    const sequence = yield* latestSequence()
    const source = yield* load()
    const initial = { source, sequence, disabled: false } satisfies State
    const target = yield* latestSequence()
    if (target !== sequence) return yield* incremental(initial, target)
    state = initial
    return clone(source)
  })

  return Object.freeze({ refresh })
})

function validRange(events: readonly Payload[], sessionID: SessionID, range: Range) {
  if (events.length !== range.target - range.after) return false
  return events.every(
    (event, index) =>
      event.durable?.aggregateID === sessionID &&
      event.durable.seq === range.after + index + 1 &&
      event.durable.seq <= range.target,
  )
}

function clone(source: readonly SessionV1.WithParts[]) {
  return structuredClone([...source])
}

function indexSource(source: readonly SessionV1.WithParts[]): SourceIndex {
  return {
    messages: new Map(source.map((item) => [item.info.id, item])),
    partOwners: new Map(source.flatMap((item) => item.parts.map((part) => [part.id, item.info.id] as const))),
  }
}

function orderingSensitive(item: SessionV1.WithParts) {
  return (
    (item.info.role === "assistant" && item.info.summary === true) ||
    item.parts.some((part) => part.type === "compaction")
  )
}

function touchedMessages(events: readonly Payload[], sessionID: SessionID, cached: SourceIndex): Touched | undefined {
  const touched = new Set<MessageID>()
  const partOwners = new Map<PartID, MessageID>()
  for (const event of events) {
    if (event.type === SessionV1.Event.MessageUpdated.type && event.durable?.version === 1) {
      const decoded = decodeMessageUpdated(event.data)
      if (Option.isNone(decoded) || decoded.value.sessionID !== sessionID) return undefined
      if (decoded.value.info.role === "assistant" && decoded.value.info.summary) return undefined
      const previous = cached.messages.get(decoded.value.info.id)
      if (previous && orderingSensitive(previous)) return undefined
      touched.add(decoded.value.info.id)
      continue
    }
    if (event.type === SessionV1.Event.PartUpdated.type && event.durable?.version === 1) {
      const decoded = decodePartUpdated(event.data)
      if (Option.isNone(decoded) || decoded.value.sessionID !== sessionID || decoded.value.part.type === "compaction") {
        return undefined
      }
      const part = decoded.value.part
      const previous = cached.messages.get(part.messageID)
      if (previous && orderingSensitive(previous)) return undefined
      const cachedOwner = cached.partOwners.get(part.id)
      const expectedOwner = partOwners.get(part.id)
      if ((cachedOwner && cachedOwner !== part.messageID) || (expectedOwner && expectedOwner !== part.messageID)) {
        return undefined
      }
      touched.add(part.messageID)
      partOwners.set(part.id, part.messageID)
      continue
    }
    if (event.type === SessionV1.Event.Updated.type && event.durable?.version === 1) {
      const decoded = decodeSessionUpdated(event.data)
      if (Option.isNone(decoded) || decoded.value.sessionID !== sessionID || decoded.value.info.revert) return undefined
      continue
    }
    return undefined
  }
  return { messageIDs: [...touched], partOwners }
}

function updateSource(
  source: readonly SessionV1.WithParts[],
  hydrated: readonly SessionV1.WithParts[],
  expectedPartOwners: ReadonlyMap<PartID, MessageID>,
) {
  if (hydrated.some(orderingSensitive)) return undefined
  const hydratedPartOwners = indexSource(hydrated).partOwners
  for (const [partID, messageID] of expectedPartOwners) {
    if (hydratedPartOwners.get(partID) !== messageID) return undefined
  }
  const existing = new Map(source.map((item) => [item.info.id, item]))
  const allIDs = new Set([...source, ...hydrated].map((item) => item.info.id))
  for (const item of hydrated) {
    const previous = existing.get(item.info.id)
    if (previous && previous.info.role !== item.info.role) return undefined
    if (item.info.role !== "assistant") continue
    if (!allIDs.has(item.info.parentID)) return undefined
    if (previous?.info.role === "assistant" && previous.info.parentID !== item.info.parentID) return undefined
  }
  const replacements = new Map(hydrated.map((item) => [item.info.id, item]))
  const replaced = source.map((item) => replacements.get(item.info.id) ?? item)
  const additions = hydrated.filter((item) => !existing.has(item.info.id))
  let tail = replaced.reduce<SessionV1.WithParts | undefined>(
    (latest, item) => (!latest || item.info.id > latest.info.id ? item : latest),
    undefined,
  )
  for (const item of additions) {
    if (tail && item.info.id <= tail.info.id) return undefined
    if (
      item.info.role === "assistant" &&
      !(
        (tail?.info.role === "user" && item.info.parentID === tail.info.id) ||
        (tail?.info.role === "assistant" && item.info.parentID === tail.info.parentID)
      )
    )
      return undefined
    tail = item
  }
  return [...replaced, ...additions]
}

export * as PromptHistory from "./prompt-history"
