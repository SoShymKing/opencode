import { Binary } from "@opencode-ai/core/util/binary"
import { retry } from "@opencode-ai/core/util/retry"
import type {
  Message,
  OpencodeClient,
  Part,
  PermissionRequest,
  QuestionRequest,
  Session,
  Todo,
} from "@opencode-ai/sdk/v2/client"
import type { FileDiffInfo } from "@opencode-ai/client/promise"
import { batch } from "solid-js"
import { isDeepEqual, isPlainObject } from "remeda"
import { createStore, produce, reconcile, unwrap } from "solid-js/store"
import { message as cleanMessage } from "@/utils/diffs"
import { sessionNotFoundError } from "@/utils/server-errors"
import { rootSession } from "@/utils/session-route"
import { normalizeSessionInfo } from "@/utils/session"
import { compareMessages, messageKey, normalizeSessionMessages, sessionMessagePartID, type NativeSessionMessage } from "@/utils/session-message"
import type { NativeServerEvent } from "./server-sdk"
import { dropSessionCaches, pickSessionCacheEvictions, SESSION_CACHE_LIMIT } from "./global-sync/session-cache"
import { normalizeTouchedSessionMessages } from "./server-session-v2-projection"
import { createV2SessionReducer, type V2SessionReduction } from "./server-session-v2-reducer"
import type { ServerApi } from "@/utils/server"
import type { SessionStatusEvent } from "@opencode-ai/schema/session-status-event"

type MessageApi = ServerApi["message"]
type SessionApi = Pick<ServerApi["session"], "get" | "message">

const cmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0)
const SKIP_PARTS = new Set(["patch", "step-start", "step-finish"])
const initialMessagePageSize = 20
const historyMessagePageSize = 200
const sessionInfoLimit = 2_048
const emptyIDs: ReadonlySet<string> = new Set()
type SnapshotDomain = "session_status" | "permission" | "question"
type SessionSnapshot = {
  readonly epoch: number
  readonly domain: SnapshotDomain
  readonly revisions: ReadonlyMap<string, number>
  readonly inferred?: ReadonlySet<string>
}

const equal = (left: unknown, right: unknown): boolean => isDeepEqual(left, right)

function needsOlderTurnRoot(source: readonly NativeSessionMessage[]) {
  const boundary = source.find(
    (message) =>
      message.type === "user" ||
      message.type === "shell" ||
      message.type === "assistant" ||
      (message.type === "synthetic" && message.text.trim()),
  )
  return boundary?.type === "assistant"
}

type OptimisticItem = {
  message: Message
  parts: Part[]
  confirmedParts?: Part[]
  confirmedMessage?: boolean
}

type MessagePage = {
  session: Message[]
  part: { id: string; part: Part[] }[]
  source?: NativeSessionMessage[]
  sourceMode?: "latest" | "older"
  projectSource?: boolean
  cursor?: string
  complete: boolean
}

function legacyMessageSource(items: { info: Message; parts: Part[] }[]): NativeSessionMessage[] {
  return items
    .slice()
    .sort((a, b) => compareMessages(a.info, b.info))
    .map((item) => {
      if (item.info.role === "user") {
        return {
          id: item.info.id,
          type: "user" as const,
          text: item.parts.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n"),
          time: item.info.time,
        }
      }
      return {
        id: item.info.id,
        type: "assistant" as const,
        agent: item.info.agent ?? item.info.mode,
        model: { id: item.info.modelID, providerID: item.info.providerID, variant: item.info.variant },
        content: [],
        time: item.info.time,
        streamEventCount: item.info.streamEventCount,
      }
    })
}

// Most markers describe the current HTTP attempt; deltaParts persists non-durable stream state across retries.
type MessageLoadState = {
  statusRevision: number
  touchedMessages: Set<string>
  removedMessages: Set<string>
  retainedMessages: Set<string>
  touchedParts: Map<string, Set<string>>
  deltaParts: Map<string, Set<string>>
  carriedDeltaParts: Map<string, Set<string>>
  removedParts: Map<string, Set<string>>
  optimisticParts: Map<string, Set<string>>
  orphanParents: Set<string>
  clearedMessageParts: Set<string>
  sourceChanges: Map<string, SourceChange>
}

type MessageLoadBaseline = Pick<
  MessageLoadState,
  "touchedMessages" | "retainedMessages" | "touchedParts" | "clearedMessageParts" | "sourceChanges"
>

type SourceFields = { [field: string]: true | SourceFields }
type SourceChange = { fields: SourceFields; content: Map<string, SourceFields>; source: NativeSessionMessage }
type NativeAssistant = Extract<NativeSessionMessage, { type: "assistant" }>
type DeltaSnapshot = { id: string; text?: string }

function changedSourceFields(before: object | undefined, after: object, previous: SourceFields = {}): SourceFields {
  const old = new Map<string, unknown>(Object.entries(before ?? {}))
  const next = new Map<string, unknown>(Object.entries(after))
  const fields = { ...previous }
  for (const key of new Set([...old.keys(), ...next.keys()])) {
    const value = next.get(key)
    const prior = old.get(key)
    if (equal(prior, value)) continue
    fields[key] = (prior === undefined || isPlainObject(prior)) && isPlainObject(value) && fields[key] !== true
      ? changedSourceFields(prior, value, fields[key]) : true
  }
  return fields
}

function mergeSourceFields<T extends object>(incoming: T, live: object, fields: SourceFields): T {
  const fetched = new Map<string, unknown>(Object.entries(incoming))
  const current = new Map<string, unknown>(Object.entries(live))
  return { ...incoming, ...Object.fromEntries(Object.entries(fields).map(([key, changed]) => {
    const value = current.get(key)
    const old = fetched.get(key)
    return [key, changed === true ? value : mergeSourceFields(isPlainObject(old) ? old : {}, isPlainObject(value) ? value : {}, changed)]
  })) }
}

function nativeContent(message: NativeAssistant) {
  return message.content.map((value) => ({ id: sessionMessagePartID(message.id, value.id), value }))
}

function mergeNativeMessage(incoming: NativeSessionMessage, live: NativeSessionMessage | undefined, changes?: SourceChange): NativeSessionMessage {
  if (!live || !changes) return incoming
  const selected = changes.source
  const result = mergeSourceFields(incoming, selected, changes.fields)
  if (incoming.type !== "assistant" || selected.type !== "assistant" || result.type !== "assistant") return result
  const current = new Map(nativeContent(selected).map((item) => [item.id, item.value]))
  const fetched = nativeContent(incoming)
  const ids = new Set(fetched.map((item) => item.id))
  return { ...result, content: [
    ...fetched.flatMap((item) => {
      const fields = changes.content.get(item.id)
      if (!fields) return [item.value]
      const value = current.get(item.id)
      return value ? [mergeSourceFields(item.value, value, fields)] : []
    }),
    ...nativeContent(selected).filter((item) => !ids.has(item.id) && changes.content.has(item.id)).map((item) => item.value),
  ] }
}

function mergeOptimisticPage(page: MessagePage, items: OptimisticItem[]) {
  if (items.length === 0) return { ...page, observed: [] as { messageID: string; parts: Part[] }[] }
  const session = [...page.session]
  const part = new Map(page.part.map((item) => [item.id, item.part]))
  const observed: { messageID: string; parts: Part[] }[] = []
  for (const item of items) {
    const result = Binary.search(session, messageKey(item.message), messageKey)
    const found = result.found
    if (!found) session.splice(result.index, 0, item.message)
    const current = part.get(item.message.id)
    const confirmed = found ? item.parts.filter((part) => current?.some((value) => value.id === part.id)) : []
    if (found) observed.push({ messageID: item.message.id, parts: confirmed })
    part.set(
      item.message.id,
      merge(
        found ? (current ?? []) : merge(item.confirmedParts ?? [], current ?? []),
        item.parts.filter((part) => !confirmed.includes(part)),
      ),
    )
  }
  return {
    ...page,
    session,
    part: [...part.entries()].sort((a, b) => cmp(a[0], b[0])).map(([id, parts]) => ({ id, part: parts })),
    observed,
  }
}

function runInflight(map: Map<string, Promise<void>>, key: string, task: () => Promise<void>) {
  const pending = map.get(key)
  if (pending) return pending
  const promise = task().finally(() => {
    if (map.get(key) === promise) map.delete(key)
  })
  map.set(key, promise)
  return promise
}

function merge<T extends { id: string }>(a: readonly T[], b: readonly T[]) {
  const items = new Map(a.map((item) => [item.id, item] as const))
  for (const item of b) items.set(item.id, item)
  return [...items.values()].sort((x, y) => cmp(x.id, y.id))
}

function reconcileFetched<T extends { id: string }>(
  fetched: T[],
  current: readonly T[],
  options: {
    touched?: ReadonlySet<string>
    retained?: ReadonlySet<string>
    removed?: ReadonlySet<string>
    preserveUnfetched?: boolean | ((item: T) => boolean)
    compare?: (a: T, b: T) => number
  } = {},
) {
  const result = new Map(fetched.map((item) => [item.id, item]))
  const live = new Map(current.map((item) => [item.id, item]))
  if (options.preserveUnfetched) {
    for (const item of current) {
      if (!result.has(item.id) && (options.preserveUnfetched === true || options.preserveUnfetched(item)))
        result.set(item.id, item)
    }
  }
  for (const id of options.retained ?? emptyIDs) {
    if (result.has(id)) continue
    const item = live.get(id)
    if (item) result.set(id, item)
  }
  // Events observed while the request is pending are the freshest client state for those identities.
  for (const id of options.touched ?? emptyIDs) {
    const item = live.get(id)
    if (item) result.set(id, item)
    if (!item) result.delete(id)
  }
  for (const id of options.removed ?? emptyIDs) result.delete(id)
  const items = [...result.values()]
  return options.compare ? items.sort(options.compare) : items
}

type ServerSessionOptions = { retry?: typeof retry; protocol?: Promise<"v1" | "v2"> }

export function createServerSession(
  client: OpencodeClient,
  sessionApiOrOptions?: SessionApi | ServerSessionOptions,
  messageApi?: MessageApi,
  currentOptions?: ServerSessionOptions,
) {
  const sessionApi = messageApi ? (sessionApiOrOptions as SessionApi) : undefined
  const options = messageApi ? currentOptions : (sessionApiOrOptions as ServerSessionOptions | undefined)
  const protocol: { kind: "v1" | "v2" } = { kind: sessionApi ? "v2" : "v1" }
  void options?.protocol?.then((kind) => { protocol.kind = kind })
  const [data, setStore] = createStore({
    info: {} as Record<string, Session | undefined>,
    session_status: {} as Record<string, typeof SessionStatusEvent.Info.Encoded>,
    pending_input: {} as Record<string, Record<string, boolean>>,
    session_diff: {} as Record<string, FileDiffInfo[]>,
    todo: {} as Record<string, Todo[]>,
    permission: {} as Record<string, PermissionRequest[]>,
    question: {} as Record<string, QuestionRequest[]>,
    message: {} as Record<string, Message[]>,
    session_message: {} as Record<string, NativeSessionMessage[]>,
    part: {} as Record<string, Part[]>,
    part_text_accum_delta: {} as Record<string, string>,
    session_working(id: string) {
      return (this.session_status[id]?.type ?? "idle") !== "idle"
    },
  })
  const snapshotState = { epoch: 0 }
  const observedExecution = new Set<string>()
  const authoritativeStatus = new Set<string>()
  const revisions = {
    session_status: new Map<string, number>(),
    permission: new Map<string, number>(),
    question: new Map<string, number>(),
  }
  const setData = new Proxy(setStore, {
    apply(target, receiver, args: unknown[]) {
      const domain = args[0]
      const selected = domain === "session_status" || domain === "permission" || domain === "question" ? domain : undefined
      const ids = selected
        ? typeof args[1] === "string" ? [args[1]] : [...new Set([...Object.keys(data[selected]), ...revisions[selected].keys()])]
        : []
      const result = Reflect.apply(target, receiver, args)
      if (selected) {
        if (typeof args[1] !== "string") ids.push(...Object.keys(data[selected]))
        new Set(ids).forEach((id) => {
          revisions[selected].set(id, (revisions[selected].get(id) ?? 0) + 1)
          if (selected !== "session_status") return
          observedExecution.delete(id)
          authoritativeStatus.delete(id)
        })
      }
      return result
    },
  })
  const writeStatus = (id: string, status: typeof SessionStatusEvent.Info.Encoded, inferred = false) => {
    revisions.session_status.set(id, (revisions.session_status.get(id) ?? 0) + 1)
    if (status.type !== "idle") observedExecution.add(id)
    if (status.type === "idle") observedExecution.delete(id)
    if (inferred && authoritativeStatus.has(id)) return
    if (!inferred) authoritativeStatus.add(id)
    setStore("session_status", id, reconcile(status))
  }
  const snapshotCurrent = (snapshot: SessionSnapshot, id: string) =>
    snapshot.epoch === snapshotState.epoch &&
    (snapshot.revisions.get(id) ?? 0) === (revisions[snapshot.domain].get(id) ?? 0)
  const snapshot = {
    connect: () => ++snapshotState.epoch,
    epoch: () => snapshotState.epoch,
    capture: (domain: SnapshotDomain): SessionSnapshot => ({
      epoch: snapshotState.epoch,
      domain,
      revisions: new Map(revisions[domain]),
    }),
    current: snapshotCurrent,
    status(
      statuses: Record<string, typeof SessionStatusEvent.Info.Encoded>,
      captured: SessionSnapshot,
      directory?: string,
    ) {
      setStore(
        "session_status",
        produce((draft) => {
          for (const id of new Set([...Object.keys(draft), ...Object.keys(statuses)])) {
            if (!snapshotCurrent(captured, id)) continue
            if (!statuses[id] && !observedExecution.has(id) &&
              (optimistic.get(id)?.size || admissions.get(id)?.size || Object.values(data.pending_input[id] ?? {}).some(Boolean))) continue
            if (directory && data.info[id]?.directory !== directory && !statuses[id]) continue
            if (statuses[id]) {
              draft[id] = statuses[id]
              if (!captured.inferred?.has(id)) authoritativeStatus.add(id)
              if (statuses[id].type !== "idle") observedExecution.add(id)
              if (statuses[id].type === "idle") observedExecution.delete(id)
            }
            if (!statuses[id]) {
              delete draft[id]
              observedExecution.delete(id)
              authoritativeStatus.delete(id)
            }
            revisions.session_status.set(id, (revisions.session_status.get(id) ?? 0) + 1)
          }
        }),
      )
    },
  }
  const requests = new Map<string, Promise<Session>>()
  const inflight = new Map<string, Promise<void>>()
  const messageRequests = new Map<string, Promise<void>>()
  const hydrations = new Map<string, Map<string, Promise<void>>>()
  const inflightTodo = new Map<string, Promise<void>>()
  const optimistic = new Map<string, Map<string, OptimisticItem>>()
  const admissions = new Map<string, Map<string, { prompt: Extract<NativeServerEvent, { type: "session.next.prompt.admitted" }>["data"]["prompt"]; delivery: "steer" | "queue" }>>()
  const v2 = createV2SessionReducer()
  const messageLoads = new Map<string, MessageLoadState>()
  const pendingParts = new Map<string, Map<string, Set<string>>>()
  const orphanParts = new Map<string, Set<string>>()
  const removedMessages = new Map<string, Set<string>>()
  const deltaBases = new Map<string, { base: string; sessionID: string }>()
  const deleteMessageParts = (
    cache: { part: Record<string, Part[] | undefined>; part_text_accum_delta: Record<string, string | undefined> },
    messageID: string,
  ) => {
    for (const part of cache.part[messageID] ?? []) {
      delete cache.part_text_accum_delta[part.id]
      deltaBases.delete(part.id)
    }
    delete cache.part[messageID]
  }
  const seen = new Set<string>()
  const infoSeen = new Set<string>()
  const pinned = new Map<string, number>()
  const generations = new Map<string, object>()
  const generation = (sessionID: string) => {
    const current = generations.get(sessionID)
    if (current) return current
    const created = {}
    generations.set(sessionID, created)
    return created
  }
  const [meta, setMeta] = createStore({
    limit: {} as Record<string, number | undefined>,
    cursor: {} as Record<string, string | undefined>,
    complete: {} as Record<string, boolean | undefined>,
    loading: {} as Record<string, boolean | undefined>,
    at: {} as Record<string, number | undefined>,
  })

  const indexLegacyMessage = (message: Message) => {
    const current = data.session_message[message.sessionID] ?? []
    if (current.some((item) => item.id === message.id)) return
    setData(
      "session_message",
      message.sessionID,
      reconcile([...current, ...legacyMessageSource([{ info: message, parts: [] }])]),
    )
  }

  const remember = (session: Session) => {
    setData("info", session.id, reconcile(session))
    infoSeen.delete(session.id)
    infoSeen.add(session.id)
    if (infoSeen.size > sessionInfoLimit) {
      const preserve = new Set([
        ...pinned.keys(),
        ...requests.keys(),
        ...inflight.keys(),
        ...inflightTodo.keys(),
        ...messageLoads.keys(),
        ...optimistic.keys(),
        ...Object.keys(data.pending_input).filter((id) => Object.keys(data.pending_input[id]).length > 0),
        ...Object.entries(data.permission)
          .filter(([, items]) => items.length > 0)
          .map(([sessionID]) => sessionID),
        ...Object.entries(data.question)
          .filter(([, items]) => items.length > 0)
          .map(([sessionID]) => sessionID),
        ...Object.entries(data.session_status)
          .filter(([, status]) => status.type !== "idle")
          .map(([sessionID]) => sessionID),
      ])
      for (const sessionID of preserve) {
        let current = data.info[sessionID]
        while (current) {
          preserve.add(current.id)
          current = current.parentID ? data.info[current.parentID] : undefined
        }
      }
      const stale: string[] = []
      for (const sessionID of infoSeen) {
        if (infoSeen.size - stale.length <= sessionInfoLimit) break
        if (!preserve.has(sessionID)) stale.push(sessionID)
      }
      stale.forEach((sessionID) => infoSeen.delete(sessionID))
      stale.forEach((sessionID) => generations.delete(sessionID))
      setData(
        "info",
        produce((draft) => stale.forEach((sessionID) => delete draft[sessionID])),
      )
    }
    return session
  }

  const resolve = async (sessionID: string, options?: { force?: boolean }): Promise<Session> => {
    if (options?.force && requests.has(sessionID)) await Promise.allSettled([requests.get(sessionID)])
    const cached = data.info[sessionID]
    if (cached && !options?.force) return Promise.resolve(cached)
    const pending = requests.get(sessionID)
    if (pending) return pending
    const active = generation(sessionID)
    const request = sessionApi
      ? sessionApi.get({ sessionID }).then(normalizeSessionInfo)
      : client.session.get({ sessionID }).then((result) => {
          if (!result.data) throw sessionNotFoundError(sessionID)
          return result.data
        })
    const resolved = request.then((result) => {
      if (generations.get(sessionID) !== active) return result
      return remember(result)
    })
    requests.set(sessionID, resolved)
    const cleanup = () => {
      if (requests.get(sessionID) === resolved) requests.delete(sessionID)
      if (
        generations.get(sessionID) === active &&
        !data.info[sessionID] &&
        !requests.has(sessionID) &&
        !messageLoads.has(sessionID) &&
        !hydrations.get(sessionID)?.size &&
        !inflight.has(sessionID) &&
        !inflightTodo.has(sessionID)
      )
        generations.delete(sessionID)
    }
    void resolved.then(cleanup, cleanup)
    return resolved
  }

  const peekLineage = (sessionID: string) => {
    const session = data.info[sessionID]
    if (!session) return
    const seen = new Set([session.id])
    let root = session
    while (root.parentID) {
      if (seen.has(root.parentID)) throw new Error(`Session parent cycle: ${root.parentID}`)
      seen.add(root.parentID)
      const parent = data.info[root.parentID]
      if (!parent) return
      root = parent
    }
    return { session, root }
  }

  const clearOptimistic = (sessionID: string, messageID?: string) => {
    if (!messageID) {
      optimistic.delete(sessionID)
      return
    }
    const items = optimistic.get(sessionID)
    if (!items) return
    items.delete(messageID)
    if (items.size === 0) optimistic.delete(sessionID)
  }

  const confirmNativeUser = (sessionID: string, messageID: string) => {
    const admitted = admissions.get(sessionID)?.delete(messageID)
    const item = optimistic.get(sessionID)?.get(messageID)
    clearOptimistic(sessionID, messageID)
    setData("pending_input", sessionID, produce((draft = {}) => { delete draft[messageID] }))
    const load = messageLoads.get(sessionID)
    if ((admitted || item) && (!load || load.statusRevision === (revisions.session_status.get(sessionID) ?? 0)))
      observedExecution.add(sessionID)
    load?.optimisticParts.delete(messageID)
    load?.clearedMessageParts.delete(messageID)
    if (!item) return
    load?.touchedMessages.delete(messageID)
    for (const part of item.parts) {
      load?.touchedParts.get(messageID)?.delete(part.id)
      load?.deltaParts.get(messageID)?.delete(part.id)
      load?.carriedDeltaParts.get(messageID)?.delete(part.id)
      deltaBases.delete(part.id)
      setData("part_text_accum_delta", produce((draft) => { delete draft[part.id] }))
    }
    setData("part", messageID, (parts = []) => parts.filter((part) => !item.parts.some((raw) => raw.id === part.id)))
  }

  const clearOptimisticPart = (sessionID: string, messageID: string, partID: string) => {
    const items = optimistic.get(sessionID)
    const item = items?.get(messageID)
    if (!items || !item) return
    const parts = item.parts.filter((part) => part.id !== partID)
    const confirmedParts = item.confirmedParts?.filter((part) => part.id !== partID)
    if (parts.length === 0) {
      clearOptimistic(sessionID, messageID)
      return
    }
    items.set(messageID, { ...item, parts, confirmedParts, confirmedMessage: true })
  }

  const confirmOptimisticPart = (sessionID: string, messageID: string, part: Part) => {
    const items = optimistic.get(sessionID)
    const item = items?.get(messageID)
    if (!items || !item) return
    const parts = item.parts.filter((value) => value.id !== part.id)
    if (parts.length === 0) {
      clearOptimistic(sessionID, messageID)
      return
    }
    items.set(messageID, {
      ...item,
      parts,
      confirmedParts: merge(item.confirmedParts ?? [], [part]),
      confirmedMessage: true,
    })
  }

  const confirmOptimistic = (sessionID: string, messageID: string, confirmedParts: Part[]) => {
    const items = optimistic.get(sessionID)
    const item = items?.get(messageID)
    if (!items || !item) return
    const confirmed = new Set(confirmedParts.map((part) => part.id))
    const parts = item.parts.filter((part) => !confirmed.has(part.id))
    if (parts.length === 0) {
      clearOptimistic(sessionID, messageID)
      return
    }
    items.set(messageID, {
      ...item,
      parts,
      confirmedParts: merge(item.confirmedParts ?? [], confirmedParts),
      confirmedMessage: true,
    })
  }

  const trackPartChange = (sessionID: string, messageID: string, partID: string) => {
    const load = messageLoads.get(sessionID)
    if (!load) return
    // A part event keeps an existing parent when the fetched page omits it without overriding fetched metadata.
    const messages = data.message[sessionID]
    if (messages?.some((message) => message.id === messageID)) load.retainedMessages.add(messageID)
    const parts = load.touchedParts.get(messageID)
    if (parts) {
      parts.add(partID)
      return
    }
    load.touchedParts.set(messageID, new Set([partID]))
  }

  const resetMessageLoad = (sessionID: string, load: MessageLoadState, baseline?: MessageLoadBaseline) => {
    load.touchedMessages.clear()
    load.retainedMessages.clear()
    load.touchedParts.clear()
    load.carriedDeltaParts.clear()
    load.clearedMessageParts.clear()
    load.sourceChanges.clear()
    for (const messageID of load.removedMessages) {
      load.touchedMessages.add(messageID)
      load.clearedMessageParts.add(messageID)
    }
    for (const [messageID, parts] of load.deltaParts) {
      load.touchedParts.set(messageID, new Set(parts))
      load.carriedDeltaParts.set(messageID, new Set(parts))
      const messages = data.message[sessionID]
      if (messages?.some((message) => message.id === messageID)) load.retainedMessages.add(messageID)
    }
    for (const [messageID, parts] of load.removedParts) {
      const touched = load.touchedParts.get(messageID) ?? new Set<string>()
      parts.forEach((partID) => touched.add(partID))
      load.touchedParts.set(messageID, touched)
      const messages = data.message[sessionID]
      if (messages?.some((message) => message.id === messageID)) load.retainedMessages.add(messageID)
    }
    for (const [messageID, parts] of load.optimisticParts) {
      load.removedMessages.delete(messageID)
      load.clearedMessageParts.add(messageID)
      load.touchedMessages.add(messageID)
      const touched = load.touchedParts.get(messageID) ?? new Set<string>()
      parts.forEach((partID) => touched.add(partID))
      load.touchedParts.set(messageID, touched)
    }
    baseline?.touchedMessages.forEach((messageID) => load.touchedMessages.add(messageID))
    baseline?.retainedMessages.forEach((messageID) => load.retainedMessages.add(messageID))
    baseline?.clearedMessageParts.forEach((messageID) => load.clearedMessageParts.add(messageID))
    baseline?.touchedParts.forEach((parts, messageID) => {
      const touched = load.touchedParts.get(messageID) ?? new Set<string>()
      parts.forEach((partID) => touched.add(partID))
      load.touchedParts.set(messageID, touched)
    })
    baseline?.sourceChanges.forEach((change, messageID) => load.sourceChanges.set(messageID, change))
  }

  const messageLoadBaseline = (load: MessageLoadState, exclude: string): MessageLoadBaseline => ({
    touchedMessages: new Set([...load.touchedMessages].filter((messageID) => messageID !== exclude)),
    retainedMessages: new Set([...load.retainedMessages].filter((messageID) => messageID !== exclude)),
    touchedParts: new Map(
      [...load.touchedParts]
        .filter(([messageID]) => messageID !== exclude)
        .map(([messageID, parts]) => [messageID, new Set(parts)]),
    ),
    clearedMessageParts: new Set([...load.clearedMessageParts].filter((messageID) => messageID !== exclude)),
    sourceChanges: new Map([...load.sourceChanges].filter(([messageID]) => messageID !== exclude)),
  })

  const evict = (sessionIDs: string[]) => {
    if (sessionIDs.length === 0) return
    const evicted = new Set(sessionIDs)
    for (const [partID, item] of deltaBases) {
      if (evicted.has(item.sessionID)) deltaBases.delete(partID)
    }
    sessionIDs.forEach((sessionID) => {
      generations.delete(sessionID)
      clearOptimistic(sessionID)
      requests.delete(sessionID)
      inflight.delete(sessionID)
      messageRequests.delete(sessionID)
      hydrations.delete(sessionID)
      inflightTodo.delete(sessionID)
      messageLoads.delete(sessionID)
      pendingParts.delete(sessionID)
      orphanParts.delete(sessionID)
      removedMessages.delete(sessionID)
      admissions.delete(sessionID)
      observedExecution.delete(sessionID)
      authoritativeStatus.delete(sessionID)
      Object.values(revisions).forEach((domain) => domain.set(sessionID, (domain.get(sessionID) ?? 0) + 1))
    })
    setData(
      produce((draft) => {
        dropSessionCaches(draft, sessionIDs)
        sessionIDs.forEach((id) => delete draft.pending_input[id])
      }),
    )
    setMeta(
      produce((draft) => {
        for (const sessionID of sessionIDs) {
          delete draft.limit[sessionID]
          delete draft.cursor[sessionID]
          delete draft.complete[sessionID]
          delete draft.loading[sessionID]
          delete draft.at[sessionID]
        }
      }),
    )
  }

  const protectedSessions = () =>
    new Set([
      ...pinned.keys(),
      ...requests.keys(),
      ...inflight.keys(),
      ...inflightTodo.keys(),
      ...messageLoads.keys(),
      ...optimistic.keys(),
      ...Object.keys(data.pending_input).filter((id) => Object.keys(data.pending_input[id]).length > 0),
      ...Object.entries(data.permission)
        .filter(([, items]) => items.length > 0)
        .map(([sessionID]) => sessionID),
      ...Object.entries(data.question)
        .filter(([, items]) => items.length > 0)
        .map(([sessionID]) => sessionID),
      ...Object.entries(data.session_status)
        .filter(([, status]) => status.type !== "idle")
        .map(([sessionID]) => sessionID),
    ])

  const touch = (sessionID: string) =>
    evict(
      pickSessionCacheEvictions({ seen, keep: sessionID, limit: SESSION_CACHE_LIMIT, preserve: protectedSessions() }),
    )

  const fetchMessages = async (sessionID: string, limit: number, before?: string, onAttempt?: () => void) => {
    if (messageApi && (await options?.protocol) !== "v1") {
      const request = (cursor?: string, baseline?: MessageLoadBaseline) =>
        (options?.retry ?? retry)(() => {
          onAttempt?.()
          const load = messageLoads.get(sessionID)
          if (load && baseline) resetMessageLoad(sessionID, load, baseline)
           const capped = Math.max(1, Math.min(historyMessagePageSize, limit))
           return messageApi.list(cursor ? { sessionID, limit: capped, cursor } : { sessionID, limit: capped, order: "desc" })
        })
      const first = await request(before)
      const pages = [first]
      while (pages.at(-1)?.cursor.next && needsOlderTurnRoot(pages.flatMap((page) => page.data).toReversed())) {
        const baseline = messageLoads.get(sessionID)
        const preserved = baseline ? messageLoadBaseline(baseline, "") : undefined
        const response = await request(pages.at(-1)!.cursor.next ?? undefined, preserved)
        pages.push(response)
        if (!response.data.length) break
      }
      const response = pages.at(-1)!
      const source = pages.flatMap((page) => page.data).toReversed()
      const normalized = normalizeSessionMessages(sessionID, source)
      return {
        session: normalized.messages.sort(compareMessages),
        part: [...normalized.parts.entries()]
          .map(([id, part]) => ({ id, part: part.sort((a, b) => cmp(a.id, b.id)) }))
          .sort((a, b) => cmp(a.id, b.id)),
        source,
        sourceMode: before ? ("older" as const) : ("latest" as const),
        projectSource: true,
        cursor: response.cursor.next ?? undefined,
        complete: response.data.length === 0,
      }
    }
    const response = await (options?.retry ?? retry)(() => {
      onAttempt?.()
      return client.session.messages({ sessionID, limit, before })
    })
    const items = (response.data ?? []).filter((item) => !!item?.info?.id)
    return {
      session: items.map((item) => cleanMessage(item.info)).sort(compareMessages),
      part: items.map((item) => ({
        id: item.info.id,
        part: item.parts.filter((part) => !!part?.id).sort((a, b) => cmp(a.id, b.id)),
      })),
      source: legacyMessageSource(items),
      sourceMode: before ? ("older" as const) : ("latest" as const),
      cursor: response.response.headers.get("x-next-cursor") ?? undefined,
      complete: !response.response.headers.get("x-next-cursor"),
    }
  }

  const fetchMessage = async (sessionID: string, messageID: string, onAttempt?: () => void) => {
    if (sessionApi && (await options?.protocol) !== "v1") {
      const response = await (options?.retry ?? retry)(() => {
        onAttempt?.()
        return sessionApi.message({ sessionID, messageID })
      })
      const normalized = normalizeSessionMessages(sessionID, [response])
      const message = normalized.messages[0]
      if (!message) throw new Error(`Message not found: ${messageID}`)
      return { message, parts: normalized.parts.get(messageID) ?? [] }
    }
    const response = await (options?.retry ?? retry)(() => {
      onAttempt?.()
      return client.session.message({ sessionID, messageID })
    })
    if (!response.data?.info?.id) throw new Error(`Message not found: ${messageID}`)
    return {
      message: cleanMessage(response.data.info),
      parts: response.data.parts.filter((part) => !!part?.id).sort((a, b) => cmp(a.id, b.id)),
    }
  }

  const replaceMessages = (sessionID: string, messages: Message[]) => {
    const messageIDs = new Set(messages.map((message) => message.id))
    const dropped = (data.message[sessionID] ?? []).filter((message) => !messageIDs.has(message.id))
    setData("message", sessionID, reconcile(messages, { key: "id" }))
    setData(
      produce((draft) => {
        for (const message of dropped) deleteMessageParts(draft, message.id)
      }),
    )
    return messageIDs
  }

  const replaceParts = (
    sessionID: string,
    items: MessagePage["part"],
    messageIDs: Set<string>,
    load?: MessageLoadState,
    original?: Map<string, readonly DeltaSnapshot[]>,
  ) => {
    for (const item of items) {
      if (!messageIDs.has(item.id)) continue
      const fetched = load?.clearedMessageParts.has(item.id)
        ? []
        : item.part.filter((part) => !SKIP_PARTS.has(part.type))
      const deltaSnapshot = original?.get(item.id) ?? fetched
      const fetchedIDs = new Set(deltaSnapshot.map((part) => part.id))
      const pending = pendingParts.get(sessionID)?.get(item.id)
      const touched = new Set([...(load?.touchedParts.get(item.id) ?? []), ...(pending ?? [])])
      for (const part of deltaSnapshot) {
        const accumulated = data.part_text_accum_delta[part.id]
        const base = deltaBases.get(part.id)?.base
        const preserveDelta =
          base !== undefined &&
          accumulated !== undefined &&
          "text" in part &&
          typeof part.text === "string" &&
          part.text.startsWith(base) &&
          accumulated.startsWith(part.text) &&
          accumulated !== part.text
        if (preserveDelta) touched.add(part.id)
        if (deltaBases.has(part.id) && !preserveDelta && !(load?.touchedParts.get(item.id)?.has(part.id) && !load?.carriedDeltaParts.get(item.id)?.has(part.id))) touched.delete(part.id)
      }
      for (const partID of new Set([...(load?.carriedDeltaParts.get(item.id) ?? []), ...(data.part[item.id] ?? []).filter((part) => deltaBases.has(part.id)).map((part) => part.id)])) {
        if (!fetchedIDs.has(partID)) touched.delete(partID)
      }
      const parts = reconcileFetched(fetched, data.part[item.id] ?? [], { touched })
      if (!parts.length) {
        orphanParts.get(sessionID)?.delete(item.id)
        setData(produce((draft) => deleteMessageParts(draft, item.id)))
        continue
      }
      const partIDs = new Set(parts.map((part) => part.id))
      setData(
        "part_text_accum_delta",
        produce((draft) => {
          for (const part of data.part[item.id] ?? []) {
            if (!partIDs.has(part.id) || !touched.has(part.id)) {
              delete draft[part.id]
              deltaBases.delete(part.id)
            }
          }
        }),
      )
      setData("part", item.id, reconcile(parts, { key: "id" }))
      orphanParts.get(sessionID)?.delete(item.id)
    }
  }

  const applyMessagePage = (
    sessionID: string,
    page: MessagePage,
    load: MessageLoadState | undefined,
    preserveUnfetched: boolean | ((message: Message) => boolean),
    cleanupOrphans: boolean,
  ) => {
    if (page.projectSource) {
      for (const message of page.source ?? []) {
        if (message.type !== "user" || load?.removedMessages.has(message.id) || removedMessages.get(sessionID)?.has(message.id)) continue
        confirmNativeUser(sessionID, message.id)
      }
    }
    const source = page.source
      ? (() => {
          const incoming = new Map(page.source.map((message) => [message.id, message]))
          const existing = data.session_message[sessionID] ?? []
          const current = existing.filter((message) => !incoming.has(message.id))
          const live = new Map(existing.map((message) => [message.id, message]))
          return (page.sourceMode === "older" ? [...page.source, ...current] : [...current, ...page.source])
            .filter((message) => !page.projectSource || (!load?.removedMessages.has(message.id) && !removedMessages.get(sessionID)?.has(message.id)))
            .map((message) => mergeNativeMessage(message, live.get(message.id), load?.sourceChanges.get(message.id)))
            .sort(compareMessages)
        })()
      : undefined
    const projected =
      page.projectSource && source
        ? (() => {
            const normalized = normalizeSessionMessages(sessionID, source)
            return {
              ...page,
              session: normalized.messages.sort(compareMessages),
              part: [...normalized.parts.entries()]
                .map(([id, part]) => ({ id, part: part.sort((a, b) => cmp(a.id, b.id)) }))
                .sort((a, b) => cmp(a.id, b.id)),
            }
          })()
        : page
    const merged = mergeOptimisticPage(projected, [...(optimistic.get(sessionID)?.values() ?? [])])
    merged.observed.forEach((item) => {
      if (!load?.clearedMessageParts.has(item.messageID)) confirmOptimistic(sessionID, item.messageID, item.parts)
    })
    const touchedMessages = new Set([...(load?.touchedMessages ?? []), ...(removedMessages.get(sessionID) ?? [])])
    const messages = reconcileFetched(merged.session, data.message[sessionID] ?? [], {
      touched: touchedMessages,
      retained: load?.retainedMessages,
      removed: load?.removedMessages,
      preserveUnfetched: (message) => data.pending_input[sessionID]?.[message.id] === true ||
        (typeof preserveUnfetched === "function" ? preserveUnfetched(message) : preserveUnfetched),
      compare: compareMessages,
    })
    const fetchedIDs = new Set(page.session.map((message) => message.id))
    const cachedIDs = new Set((data.message[sessionID] ?? []).map((message) => message.id))
    const retainPagination =
      cleanupOrphans &&
      !page.complete &&
      meta.limit[sessionID] !== undefined &&
      messages.some(
        (message) =>
          cachedIDs.has(message.id) &&
          !fetchedIDs.has(message.id) &&
          (typeof preserveUnfetched === "function" ? preserveUnfetched(message) : preserveUnfetched),
      )
    batch(() => {
      const messageIDs = replaceMessages(sessionID, messages)
      replaceParts(sessionID, merged.part, messageIDs, load, page.projectSource ? new Map<string, readonly DeltaSnapshot[]>([
        ...page.part.map((item) => [item.id, item.part] as const),
        ...(page.source ?? []).flatMap((message) => message.type === "assistant" ? [[message.id, nativeContent(message).map((item) => ({ id: item.id, text: item.value.type === "tool" ? undefined : item.value.text }))] as const] : []),
      ]) : undefined)
      if (source) setData("session_message", sessionID, reconcile(page.projectSource ? source.map((message) => {
        if (message.type === "user") {
          const text = data.part[message.id]?.find((part) => part.id === sessionMessagePartID(message.id, "text:0"))
          return text?.type === "text" ? { ...message, text: text.text } : message
        }
        if (message.type !== "assistant") return message
        const parts = new Map((data.part[message.id] ?? []).map((part) => [part.id, part]))
        return { ...message, content: nativeContent(message).flatMap((item) => {
          const part = parts.get(item.id)
          if (!part) return item.value.type !== "tool" && !item.value.text.trim() ? [item.value] : []
          if ((item.value.type === "text" && part.type === "text") || (item.value.type === "reasoning" && part.type === "reasoning")) return [{ ...item.value, text: part.text }]
          return [item.value]
        }) }
      }) : source))
      const orphans = orphanParts.get(sessionID)
      if (cleanupOrphans && page.complete && orphans) {
        for (const messageID of orphans) {
          if (!messageIDs.has(messageID)) setData(produce((draft) => deleteMessageParts(draft, messageID)))
        }
        orphanParts.delete(sessionID)
      }
      setMeta("limit", sessionID, messages.length)
      if (!retainPagination) {
        setMeta("cursor", sessionID, merged.cursor)
        setMeta("complete", sessionID, merged.complete)
      }
      setMeta("at", sessionID, Date.now())
    })
  }

  const loadMessagePage = async (sessionID: string, limit: number, before?: string, mode?: "replace" | "prepend") => {
    if (meta.loading[sessionID]) return
    const active = generation(sessionID)
    const load: MessageLoadState = {
      statusRevision: revisions.session_status.get(sessionID) ?? 0,
      touchedMessages: new Set(),
      removedMessages: new Set(),
      retainedMessages: new Set(),
      touchedParts: new Map(),
      deltaParts: new Map(),
      carriedDeltaParts: new Map(),
      removedParts: new Map(),
      optimisticParts: new Map(),
      orphanParents: new Set(),
      clearedMessageParts: new Set(),
      sourceChanges: new Map(),
    }
    messageLoads.set(sessionID, load)
    setMeta("loading", sessionID, true)
    let applied = false
    try {
      const page = await fetchMessages(sessionID, limit, before, () => resetMessageLoad(sessionID, load))
      const first = page.session.reduce<Message | undefined>(
        (oldest, message) => (!oldest || compareMessages(message, oldest) < 0 ? message : oldest),
        undefined,
      )
      if (generations.get(sessionID) !== active) return

      const parents = [] as Awaited<ReturnType<typeof fetchMessage>>[]
      if (mode !== "prepend") {
        const users = new Set([
          ...page.session.filter((message) => message.role === "user").map((message) => message.id),
          ...(data.message[sessionID] ?? [])
            .filter((message) => {
              if (message.role !== "user") return false
              const item = optimistic.get(sessionID)?.get(message.id)
              return load.touchedMessages.has(message.id) && (!item || item.confirmedMessage === true)
            })
            .map((message) => message.id),
        ])
        const parentIDs = [
          ...new Set(
            page.session.flatMap((message) =>
              message.role === "assistant" && !users.has(message.parentID) ? [message.parentID] : [],
            ),
          ),
        ]
        for (const parentID of parentIDs) {
          if (generations.get(sessionID) !== active) break
          const parent = await fetchMessage(sessionID, parentID, () =>
            resetMessageLoad(sessionID, load, messageLoadBaseline(load, parentID)),
          ).catch((error) => {
            const cause = error instanceof Error && typeof error.cause === "object" ? error.cause : undefined
            if (cause && "status" in cause && cause.status === 404) {
              load.removedMessages.add(parentID)
              return
            }
            throw error
          })
          if (!parent) continue
          if (parent.message.role !== "user") throw new Error(`Assistant parent is not a user message: ${parentID}`)
          parents.push(parent)
        }
      }
      if (generations.get(sessionID) !== active) return
      const result =
        mode === "prepend"
          ? page
          : {
              ...page,
              session: merge(
                page.session,
                parents.map((parent) => parent.message),
              ).sort(compareMessages),
              part: merge(
                page.part,
                parents.map((parent) => ({ id: parent.message.id, part: parent.parts })),
              ),
            }
      const preserveUnfetched =
        mode === "prepend" ||
        (!result.complete && (!first || ((message: Message) => compareMessages(message, first) < 0)))
      applyMessagePage(
        sessionID,
        result,
        messageLoads.get(sessionID) === load ? load : undefined,
        preserveUnfetched,
        mode !== "prepend",
      )
      applied = true
    } finally {
      if (!applied && generations.get(sessionID) === active && messageLoads.get(sessionID) === load) {
        for (const messageID of load.orphanParents) {
          if (!orphanParts.get(sessionID)?.has(messageID)) continue
          setData(produce((draft) => deleteMessageParts(draft, messageID)))
          orphanParts.get(sessionID)?.delete(messageID)
        }
        if (orphanParts.get(sessionID)?.size === 0) orphanParts.delete(sessionID)
      }
      if (messageLoads.get(sessionID) === load) messageLoads.delete(sessionID)
      if (generations.get(sessionID) === active) setMeta("loading", sessionID, false)
    }
  }

  const loadMessages = (sessionID: string, limit: number, before?: string, mode?: "replace" | "prepend") =>
    runInflight(messageRequests, sessionID, () => loadMessagePage(sessionID, limit, before, mode))

  const sync = async (sessionID: string, options?: { force?: boolean; messageLimit?: number }) => {
    touch(sessionID)
    if (options?.force && (inflight.has(sessionID) || messageRequests.has(sessionID) || requests.has(sessionID) || hydrations.get(sessionID)?.size)) {
      await Promise.allSettled([inflight.get(sessionID), messageRequests.get(sessionID), requests.get(sessionID), ...(hydrations.get(sessionID)?.values() ?? [])])
    }
    return runInflight(inflight, sessionID, async () => {
      const cached = data.message[sessionID] !== undefined && meta.limit[sessionID] !== undefined
      if (cached && data.info[sessionID] && !options?.force) return
      await Promise.all([
        resolve(sessionID, options),
        cached && !options?.force
          ? Promise.resolve()
          : loadMessages(sessionID, options?.messageLimit ?? meta.limit[sessionID] ?? initialMessagePageSize),
      ])
    })
  }

  const prefetch = async (sessionID: string, limit: number) => {
    touch(sessionID)
    await inflight.get(sessionID)
    if (
      Date.now() - (meta.at[sessionID] ?? 0) <= 15_000 &&
      (meta.complete[sessionID] || (data.message[sessionID]?.length ?? 0) >= limit)
    )
      return
    await runInflight(inflight, sessionID, () => loadMessages(sessionID, limit))
  }

  const eventSessionID = (event: { type: string; properties?: unknown }) => {
    const properties = event.properties
    if (!properties || typeof properties !== "object") return
    if ("sessionID" in properties && typeof properties.sessionID === "string") return properties.sessionID
    if (
      "info" in properties &&
      properties.info &&
      typeof properties.info === "object" &&
      "sessionID" in properties.info &&
      typeof properties.info.sessionID === "string"
    )
      return properties.info.sessionID
    if (
      "part" in properties &&
      properties.part &&
      typeof properties.part === "object" &&
      "sessionID" in properties.part &&
      typeof properties.part.sessionID === "string"
    )
      return properties.part.sessionID
  }

  const projectV2 = (reduction: V2SessionReduction, delta = false) => {
    const previous = data.session_message[reduction.sessionID] ?? []
    const source = reduction.messages.toSorted(compareMessages)
    const oldSource = new Map(previous.map((message) => [message.id, message]))
    const load = messageLoads.get(reduction.sessionID)
    for (const message of source) {
      if (!load || !reduction.touched.includes(message.id)) continue
      const old = oldSource.get(message.id)
      const changes = load.sourceChanges.get(message.id)
      const content = new Map(changes?.content)
      if (message.type === "assistant") {
        const before = new Map(old?.type === "assistant" ? nativeContent(old).map((item) => [item.id, item.value]) : [])
        const after = new Map(nativeContent(message).map((item) => [item.id, item.value]))
        for (const id of new Set([...before.keys(), ...after.keys()])) {
          if (equal(before.get(id), after.get(id))) continue
          content.set(id, changedSourceFields(before.get(id), after.get(id) ?? {}, content.get(id)))
        }
      }
      load.sourceChanges.set(message.id, {
        source: structuredClone(unwrap(message)),
        fields: changedSourceFields(old && Object.fromEntries(Object.entries(old).filter(([key]) => key !== "content")), Object.fromEntries(Object.entries(message).filter(([key]) => key !== "content")), changes?.fields),
        content,
      })
    }
    const before = normalizeTouchedSessionMessages(reduction.sessionID, previous, reduction.touched)
    const after = normalizeTouchedSessionMessages(reduction.sessionID, source, reduction.touched)
    const expanded = [...new Set([...before.touched, ...after.touched])]
    const normalized = expanded.every((id) => after.touched.has(id))
      ? after
      : normalizeTouchedSessionMessages(reduction.sessionID, source, expanded)
    const oldMessages = new Map(before.messages.map((message) => [message.id, message]))
    batch(() => {
      for (const message of normalized.messages) {
        if (equal(oldMessages.get(message.id), message)) continue
        apply({ type: "message.updated", properties: { sessionID: reduction.sessionID, info: message } }, true)
      }
      for (const messageID of expanded) {
        const next = normalized.parts.get(messageID) ?? []
        const oldParts = new Map((before.parts.get(messageID) ?? []).map((part) => [part.id, part]))
        const oldMessage = oldSource.get(messageID)
        const oldContent = new Map(oldMessage?.type === "assistant" ? nativeContent(oldMessage).map((item) => [item.id, item.value]) : [])
        const nextIDs = new Set(next.map((part) => part.id))
        for (const part of next) {
          const old = oldParts.get(part.id)
          if (equal(old, part)) continue
          const content = old ?? oldContent.get(part.id)
          if (delta && (content?.type === "text" || content?.type === "reasoning") && (part.type === "text" || part.type === "reasoning")) {
            if (!data.part[messageID]?.some((value) => value.id === part.id)) apply({ type: "message.part.updated", properties: { part: { ...part, text: "" } } }, true)
            apply({ type: "message.part.delta", properties: { sessionID: reduction.sessionID, messageID, partID: part.id, field: "text", delta: part.text.slice(content.text.length) } })
            continue
          }
          apply({ type: "message.part.updated", properties: { sessionID: reduction.sessionID, part } }, true)
        }
        for (const partID of oldParts.keys()) {
          if (nextIDs.has(partID)) continue
          apply({
            type: "message.part.removed",
            properties: { sessionID: reduction.sessionID, messageID, partID },
          }, true)
        }
      }
      setData("session_message", reduction.sessionID, reconcile(source))
    })
  }

  const hydrateV2Message = (sessionID: string, messageID: string) => {
    if (!sessionApi) return
    const pending = hydrations.get(sessionID) ?? new Map<string, Promise<void>>()
    hydrations.set(sessionID, pending)
    void runInflight(pending, messageID, async () => {
      const active = generation(sessionID)
      const epoch = snapshot.epoch()
      const source = structuredClone(unwrap(data.session_message[sessionID]?.find((message) => message.id === messageID)))
      const message = await sessionApi.message({ sessionID, messageID })
      const current = data.session_message[sessionID] ?? []
      if (generations.get(sessionID) !== active || snapshot.epoch() !== epoch || removedMessages.get(sessionID)?.has(messageID)) return
      if (!equal(source, current.find((item) => item.id === messageID))) return
      const messages = [...current.filter((item) => item.id !== message.id), message].sort(compareMessages)
      projectV2({ sessionID, messages, touched: [message.id] })
    })
      .finally(() => {
        if (hydrations.get(sessionID) === pending && pending.size === 0) hydrations.delete(sessionID)
      })
      .catch(() => undefined)
  }

  const applyV2 = (event: NativeServerEvent) => {
    if (!("data" in event) || !("sessionID" in event.data) || typeof event.data.sessionID !== "string") return
    const sessionID = event.data.sessionID
    touch(sessionID)
    if (event.type === "session.next.prompt.admitted") {
      const pending = admissions.get(sessionID) ?? new Map()
      pending.set(event.data.messageID, { prompt: event.data.prompt, delivery: event.data.delivery })
      admissions.set(sessionID, pending)
    }
    const reduction = v2.reduce(data.session_message[sessionID] ?? [], event)
    if (reduction) {
      const admitted = reduction.admitted
      if (admitted) setData("pending_input", sessionID, (current = {}) => ({ ...current, [admitted]: true }))
      reduction.promoted?.forEach((id) => confirmNativeUser(sessionID, id))
      projectV2(reduction, event.type === "session.next.text.delta" || event.type === "session.next.reasoning.delta")
      if (reduction.pendingMessage && !data.message[sessionID]?.some((message) => message.id === reduction.pendingMessage?.id)) {
        const normalized = normalizeSessionMessages(sessionID, [reduction.pendingMessage])
        setData("message", sessionID, (messages = []) => merge(messages, normalized.messages).sort(compareMessages))
        normalized.parts.forEach((parts, id) => setData("part", id, parts))
        messageLoads.get(sessionID)?.retainedMessages.add(reduction.pendingMessage.id)
      }
      if (reduction.missing) hydrateV2Message(sessionID, reduction.missing)
    }
    if (event.type === "session.next.text.ended" || event.type === "session.next.reasoning.ended") {
      const contentID = event.type === "session.next.text.ended" ? event.data.textID : event.data.reasoningID
      const partID = sessionMessagePartID(event.data.assistantMessageID, contentID)
      deltaBases.delete(partID)
      setData("part_text_accum_delta", produce((draft) => { delete draft[partID] }))
      messageLoads.get(sessionID)?.deltaParts.get(event.data.assistantMessageID)?.delete(partID)
      messageLoads.get(sessionID)?.carriedDeltaParts.get(event.data.assistantMessageID)?.delete(partID)
    }

    const info = data.info[sessionID]
    if (event.type === "session.next.moved" && info)
      remember({
        ...info,
        workspaceID: event.data.location.workspaceID,
        directory: event.data.location.directory,
        path: event.data.subdirectory,
        time: { ...info.time, updated: event.data.timestamp },
      })
    // if (event.type === "session.archived") {
    //   if (info) remember({ ...info, time: { ...info.time, archived: event.created, updated: event.created } })
    //   evict([sessionID])
    // }
    if (event.type === "session.next.prompted" || event.type.startsWith("session.next.step.") || event.type.startsWith("session.next.text.") || event.type.startsWith("session.next.reasoning.") || event.type.startsWith("session.next.tool.") || event.type.startsWith("session.next.shell.")) {
      writeStatus(sessionID, { type: "busy" }, true)
    }
    if (event.type === "session.next.retried") {
      writeStatus(sessionID, {
        type: "retry",
        attempt: event.data.attempt,
        message: event.data.error.message,
        next: event.data.timestamp,
      }, true)
    }
    if (
      event.type === "session.next.revert.staged" ||
      event.type === "session.next.revert.cleared" ||
      event.type === "session.next.revert.committed"
    )
      void resolve(sessionID, { force: true }).catch(() => undefined)
  }

  const apply = (event: { type: string; properties?: unknown }, projected = false) => {
    const eventID = eventSessionID(event)
    if (eventID) {
      touch(eventID)
      if (
        !data.info[eventID] &&
        event.type !== "session.created" &&
        event.type !== "session.updated" &&
        event.type !== "session.deleted"
      )
        void resolve(eventID).catch(() => undefined)
    }
    switch (event.type) {
      case "session.created":
        remember((event.properties as { info: Session }).info)
        return
      case "session.updated": {
        const info = (event.properties as { info: Session }).info
        remember(info)
        if (info.time.archived) evict([info.id])
        return
      }
      case "session.deleted": {
        const properties = event.properties as { sessionID?: string; info?: Session }
        const sessionID = properties.info?.id ?? properties.sessionID
        if (!sessionID) return
        infoSeen.delete(sessionID)
        setData(
          "info",
          produce((draft) => void delete draft[sessionID]),
        )
        evict([sessionID])
        return
      }
      case "todo.updated": {
        const props = event.properties as { sessionID: string; todos: Todo[] }
        setData("todo", props.sessionID, reconcile(props.todos, { key: "id" }))
        return
      }
      case "session.status": {
        const props = event.properties as { sessionID: string; status: typeof SessionStatusEvent.Info.Encoded }
        writeStatus(props.sessionID, props.status)
        const owner = props.status.type !== "idle" ? props.status.activity?.userMessageID : undefined
        if (owner && protocol.kind === "v1") {
          const boundary = data.message[props.sessionID]?.find((message) => message.id === owner)
          if (boundary)
            setData("pending_input", props.sessionID, produce((draft = {}) => {
              data.message[props.sessionID]?.forEach((message) => {
                if (message.role === "user" && compareMessages(message, boundary) <= 0) delete draft[message.id]
              })
            }))
        }
        return
      }
      case "message.updated": {
        const info = cleanMessage((event.properties as { info: Message }).info)
        if (!projected && protocol.kind === "v1" && info.role === "assistant" && info.time.completed === undefined) {
          const boundary = data.message[info.sessionID]?.find((message) => message.id === info.parentID)
          if (boundary)
            setData("pending_input", info.sessionID, produce((draft = {}) => {
              data.message[info.sessionID]?.forEach((message) => {
                if (message.role === "user" && compareMessages(message, boundary) <= 0) delete draft[message.id]
              })
            }))
        }
        if (!projected) indexLegacyMessage(info)
        const load = messageLoads.get(info.sessionID)
        if (!projected) load?.touchedMessages.add(info.id)
        load?.removedMessages.delete(info.id)
        const items = optimistic.get(info.sessionID)
        const item = items?.get(info.id)
        if (!projected && items && item) {
          if (item.parts.length === 0) clearOptimistic(info.sessionID, info.id)
          if (item.parts.length > 0) items.set(info.id, { ...item, confirmedMessage: true })
        }
        const orphans = orphanParts.get(info.sessionID)
        orphans?.delete(info.id)
        if (orphans?.size === 0) orphanParts.delete(info.sessionID)
        const removedMessagesForSession = removedMessages.get(info.sessionID)
        removedMessagesForSession?.delete(info.id)
        if (removedMessagesForSession?.size === 0) removedMessages.delete(info.sessionID)
        const messages = data.message[info.sessionID]
        if (!messages) {
          setData("message", info.sessionID, [info])
          return
        }
        const existing = messages.find((message) => message.id === info.id)
        if (existing && messageKey(existing) !== messageKey(info)) {
          setData("message", info.sessionID, reconcile([...messages.filter((message) => message.id !== info.id), info].sort(compareMessages), { key: "id" }))
          return
        }
        const result = Binary.search(messages, messageKey(info), messageKey)
        if (result.found) setData("message", info.sessionID, result.index, reconcile(info))
        if (!result.found)
          setData("message", info.sessionID, (value = []) => {
            const next = value.slice()
            next.splice(result.index, 0, info)
            return next
          })
        return
      }
      case "message.removed": {
        const props = event.properties as { sessionID: string; messageID: string }
        setData("session_message", props.sessionID, (messages) =>
          messages?.filter((message) => message.id !== props.messageID),
        )
        const load = messageLoads.get(props.sessionID)
        load?.touchedMessages.add(props.messageID)
        load?.removedMessages.add(props.messageID)
        load?.clearedMessageParts.add(props.messageID)
        load?.deltaParts.delete(props.messageID)
        load?.carriedDeltaParts.delete(props.messageID)
        load?.removedParts.delete(props.messageID)
        load?.optimisticParts.delete(props.messageID)
        pendingParts.get(props.sessionID)?.delete(props.messageID)
        if (pendingParts.get(props.sessionID)?.size === 0) pendingParts.delete(props.sessionID)
        const removedMessagesForSession = removedMessages.get(props.sessionID) ?? new Set<string>()
        removedMessagesForSession.add(props.messageID)
        removedMessages.set(props.sessionID, removedMessagesForSession)
        clearOptimistic(props.sessionID, props.messageID)
        setData(
          produce((draft) => {
            const messages = draft.message[props.sessionID]
            if (messages) {
              const index = messages.findIndex((message) => message.id === props.messageID)
              if (index >= 0) messages.splice(index, 1)
            }
            deleteMessageParts(draft, props.messageID)
          }),
        )
        return
      }
      case "message.part.updated": {
        const part = (event.properties as { part: Part }).part
        if (SKIP_PARTS.has(part.type)) return
        const messages = data.message[part.sessionID]
        const load = messageLoads.get(part.sessionID)
        const missing = !messages?.some((message) => message.id === part.messageID)
        // Outside a page load, accepting a part without its ordered parent event would create an unbounded orphan.
        if (
          missing &&
          (!load ||
            load.clearedMessageParts.has(part.messageID) ||
            removedMessages.get(part.sessionID)?.has(part.messageID))
        )
          return
        if (missing) {
          const orphans = orphanParts.get(part.sessionID) ?? new Set<string>()
          orphans.add(part.messageID)
          orphanParts.set(part.sessionID, orphans)
          load?.orphanParents.add(part.messageID)
        }
        const deltas = load?.deltaParts.get(part.messageID)
        deltas?.delete(part.id)
        if (deltas?.size === 0) load?.deltaParts.delete(part.messageID)
        const carried = load?.carriedDeltaParts.get(part.messageID)
        carried?.delete(part.id)
        if (carried?.size === 0) load?.carriedDeltaParts.delete(part.messageID)
        const removed = load?.removedParts.get(part.messageID)
        removed?.delete(part.id)
        if (removed?.size === 0) load?.removedParts.delete(part.messageID)
        const pending = pendingParts.get(part.sessionID)?.get(part.messageID)
        pending?.delete(part.id)
        if (pending?.size === 0) pendingParts.get(part.sessionID)?.delete(part.messageID)
        if (pendingParts.get(part.sessionID)?.size === 0) pendingParts.delete(part.sessionID)
        const optimistic = load?.optimisticParts.get(part.messageID)
        optimistic?.delete(part.id)
        if (optimistic?.size === 0) load?.optimisticParts.delete(part.messageID)
        deltaBases.delete(part.id)
        if (!projected) trackPartChange(part.sessionID, part.messageID, part.id)
        if (!projected) confirmOptimisticPart(part.sessionID, part.messageID, part)
        setData(
          "part_text_accum_delta",
          produce((draft) => void delete draft[part.id]),
        )
        const parts = data.part[part.messageID]
        if (!parts) {
          setData("part", part.messageID, [part])
          return
        }
        const result = Binary.search(parts, part.id, (item) => item.id)
        if (result.found) setData("part", part.messageID, result.index, reconcile(part))
        if (!result.found)
          setData("part", part.messageID, (value = []) => {
            const next = value.slice()
            next.splice(result.index, 0, part)
            return next
          })
        return
      }
      case "message.part.removed": {
        const props = event.properties as { sessionID: string; messageID: string; partID: string }
        // Part removal is event-only on the server, so its tombstone lasts until a later update or eviction.
        if (!projected) {
          const pending = pendingParts.get(props.sessionID) ?? new Map<string, Set<string>>()
          const parts = pending.get(props.messageID) ?? new Set<string>()
          parts.add(props.partID)
          pending.set(props.messageID, parts)
          pendingParts.set(props.sessionID, pending)
        }
        const deltas = messageLoads.get(props.sessionID)?.deltaParts.get(props.messageID)
        deltas?.delete(props.partID)
        if (deltas?.size === 0) messageLoads.get(props.sessionID)?.deltaParts.delete(props.messageID)
        const load = messageLoads.get(props.sessionID)
        const carried = load?.carriedDeltaParts.get(props.messageID)
        carried?.delete(props.partID)
        if (carried?.size === 0) load?.carriedDeltaParts.delete(props.messageID)
        if (load && !projected) {
          const parts = load.removedParts.get(props.messageID) ?? new Set<string>()
          parts.add(props.partID)
          load.removedParts.set(props.messageID, parts)
          const optimistic = load.optimisticParts.get(props.messageID)
          optimistic?.delete(props.partID)
          if (optimistic?.size === 0) load.optimisticParts.delete(props.messageID)
        }
        if (!projected) trackPartChange(props.sessionID, props.messageID, props.partID)
        clearOptimisticPart(props.sessionID, props.messageID, props.partID)
        setData(
          produce((draft) => {
            delete draft.part_text_accum_delta[props.partID]
            deltaBases.delete(props.partID)
            const parts = draft.part[props.messageID]
            if (!parts) return
            const result = Binary.search(parts, props.partID, (part) => part.id)
            if (result.found) parts.splice(result.index, 1)
            if (parts.length === 0) delete draft.part[props.messageID]
          }),
        )
        return
      }
      case "message.part.delta": {
        const props = event.properties as {
          sessionID: string
          messageID: string
          partID: string
          field: string
          delta: string
        }
        const parts = data.part[props.messageID]
        if (!parts) return
        const result = Binary.search(parts, props.partID, (part) => part.id)
        if (!result.found) return
        trackPartChange(props.sessionID, props.messageID, props.partID)
        const load = messageLoads.get(props.sessionID)
        if (load) {
          const parts = load.deltaParts.get(props.messageID) ?? new Set<string>()
          parts.add(props.partID)
          load.deltaParts.set(props.messageID, parts)
          const carried = load.carriedDeltaParts.get(props.messageID)
          carried?.delete(props.partID)
          if (carried?.size === 0) load.carriedDeltaParts.delete(props.messageID)
        }
        const field = props.field as keyof (typeof parts)[number]
        const current = parts[result.index]?.[field]
        if (!deltaBases.has(props.partID) && typeof current === "string")
          deltaBases.set(props.partID, { base: current, sessionID: props.sessionID })
        setData(
          "part_text_accum_delta",
          props.partID,
          (value) => (value ?? (typeof current === "string" ? current : "")) + props.delta,
        )
        setData(
          "part",
          props.messageID,
          produce((draft) => {
            if (!draft) return
            const part = draft[result.index]
            const field = props.field as keyof typeof part
            ;(part[field] as string) = ((part[field] as string | undefined) ?? "") + props.delta
          }),
        )
        return
      }
      case "permission.asked": {
        const permission = event.properties as PermissionRequest
        const permissions = data.permission[permission.sessionID]
        if (!permissions) {
          setData("permission", permission.sessionID, [permission])
          return
        }
        const result = Binary.search(permissions, permission.id, (item) => item.id)
        if (result.found) setData("permission", permission.sessionID, result.index, reconcile(permission))
        if (!result.found)
          setData(
            "permission",
            permission.sessionID,
            produce((draft) => void draft.splice(result.index, 0, permission)),
          )
        return
      }
      case "permission.replied": {
        const props = event.properties as { sessionID: string; requestID: string }
        setData(
          "permission",
          props.sessionID,
          produce((draft) => {
            if (!draft) return
            const result = Binary.search(draft, props.requestID, (item) => item.id)
            if (result.found) draft.splice(result.index, 1)
          }),
        )
        return
      }
      case "question.asked": {
        const question = event.properties as QuestionRequest
        const questions = data.question[question.sessionID]
        if (!questions) {
          setData("question", question.sessionID, [question])
          return
        }
        const result = Binary.search(questions, question.id, (item) => item.id)
        if (result.found) setData("question", question.sessionID, result.index, reconcile(question))
        if (!result.found)
          setData(
            "question",
            question.sessionID,
            produce((draft) => void draft.splice(result.index, 0, question)),
          )
        return
      }
      case "question.replied":
      case "question.rejected": {
        const props = event.properties as { sessionID: string; requestID: string }
        setData(
          "question",
          props.sessionID,
          produce((draft) => {
            if (!draft) return
            const result = Binary.search(draft, props.requestID, (item) => item.id)
            if (result.found) draft.splice(result.index, 1)
          }),
        )
      }
    }
  }

  return {
    data,
    set: setData,
    snapshot,
    retained: () =>
      [...new Set([...seen, ...Object.keys(data.message), ...Object.keys(data.session_message), ...protectedSessions()])],
    get: (sessionID: string) => data.info[sessionID],
    peek: (sessionID: string) => data.info[sessionID],
    remember,
    resolve,
    lineage: {
      peek: peekLineage,
      async resolve(sessionID: string) {
        const session = await resolve(sessionID)
        return { session, root: await rootSession(session, resolve) }
      },
    },
    sync,
    prefetch,
    shouldPrefetch(sessionID: string, limit: number) {
      if (data.message[sessionID] === undefined) return true
      if (Date.now() - (meta.at[sessionID] ?? 0) > 15_000) return true
      if (meta.complete[sessionID]) return false
      return (meta.limit[sessionID] ?? 0) <= limit
    },
    fresh(sessionID: string, ttl: number) {
      return Date.now() - (meta.at[sessionID] ?? 0) <= ttl
    },
    optimistic: {
      add(input: { sessionID: string; message: Message; parts: Part[] }) {
        setData("pending_input", input.sessionID, (current = {}) => ({ ...current, [input.message.id]: true }))
        const parts = input.parts
          .filter((part) => !!part?.id && !SKIP_PARTS.has(part.type))
          .sort((a, b) => cmp(a.id, b.id))
        const load = messageLoads.get(input.sessionID)
        if (load?.clearedMessageParts.has(input.message.id)) {
          const touched = load.touchedParts.get(input.message.id) ?? new Set<string>()
          parts.forEach((part) => touched.add(part.id))
          load.touchedParts.set(input.message.id, touched)
        }
        if (load) {
          load.removedMessages.delete(input.message.id)
          load.optimisticParts.set(input.message.id, new Set(parts.map((part) => part.id)))
        }
        const items = optimistic.get(input.sessionID)
        const removedMessagesForSession = removedMessages.get(input.sessionID)
        removedMessagesForSession?.delete(input.message.id)
        if (removedMessagesForSession?.size === 0) removedMessages.delete(input.sessionID)
        if (items) items.set(input.message.id, { ...input, parts, confirmedParts: [] })
        if (!items)
          optimistic.set(input.sessionID, new Map([[input.message.id, { ...input, parts, confirmedParts: [] }]]))
        setData("message", input.sessionID, (messages = []) => merge(messages, [input.message]).sort(compareMessages))
        setData(
          "part_text_accum_delta",
          produce((draft) => {
            for (const part of [...(data.part[input.message.id] ?? []), ...parts]) {
              delete draft[part.id]
              deltaBases.delete(part.id)
            }
          }),
        )
        setData("part", input.message.id, parts)
      },
      remove(input: { sessionID: string; messageID: string }) {
        setData("pending_input", input.sessionID, produce((draft = {}) => { delete draft[input.messageID] }))
        const item = optimistic.get(input.sessionID)?.get(input.messageID)
        if (!item) return
        messageLoads.get(input.sessionID)?.optimisticParts.delete(input.messageID)
        clearOptimistic(input.sessionID, input.messageID)
        if (item.confirmedMessage) {
          const partIDs = new Set(item.parts.map((part) => part.id))
          setData(
            produce((draft) => {
              for (const part of item.parts) {
                delete draft.part_text_accum_delta[part.id]
                deltaBases.delete(part.id)
              }
              const parts = draft.part[input.messageID]
              if (!parts) return
              draft.part[input.messageID] = parts.filter((part) => !partIDs.has(part.id))
              if (draft.part[input.messageID]?.length === 0) delete draft.part[input.messageID]
            }),
          )
          return
        }
        setData("message", input.sessionID, (messages) => messages?.filter((message) => message.id !== input.messageID))
        setData(produce((draft) => deleteMessageParts(draft, input.messageID)))
      },
    },
    async todo(sessionID: string, request?: { force?: boolean }) {
      touch(sessionID)
      if (data.todo[sessionID] !== undefined && !request?.force) return
      if ((await options?.protocol) === "v2") {
        setData("todo", sessionID, [])
        return
      }
      return runInflight(inflightTodo, sessionID, () => {
        const active = generation(sessionID)
        return (options?.retry ?? retry)(() => client.session.todo({ sessionID })).then((result) => {
          if (generations.get(sessionID) !== active) return
          setData("todo", sessionID, reconcile(result.data ?? [], { key: "id" }))
        })
      })
    },
    history: {
      more: (sessionID: string) =>
        data.message[sessionID] !== undefined &&
        meta.limit[sessionID] !== undefined &&
        !meta.complete[sessionID] &&
        !!meta.cursor[sessionID],
      loading: (sessionID: string) => meta.loading[sessionID] ?? false,
      async loadMore(sessionID: string, count = historyMessagePageSize) {
        touch(sessionID)
        if (meta.loading[sessionID] || meta.complete[sessionID] || !meta.cursor[sessionID]) return
        await loadMessages(sessionID, count, meta.cursor[sessionID], "prepend")
      },
    },
    evict(sessionID: string) {
      if (protectedSessions().has(sessionID)) return
      seen.delete(sessionID)
      evict([sessionID])
    },
    pin(sessionID: string) {
      pinned.set(sessionID, (pinned.get(sessionID) ?? 0) + 1)
      touch(sessionID)
    },
    unpin(sessionID: string) {
      const count = pinned.get(sessionID)
      if (!count || count === 1) pinned.delete(sessionID)
      if (count && count > 1) pinned.set(sessionID, count - 1)
    },
    apply,
    applyV2,
  }
}

export type ServerSession = ReturnType<typeof createServerSession>
