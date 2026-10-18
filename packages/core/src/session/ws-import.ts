export * as SessionWsImport from "./ws-import.js"

import { DateTime, Effect, Schema } from "effect"
import { sql } from "drizzle-orm"
import { isDeepStrictEqual } from "node:util"
import { DatabaseWsArchive } from "../database/ws-archive.js"
import type { EffectDrizzleSqlite } from "../database/drizzle.js"
import { WsEventCodec } from "../database/ws-event-codec.js"
import { WsMessageCodec } from "../database/ws-message-codec.js"
import type { SessionV1 } from "@opencode/schema/v1/session"

type Message = typeof WsMessageCodec.Message.Type
type Assistant = Extract<Message, { type: "assistant" }>
type Content = Assistant["content"][number]
type Input = Extract<WsEventCodec.Event, { type: "session.next.prompt.admitted.1" }>["data"]
type Info = Omit<Extract<WsEventCodec.Event, { type: "session.created.1" }>["data"]["info"], "revert"> & {
  readonly revert?: Extract<WsEventCodec.Event, { type: "session.next.revert.staged.1" }>["data"]["revert"]
}
type Part = typeof SessionV1.Part.Type

export function fold(rows: readonly WsEventCodec.Row[]) {
  const events = rows.map(WsEventCodec.decode)
  const messages = new Map<string, { seq: number; message: Message }>()
  const inputs = new Map<string, { admitted: number; promoted?: number; data: Input }>()
  const legacyMessages = new Map<string, { created: number; info: typeof SessionV1.Info.Type }>()
  const legacyParts = new Map<string, { created: number; part: Part }>()
  const missing = new Set<string>()
  const ids = new Set<string>()
  const state: { info?: Info; deleted: boolean; through: number } = { deleted: false, through: -1 }
  const append = (seq: number, message: Message) => {
    if (messages.has(message.id)) throw new WsEventCodec.SourceEventError(`Duplicate source message ${message.id}`)
    messages.set(message.id, { seq, message })
  }
  const update = (id: string, change: (message: Assistant) => Assistant) => {
    const row = messages.get(id)
    if (row?.message.type !== "assistant") { missing.add(`assistant:${id}`); return }
    messages.set(id, { ...row, message: change(row.message) })
  }
  const child = (id: string, kind: Content["type"], key: string, change: (value: Content) => Content) =>
    update(id, (message) => {
      const position = message.content.findLastIndex((value) => value.type === kind && value.id === key)
      if (position < 0) missing.add(`content:${id}:${kind}:${key}`)
      return { ...message, content: message.content.map((value, index) => index === position ? change(value) : value) }
    })
  const usage = (part: Part, sign: number) => {
    if (part.type !== "step-finish" || !state.info) return
    const previous = state.info.tokens ?? { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } }
    state.info = { ...state.info, cost: (state.info.cost ?? 0) + sign * part.cost,
      tokens: { input: previous.input + sign * part.tokens.input, output: previous.output + sign * part.tokens.output,
        reasoning: previous.reasoning + sign * part.tokens.reasoning,
        cache: { read: previous.cache.read + sign * part.tokens.cache.read, write: previous.cache.write + sign * part.tokens.cache.write } } }
  }
  for (const { row, event } of events) {
    if (row.aggregate_id !== rows[0]?.aggregate_id || row.seq <= state.through || ids.has(row.id))
      throw new WsEventCodec.SourceEventError("Source events must have unique identities and increasing aggregate sequences")
    if (row.seq !== state.through + 1) missing.add(`sequence:${state.through + 1}:${row.seq - 1}`)
    ids.add(row.id)
    state.through = row.seq
    switch (event.type) {
      case "session.created.1":
        if (state.info) throw new WsEventCodec.SourceEventError("Duplicate source creation")
        state.info = event.data.info
        state.deleted = false
        break
      case "session.updated.1":
        if (!state.info) missing.add("session:before-update")
        state.info = event.data.info
        break
      case "session.deleted.1":
        state.info = undefined
        state.deleted = true
        messages.clear(); inputs.clear(); legacyMessages.clear(); legacyParts.clear()
        break
      case "message.updated.1": {
        const previous = legacyMessages.get(event.data.info.id)
        legacyMessages.set(event.data.info.id, { created: previous?.created ?? event.data.info.time.created, info: event.data.info })
        break
      }
      case "message.removed.1":
        for (const [id, value] of legacyParts) {
          if (value.part.messageID !== event.data.messageID) continue
          usage(value.part, -1)
          legacyParts.delete(id)
        }
        legacyMessages.delete(event.data.messageID)
        break
      case "message.part.updated.1": {
        const previous = legacyParts.get(event.data.part.id)
        if (previous) usage(previous.part, -1)
        legacyParts.set(event.data.part.id, { created: previous?.created ?? event.data.time, part: event.data.part })
        usage(event.data.part, 1)
        break
      }
      case "message.part.removed.1": {
        const previous = legacyParts.get(event.data.partID)
        if (previous) usage(previous.part, -1)
        legacyParts.delete(event.data.partID)
        break
      }
      case "session.next.agent.switched.1":
        if (state.info) state.info = { ...state.info, agent: event.data.agent, time: { ...state.info.time, updated: DateTime.toEpochMillis(event.data.timestamp) } }
        append(row.seq, { id: event.data.messageID, type: "agent-switched", agent: event.data.agent, time: { created: event.data.timestamp } })
        break
      case "session.next.model.switched.1":
        if (state.info) state.info = { ...state.info, model: event.data.model, time: { ...state.info.time, updated: DateTime.toEpochMillis(event.data.timestamp) } }
        append(row.seq, { id: event.data.messageID, type: "model-switched", model: event.data.model, time: { created: event.data.timestamp } })
        break
      case "session.next.moved.1":
        if (state.info) state.info = { ...state.info, directory: event.data.location.directory, path: event.data.subdirectory,
          workspaceID: event.data.location.workspaceID, time: { ...state.info.time, updated: DateTime.toEpochMillis(event.data.timestamp) } }
        break
      case "session.next.prompt.admitted.1":
        if (inputs.has(event.data.messageID) || messages.has(event.data.messageID))
          throw new WsEventCodec.SourceEventError("Source input admission conflict")
        inputs.set(event.data.messageID, { admitted: row.seq, data: event.data })
        break
      case "session.next.prompted.1": {
        const previous = inputs.get(event.data.messageID)
        if (previous && (!isDeepStrictEqual(previous.data.prompt, event.data.prompt) || previous.data.delivery !== event.data.delivery))
          throw new WsEventCodec.SourceEventError("Source input promotion conflict")
        inputs.set(event.data.messageID, { admitted: previous?.admitted ?? row.seq, promoted: row.seq, data: previous?.data ?? event.data })
        append(row.seq, { id: event.data.messageID, type: "user", ...event.data.prompt, time: { created: event.data.timestamp } })
        break
      }
      case "session.next.context.updated.1":
        append(row.seq, { id: event.data.messageID, type: "system", text: event.data.text, time: { created: event.data.timestamp } })
        break
      case "session.next.synthetic.1":
        append(row.seq, { id: event.data.messageID, type: "synthetic", sessionID: event.data.sessionID, text: event.data.text, time: { created: event.data.timestamp } })
        break
      case "session.next.shell.started.1":
        append(row.seq, { id: event.data.messageID, type: "shell", callID: event.data.callID, command: event.data.command, output: "", time: { created: event.data.timestamp } })
        break
      case "session.next.shell.ended.1": {
        const target = [...messages.values()].findLast((value) => value.message.type === "shell" && value.message.callID === event.data.callID)
        if (target?.message.type === "shell") messages.set(target.message.id, { ...target, message: { ...target.message,
          output: event.data.output, time: { ...target.message.time, completed: event.data.timestamp } } })
        else missing.add(`shell:${event.data.callID}`)
        break
      }
      case "session.next.step.started.1": {
        const previous = [...messages.values()].findLast((value) => value.message.type === "assistant")
        if (previous?.message.type === "assistant" && !previous.message.time.completed)
          update(previous.message.id, (message) => ({ ...message, time: { ...message.time, completed: event.data.timestamp } }))
        append(row.seq, { id: event.data.assistantMessageID, type: "assistant", agent: event.data.agent,
          model: event.data.model, content: [], snapshot: event.data.snapshot ? { start: event.data.snapshot } : undefined,
          streamEventCount: event.data.streamEventCount, time: { created: event.data.timestamp } })
        break
      }
      case "session.next.step.stream.updated.1":
        update(event.data.assistantMessageID, (message) => ({ ...message, streamEventCount: event.data.streamEventCount }))
        break
      case "session.next.step.ended.2":
        update(event.data.assistantMessageID, (message) => ({ ...message, finish: event.data.finish, cost: event.data.cost,
          tokens: event.data.tokens, time: { ...message.time, completed: event.data.timestamp },
          snapshot: event.data.snapshot || event.data.files ? { ...message.snapshot, end: event.data.snapshot, files: event.data.files } : message.snapshot }))
        break
      case "session.next.step.failed.2":
        update(event.data.assistantMessageID, (message) => ({ ...message, finish: "error", error: event.data.error, time: { ...message.time, completed: event.data.timestamp } }))
        break
      case "session.next.text.started.1":
        update(event.data.assistantMessageID, (message) => ({ ...message, content: [...message.content, { type: "text", id: event.data.textID, text: "" }] }))
        break
      case "session.next.text.ended.1":
        child(event.data.assistantMessageID, "text", event.data.textID, (value) => value.type === "text" ? { ...value, text: event.data.text } : value)
        break
      case "session.next.reasoning.started.1":
        update(event.data.assistantMessageID, (message) => ({ ...message, content: [...message.content, { type: "reasoning", id: event.data.reasoningID,
          text: "", providerMetadata: event.data.providerMetadata, time: { created: event.data.timestamp } }] }))
        break
      case "session.next.reasoning.ended.1":
        child(event.data.assistantMessageID, "reasoning", event.data.reasoningID, (value) => value.type === "reasoning" ? { ...value, text: event.data.text,
          providerMetadata: event.data.providerMetadata ?? value.providerMetadata,
          time: { created: value.time?.created ?? event.data.timestamp, completed: event.data.timestamp } } : value)
        break
      case "session.next.tool.input.started.1":
        update(event.data.assistantMessageID, (message) => ({ ...message, content: [...message.content, { type: "tool", id: event.data.callID,
          name: event.data.name, state: { status: "pending", input: "" }, time: { created: event.data.timestamp } }] }))
        break
      case "session.next.tool.input.ended.1":
        child(event.data.assistantMessageID, "tool", event.data.callID, (value) => value.type === "tool" && value.state.status === "pending"
          ? { ...value, state: { status: "pending", input: event.data.text } } : value)
        break
      case "session.next.tool.called.1":
        child(event.data.assistantMessageID, "tool", event.data.callID, (value) => value.type === "tool" ? { ...value, provider: event.data.provider,
          time: { ...value.time, ran: event.data.timestamp }, state: { status: "running", input: event.data.input, structured: {}, content: [] } } : value)
        break
      case "session.next.tool.progress.1":
        child(event.data.assistantMessageID, "tool", event.data.callID, (value) => value.type === "tool" && value.state.status === "running"
          ? { ...value, state: { ...value.state, structured: event.data.structured, content: event.data.content } } : value)
        break
      case "session.next.tool.success.1":
        child(event.data.assistantMessageID, "tool", event.data.callID, (value) => value.type === "tool" && value.state.status === "running"
          ? { ...value, time: { ...value.time, completed: event.data.timestamp },
            provider: { executed: event.data.provider.executed || value.provider?.executed === true, metadata: value.provider?.metadata, resultMetadata: event.data.provider.metadata },
            state: { status: "completed", input: value.state.input, structured: event.data.structured, content: event.data.content,
              outputPaths: event.data.outputPaths ?? [], result: event.data.result } } : value)
        break
      case "session.next.tool.failed.1":
        child(event.data.assistantMessageID, "tool", event.data.callID, (value) => value.type === "tool" && (value.state.status === "pending" || value.state.status === "running")
          ? { ...value, time: { ...value.time, completed: event.data.timestamp },
            provider: { executed: event.data.provider.executed || value.provider?.executed === true, metadata: value.provider?.metadata, resultMetadata: event.data.provider.metadata },
            state: { status: "error", input: value.state.status === "pending" ? {} : value.state.input,
              structured: value.state.status === "running" ? value.state.structured : {}, content: value.state.status === "running" ? value.state.content : [],
              error: event.data.error, result: event.data.result } } : value)
        break
      case "session.next.retried.1":
      case "session.next.compaction.started.1":
        break
      case "session.next.compaction.ended.1":
        append(row.seq, { id: event.data.messageID, type: "compaction", reason: event.data.reason,
          summary: event.data.text, recent: event.data.recent, time: { created: event.data.timestamp } })
        break
      case "session.next.revert.staged.1":
        if (state.info) state.info = { ...state.info, revert: event.data.revert, time: { ...state.info.time, updated: DateTime.toEpochMillis(event.data.timestamp) } }
        break
      case "session.next.revert.cleared.1":
        if (state.info) state.info = { ...state.info, revert: undefined, time: { ...state.info.time, updated: DateTime.toEpochMillis(event.data.timestamp) } }
        break
      case "session.next.revert.committed.1": {
        const boundary = messages.get(event.data.messageID)
        if (!boundary) { missing.add(`revert:${event.data.messageID}`); break }
        for (const [id, value] of messages) if (value.seq > boundary.seq) messages.delete(id)
        for (const [id, value] of inputs) if (value.admitted > boundary.seq || (value.promoted !== undefined && value.promoted > boundary.seq)) inputs.delete(id)
        if (state.info) state.info = { ...state.info, revert: undefined, time: { ...state.info.time, updated: DateTime.toEpochMillis(event.data.timestamp) } }
        break
      }
      default: {
        const unreachable: never = event
        throw new WsEventCodec.SourceEventError(`Unsupported source event ${unreachable}`)
      }
    }
  }
  return { events, ...state, messages: [...messages.values()], inputs: [...inputs.values()],
    legacyMessages: [...legacyMessages.values()], legacyParts: [...legacyParts.values()],
    evidence: { missing: [...missing], projectionRequired: true as const } }
}

export function history(db: EffectDrizzleSqlite.EffectSQLiteDatabase, aggregateID: string) {
  return Effect.gen(function* () {
    const root = yield* DatabaseWsArchive.readRoot(db, aggregateID)
    if (!root) return yield* Effect.fail(new WsEventCodec.SourceEventError("Missing WS import root"))
    if (root.codec_version !== 1) return yield* Effect.fail(new WsEventCodec.SourceEventError("Unsupported WS import codec"))
    const archive = yield* DatabaseWsArchive.read(db, root.archive_id)
    if (!archive) return yield* Effect.fail(new WsEventCodec.SourceEventError("Missing WS archive"))
    const table = DatabaseWsArchive.sourceTable(archive, "event")
    const rows = table ? yield* db.all<WsEventCodec.Row>(sql`SELECT id, aggregate_id, seq, type, data
      FROM ${DatabaseWsArchive.identifier(table)} WHERE aggregate_id = ${aggregateID} ORDER BY seq`) : []
    const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(WsEventCodec.Row))(rows)
    if (decoded.some((row) => row.seq > root.through_seq))
      return yield* Effect.fail(new WsEventCodec.SourceEventError("Source event exceeds import boundary"))
    const facts = yield* Effect.try({ try: () => fold(decoded), catch: (cause) =>
      new WsEventCodec.SourceEventError("Cannot fold archived source events", { cause }) })
    const epochTable = DatabaseWsArchive.sourceTable(archive, "session_context_epoch")
    const epoch = epochTable ? yield* db.get<{ baseline: string; baseline_seq: number; snapshot: string }>(sql`
      SELECT baseline, baseline_seq, snapshot FROM ${DatabaseWsArchive.identifier(epochTable)} WHERE session_id = ${aggregateID}`) : undefined
    const instructions = epoch ? {
      baseline: epoch.baseline,
      baselineSeq: epoch.baseline_seq,
      snapshot: yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Record(Schema.String,
        Schema.Struct({ value: Schema.Json, removed: Schema.optional(Schema.String) }))))(epoch.snapshot),
      initial: "unknown" as const,
    } : undefined
    return { root, archive, facts, instructions }
  })
}
