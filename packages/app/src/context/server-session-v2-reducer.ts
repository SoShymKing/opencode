import type { NativeSessionMessage } from "@/utils/session-message"
import type { NativeServerEvent } from "./server-sdk"

type Assistant = Extract<NativeSessionMessage, { type: "assistant" }>
type Content = Assistant["content"][number]

export type V2SessionReduction = {
  sessionID: string
  messages: NativeSessionMessage[]
  touched: string[]
  missing?: string
  admitted?: string
  promoted?: string[]
  pendingMessage?: NativeSessionMessage
}

export function createV2SessionReducer() {
  const reduce = (
    source: readonly NativeSessionMessage[],
    event: NativeServerEvent,
  ): V2SessionReduction | undefined => {
    if (!("sessionID" in event.data) || typeof event.data.sessionID !== "string") return
    const sessionID = event.data.sessionID
    const metadata = event.metadata === undefined ? {} : { metadata: event.metadata }
    const result = (messages: NativeSessionMessage[], touched: string[] = []): V2SessionReduction => ({
      sessionID,
      messages,
      touched,
    })
    const append = (message: NativeSessionMessage) =>
      result(source.some((item) => item.id === message.id) ? [...source] : [...source, message], [message.id])
    const update = (id: string, apply: (item: NativeSessionMessage) => NativeSessionMessage): V2SessionReduction => {
      if (!source.some((item) => item.id === id)) return { ...result([...source]), missing: id }
      return result(source.map((item) => (item.id === id ? apply(item) : item)), [id])
    }
    const assistant = (id: string, apply: (item: Assistant) => Assistant) =>
      update(id, (item) => (item.type === "assistant" ? apply(item) : item))
    const content = (messageID: string, contentID: string, apply: (item: Content) => Content): V2SessionReduction => {
      const current = source.find((item) => item.id === messageID)
      if (current?.type !== "assistant" || !current.content.some((item) => item.id === contentID))
        return { ...result([...source]), missing: messageID }
      return assistant(messageID, (item) => ({
        ...item,
        content: item.content.map((part) => (part.id === contentID ? apply(part) : part)),
      }))
    }
    const insert = (messageID: string, part: Content) =>
      assistant(messageID, (item) => ({
        ...item,
        content: item.content.some((entry) => entry.id === part.id) ? item.content : [...item.content, part],
      }))

    switch (event.type) {
      case "session.next.prompt.admitted":
        return {
          ...result([...source]),
          admitted: event.data.messageID,
          pendingMessage: {
            id: event.data.messageID,
            type: "user",
            ...event.data.prompt,
            ...metadata,
            time: { created: event.data.timestamp },
          },
        }
      case "session.next.retried":
      case "session.next.compaction.started":
      case "session.next.compaction.delta":
        return result([...source])
      case "session.next.prompted":
        return {
          ...append({
            id: event.data.messageID,
            type: "user",
            ...event.data.prompt,
            ...metadata,
            time: { created: event.data.timestamp },
          }),
          promoted: [event.data.messageID],
        }
      case "session.next.agent.switched":
        return append({
          id: event.data.messageID,
          type: "agent-switched",
          agent: event.data.agent,
          ...metadata,
          time: { created: event.data.timestamp },
        })
      case "session.next.model.switched":
        return append({
          id: event.data.messageID,
          type: "model-switched",
          model: event.data.model,
          ...metadata,
          time: { created: event.data.timestamp },
        })
      case "session.next.context.updated":
        return append({
          id: event.data.messageID,
          type: "system",
          text: event.data.text,
          ...metadata,
          time: { created: event.data.timestamp },
        })
      case "session.next.synthetic":
        return append({
          id: event.data.messageID,
          type: "synthetic",
          sessionID,
          text: event.data.text,
          ...metadata,
          time: { created: event.data.timestamp },
        })
      case "session.next.shell.started":
        return append({
          id: event.data.messageID,
          type: "shell",
          callID: event.data.callID,
          command: event.data.command,
          output: "",
          ...metadata,
          time: { created: event.data.timestamp },
        })
      case "session.next.shell.ended": {
        const shell = source.findLast((item) => item.type === "shell" && item.callID === event.data.callID)
        if (!shell) return result([...source])
        return update(shell.id, (item) =>
          item.type === "shell"
            ? { ...item, output: event.data.output, time: { ...item.time, completed: event.data.timestamp } }
            : item,
        )
      }
      case "session.next.step.started": {
        if (source.some((item) => item.id === event.data.assistantMessageID)) return result([...source])
        const current = source.findLast((item) => item.type === "assistant")
        const messages = source.map((item) => {
          if (item.type !== "assistant") return item
          if (item.id === current?.id && item.time.completed === undefined)
            return { ...item, time: { ...item.time, completed: event.data.timestamp } }
          return item
        })
        if (!messages.some((item) => item.id === event.data.assistantMessageID))
          messages.push({
            id: event.data.assistantMessageID,
            type: "assistant",
            agent: event.data.agent,
            model: event.data.model,
            ...metadata,
            content: [],
            ...(event.data.streamEventCount === undefined ? {} : { streamEventCount: event.data.streamEventCount }),
            ...(event.data.snapshot === undefined ? {} : { snapshot: { start: event.data.snapshot } }),
            time: { created: event.data.timestamp },
          })
        return result(
          messages,
          current?.type === "assistant" && current.id !== event.data.assistantMessageID && current.time.completed === undefined
            ? [current.id, event.data.assistantMessageID]
            : [event.data.assistantMessageID],
        )
      }
      case "session.next.step.stream.updated": {
        const current = source.find((item) => item.id === event.data.assistantMessageID)
        if (current?.type !== "assistant" || current.time.completed !== undefined) return result([...source])
        return assistant(event.data.assistantMessageID, (item) => ({
          ...item,
          streamEventCount: event.data.streamEventCount,
        }))
      }
      case "session.next.step.ended":
        return assistant(event.data.assistantMessageID, (item) => ({
          ...item,
          finish: event.data.finish,
          cost: event.data.cost,
          tokens: event.data.tokens,
          ...(event.data.snapshot === undefined && event.data.files === undefined ? {} : {
            snapshot: {
              ...item.snapshot,
              ...(event.data.snapshot === undefined ? {} : { end: event.data.snapshot }),
              ...(event.data.files === undefined ? {} : { files: event.data.files }),
            },
          }),
          time: { ...item.time, completed: event.data.timestamp },
        }))
      case "session.next.step.failed":
        return assistant(event.data.assistantMessageID, (item) => ({
          ...item,
          finish: "error",
          error: event.data.error,
          time: { ...item.time, completed: event.data.timestamp },
        }))
      case "session.next.text.started":
        return insert(event.data.assistantMessageID, { type: "text", id: event.data.textID, text: "" })
      case "session.next.text.delta":
        return content(event.data.assistantMessageID, event.data.textID, (item) => item.type === "text" ? { ...item, text: item.text + event.data.delta } : item)
      case "session.next.text.ended":
        return content(event.data.assistantMessageID, event.data.textID, (item) => item.type === "text" ? { ...item, text: event.data.text } : item)
      case "session.next.reasoning.started":
        return insert(event.data.assistantMessageID, { type: "reasoning", id: event.data.reasoningID, text: "", ...(event.data.providerMetadata === undefined ? {} : { providerMetadata: event.data.providerMetadata }), time: { created: event.data.timestamp } })
      case "session.next.reasoning.delta":
        return content(event.data.assistantMessageID, event.data.reasoningID, (item) => item.type === "reasoning" ? { ...item, text: item.text + event.data.delta } : item)
      case "session.next.reasoning.ended":
        return content(event.data.assistantMessageID, event.data.reasoningID, (item) => item.type === "reasoning" ? {
          ...item, text: event.data.text, ...(event.data.providerMetadata === undefined ? {} : { providerMetadata: event.data.providerMetadata }),
          time: { created: item.time?.created ?? event.data.timestamp, completed: event.data.timestamp },
        } : item)
      case "session.next.tool.input.started":
        return insert(event.data.assistantMessageID, { type: "tool", id: event.data.callID, name: event.data.name, state: { status: "pending", input: "" }, time: { created: event.data.timestamp } })
      case "session.next.tool.input.delta":
        return content(event.data.assistantMessageID, event.data.callID, (item) => item.type === "tool" && item.state.status === "pending" ? { ...item, state: { ...item.state, input: item.state.input + event.data.delta } } : item)
      case "session.next.tool.input.ended":
        return content(event.data.assistantMessageID, event.data.callID, (item) => item.type === "tool" && item.state.status === "pending" ? { ...item, state: { ...item.state, input: event.data.text } } : item)
      case "session.next.tool.called":
        return content(event.data.assistantMessageID, event.data.callID, (item) => item.type === "tool" ? {
          ...item, name: event.data.tool, provider: event.data.provider, state: { status: "running", input: event.data.input, structured: {}, content: [] }, time: { ...item.time, ran: event.data.timestamp },
        } : item)
      case "session.next.tool.progress":
        return content(event.data.assistantMessageID, event.data.callID, (item) => item.type === "tool" && item.state.status === "running" ? {
          ...item, state: { ...item.state, structured: event.data.structured, content: event.data.content },
        } : item)
      case "session.next.tool.success":
        return content(event.data.assistantMessageID, event.data.callID, (item) => item.type === "tool" && item.state.status === "running" ? {
          ...item, provider: {
            executed: event.data.provider.executed || item.provider?.executed === true,
            ...(item.provider?.metadata === undefined ? {} : { metadata: item.provider.metadata }),
            ...(event.data.provider.metadata === undefined ? {} : { resultMetadata: event.data.provider.metadata }),
          },
          state: {
            status: "completed", input: item.state.input, structured: event.data.structured, content: event.data.content,
            ...(event.data.outputPaths === undefined ? {} : { outputPaths: event.data.outputPaths }),
            ...(event.data.result === undefined ? {} : { result: event.data.result }),
          },
          time: { ...item.time, completed: event.data.timestamp },
        } : item)
      case "session.next.tool.failed":
        return content(event.data.assistantMessageID, event.data.callID, (item) => item.type === "tool" && (item.state.status === "pending" || item.state.status === "running") ? {
          ...item, provider: {
            executed: event.data.provider.executed || item.provider?.executed === true,
            ...(item.provider?.metadata === undefined ? {} : { metadata: item.provider.metadata }),
            ...(event.data.provider.metadata === undefined ? {} : { resultMetadata: event.data.provider.metadata }),
          },
          state: { status: "error", input: typeof item.state.input === "string" ? {} : item.state.input,
            structured: item.state.status === "running" ? item.state.structured : {}, content: item.state.status === "running" ? item.state.content : [], error: event.data.error,
            ...(event.data.result === undefined ? {} : { result: event.data.result }),
          },
          time: { ...item.time, completed: event.data.timestamp },
        } : item)
      case "session.next.compaction.ended":
        return append({ id: event.data.messageID, type: "compaction", reason: event.data.reason, summary: event.data.text, recent: event.data.recent, ...metadata, time: { created: event.data.timestamp } })
      default:
        return undefined
    }
  }
  return { reduce }
}
