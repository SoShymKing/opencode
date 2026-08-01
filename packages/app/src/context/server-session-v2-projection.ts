import type { SessionMessageInfo } from "@opencode-ai/client/promise"
import type { Message, Part } from "@opencode-ai/sdk/v2/client"
import { normalizeSessionMessages } from "@/utils/session-message"

type ContextSource = {
  position: number
  message: SessionMessageInfo
}

type RootUnit = {
  rootID: string
  source: SessionMessageInfo[]
  agent: ContextSource | undefined
  model: ContextSource | undefined
  selected: boolean
}

export function normalizeTouchedSessionMessages(
  sessionID: string,
  source: readonly SessionMessageInfo[],
  touched: readonly string[],
) {
  const expanded = new Set(touched)
  const messages: Message[] = []
  const parts = new Map<string, Part[]>()
  let latestAgent: ContextSource | undefined
  let latestModel: ContextSource | undefined
  let current: RootUnit | undefined

  const project = (unit: RootUnit) => {
    if (!unit.selected) return
    const context = new Map<number, SessionMessageInfo>()
    if (unit.agent) context.set(unit.agent.position, unit.agent.message)
    if (unit.model) context.set(unit.model.position, unit.model.message)
    const normalized = normalizeSessionMessages(sessionID, [
      ...[...context.entries()].sort(([left], [right]) => left - right).map(([, message]) => message),
      ...unit.source,
    ])
    messages.push(...normalized.messages.filter((message) => expanded.has(message.id)))
    for (const [messageID, value] of normalized.parts) {
      if (expanded.has(messageID)) parts.set(messageID, value)
    }
  }

  source.forEach((message, position) => {
    const root = message.type === "user" || (message.type === "synthetic" && message.description?.trim())
    if (root || message.type === "shell") {
      if (current) project(current)
      current = undefined
    }
    if (root) {
      current = {
        rootID: message.id,
        source: [message],
        agent: latestAgent,
        model: latestModel,
        selected: expanded.has(message.id),
      }
      return
    }
    if (message.type === "shell") {
      if (!expanded.has(message.id)) return
      expanded.add(`${message.id}:assistant`)
      project({
        rootID: message.id,
        source: [message],
        agent: latestAgent,
        model: latestModel,
        selected: true,
      })
      return
    }

    if (current) {
      current.source.push(message)
      if ((message.type === "assistant" || message.type === "compaction") && expanded.has(message.id)) {
        current.selected = true
        expanded.add(current.rootID)
      }
    }
    if (message.type === "agent-switched" || message.type === "assistant")
      latestAgent = { position, message }
    if (message.type === "model-switched" || message.type === "assistant")
      latestModel = { position, message }
  })
  if (current) project(current)

  return { messages, parts, touched: expanded }
}
