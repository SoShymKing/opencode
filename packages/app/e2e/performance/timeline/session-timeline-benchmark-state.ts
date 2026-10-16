import type { OpenCodeEvent, SessionMessageInfo } from "@opencode/client/promise"

export function applyBenchmarkEvent(messages: SessionMessageInfo[], event: OpenCodeEvent) {
  switch (event.type) {
    case "session.text.started":
    case "session.text.delta":
    case "session.text.ended": {
      const index = messages.findIndex((message) => message.id === event.data.assistantMessageID)
      const message = messages[index]
      if (!message || message.type !== "assistant") throw new Error("Missing benchmark assistant")
      const content = [...message.content]
      const textIndexes = content.flatMap((part, index) => (part.type === "text" ? [index] : []))
      const partIndex = textIndexes[event.data.ordinal]
      if (event.type === "session.text.started") {
        if (partIndex !== undefined) return
        if (event.data.ordinal !== textIndexes.length) throw new Error("Missing benchmark text ordinal")
        content.push({ type: "text", text: "" })
        messages[index] = { ...message, content }
        return
      }
      const part = partIndex === undefined ? undefined : content[partIndex]
      if (partIndex === undefined || part?.type !== "text") throw new Error("Missing benchmark text")
      content[partIndex] = {
        ...part,
        text: event.type === "session.text.delta" ? part.text + event.data.delta : event.data.text,
      }
      messages[index] = { ...message, content }
      return
    }
    case "session.step.ended": {
      const index = messages.findIndex((message) => message.id === event.data.assistantMessageID)
      const message = messages[index]
      if (!message || message.type !== "assistant") throw new Error("Missing benchmark assistant")
      messages[index] = {
        ...message,
        time: { ...message.time, completed: event.created },
        finish: event.data.finish,
        cost: event.data.cost,
        tokens: event.data.tokens,
        ...(event.data.streamEventCount === undefined ? {} : { streamEventCount: event.data.streamEventCount }),
      }
      return
    }
    default:
      throw new Error(`Unsupported benchmark event: ${event.type}`)
  }
}
