import type { SessionMessageAssistantTool, SessionMessageRecordedFile, SessionMessageRecordedShell } from "@opencode/client"
import stripAnsi from "strip-ansi"

export function historyText(text: string) {
  return stripAnsi(text)
    .replace(/\r\n?/g, "\n")
    .replace(/[\x00-\x08\x0b-\x1f\x7f-\x9f]/g, "")
}

export function recordedShellText(message: SessionMessageRecordedShell) {
  return historyText(`Recorded shell (outcome unknown)\n$ ${message.command}\n${message.output}`)
}

export function recordedFileText(file: SessionMessageRecordedFile) {
  return historyText(`Unavailable file: ${file.name ? `${file.name} - ` : ""}${file.uri}`)
}

export function recordedToolText(part: SessionMessageAssistantTool) {
  const terminal = part.state.status === "completed" || part.state.status === "error"
  const content = [
    ...(part.recorded?.content ?? []),
    ...(part.state.status === "completed" || part.state.status === "error" ? part.state.content ?? [] : []),
  ].map((item) =>
    item.type === "text" ? item.text : recordedFileText({ ...item, name: item.name ?? undefined }),
  )
  const facts = Object.fromEntries(
    Object.entries(part.recorded ?? {}).filter(([key]) => !["content", "attachments", "result"].includes(key)),
  )
  return historyText(
    [
      `Recorded ${part.name} (${terminal ? `source ${part.state.status}` : "outcome unknown"})`,
      `Input: ${typeof part.state.input === "string" ? part.state.input : JSON.stringify(part.state.input)}`,
      ...new Set(content),
      ...(part.state.status === "error" ? [`Error: ${JSON.stringify(part.state.error)}`] : []),
      ...(part.recorded?.result !== undefined ? [`Recorded result: ${JSON.stringify(part.recorded.result)}`] : []),
      ...(part.recorded?.attachments ?? []).map(recordedFileText),
      ...(Object.keys(facts).length ? [`Recorded facts: ${JSON.stringify(facts)}`] : []),
    ].join("\n"),
  )
}
