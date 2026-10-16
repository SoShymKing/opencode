import type { SessionMessageInfo } from "../promise"

type PartRevision = { revision: number; base?: string }
type Revision = { message: number; envelope: number; parts: Map<number, PartRevision>; removed: boolean }

export function createHistoryRevisions() {
  let revision = 0
  const sessions = new Map<string, Map<string, Revision>>()
  function touch(sessionID: string, messageID: string, part?: "envelope" | number | { index: number; base: string }, removed = false) {
    const messages = sessions.get(sessionID) ?? new Map<string, Revision>()
    sessions.set(sessionID, messages)
    const entry = messages.get(messageID) ?? { message: 0, envelope: 0, parts: new Map<number, PartRevision>(), removed: false }
    messages.set(messageID, entry)
    entry.removed = removed
    if (removed) entry.parts.clear()
    if (part === undefined) entry.message = ++revision
    else if (part === "envelope") entry.envelope = ++revision
    else if (typeof part === "number") entry.parts.set(part, { revision: ++revision })
    else entry.parts.set(part.index, { revision: ++revision, base: entry.parts.get(part.index)?.base ?? part.base })
  }
  return {
    capture: () => revision,
    touch,
    clear: (sessionID: string) => sessions.delete(sessionID),
    merge(sessionID: string, captured: number, fetched: SessionMessageInfo[], current: SessionMessageInfo[]) {
      const revisions = sessions.get(sessionID)
      const live = new Map(current.map((item) => [item.id, item]))
      const merged = new Map(fetched.flatMap((item): [string, SessionMessageInfo][] => {
        const entry = revisions?.get(item.id)
        if (entry?.removed) return []
        const existing = live.get(item.id)
        if (!existing || !entry) return [[item.id, item]]
        if (entry.message > captured) return [[item.id, existing]]
        if (item.type !== "assistant" || existing.type !== "assistant") return [[item.id, item]]
        const content = [...item.content]
        entry.parts.forEach((changed, index) => {
          const part = existing.content[index]
          if (changed.revision <= captured) {
            if (changed.base === undefined) return
            const checkpoint = content[index]
            if (item.time.completed === undefined && part && checkpoint &&
              (part.type === "text" || part.type === "reasoning") &&
              (checkpoint.type === "text" || checkpoint.type === "reasoning") && part.type === checkpoint.type &&
              (checkpoint.type !== "reasoning" || checkpoint.time?.completed === undefined) &&
              checkpoint.text.startsWith(changed.base) && part.text.startsWith(checkpoint.text) && part.text !== checkpoint.text) {
              content[index] = { ...checkpoint, text: part.text }
              return
            }
            changed.base = undefined
            return
          }
          if (part) {
            if (content.length < index) content.push(...existing.content.slice(content.length, index))
            content[index] = part
          }
          else content.splice(index, 1)
        })
        return [[item.id, { ...(entry.envelope > captured ? existing : item), content }]]
      }))
      current.forEach((item) => {
        const entry = revisions?.get(item.id)
        if (!entry || entry.removed || merged.has(item.id)) return
        if (entry.message > captured || entry.envelope > captured || [...entry.parts.values()].some((changed) => changed.revision > captured))
          merged.set(item.id, item)
      })
      return [...merged.values()]
    },
  }
}
