export const SESSION_LIST_REQUEST_WINDOW_KEY = "session_list_request_window_days"

const DAY = 24 * 60 * 60 * 1000
const DEFAULT_SESSION_LIST_REQUEST_WINDOW = 30

export type SessionListRequestWindow = number | "all"

export function parseSessionListRequestWindowInput(value: string): SessionListRequestWindow | undefined {
  const input = value.trim().toLowerCase()
  if (input === "all") return "all"
  if (!/^[1-9]\d*$/.test(input)) return
  const days = Number(input)
  if (!isValidSessionListRequestWindowDays(days)) return
  return days
}

export function sessionListRequestWindow(value: unknown): SessionListRequestWindow {
  if (typeof value === "number" && isValidSessionListRequestWindowDays(value)) return value
  if (typeof value === "string") return parseSessionListRequestWindowInput(value) ?? DEFAULT_SESSION_LIST_REQUEST_WINDOW
  return DEFAULT_SESSION_LIST_REQUEST_WINDOW
}

export function sessionListRequestWindowLabel(value: unknown) {
  const window = sessionListRequestWindow(value)
  if (window === "all") return "all sessions"
  return `${window} day${window === 1 ? "" : "s"}`
}

export function sessionListRequestWindowInput(value: unknown) {
  return String(sessionListRequestWindow(value))
}

export function sessionListRequestStart(value: unknown, now = Date.now()) {
  const window = sessionListRequestWindow(value)
  if (window === "all") return
  return now - window * DAY
}

function isValidSessionListRequestWindowDays(value: number) {
  return Number.isSafeInteger(value) && value > 0 && Number.isSafeInteger(value * DAY)
}
