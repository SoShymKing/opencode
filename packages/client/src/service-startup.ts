import type { DiscoverOptions, Info } from "./service.js"
import { same, type LocalService } from "./service-probe.js"
import { matchesVersion } from "./service-version.js"

export function decodeRegistration(input: unknown): Info | undefined {
  if (typeof input !== "object" || input === null) return
  if (!("url" in input) || typeof input.url !== "string") return
  if (!("pid" in input) || typeof input.pid !== "number" || !Number.isInteger(input.pid) || input.pid <= 0) return
  const id = "id" in input ? input.id : undefined
  const version = "version" in input ? input.version : undefined
  const password = "password" in input ? input.password : undefined
  const startup = "startup" in input ? input.startup : undefined
  if (id !== undefined && typeof id !== "string") return
  if (version !== undefined && typeof version !== "string") return
  if (password !== undefined && typeof password !== "string") return
  if (startup !== undefined && startup !== "database-preparation") return
  return { id, version, url: input.url, pid: input.pid, password, startup }
}

export function preparing(info: Info | undefined, service?: LocalService, options: DiscoverOptions = {}) {
  if (info?.startup !== "database-preparation" || !info.id) return false
  if (service?.compatible && same(info, service.info)) {
    if (service.state === "failed") return false
    if (service.state === "ready" && matchesVersion(service.version, options)) return false
  }
  try {
    process.kill(info.pid, 0)
    return true
  } catch (error) {
    if (error instanceof Error && "code" in error) {
      if (error.code === "ESRCH") return false
      if (error.code === "EPERM") return true
    }
    throw error
  }
}

export function ordinaryTime(start: number) {
  let previous = start
  let paused = false
  let elapsed = 0
  return (now: number, protectedOwner: boolean) => {
    if (!paused && !protectedOwner) elapsed += Math.max(0, now - previous)
    previous = now
    paused = protectedOwner
    return elapsed
  }
}
