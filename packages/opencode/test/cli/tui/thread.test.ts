import { describe, expect, test } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { tmpdir } from "../../fixture/fixture"
import { createWorkerFetch, resolveThreadDirectory } from "../../../src/cli/cmd/tui/thread"

describe("tui thread", () => {
  async function check(project?: string) {
    await using tmp = await tmpdir({ git: true })
    const link = path.join(path.dirname(tmp.path), path.basename(tmp.path) + "-link")
    const type = process.platform === "win32" ? "junction" : "dir"

    try {
      await fs.symlink(tmp.path, link, type)
      expect(resolveThreadDirectory(project, link, tmp.path)).toBe(tmp.path)
    } finally {
      await fs.rm(link, { recursive: true, force: true }).catch(() => undefined)
    }
  }

  test("uses the real cwd when PWD points at a symlink", async () => {
    await check()
  })

  test("uses the real cwd after resolving a relative project from PWD", async () => {
    await check(".")
  })

  test("times out session abort fetch when worker RPC does not respond", async () => {
    const fetch = createWorkerFetch(
      {
        call: () => new Promise<{ status: number; headers: Record<string, string>; body: string }>(() => {}),
      },
      { abortTimeout: 10 },
    )

    await expect(fetch("http://opencode.internal/session/ses_test/abort", { method: "POST" })).rejects.toThrow(
      "Abort request timed out after 10ms",
    )
  })

  test("calls onAbortTimeout only once when automatic retry also times out", async () => {
    let timeoutCount = 0
    let callCount = 0
    const fetch = createWorkerFetch(
      {
        call: () => {
          callCount += 1
          return new Promise<{ status: number; headers: Record<string, string>; body: string }>(() => {})
        },
      },
      {
        abortTimeout: 10,
        onAbortTimeout: () => {
          timeoutCount += 1
        },
      },
    )

    await expect(fetch("http://opencode.internal/session/ses_test/abort", { method: "POST" })).rejects.toThrow(
      "Abort request timed out after 10ms",
    )
    expect(timeoutCount).toBe(1)
    expect(callCount).toBe(2)
  })

  test("does not apply abort timeout to other worker fetches", async () => {
    let timeoutCount = 0
    const fetch = createWorkerFetch(
      {
        call: async () => {
          await Bun.sleep(20)
          return { status: 200, headers: {}, body: "ok" }
        },
      },
      {
        abortTimeout: 1,
        onAbortTimeout: () => {
          timeoutCount += 1
        },
      },
    )

    expect(await (await fetch("http://opencode.internal/session/ses_test", { method: "GET" })).text()).toBe("ok")
    expect(timeoutCount).toBe(0)
  })

  test("automatically retries abort with the replacement client after timeout recovery", async () => {
    let firstAbortCalls = 0
    const firstClient = {
      call: () => {
        firstAbortCalls += 1
        return new Promise<{ status: number; headers: Record<string, string>; body: string }>(() => {})
      },
    }
    let secondAbortCalls = 0
    const secondClient = {
      call: async () => {
        secondAbortCalls += 1
        return { status: 200, headers: {}, body: "true" }
      },
    }
    let currentClient = firstClient
    const fetch = createWorkerFetch(() => currentClient, {
      abortTimeout: 10,
      onAbortTimeout: () => {
        currentClient = secondClient
      },
    })

    const response = await fetch("http://opencode.internal/session/ses_test/abort", { method: "POST" })
    expect(response.status).toBe(200)
    expect(await response.text()).toBe("true")
    expect(response.headers.get("x-opencode-abort-retried-after-worker-restart")).toBe("true")
    expect(firstAbortCalls).toBe(1)
    expect(secondAbortCalls).toBe(1)
  })

  test("returns successful session abort worker fetch responses", async () => {
    const fetch = createWorkerFetch(
      {
        call: async (method: "fetch", input: { url: string; method: string }) => {
          expect(method).toBe("fetch")
          expect(input.url).toBe("http://opencode.internal/session/ses_test/abort")
          expect(input.method).toBe("POST")
          return { status: 200, headers: {}, body: "true" }
        },
      },
      { abortTimeout: 10 },
    )

    const response = await fetch("http://opencode.internal/session/ses_test/abort", { method: "POST" })
    expect(response.status).toBe(200)
    expect(await response.text()).toBe("true")
  })
})
