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
        call: () => new Promise(() => {}),
      },
      { abortTimeout: 10 },
    )

    await expect(fetch("http://opencode.internal/session/ses_test/abort", { method: "POST" })).rejects.toThrow(
      "Abort request timed out after 10ms",
    )
  })

  test("does not apply abort timeout to other worker fetches", async () => {
    const fetch = createWorkerFetch(
      {
        call: async () => {
          await Bun.sleep(20)
          return { status: 200, headers: {}, body: "ok" }
        },
      },
      { abortTimeout: 1 },
    )

    expect(await (await fetch("http://opencode.internal/session/ses_test", { method: "GET" })).text()).toBe("ok")
  })

  test("returns successful session abort worker fetch responses", async () => {
    const fetch = createWorkerFetch(
      {
        call: async (method, input) => {
          expect(method).toBe("fetch")
          expect(input.requestID).toBe("abort-0")
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
