import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Deferred, Effect, Sink, Stream } from "effect"
import { ChildProcessSpawner } from "effect/unstable/process"
import { Agent } from "@/agent/agent"
import { Config } from "@/config/config"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Plugin } from "@/plugin"
import { MessageID, SessionID } from "@/session/schema"
import { createShellToolForTest } from "@/tool/shell"
import { ShellProgress } from "@/tool/shell-progress"
import { Truncate } from "@/tool/truncate"
import { isRecord } from "@/util/record"
import type { Tool } from "@/tool/tool"

export const shellCheckpointNodes = LayerNode.group([
  CrossSpawnSpawner.node,
  FSUtil.node,
  Plugin.node,
  Truncate.node,
  Config.node,
  Agent.node,
  RuntimeFlags.node,
])

export type SpawnOptions = {
  readonly chunks: readonly string[]
  readonly exitCode?: number
  readonly hang?: boolean
  readonly drained?: Deferred.Deferred<void>
  readonly release?: Deferred.Deferred<void>
  readonly exitBeforeDrain?: boolean
  readonly killDefect?: string
}

type ExecuteOptions = SpawnOptions & {
  readonly context?: Tool.Context
  readonly interval?: number
  readonly timeout?: number
}

function chunkSpawner(options: SpawnOptions) {
  const encoder = new TextEncoder()
  return ChildProcessSpawner.make(() =>
    Effect.gen(function* () {
      const exited = yield* Deferred.make<void>()
      if (options.exitBeforeDrain) yield* Deferred.succeed(exited, undefined)
      const output = options.release
        ? Stream.concat(
            Stream.fromIterable(options.chunks.slice(0, 1)),
            Stream.concat(
              Stream.fromEffect(
                options.drained ? Deferred.succeed(options.drained, undefined) : Effect.void,
              ).pipe(Stream.drain),
              Stream.concat(
                Stream.fromEffect(Deferred.await(options.release)).pipe(Stream.drain),
                Stream.fromIterable(options.chunks.slice(1)),
              ),
            ),
          ).pipe(Stream.map((chunk) => encoder.encode(chunk)))
        : Stream.fromIterable(options.chunks).pipe(Stream.map((chunk) => encoder.encode(chunk)))
      const drained = Stream.fromEffect(
        options.drained && !options.release ? Deferred.succeed(options.drained, undefined) : Effect.void,
      ).pipe(Stream.drain)
      const finish = options.hang
        ? Stream.fromEffect(Deferred.await(exited)).pipe(Stream.drain)
        : Stream.fromEffect(Deferred.succeed(exited, undefined)).pipe(Stream.drain)
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(0),
        exitCode: Deferred.await(exited).pipe(Effect.as(ChildProcessSpawner.ExitCode(options.exitCode ?? 0))),
        isRunning: Deferred.isDone(exited).pipe(Effect.map((done) => !done)),
        kill: () =>
          options.killDefect
            ? Effect.die(options.killDefect)
            : Deferred.succeed(exited, undefined).pipe(Effect.asVoid),
        stdin: Sink.drain,
        stdout: Stream.empty,
        stderr: Stream.empty,
        all: Stream.concat(output, Stream.concat(drained, finish)),
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      })
    }),
  )
}

export const baseShellContext = {
  sessionID: SessionID.make("ses_shell_checkpoint"),
  messageID: MessageID.make("msg_shell_checkpoint"),
  callID: "call_shell_checkpoint",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
}

export function executeShell(options: ExecuteOptions) {
  return Effect.gen(function* () {
    const info = yield* createShellToolForTest(options.interval ?? 60_000)
    const tool = yield* info.init()
    return yield* tool.execute(
      {
        command: "echo deterministic",
        ...(options.timeout === undefined ? {} : { timeout: options.timeout }),
      },
      options.context ?? baseShellContext,
    )
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, chunkSpawner(options)))
}

export function captureShellProgress(updates: ShellProgress.Snapshot[], abort = AbortSignal.any([])) {
  return {
    ...baseShellContext,
    abort,
    metadata: (input: { metadata?: Record<string, unknown> }) =>
      Effect.sync(() => {
        if (!isRecord(input.metadata) || typeof input.metadata.output !== "string") return
        updates.push({
          output: input.metadata.output,
          ...(input.metadata.truncated === true ? { truncated: true } : {}),
          ...(typeof input.metadata.outputPath === "string" ? { outputPath: input.metadata.outputPath } : {}),
        })
      }),
  }
}
