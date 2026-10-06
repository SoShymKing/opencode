import { createOpencodeClient } from "@opencode-ai/sdk/v2/client"
import { OpenCode, type SessionPromptInput } from "@opencode-ai/client/promise"
import { EventManifest } from "@opencode-ai/schema/event-manifest"
import type { PromptInput } from "@opencode-ai/schema/prompt-input"
import { SessionInput } from "@opencode-ai/schema/session-input"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { SessionActive } from "@opencode-ai/protocol/groups/session"
import { Schema } from "effect"
import type { ServerConnection } from "@/context/server"
import { decode64 } from "@/utils/base64"

export function authTokenFromCredentials(input: { username?: string; password: string }) {
  return btoa(`${input.username ?? "opencode"}:${input.password}`)
}

export function authFromToken(token: string | null) {
  const decoded = decode64(token ?? undefined)
  if (!decoded) return
  const separator = decoded.indexOf(":")
  if (separator === -1) return
  return {
    username: decoded.slice(0, separator) || "opencode",
    password: decoded.slice(separator + 1),
  }
}

export function createSdkForServer({
  server,
  ...config
}: Omit<NonNullable<Parameters<typeof createOpencodeClient>[0]>, "baseUrl"> & {
  server: ServerConnection.HttpBase
}) {
  const auth = (() => {
    if (!server.password) return
    return {
      Authorization: `Basic ${authTokenFromCredentials({ username: server.username, password: server.password })}`,
    }
  })()

  return createOpencodeClient({
    ...config,
    headers: {
      ...(config.headers instanceof Headers ? Object.fromEntries(config.headers.entries()) : config.headers),
      ...auth,
    },
    baseUrl: server.url,
  })
}

export function createApiForServer(input: {
  server: ServerConnection.HttpBase
  fetch?: typeof globalThis.fetch
}) {
  const config = {
    baseUrl: input.server.url,
    fetch: input.fetch,
    headers: input.server.password
      ? {
          Authorization: `Basic ${authTokenFromCredentials({
            username: input.server.username,
            password: input.server.password,
          })}`,
        }
      : undefined,
  }
  const client = OpenCode.make(config)
  return {
    ...client,
    session: {
      ...client.session,
      async active(options?: Parameters<typeof client.session.active>[0]): Promise<SessionActiveSnapshot> {
        return decodeActive(await client.session.active(options))
      },
      async prompt(
        value: SessionPromptInput,
        options?: Parameters<typeof client.session.prompt>[1],
      ): Promise<typeof SessionInput.Admitted.Encoded> {
        const body = {
          id: value.id ?? undefined,
          prompt: {
            text: value.text,
            files: value.files?.map((file) => ({
              uri: file.uri,
              name: file.name,
              description: file.description,
              source: file.mention,
            })),
            agents: value.agents?.map((agent) => ({ name: agent.name, source: agent.mention })),
          } satisfies typeof PromptInput.Prompt.Encoded,
          delivery: value.delivery ?? undefined,
          resume: value.resume ?? undefined,
        }
        const transport = OpenCode.make({
          ...config,
          fetch: Object.assign(
            (url: string | URL | Request, init?: RequestInit) =>
              (input.fetch ?? globalThis.fetch)(url, { ...init, body: JSON.stringify(body) }),
            { preconnect: (input.fetch ?? globalThis.fetch).preconnect },
          ),
        })
        return decodeAdmission(await transport.session.prompt(value, options))
      },
      async message(...args: Parameters<typeof client.session.message>): Promise<typeof SessionMessage.Message.Encoded> {
        return decodeMessage(await client.session.message(...args))
      },
    },
    message: {
      ...client.message,
      async list(...args: Parameters<typeof client.message.list>): Promise<{
        readonly data: readonly (typeof SessionMessage.Message.Encoded)[]
        readonly cursor: { readonly previous?: string; readonly next?: string }
      }> {
        return decodeMessages(await client.message.list(...args))
      },
    },
    event: {
      ...client.event,
      async *subscribe(
        ...args: Parameters<typeof client.event.subscribe>
      ): AsyncGenerator<(typeof EventManifest.Definitions)[number]["Encoded"]> {
        for await (const event of client.event.subscribe(...args)) yield decodeEvent(event)
      },
    },
  }
}

const decodeMessage = Schema.decodeUnknownSync(Schema.toEncoded(SessionMessage.Message))
const decodeMessages = Schema.decodeUnknownSync(
  Schema.Struct({
    data: Schema.Array(Schema.toEncoded(SessionMessage.Message)),
    cursor: Schema.Struct({ previous: Schema.optional(Schema.String), next: Schema.optional(Schema.String) }),
  }),
)
const decodeAdmission = Schema.decodeUnknownSync(Schema.toEncoded(SessionInput.Admitted))
const decodeActive = Schema.decodeUnknownSync(Schema.Record(Schema.String, Schema.toEncoded(SessionActive)))
const decodeEvent = Schema.decodeUnknownSync(Schema.toEncoded(Schema.Union(EventManifest.Definitions)))

export type ServerApi = ReturnType<typeof createApiForServer>
export type SessionActiveSnapshot = Record<string, typeof SessionActive.Encoded>
