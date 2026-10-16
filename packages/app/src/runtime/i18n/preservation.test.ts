import { expect, test } from "bun:test"
import { Schema } from "effect"
import { dict } from "./en"

const decode = Schema.decodeUnknownSync(Schema.Struct({ dict: Schema.Record(Schema.String, Schema.String) }))

const locales =
  "am ar az bg bn br bs ca cs da de dv dz el en es et fa fi fo fr hi hr hu hy id is it ja ka km ko lo lt lv mk mn ms my ne nl no pa pl ro ru si sk sl sq sr sv tg th tk tr uk ur uz vi zh zht".split(" ")

test("retains all 768 reviewed ws locale values after old dictionary paths retire", async () => {
  const keys = Object.keys(dict).filter((key) =>
    /^(?:session\.(?:activity\.|input\.pending$)|command\.session\.(?:(?:previous|next)\.unseen$|archived\.list$)|dialog\.archivedSessions\.|toast\.session\.restore\.)/.test(key),
  )

  const values = await Promise.all(
    locales.map(async (locale) => {
      const target = decode(await import(`./${locale}.ts`)).dict

      return keys
        .filter((key) => locale === "en" || locale === "ko" || !key.startsWith("session."))
        .map((key) => [`${locale}.ts`, key, target[key]])
    }),
  )

  const entries = values.flat().sort(
    (left, right) => left[0].localeCompare(right[0], "en") || left[1].localeCompare(right[1], "en"),
  )

  expect(entries).toHaveLength(768)
  expect(new Bun.CryptoHasher("sha256").update(JSON.stringify(entries)).digest("hex")).toBe(
    "60bec5daa4f99ba4b392eafdd4e41f67b3f2120a206c5ffb186e8cb5e6c7ade4",
  )
})
