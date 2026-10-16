import { describe, expect, test } from "bun:test"
import { createUiI18n, localizedListSeparator, pluralCategory, useI18n, type UiI18nSource } from "./i18n"

test("formats complete thinking stream labels through the typed plural API", async () => {
  const language = useI18n()
  expect(language.plural("ui.sessionTurn.status.thinkingStreams", 0)).toBe("Thinking(0 streams)")
  expect(language.plural("ui.sessionTurn.status.thinkingStreams", 1)).toBe("Thinking(1 stream)")
  expect(language.plural("ui.sessionTurn.status.thinkingStreams", 11)).toBe("Thinking(11 streams)")
  expect(language.t("ui.sessionTurn.status.thinking")).toBe("Thinking")
  const korean = await import("../i18n/ko")
  expect(korean.dict["ui.sessionTurn.status.thinkingStreams.other"].replace("{{count}}", "11")).toBe(
    "생각 중(11 스트림)",
  )
})

const i18n = (locale: string, translated: string) => {
  const source: UiI18nSource = {
    locale: () => locale,
    t: () => translated,
    plural: () => "",
  }

  return createUiI18n(source)
}

describe("pluralCategory", () => {
  test.each([
    ["en", 0, "other"],
    ["en", 1, "one"],
    ["fr", 0, "one"],
    ["fr", 1_000_000, "many"],
    ["ru", 1, "one"],
    ["ru", 2, "few"],
    ["ru", 5, "many"],
    ["ru", 21, "one"],
    ["ar", 0, "zero"],
    ["ar", 1, "one"],
    ["ar", 2, "two"],
    ["ar", 3, "few"],
    ["ar", 11, "many"],
    ["ar", 100, "other"],
    ["ja", 1, "other"],
  ] as const)("selects %s for %d as %s", (locale, count, expected) => {
    expect(pluralCategory(locale, count)).toBe(expected)
  })
})

describe("dynamic source copy", () => {
  test("keeps runtime copy for English locale tags", () => {
    expect(
      i18n("en-US", "Dictionary copy").tDynamic("dialog.usageExceeded.freeTier.title", "Runtime {{name}}", {
        name: "copy",
      }),
    ).toBe("Runtime copy")
  })

  test("uses dictionary copy for non-English locales", () => {
    expect(i18n("fr", "Texte traduit").tDynamic("dialog.usageExceeded.freeTier.title", "Runtime copy")).toBe(
      "Texte traduit",
    )
  })
})

describe("localizedListSeparator", () => {
  test("uses locale list punctuation and conjunctions", () => {
    expect(localizedListSeparator("en", 1, 3)).toBe(", ")
    expect(localizedListSeparator("en", 2, 3)).toBe(", and ")
    expect(localizedListSeparator("de", 1, 2)).toBe(" und ")
  })

  test("formats a complete localized list", () => {
    expect(i18n("en", "").list(["Read", "Search", "List"])).toBe("Read, Search, List")
    expect(i18n("en-US", "").list(["Read", "Search"])).toBe("Read, Search")
    expect(i18n("en", "").listSeparator(2, 3)).toBe(",")
    expect(i18n("de", "").list(["Lesen", "Suchen"])).toBe("Lesen und Suchen")
  })
})
