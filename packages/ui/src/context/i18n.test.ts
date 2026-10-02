import { describe, expect, test } from "bun:test"
import { pluralCategory, useI18n } from "./i18n"

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
