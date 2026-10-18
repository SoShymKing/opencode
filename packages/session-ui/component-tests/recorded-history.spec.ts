import { expect, story } from "../../storybook/playwright/story"

for (const direction of ["ltr", "rtl"]) {
  story(`renders recorded history without live jobs in ${direction}`, async ({ mount, page }) => {
    const requests: string[] = []
    page.on("request", (request) => {
      if (/\/api\/|javascript:|file:\/\//.test(request.url())) requests.push(request.url())
    })

    const root = await mount("current-session-timeline-rows--recorded-history", {
      globals: { direction, locale: direction === "rtl" ? "ar" : "en" },
    })

    const shell = root.locator('[data-component="session-recorded-shell-message"]')
    const progress = root.locator('[data-timeline-part-id="recorded-progress"]')
    const trigger = shell.locator('[data-slot="collapsible-trigger"]')

    await expect(shell).toContainText("Outcome unknown")
    await expect(progress).toContainText("Saved progress, not a live job")
    await expect(root.getByText("Recorded file unavailable", { exact: true })).toHaveCount(2)
    await expect(root.locator('[data-component="attachment-card"] a, [data-component="attachment-card"] img, [data-component="attachment-card"][data-clickable]')).toHaveCount(0)
    await expect(root.locator('[data-component="session-shell-message"]')).toContainText("/native/repo")
    await expect(root.locator('[data-component="text-shimmer"][data-active="true"]')).toHaveCount(0)

    await trigger.click()
    await expect(trigger).toHaveAttribute("aria-expanded", "false")
    await trigger.focus()
    await page.keyboard.press("Enter")
    await expect(trigger).toHaveAttribute("aria-expanded", "true")

    const output = shell.getByRole("region")
    await expect(output).toContainText("Earlier recorded output omitted")
    expect((await output.textContent())?.length).toBeLessThan(66_000)
    await output.focus()
    await page.keyboard.press("ControlOrMeta+End")
    await expect.poll(() => output.evaluate((element) => element.scrollTop)).toBeGreaterThan(0)

    for (const width of [1280, 390]) {
      await page.setViewportSize({ width, height: 900 })
      await page.evaluate(() => document.fonts.ready.then(() => undefined))
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
      expect(await output.evaluate((element) => getComputedStyle(element.closest('[data-component="bash-output"]') ?? element).direction)).toBe("ltr")
    }

    expect(requests).toEqual([])
    await expect(root.getByTestId("recorded-live-reads")).toHaveText("0")
  })
}

story("keeps recorded shells in collapsible shell history groups", async ({ mount }) => {
  const root = await mount("current-session-timeline-rows--recorded-history", { args: { grouped: true } })
  const group = root.locator('[data-timeline-part-ids="recorded-shell"]')
  const trigger = group.locator(':scope > [data-component="collapsible"] > [data-slot="collapsible-trigger"]')

  await expect(trigger).toHaveAttribute("aria-expanded", "false")
  await trigger.click()
  const shell = group.locator('[data-component="session-recorded-shell-message"]')
  await expect(shell).toContainText("Outcome unknown")
  await trigger.click()
  await expect(trigger).toHaveAttribute("aria-expanded", "false")
  await trigger.click()
  await expect(shell).toBeVisible()
})
