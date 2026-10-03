import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const banner = (page: Page) => page.locator("#consensus .banner");

/** Fails the test on any console error or uncaught exception. */
function watchErrors(page: Page) {
  const errors: string[] = [];
  page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
  page.on("pageerror", (e) => errors.push(e.message));
  return errors;
}

async function leaderOf(page: Page) {
  const text = await banner(page).innerText();
  return /leader (n\d)/.exec(text)?.[1] ?? null;
}

test("boots real Python, elects a leader, no console errors", async ({ page }) => {
  const errors = watchErrors(page);
  const t0 = Date.now();
  await page.goto("/");
  await expect(banner(page)).toContainText("Quorum", { timeout: 20_000 });
  expect(Date.now() - t0).toBeLessThan(20_000);
  await expect(page.locator("#consensus .node")).toHaveCount(5);
  expect(errors).toEqual([]);
});

test("killing the leader elects a different one", async ({ page }) => {
  await page.goto("/");
  await expect(banner(page)).toContainText("Quorum");
  const first = await leaderOf(page);
  await page.getByRole("button", { name: "Kill the leader" }).click();
  await expect(page.getByRole("button", { name: new RegExp(`^${first}, crashed`) })).toBeVisible();
  await expect.poll(async () => {
    const now = await leaderOf(page);
    return now && now !== first;
  }, { timeout: 30_000 }).toBeTruthy();
  await expect(banner(page)).toContainText("4 of 5");
});

test("a 2|3 split leaves only the majority able to commit", async ({ page }) => {
  await page.goto("/");
  await expect(banner(page)).toContainText("Quorum");
  await page.getByRole("button", { name: /Split network/ }).click();
  await expect(banner(page)).toContainText(/3 of 5 nodes reach leader n[345]/, { timeout: 30_000 });
  await page.getByRole("button", { name: "Heal the network" }).click();
  await expect(banner(page)).toContainText("5 of 5", { timeout: 30_000 });
});

test("writes reach every node's log and commit", async ({ page }) => {
  await page.goto("/");
  await expect(banner(page)).toContainText("Quorum");
  await page.getByRole("button", { name: "Write a value" }).click();
  await expect(page.locator('#consensus .chip.committed:has-text("x=1")')).toHaveCount(5, { timeout: 20_000 });
});

test("a node can be crashed and restarted from the keyboard", async ({ page }) => {
  await page.goto("/");
  await expect(banner(page)).toContainText("Quorum");
  const node = page.locator("#consensus .node").first();
  await node.focus();
  await page.keyboard.press("Enter");
  await expect(node).toHaveAttribute("aria-label", /crashed/);
  await page.keyboard.press("Enter");
  await expect(node).not.toHaveAttribute("aria-label", /crashed/);
});

test("sharded world loses no agent when the busiest server dies", async ({ page }) => {
  await page.goto("/");
  await page.locator("#world").scrollIntoViewIfNeeded();
  const world = page.locator("#world .banner");
  await expect(world).toContainText("8/8", { timeout: 40_000 });
  await page.getByRole("button", { name: "Kill the busiest server" }).click();
  await expect(page.locator("#world .server.down")).toHaveCount(1);
  // every shard gets a new leader among the survivors, and nothing is ever lost
  await expect(page.locator("#world .shard-sub", { hasText: "electing" })).toHaveCount(0, { timeout: 40_000 });
  await expect(page.locator("#world .shard-sub", { hasText: "led by" })).toHaveCount(4);
  for (let i = 0; i < 10; i++) {
    await expect(world).toContainText("lost 0");
    await page.waitForTimeout(500);
  }
  await expect(world).toContainText("8/8");
});

test("training chart shows all three recorded runs", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator(".chart path")).toHaveCount(3);
  await expect(page.locator(".series li")).toHaveCount(3);
  await expect(page.locator(".stat b").nth(0)).toHaveText("0.15");
  await expect(page.locator(".stat b").nth(2)).toHaveText("1.02");
  await page.getByRole("button", { name: "Replay" }).click();
  await expect(page.getByRole("button", { name: "Pause" })).toBeVisible();
});

test("works with reduced motion", async ({ browser }) => {
  const ctx = await browser.newContext({ reducedMotion: "reduce" });
  const page = await ctx.newPage();
  const errors = watchErrors(page);
  await page.goto("/");
  await expect(banner(page)).toContainText("Quorum");
  expect(errors).toEqual([]);
  await ctx.close();
});

test("no serious accessibility violations", async ({ page }) => {
  await page.goto("/");
  await expect(banner(page)).toContainText("Quorum");
  const { violations } = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa"]).analyze();
  expect(violations.filter((v) => v.impact === "serious" || v.impact === "critical"), JSON.stringify(violations.map((v) => [v.id, v.nodes.length]))).toEqual([]);
});

for (const width of [375, 768, 1280, 1920]) {
  test(`layout holds at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/");
    await expect(banner(page)).toContainText("Quorum");
    expect(await page.evaluate(() => document.documentElement.scrollWidth - innerWidth)).toBeLessThanOrEqual(0);
    await page.screenshot({ path: `screenshots/page-${width}.png`, fullPage: true });
  });
}
