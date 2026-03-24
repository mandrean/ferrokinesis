import { expect, test, type Page } from "@playwright/test";

test("guided browser flow works end to end", async ({ page }) => {
  await page.goto("/");

  await expect(
    page.getByRole("heading", { name: "Try Kinesis in your browser" }),
  ).toBeVisible();
  await expect(page.getByTestId("banner")).toContainText("Fresh in-memory instance ready");

  await runPreset(page, "preset-create-stream");
  await expect(page.getByTestId("response-status")).toHaveText("200");

  await runPreset(page, "preset-put-record");
  await expect(page.getByTestId("response-body")).toContainText('"SequenceNumber"');

  await runPreset(page, "preset-get-shard-iterator");
  await expect(page.getByTestId("response-body")).toContainText('"ShardIterator"');

  await runPreset(page, "preset-get-records");
  await expect(page.getByTestId("response-body")).toContainText(
    '"Data": "aGVsbG8gZnJvbSBmZXJyb2tpbmVzaXM="',
  );

  await runPreset(page, "preset-list-streams");
  await expect(page.getByTestId("response-body")).toContainText("browser-demo-stream");
});

test("reset recovers after an initial wasm bootstrap failure", async ({ page }) => {
  let aborted = false;
  await page.route("**/*.wasm", async (route) => {
    if (!aborted) {
      aborted = true;
      await route.abort();
      return;
    }

    await route.continue();
  });

  await page.goto("/");

  await expect(page.getByTestId("banner")).toContainText("Failed to fetch");

  await page.getByTestId("reset-state").click();
  await expect(page.getByTestId("banner")).toContainText("State reset");

  await runPreset(page, "preset-create-stream");
  await expect(page.getByTestId("response-status")).toHaveText("200");
});

async function runPreset(page: Page, presetTestId: string) {
  await page.getByTestId(presetTestId).click();
  await page.getByTestId("send-request").click();
}
