import { test, expect } from "@playwright/test";
import { demoFleet } from "../src/demo";

test("routes are primary and debugger is expandable", async ({ page }) => {
  await page.goto("/?demo");
  await expect(
    page.getByRole("heading", { name: "Routes", exact: true }).first(),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Stop new connections", exact: true }),
  ).toBeDisabled();
  await page
    .locator("summary")
    .filter({ hasText: "Hostname debugger" })
    .click();
  await page.getByRole("button", { name: "Resolve hostname" }).click();
  await expect(page.locator(".resolution")).toContainText(
    "local backend available",
  );
  await page.getByRole("button", { name: /Nodes/ }).first().click();
  await expect(page.locator(".node-card")).toHaveCount(3);
});

test("stop needs no reason, kill confirms scope and reports partial failure", async ({
  page,
}) => {
  const instances = demoFleet();
  for (const i of instances)
    for (const r of i.data?.routes.routes ?? []) {
      r.activeConnections = 3;
      r.admission = "normal";
    }
  instances.push({
    instanceId: "unreachable",
    reachable: false,
    observedAt: 100,
    nodeName: "lost",
    error: "offline",
  });
  const requests: { body: unknown; headers: Record<string, string> }[] = [];
  await page.route("**/api/instances", (r) =>
    r.fulfill({ json: { instances, observedAt: 100 } }),
  );
  await page.route("**/api/instances/*/route", (r) => {
    requests.push({
      body: r.request().postDataJSON(),
      headers: r.request().headers(),
    });
    return r.fulfill({
      status: r.request().url().includes("unreachable") ? 503 : 200,
      json: { ok: true },
    });
  });
  await page.goto("/");
  await page
    .getByRole("button", { name: "Stop new connections", exact: true })
    .click();
  await expect(page.getByRole("status")).toContainText("completed");
  await expect(page.getByRole("dialog")).not.toBeVisible();
  expect(requests[0].body).toEqual({
    routeId: "asorda/production",
    action: "stop",
  });
  expect(requests[0].headers["x-aio-request"]).toBe("1");
  expect(requests[0].headers.authorization).toBeUndefined();
  await page.getByLabel("Action scope").selectOption("all");
  await page
    .getByRole("button", {
      name: "Stop connections and disconnect players",
      exact: true,
    })
    .click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toContainText("known route sessions");
  await expect(dialog).toContainText("unreachable");
  await expect(dialog.getByLabel("Reason", { exact: true })).toHaveCount(0);
  expect(requests).toHaveLength(1);
  await dialog
    .getByRole("button", {
      name: "Stop connections and disconnect players",
      exact: true,
    })
    .click();
  await expect(page.getByRole("status")).toContainText("lost:");
  expect(requests).toHaveLength(instances.filter((i) => !i.stale).length + 1);
});

test("mobile route controls fit viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/?demo");
  await page.getByLabel("Filter routes").fill("construct");
  await page.locator(".route-item").click();
  await expect(page.locator(".route-detail")).toContainText(
    "construct.asorda.net",
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: "test-results/routes-mobile.png",
    fullPage: true,
  });
});

test("expired authentication is explicit", async ({ page }) => {
  await page.route("**/api/instances", (r) =>
    r.fulfill({ status: 401, body: "Unauthenticated" }),
  );
  await page.goto("/");
  await expect(
    page.getByRole("link", { name: "Reauthenticate through Traefik" }),
  ).toBeVisible();
});
