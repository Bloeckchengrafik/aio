import { test, expect } from "@playwright/test";
import { demoFleet } from "../src/demo";

test("demo groups rollout instances and resolves readable routing paths", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/?demo");
  await expect(page.getByText("Read-only demo", { exact: true })).toBeVisible();
  await expect(page.locator(".node-card")).toHaveCount(3);
  await expect(page.locator(".instance-row")).toHaveCount(4);
  await expect(page.getByText("Draining", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Resolve hostname" }).click();
  await expect(page.locator(".resolution")).toContainText(
    "Exact match → asorda/production → local backend available",
  );
  await expect(
    page.getByRole("button", { name: "Pause admission" }),
  ).toBeDisabled();
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({
    path: "test-results/overview-desktop.png",
    fullPage: true,
  });
  expect(errors).toEqual([]);
});

test("live operator dialog pins instance, submits reason and same-origin marker", async ({
  page,
}) => {
  const instances = demoFleet();
  const requests: {
    headers: Record<string, string>;
    body: Record<string, unknown>;
  }[] = [];
  await page.route("**/api/instances", (route) =>
    route.fulfill({
      json: { instances, observedAt: Math.floor(Date.now() / 1000) },
    }),
  );
  await page.route("**/api/instances/*/admission", async (route) => {
    requests.push({
      headers: route.request().headers(),
      body: route.request().postDataJSON(),
    });
    instances[0].data!.status.accepting = false;
    await route.fulfill({ json: instances[0].data!.status });
  });
  await page.goto("/");
  await expect(page.locator("input[type=password]")).toHaveCount(0);
  await page
    .getByLabel("Observe", { exact: true })
    .selectOption("nbg-01-current");
  await page.getByRole("button", { name: "Pause admission" }).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toContainText("nbg-01-current");
  await expect(
    dialog.getByRole("button", { name: "Pause new connections", exact: true }),
  ).toBeDisabled();
  await dialog
    .getByLabel("Reason", { exact: true })
    .fill("Investigating ingress update");
  await dialog
    .getByRole("button", { name: "Pause new connections", exact: true })
    .click();
  await expect(dialog).not.toBeVisible();
  await expect(page.getByRole("status")).toContainText("completed on nbg-01");
  expect(requests).toHaveLength(1);
  expect(requests[0].headers.authorization).toBeUndefined();
  expect(requests[0].headers["x-aio-request"]).toBe("1");
  expect(requests[0].body).toEqual({
    accepting: false,
    reason: "Investigating ingress update",
  });
  await expect(
    page.getByRole("button", { name: "Resume admission" }),
  ).toBeVisible();
});

test("mobile layout remains within viewport and navigation works", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/?demo");
  await expect(page.locator(".node-card")).toHaveCount(3);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.getByRole("button", { name: "Routes & backends" }).click();
  await expect(
    page.getByRole("heading", { name: "Find the path." }),
  ).toBeVisible();
  await page.getByLabel("Filter routes").fill("construct");
  await page.locator(".route-item").click();
  await expect(page.locator(".route-detail")).toContainText(
    "construct.asorda.net",
  );
  await expect(page.locator(".route-detail")).toContainText(
    "No ready local endpoints",
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: "test-results/routes-mobile.png",
    fullPage: true,
  });
});

test("expired ingress authentication and stale instances are explicit", async ({
  page,
}) => {
  await page.route("**/api/instances", (route) =>
    route.fulfill({ status: 401, body: "Unauthenticated" }),
  );
  await page.goto("/");
  await expect(
    page.getByRole("link", { name: "Reauthenticate through Traefik" }),
  ).toBeVisible();
  await page.unroute("**/api/instances");
  const stale = {
    instanceId: "failed-node",
    nodeName: "nbg-04",
    reachable: false,
    stale: true,
    lastHeartbeat: 100,
    observedAt: 140,
    error: "registration expired or removed",
  };
  await page.route("**/api/instances", (route) =>
    route.fulfill({ json: { instances: [stale], observedAt: 140 } }),
  );
  await page.getByRole("button", { name: "Refresh fleet" }).first().click();
  await expect(page.getByText("Stale · nbg-04")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Pause admission" }),
  ).toHaveCount(0);
});
