// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Page Load & Navigation Timing Tests
 *
 * Measures what the user actually experiences: how long from navigating
 * until meaningful content is visible on screen.
 *
 * Each page has its own cold load test so they can be run individually.
 */

import { Page, test } from "@playwright/test";

import {
  CLUSTER_ID,
  CLUSTER_NAME,
  clusterDetailUrl,
  clustersListUrl,
  createAuthenticatedContext,
  objectExplorerUrl,
  registerQueryListeners,
  sinksListUrl,
  sourcesListUrl,
} from "./helpers";

async function measureUntilVisible(
  page: Page,
  selectors: string | string[],
  timeoutMs = 30_000,
): Promise<number> {
  const start = performance.now();
  const selectorList = Array.isArray(selectors) ? selectors : [selectors];
  await Promise.race(
    selectorList.map((s) =>
      page.locator(s).first().waitFor({ state: "visible", timeout: timeoutMs }),
    ),
  );
  return performance.now() - start;
}

async function measureColdLoad(
  context: Awaited<ReturnType<typeof createAuthenticatedContext>>,
  name: string,
  url: string,
  selector: string | string[],
) {
  const page = await context.newPage();
  const { records, finalize } = registerQueryListeners(page, 0);

  console.log(`\nLoading: ${name}`);
  await page.goto(url);
  const durationMs = await measureUntilVisible(page, selector);
  console.log(`  Content visible in ${Math.round(durationMs)}ms`);

  await page.waitForTimeout(3_000);
  finalize();
  console.log(`  Queries during load: ${records.length}`);

  await page.close();
  return { name, durationMs, queries: records.length };
}

test.describe.configure({ mode: "serial" });

test.describe("Page Load Timing", () => {
  test.setTimeout(5 * 60_000);

  test("cold load: cluster detail", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    await measureColdLoad(
      context,
      "Cluster Detail",
      clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME),
      'text="Resource Usage"',
    );
    await context.close();
  });

  test("cold load: cluster list", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    await measureColdLoad(
      context,
      "Cluster List",
      clustersListUrl(),
      "[data-testid=cluster-table]",
    );
    await context.close();
  });

  test("cold load: sources list", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    await measureColdLoad(context, "Sources List", sourcesListUrl(), [
      "[data-testid=source-table]",
      'text="No available sources"',
    ]);
    await context.close();
  });

  test("cold load: sinks list", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    await measureColdLoad(context, "Sinks List", sinksListUrl(), [
      "[data-testid=sink-table]",
      'text="No available sinks"',
    ]);
    await context.close();
  });

  test("cold load: object explorer", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    await measureColdLoad(
      context,
      "Object Explorer",
      objectExplorerUrl(),
      'input[placeholder="Search"]',
    );
    await context.close();
  });

  /**
   * Navigation flow: cluster list → click row → cluster detail → back → revisit.
   * Tests whether in-app navigation and React Query cache are working.
   */
  test("navigation flow: cluster list to detail", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const page = await context.newPage();
    const { records, finalize } = registerQueryListeners(page, 0);

    const timings: { step: string; durationMs: number; queries: number }[] = [];

    async function measureStep(
      label: string,
      action: () => Promise<void>,
      contentSelector: string | string[],
    ) {
      const queriesBefore = records.length;
      await action();
      const durationMs = await measureUntilVisible(page, contentSelector);
      timings.push({
        step: label,
        durationMs,
        queries: records.length - queriesBefore,
      });
      console.log(
        `  ${label}: ${Math.round(durationMs)}ms, ${records.length - queriesBefore} queries`,
      );
    }

    await measureStep(
      "1. Cold: Cluster List",
      () => page.goto(clustersListUrl()).then(() => {}),
      "[data-testid=cluster-table]",
    );

    await measureStep(
      "2. Click: Cluster Detail",
      () =>
        page.locator("[data-testid=cluster-table] tbody tr").first().click(),
      'text="Resource Usage"',
    );

    await measureStep(
      "3. Back: Cluster List",
      () => page.goto(clustersListUrl()).then(() => {}),
      "[data-testid=cluster-table]",
    );

    await measureStep(
      "4. Click: Cluster Detail (revisit)",
      () =>
        page.locator("[data-testid=cluster-table] tbody tr").first().click(),
      'text="Resource Usage"',
    );

    finalize();

    console.log("\n=== Navigation Flow Results ===");
    console.table(
      Object.fromEntries(
        timings.map((t) => [
          t.step,
          { durationMs: Math.round(t.durationMs), queries: t.queries },
        ]),
      ),
    );

    const cold = timings.find((t) => t.step.includes("2. Click"));
    const cached = timings.find((t) => t.step.includes("4. Click"));
    if (cold && cached) {
      const speedup = Math.round(
        (1 - cached.durationMs / cold.durationMs) * 100,
      );
      console.log(
        `\nCache speedup (Cluster Detail): ${speedup}% faster on revisit`,
      );
      console.log(
        `  Cold: ${Math.round(cold.durationMs)}ms (${cold.queries} queries)`,
      );
      console.log(
        `  Cached: ${Math.round(cached.durationMs)}ms (${cached.queries} queries)`,
      );
    }

    await context.close();
  });
});
