// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Multi-Tab Stress Tests
 *
 * Tests how the console behaves with multiple tabs open simultaneously.
 * Measures query volume, concurrency, and error rates under multi-tab load.
 *
 * Setup: see helpers.ts for environment configuration.
 */

import { test } from "@playwright/test";

import {
  assertNoErrors,
  CLUSTER_ID,
  CLUSTER_NAME,
  clusterDetailUrl,
  clustersListUrl,
  createAuthenticatedContext,
  HOLD_DURATION_MS,
  MAX_TABS,
  OpenedTab,
  openTab,
  printPerTabSummary,
  printQuerySummary,
  QueryRecord,
  sinksListUrl,
  sourcesListUrl,
} from "./helpers";

// Run sequentially — these tests share the same Materialize instance and
// concurrent test runs would skew query latencies and concurrency measurements.
test.describe.configure({ mode: "serial" });

test.describe("Multi-Tab Stress", () => {
  test.setTimeout(10 * 60_000);

  /**
   * Opens tabs one at a time on the Cluster Detail page. After each tab,
   * checks that no existing tab shows an error. Records query timings
   * across all tabs to identify the breaking point.
   */
  test("progressive tab stress on cluster detail", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const tabs: OpenedTab[] = [];
    const url = clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME);

    console.log(`Opening up to ${MAX_TABS} tabs on: ${url}`);

    for (let i = 0; i < MAX_TABS; i++) {
      console.log(`\n--- Opening tab ${i + 1} ---`);
      const tab = await openTab(context, url, i);
      tabs.push(tab);

      await tab.page.waitForTimeout(5_000);

      for (let j = 0; j <= i; j++) {
        await assertNoErrors(
          tabs[j].page,
          `tab ${j + 1} (after opening tab ${i + 1})`,
        );
      }
      const queryCount = tabs.reduce((acc, t) => acc + t.records.length, 0);
      console.log(`Tab ${i + 1}: OK — ${queryCount} total queries so far`);
    }

    console.log(
      `\nHolding ${MAX_TABS} tabs open for ${HOLD_DURATION_MS / 1000}s...`,
    );
    const holdStart = Date.now();
    while (Date.now() - holdStart < HOLD_DURATION_MS) {
      await tabs[0].page.waitForTimeout(10_000);
      for (let j = 0; j < tabs.length; j++) {
        await assertNoErrors(tabs[j].page, `tab ${j + 1} (during hold)`);
      }
    }

    tabs.forEach((t) => t.finalize());
    const records: QueryRecord[] = tabs.flatMap((t) => t.records);
    printQuerySummary(records);
    await context.close();
  });

  /**
   * Opens 6 tabs across different page types — cluster detail, cluster list,
   * sources list, sinks list. Holds them open for 3 minutes.
   */
  test("mixed page multi-tab", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);

    const tabSpecs = [
      {
        url: clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME),
        label: "Cluster Detail",
      },
      {
        url: clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME),
        label: "Cluster Detail 2",
      },
      { url: clustersListUrl(), label: "Clusters List" },
      { url: sourcesListUrl(), label: "Sources List" },
      { url: sinksListUrl(), label: "Sinks List" },
      { url: clustersListUrl(), label: "Clusters List 2" },
    ];

    const tabs: OpenedTab[] = [];

    for (let i = 0; i < tabSpecs.length; i++) {
      console.log(`Opening tab ${i + 1}: ${tabSpecs[i].label}`);
      const tab = await openTab(context, tabSpecs[i].url, i);
      tabs.push(tab);
      await tab.page.waitForTimeout(3_000);
    }

    console.log("\nHolding 6 tabs open for 3 minutes...");
    for (let check = 0; check < 6; check++) {
      await tabs[0].page.waitForTimeout(30_000);
      for (let j = 0; j < tabs.length; j++) {
        await assertNoErrors(
          tabs[j].page,
          `${tabSpecs[j].label} (check ${check + 1}/6)`,
        );
      }
      console.log(`Check ${check + 1}/6: all tabs OK`);
    }

    tabs.forEach((t) => t.finalize());
    const records: QueryRecord[] = tabs.flatMap((t) => t.records);
    printQuerySummary(records);
    printPerTabSummary(
      records,
      tabSpecs.map((t) => t.label),
    );
    await context.close();
  });
});
