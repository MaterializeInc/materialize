// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Per-Page Query Timing Audits
 *
 * Opens a single tab on each major console page and observes every /api/sql
 * request for 2 minutes. Produces a timing report showing which queries are
 * slowest, how often each fires, and whether any fail.
 *
 * These tests establish baselines for each page's query fingerprint:
 * what fires, how often, and how fast.
 *
 * Setup: see helpers.ts for environment configuration.
 */

import { Browser, test } from "@playwright/test";

import {
  assertNoErrors,
  CLUSTER_ID,
  CLUSTER_NAME,
  clusterDetailUrl,
  clustersListUrl,
  createAuthenticatedContext,
  expectNoFailedHealthChecks,
  objectExplorerUrl,
  openTab,
  printQuerySummary,
  sinksListUrl,
  sourcesListUrl,
} from "./helpers";

const DATABASE_NAME = process.env.DATABASE_NAME || "materialize";

const AUDIT_DURATION_MS = 120_000;

async function auditPage(browser: Browser, label: string, url: string) {
  const context = await createAuthenticatedContext(browser);
  console.log(`Auditing queries on: ${url}`);
  const { page, records, finalize } = await openTab(context, url, 0);

  console.log(`Recording queries for ${AUDIT_DURATION_MS / 1000}s...`);
  await page.waitForTimeout(AUDIT_DURATION_MS);

  finalize();
  await assertNoErrors(page, `${label} audit`);
  printQuerySummary(records);

  const slowQueries = records.filter((r) => r.durationMs > 10_000);
  if (slowQueries.length > 0) {
    console.warn(
      `\n${slowQueries.length} queries took >10s — this will cause health check starvation with multiple tabs.`,
    );
  }

  expectNoFailedHealthChecks(records);

  console.log(
    `\n${label}: ${records.length} total queries, ${records.filter((r) => !r.incomplete && (r.failed || r.status !== 200)).length} failures, ${records.filter((r) => r.incomplete).length} incomplete`,
  );

  await context.close();
}

test.describe.configure({ mode: "serial" });

test.describe("Query Timing Audits", () => {
  test.setTimeout(5 * 60_000);

  test("cluster detail", async ({ browser }) => {
    await auditPage(
      browser,
      "Cluster detail",
      clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME),
    );
  });

  test("cluster list", async ({ browser }) => {
    await auditPage(browser, "Cluster list", clustersListUrl());
  });

  test("sources list", async ({ browser }) => {
    await auditPage(browser, "Sources list", sourcesListUrl());
  });

  test("sinks list", async ({ browser }) => {
    await auditPage(browser, "Sinks list", sinksListUrl());
  });

  test("object explorer", async ({ browser }) => {
    await auditPage(
      browser,
      "Object explorer",
      objectExplorerUrl(DATABASE_NAME),
    );
  });
});
