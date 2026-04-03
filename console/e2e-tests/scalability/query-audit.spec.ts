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

import { expect, test } from "@playwright/test";

import {
  assertNoErrors,
  CLUSTER_ID,
  CLUSTER_NAME,
  clusterDetailUrl,
  clustersListUrl,
  createAuthenticatedContext,
  objectExplorerUrl,
  observeQueries,
  openTab,
  printQuerySummary,
  QueryRecord,
  sinksListUrl,
  sourcesListUrl,
} from "./helpers";

const DATABASE_NAME = process.env.DATABASE_NAME || "materialize";

const AUDIT_DURATION_MS = 120_000;

test.describe.configure({ mode: "serial" });

test.describe("Query Timing Audits", () => {
  test.setTimeout(5 * 60_000);

  test("cluster detail", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];
    const url = clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME);

    console.log(`Auditing queries on: ${url}`);
    const page = await openTab(context, url, records, 0);

    console.log(`Recording queries for ${AUDIT_DURATION_MS / 1000}s...`);
    await page.waitForTimeout(AUDIT_DURATION_MS);

    await assertNoErrors(page, "cluster detail audit");
    printQuerySummary(records);

    const slowQueries = records.filter((r) => r.durationMs > 10_000);
    if (slowQueries.length > 0) {
      console.warn(
        `\n${slowQueries.length} queries took >10s — this will cause health check starvation with multiple tabs.`,
      );
    }

    const healthChecks = records.filter((r) => r.sql.includes("healthCheck"));
    const failedHealthChecks = healthChecks.filter(
      (r) => r.failed || r.status !== 200,
    );
    expect(
      failedHealthChecks.length,
      `${failedHealthChecks.length} health checks failed`,
    ).toBe(0);

    await context.close();
  });

  test("cluster list", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];
    const url = clustersListUrl();

    console.log(`Auditing queries on: ${url}`);
    const page = await openTab(context, url, records, 0);

    console.log(`Recording queries for ${AUDIT_DURATION_MS / 1000}s...`);
    await page.waitForTimeout(AUDIT_DURATION_MS);

    await assertNoErrors(page, "cluster list audit");
    printQuerySummary(records);

    console.log(
      `\nCluster list: ${records.length} total queries, ${records.filter((r) => r.failed || r.status !== 200).length} failures`,
    );
    await context.close();
  });

  test("sources list", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];
    const url = sourcesListUrl();

    console.log(`Auditing queries on: ${url}`);
    const page = await openTab(context, url, records, 0);

    console.log(`Recording queries for ${AUDIT_DURATION_MS / 1000}s...`);
    await page.waitForTimeout(AUDIT_DURATION_MS);

    await assertNoErrors(page, "sources list audit");
    printQuerySummary(records);

    console.log(
      `\nSources list: ${records.length} total queries, ${records.filter((r) => r.failed || r.status !== 200).length} failures`,
    );
    await context.close();
  });

  test("sinks list", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];
    const url = sinksListUrl();

    console.log(`Auditing queries on: ${url}`);
    const page = await openTab(context, url, records, 0);

    console.log(`Recording queries for ${AUDIT_DURATION_MS / 1000}s...`);
    await page.waitForTimeout(AUDIT_DURATION_MS);

    await assertNoErrors(page, "sinks list audit");
    printQuerySummary(records);

    console.log(
      `\nSinks list: ${records.length} total queries, ${records.filter((r) => r.failed || r.status !== 200).length} failures`,
    );
    await context.close();
  });

  test("object explorer", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];
    const page = await context.newPage();
    observeQueries(page, records, 0);

    const url = objectExplorerUrl(DATABASE_NAME);
    console.log(`Auditing queries on: ${url}`);
    await page.goto(url);
    await page
      .locator('input[placeholder="Search"]')
      .waitFor({ state: "visible", timeout: 30_000 });

    console.log(`Recording queries for ${AUDIT_DURATION_MS / 1000}s...`);
    await page.waitForTimeout(AUDIT_DURATION_MS);

    printQuerySummary(records);

    console.log(
      `\nObject explorer: ${records.length} total queries, ${records.filter((r) => r.failed || r.status !== 200).length} failures`,
    );
    await context.close();
  });
});
