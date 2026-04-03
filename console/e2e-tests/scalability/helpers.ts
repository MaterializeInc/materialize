// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Shared infrastructure for console scalability tests.
 *
 * Provides query observation, timing reports, concurrency analysis,
 * SQL labeling, and authentication helpers.
 */

import { BrowserContext, Page, Request } from "@playwright/test";

export const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
export const REGION_SLUG =
  process.env.REGION_SLUG || "local-flexible-deployment";
export const MAX_TABS = parseInt(process.env.MAX_TABS || "6");
export const HOLD_DURATION_MS = parseInt(process.env.HOLD_DURATION || "120000");

export const CLUSTER_NAME = process.env.CLUSTER_NAME || "mz_catalog_server";
export const CLUSTER_ID = process.env.CLUSTER_ID || "s2";

// Frontegg auth — only needed for staging/cloud environments.
const E2E_EMAIL = process.env.E2E_EMAIL;
const E2E_PASSWORD = process.env.E2E_PASSWORD;
const NEEDS_AUTH = !!E2E_EMAIL && !!E2E_PASSWORD;

export interface QueryRecord {
  timestamp: string;
  sql: string;
  durationMs: number;
  /** Time waiting in browser queue + DNS + connection setup before request was sent */
  queuedMs: number;
  /** Time to first byte — how long the server spent processing */
  serverMs: number;
  /** Time to download the full response body */
  downloadMs: number;
  responseBytes: number;
  status: number;
  failed: boolean;
  tabIndex: number;
}

export function clusterDetailUrl(
  clusterId: string,
  clusterName: string,
): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/clusters/${clusterId}/${clusterName}`;
}

export function clustersListUrl(): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/clusters`;
}

export function sourcesListUrl(): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/sources`;
}

export function sinksListUrl(): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/sinks`;
}

export function objectExplorerUrl(databaseName?: string): string {
  const base = `${BASE_URL}/regions/${REGION_SLUG}/objects`;
  return databaseName ? `${base}/${databaseName}` : base;
}

const ERROR_PATTERNS = [
  "trouble reaching your environment",
  "An error occurred loading cluster data",
  "Health check failed",
  "Try again",
] as const;

/** Locates known console error messages on the page. */
function errorLocator(page: Page) {
  return page.locator(ERROR_PATTERNS.map((e) => `text="${e}"`).join(", "));
}

/** Throws if any known error message is visible on the page. */
export async function assertNoErrors(page: Page, label: string) {
  const errors = errorLocator(page);
  const count = await errors.count();
  if (count > 0) {
    const text = await errors.first().textContent();
    throw new Error(`[${label}] Error visible on page: "${text}"`);
  }
}

/**
 * SQL query labels grouped by page.
 *
 * Each label maps to a regex that uniquely identifies the query in the
 * compiled SQL sent to /api/sql. Patterns are derived from the Kysely
 * query builders in console/src/api/materialize/ and verified against
 * compiled snapshots in __snapshots__/ directories.
 *
 * When adding a new label, put it in the correct page group and ensure
 * the regex is specific enough to not match other queries.
 */

/** Queries that fire on every page (health checks, auth). */
export const GLOBAL_QUERIES: Record<string, RegExp> = {
  healthCheck: /mz_version/,
  canCreateObjects: /mz_show_my_schema_privileges/,
  rbacCheck: /mz_is_superuser/,
};

/** Queries fired on the cluster detail page. */
export const CLUSTER_DETAIL_QUERIES: Record<string, RegExp> = {
  clusterFreshness: /lag_history_with_temporal_filter/,
  replicaUtilizationOverview: /mz_console_cluster_utilization_overview/,
  replicaUtilizationHistory:
    /date_bin.*replica_metrics_history|mz_cluster_replica_metrics_history/,
  deploymentLineage:
    /mz_cluster_deployment_lineage|current_deployment_cluster_id/,
  largestMaintainedQueries:
    /mz_dataflow_arrangement_sizes.*mz_compute_exports.*export_id/,
  arrangementMemory:
    /mz_objects.*mz_compute_exports.*mz_dataflow_arrangement_sizes/,
  indexesList: /mz_indexes.*mz_show_indexes/,
  largestClusterReplica: /mz_hydration_statuses.*heap_limit|bool_and.*hydrated/,
  replicasWithUtilization:
    /cpu_percent.*disk_percent|mz_cluster_replica_utilization/,
  materializationLag: /mz_wallclock_global_lag_recent_history/,
  maxReplicasPerCluster: /SHOW max_replicas_per_cluster/,
  availableClusterSizes: /mz_cluster_replica_sizes.*not like/,
  clusterReplicas:
    /mz_cluster_replicas.*mz_cluster_replica_sizes.*heap_limit(?!.*hydrated)/,
};

/** Queries fired on the cluster list page. */
export const CLUSTER_LIST_QUERIES: Record<string, RegExp> = {
  clustersList: /mz_clusters.*jsonArrayFrom|mz_clusters.*latest_status_update/,
};

/** Queries fired on the sources list page. */
export const SOURCES_LIST_QUERIES: Record<string, RegExp> = {
  sourcesList: /mz_sources.*mz_source_statuses/,
  sourceStatistics: /mz_source_statistics_with_history/,
};

/** Queries fired on the sinks list page. */
export const SINKS_LIST_QUERIES: Record<string, RegExp> = {
  sinksList: /mz_sinks.*mz_sink_statuses/,
  sinkStatistics: /mz_sink_statistics/,
  databaseList: /from.*mz_databases.*order by.*name/,
  databaseDetails: /mz_databases.*mz_object_lifetimes.*occurred_at/,
};

/**
 * All labels in match order. Page-specific queries are checked first
 * since they contain more distinctive SQL patterns. Global queries
 * (healthCheck, rbacCheck) are checked last as fallbacks — their
 * patterns (mz_version, mz_is_superuser) also appear as subexpressions
 * in page-specific queries.
 */
const SQL_LABEL_ENTRIES: [string, RegExp][] = [
  ...Object.entries(CLUSTER_DETAIL_QUERIES),
  ...Object.entries(CLUSTER_LIST_QUERIES),
  ...Object.entries(SOURCES_LIST_QUERIES),
  ...Object.entries(SINKS_LIST_QUERIES),
  ...Object.entries(GLOBAL_QUERIES),
];

function labelSql(sql: string): string {
  for (const [label, pattern] of SQL_LABEL_ENTRIES) {
    if (pattern.test(sql)) return label;
  }
  return sql.substring(0, 80);
}

/** Extracts SQL from a request body and maps each query to a human-readable label. */
function parseSql(request: Request): string {
  try {
    const body = JSON.parse(request.postData() || "{}");
    const queries: string[] = body.queries
      ? body.queries.map((q: { query: string }) => q.query)
      : body.query
        ? [body.query]
        : [];
    if (queries.length === 0) return "unknown";

    const labels = queries.map((q) => labelSql(q));
    const unique = labels.filter((l, i) => i === 0 || l !== labels[i - 1]);
    return unique.join(" + ");
  } catch {
    return "parse-error";
  }
}

/**
 * Attaches request/response listeners to record all /api/sql requests on a page.
 * Uses events instead of route interception so queries are never blocked.
 */
export function observeQueries(
  page: Page,
  records: QueryRecord[],
  tabIndex: number,
) {
  const pending = new Map<Request, { sql: string; startTime: number }>();

  page.on("request", (request) => {
    if (!request.url().includes("/api/sql")) return;
    if (request.method() !== "POST") return;
    pending.set(request, {
      sql: parseSql(request),
      startTime: Date.now(),
    });
  });

  page.on("requestfinished", async (request) => {
    const entry = pending.get(request);
    if (!entry) return;
    pending.delete(request);

    const response = await request.response();
    const timing = request.timing();
    let responseBytes = 0;
    try {
      const body = await response?.body();
      responseBytes = body?.length ?? 0;
    } catch {
      // Response body may not be available for failed/cancelled requests
    }

    const queuedMs = Math.max(0, timing.requestStart);
    const serverMs = Math.max(0, timing.responseStart - timing.requestStart);
    const downloadMs = Math.max(0, timing.responseEnd - timing.responseStart);

    records.push({
      timestamp: new Date().toISOString(),
      sql: entry.sql,
      durationMs: Date.now() - entry.startTime,
      queuedMs,
      serverMs,
      downloadMs,
      responseBytes,
      status: response?.status() ?? 0,
      failed: false,
      tabIndex,
    });
  });

  page.on("requestfailed", (request) => {
    const entry = pending.get(request);
    if (!entry) return;
    pending.delete(request);

    records.push({
      timestamp: new Date().toISOString(),
      sql: entry.sql,
      durationMs: Date.now() - entry.startTime,
      queuedMs: 0,
      serverMs: 0,
      downloadMs: 0,
      responseBytes: 0,
      status: 0,
      failed: true,
      tabIndex,
    });
  });
}

/** Waits for the console shell to render, then settles for 2s. */
export async function waitForPageLoad(page: Page, timeoutMs = 30_000) {
  await page
    .waitForSelector("[data-testid=page-layout]", { timeout: timeoutMs })
    .catch(() => {
      console.warn(
        `  page-layout not found within ${timeoutMs}ms — page may not have loaded`,
      );
    });
  await page.waitForTimeout(2_000);
}

/** Opens a new browser tab, attaches query observers, navigates to the URL, and waits for load. */
export async function openTab(
  browser: BrowserContext,
  url: string,
  records: QueryRecord[],
  tabIndex: number,
): Promise<Page> {
  const page = await browser.newPage();
  observeQueries(page, records, tabIndex);
  await page.goto(url);
  console.log(`  [tab ${tabIndex}] final URL: ${page.url()}`);
  await waitForPageLoad(page);
  return page;
}

/**
 * Signs in via Frontegg and saves browser state so subsequent tabs are pre-authenticated.
 * Similar to TestContext.signIn in e2e-tests/util.ts but decoupled from appConfig
 * so it works in both cloud and flexible-deployment modes.
 */
async function signIn(page: Page): Promise<string> {
  if (!E2E_EMAIL || !E2E_PASSWORD) {
    throw new Error(
      "E2E_EMAIL and E2E_PASSWORD env vars are required for staging/cloud auth",
    );
  }
  console.log(`  Signing in as ${E2E_EMAIL}...`);
  await page.goto(BASE_URL);
  await page.waitForSelector("[data-test-id=input-identifier]", {
    timeout: 60_000,
  });
  await page.fill("[name=identifier]", E2E_EMAIL);
  await page.press("[name=identifier]", "Enter");
  await page.waitForSelector("[name=password]");
  await page.fill("[name=password]", E2E_PASSWORD);
  await page.press("[name=password]", "Enter");
  await page.waitForSelector("[data-testid=page-layout]", { timeout: 60_000 });
  console.log("  Sign-in complete.");
  // Save storage state so new tabs in the same context are already authenticated.
  const statePath = "e2e-tests/scalability-auth-state.json";
  await page.context().storageState({ path: statePath });
  return statePath;
}

/** Creates a browser context, signing in via Frontegg first if E2E_EMAIL/E2E_PASSWORD are set. */
export async function createAuthenticatedContext(browser: {
  newContext: (opts?: object) => Promise<BrowserContext>;
}): Promise<BrowserContext> {
  if (!NEEDS_AUTH) {
    return browser.newContext();
  }

  const setupContext = await browser.newContext();
  const setupPage = await setupContext.newPage();
  const statePath = await signIn(setupPage);
  await setupContext.close();

  return browser.newContext({ storageState: statePath });
}

/**
 * Prints a full report: query summary table, timing breakdown (queue/server/download),
 * health check diagnostics, and peak concurrency analysis.
 */
export function printQuerySummary(records: QueryRecord[]) {
  if (records.length === 0) {
    console.log("No queries recorded.");
    return;
  }

  const grouped = new Map<
    string,
    {
      count: number;
      totalMs: number;
      maxMs: number;
      totalQueueMs: number;
      maxQueueMs: number;
      totalServerMs: number;
      maxServerMs: number;
      totalDownloadMs: number;
      maxDownloadMs: number;
      totalBytes: number;
      failures: number;
    }
  >();
  for (const r of records) {
    const key = r.sql;
    const entry = grouped.get(key) || {
      count: 0,
      totalMs: 0,
      maxMs: 0,
      totalQueueMs: 0,
      maxQueueMs: 0,
      totalServerMs: 0,
      maxServerMs: 0,
      totalDownloadMs: 0,
      maxDownloadMs: 0,
      totalBytes: 0,
      failures: 0,
    };
    entry.count++;
    entry.totalMs += r.durationMs;
    entry.maxMs = Math.max(entry.maxMs, r.durationMs);
    entry.totalQueueMs += r.queuedMs;
    entry.maxQueueMs = Math.max(entry.maxQueueMs, r.queuedMs);
    entry.totalServerMs += r.serverMs;
    entry.maxServerMs = Math.max(entry.maxServerMs, r.serverMs);
    entry.totalDownloadMs += r.downloadMs;
    entry.maxDownloadMs = Math.max(entry.maxDownloadMs, r.downloadMs);
    entry.totalBytes += r.responseBytes;
    if (r.failed || r.status !== 200) entry.failures++;
    grouped.set(key, entry);
  }

  // Main summary table
  console.log("\n=== Query Summary ===");
  console.log(
    `${"Query".padEnd(50)} ${"Cnt".padStart(4)} ${"Avg".padStart(7)} ${"Max".padStart(7)} ${"Err".padStart(4)}`,
  );
  console.log("-".repeat(75));

  const sorted = [...grouped.entries()].sort((a, b) => b[1].maxMs - a[1].maxMs);
  for (const [label, s] of sorted) {
    console.log(
      `${label.padEnd(50)} ${String(s.count).padStart(4)} ${(Math.round(s.totalMs / s.count) + "ms").padStart(7)} ${(s.maxMs + "ms").padStart(7)} ${String(s.failures).padStart(4)}`,
    );
  }

  // Timing breakdown table: queue vs server vs download
  console.log("\n=== Timing Breakdown (avg / max ms) ===");
  console.log(
    `${"Query".padEnd(50)} ${"Queue".padStart(12)} ${"Server".padStart(12)} ${"Download".padStart(12)}`,
  );
  console.log("-".repeat(88));

  for (const [label, s] of sorted) {
    const avgQ = Math.round(s.totalQueueMs / s.count);
    const avgS = Math.round(s.totalServerMs / s.count);
    const avgD = Math.round(s.totalDownloadMs / s.count);
    console.log(
      `${label.padEnd(50)} ${`${avgQ}/${s.maxQueueMs}`.padStart(12)} ${`${avgS}/${s.maxServerMs}`.padStart(12)} ${`${avgD}/${s.maxDownloadMs}`.padStart(12)}`,
    );
  }

  // Health check summary
  const healthChecks = records.filter((r) => r.sql.includes("healthCheck"));
  if (healthChecks.length > 0) {
    const maxHc = Math.max(...healthChecks.map((r) => r.durationMs));
    const maxHcQueue = Math.max(...healthChecks.map((r) => r.queuedMs));
    const maxHcServer = Math.max(...healthChecks.map((r) => r.serverMs));
    const failedHc = healthChecks.filter(
      (r) => r.failed || r.status !== 200,
    ).length;
    console.log(
      `\nHealth check: ${healthChecks.length} calls, max ${maxHc}ms (queue: ${maxHcQueue}ms, server: ${maxHcServer}ms), ${failedHc} failures`,
    );
    if (maxHcQueue > 2000) {
      console.log(
        `  !! Health checks queued up to ${maxHcQueue}ms — browser connection pool starvation`,
      );
    }
    if (maxHcServer > 5000) {
      console.log(
        `  !! Health checks waited ${maxHcServer}ms for server response — mz_catalog_server saturated`,
      );
    }
  }

  // Concurrency analysis
  printConcurrencySummary(records);

  console.log(`\nTotal requests: ${records.length}`);
  console.log(
    `Failed requests: ${records.filter((r) => r.failed || r.status !== 200).length}`,
  );
}

/**
 * Compute peak concurrency: the maximum number of requests that were in-flight
 * simultaneously across all tabs.
 */
function printConcurrencySummary(records: QueryRecord[]) {
  if (records.length === 0) return;

  const events: { time: number; delta: 1 | -1; sql: string }[] = [];
  for (const r of records) {
    const start = new Date(r.timestamp).getTime() - r.durationMs;
    const end = new Date(r.timestamp).getTime();
    events.push({ time: start, delta: 1, sql: r.sql });
    events.push({ time: end, delta: -1, sql: r.sql });
  }
  events.sort((a, b) => a.time - b.time || a.delta - b.delta);

  let current = 0;
  let peak = 0;
  let peakTime = 0;
  for (const e of events) {
    current += e.delta;
    if (current > peak) {
      peak = current;
      peakTime = e.time;
    }
  }

  const peakRequests = records.filter((r) => {
    const start = new Date(r.timestamp).getTime() - r.durationMs;
    const end = new Date(r.timestamp).getTime();
    return start <= peakTime && end > peakTime;
  });

  console.log(`\n=== Concurrency ===`);
  console.log(
    `Peak concurrent requests: ${peak} (at ${new Date(peakTime).toISOString()})`,
  );
  if (peakRequests.length > 0) {
    const tabSet = new Set(peakRequests.map((r) => r.tabIndex));
    console.log(
      `  Across ${tabSet.size} tab(s): ${[...tabSet].map((t) => `tab ${t + 1}`).join(", ")}`,
    );
    const queryBreakdown = new Map<string, number>();
    for (const r of peakRequests) {
      queryBreakdown.set(r.sql, (queryBreakdown.get(r.sql) || 0) + 1);
    }
    for (const [sql, count] of [...queryBreakdown.entries()].sort(
      (a, b) => b[1] - a[1],
    )) {
      console.log(`  ${String(count).padStart(3)}x ${sql}`);
    }
  }
}

/** Prints query count and failure count per tab. */
export function printPerTabSummary(
  records: QueryRecord[],
  tabLabels: string[],
) {
  console.log("\n=== Per-Tab Query Count ===");
  for (let i = 0; i < tabLabels.length; i++) {
    const tabRecords = records.filter((r) => r.tabIndex === i);
    const failed = tabRecords.filter(
      (r) => r.failed || r.status !== 200,
    ).length;
    console.log(
      `Tab ${i + 1} (${tabLabels[i]}): ${tabRecords.length} queries, ${failed} failures`,
    );
  }
}
