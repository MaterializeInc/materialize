/**
 * Console Scalability Tests
 *
 * These tests measure how the console behaves under load — multiple tabs,
 * heavy queries, health check starvation. They run against a real Materialize
 * instance (local via workload-replay, or staging).
 *
 * Setup (local):
 *   Terminal 1: bin/mzcompose --find workload-replay run default workload_prod_analytics.yml --runtime 900 --no-run-queries
 *   Terminal 2: CONSOLE_DEPLOYMENT_MODE='flexible-deployment' yarn start
 *   Terminal 3: yarn playwright test scalability.spec.ts --project=chromium
 *
 * Setup (staging):
 *   export BASE_URL=https://staging.console.materialize.com
 *   export REGION_SLUG=aws/eu-west-1
 *   export E2E_EMAIL=<your-staging-email>
 *   export E2E_PASSWORD=<your-staging-password>
 *   export CLUSTER_NAME=<cluster-name>
 *   export CLUSTER_ID=<cluster-id>
 *   yarn playwright test scalability.spec.ts --project=chromium
 */

import { test, expect, Page, BrowserContext, Request } from "@playwright/test";

// --- Configuration ---

const BASE_URL = process.env.BASE_URL || "http://localhost:3000";
const REGION_SLUG = process.env.REGION_SLUG || "local-flexible-deployment";
const MAX_TABS = parseInt(process.env.MAX_TABS || "6");
const HOLD_DURATION_MS = parseInt(process.env.HOLD_DURATION || "120000");

const CLUSTER_NAME = process.env.CLUSTER_NAME || "mz_catalog_server";
const CLUSTER_ID = process.env.CLUSTER_ID || "s2";

// Frontegg auth — only needed for staging/cloud environments.
const E2E_EMAIL = process.env.E2E_EMAIL;
const E2E_PASSWORD = process.env.E2E_PASSWORD;
const NEEDS_AUTH = !!E2E_EMAIL && !!E2E_PASSWORD;

// --- Types ---

interface QueryRecord {
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

// --- Helpers ---

function clusterDetailUrl(clusterId: string, clusterName: string): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/clusters/${clusterId}/${clusterName}`;
}

function clustersListUrl(): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/clusters`;
}

function sourcesListUrl(): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/sources`;
}

function sinksListUrl(): string {
  return `${BASE_URL}/regions/${REGION_SLUG}/sinks`;
}

/** Error strings the console shows when things go wrong. */
const ERROR_PATTERNS = [
  "trouble reaching your environment",
  "An error occurred loading cluster data",
  "Health check failed",
  "Try again",
] as const;

function errorLocator(page: Page) {
  return page.locator(ERROR_PATTERNS.map((e) => `text="${e}"`).join(", "));
}

/**
 * Sign in via Frontegg on the given page. Only needed for staging/cloud.
 * After sign-in, saves browser storage state so subsequent tabs can reuse it.
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

async function waitForPageLoad(page: Page, timeoutMs = 30_000) {
  await page
    .waitForSelector("[data-testid=page-layout]", { timeout: timeoutMs })
    .catch(() => {
      console.warn(`  ⚠ page-layout not found within ${timeoutMs}ms — page may not have loaded`);
    });
  await page.waitForTimeout(2_000);
}

/**
 * Map distinctive SQL patterns to human-readable hook/function names.
 * Checked in order — more specific patterns first.
 */
const SQL_LABELS: [RegExp, string][] = [
  [/mz_version/, "healthCheck"],
  [/lag_history_with_temporal_filter/, "clusterFreshness (useClusterFreshness)"],
  [/replica_history.*replica_metrics_history|mz_cluster_replica_metrics_history/, "replicaUtilizationHistory"],
  [/mz_console_cluster_utilization_overview/, "replicaUtilizationOverview"],
  [/mz_cluster_deployment_lineage|current_deployment_cluster_id/, "deploymentLineage"],
  [/mz_dataflow_arrangement_sizes.*mz_schemas|mz_schemas.*mz_dataflow_arrangement_sizes|mz_arrangement_heap_size_raw/, "largestMaintainedQueries"],
  [/mz_dataflow_arrangement_sizes/, "arrangementMemory (useArrangementsMemory)"],
  [/mz_indexes/, "indexesList (useIndexesList)"],
  [/mz_hydration_statuses.*mz_cluster_replica_sizes|mz_cluster_replica_sizes.*mz_hydration_statuses/, "largestClusterReplica"],
  [/"owners".*"isOwner"|"isOwner".*"owners"/, "clusters (useClusters)"],
  [/mz_cluster_replicas.*mz_cluster_replica_statuses/, "replicasWithUtilization"],
  [/mz_wallclock_global_lag_recent_history/, "materializationLag"],
  [/mz_is_superuser/, "rbacCheck"],
  [/canCreateCluster/, "canCreateCluster"],
  [/useCanCreateObjects/, "canCreateObjects"],
  [/mz_cluster_replica_sizes/, "availableClusterSizes"],
  [/mz_materialized_views/, "materializedViews"],
  [/mz_sources/, "sources"],
  [/mz_sinks/, "sinks"],
];

function labelSql(sql: string): string {
  for (const [pattern, label] of SQL_LABELS) {
    if (pattern.test(sql)) return label;
  }
  return sql.substring(0, 80);
}

/** Extract SQL from a request's post body and label each query. */
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
    // Deduplicate adjacent labels for batched requests
    const unique = labels.filter((l, i) => i === 0 || l !== labels[i - 1]);
    return unique.join(" + ");
  } catch {
    return "parse-error";
  }
}

/**
 * Observe all /api/sql requests on a page without blocking them.
 * Uses request/response events instead of route interception, so queries
 * flow through normally even if they take minutes.
 */
function observeQueries(page: Page, records: QueryRecord[], tabIndex: number) {
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
      // Response body may not be available
    }

    // timing.requestStart: ms from start until request was sent (queue + DNS + connect)
    // timing.responseStart: ms from start until first response byte arrived
    // timing.responseEnd: ms from start until response fully downloaded
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

/** Open a new tab navigated to the given URL. */
async function openTab(
  browser: BrowserContext,
  url: string,
  records: QueryRecord[],
  tabIndex: number
): Promise<Page> {
  const page = await browser.newPage();
  observeQueries(page, records, tabIndex);
  await page.goto(url);
  console.log(`  [tab ${tabIndex}] final URL: ${page.url()}`);
  await waitForPageLoad(page);
  return page;
}

async function assertNoErrors(page: Page, label: string) {
  const errors = errorLocator(page);
  const count = await errors.count();
  if (count > 0) {
    const text = await errors.first().textContent();
    throw new Error(`[${label}] Error visible on page: "${text}"`);
  }
}

function printQuerySummary(records: QueryRecord[]) {
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

  // Concurrency analysis: how many requests were in-flight at the same time?
  printConcurrencySummary(records);

  console.log(`\nTotal requests: ${records.length}`);
  console.log(
    `Failed requests: ${records.filter((r) => r.failed || r.status !== 200).length}`,
  );
}

/**
 * Compute peak concurrency: the maximum number of requests that were in-flight
 * simultaneously across all tabs. High concurrency = browser connection pool
 * contention and server queueing.
 */
function printConcurrencySummary(records: QueryRecord[]) {
  if (records.length === 0) return;

  // Build timeline events: each request creates a "start" and "end" event
  const events: { time: number; delta: 1 | -1; sql: string }[] = [];
  for (const r of records) {
    // r.timestamp is when the request finished; derive the actual start time
    const start = new Date(r.timestamp).getTime() - r.durationMs;
    const end = new Date(r.timestamp).getTime();
    events.push({ time: start, delta: 1, sql: r.sql });
    events.push({ time: end, delta: -1, sql: r.sql });
  }
  events.sort((a, b) => a.time - b.time || a.delta - b.delta);

  // Walk the timeline to find peak concurrency
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

  // Find which requests were in-flight at peak
  const peakRequests = records.filter((r) => {
    const start = new Date(r.timestamp).getTime() - r.durationMs;
    const end = new Date(r.timestamp).getTime();
    return start <= peakTime && end > peakTime;
  });

  console.log(`\n=== Concurrency ===`);
  console.log(`Peak concurrent requests: ${peak} (at ${new Date(peakTime).toISOString()})`);
  if (peakRequests.length > 0) {
    const tabSet = new Set(peakRequests.map((r) => r.tabIndex));
    console.log(`  Across ${tabSet.size} tab(s): ${[...tabSet].map((t) => `tab ${t + 1}`).join(", ")}`);
    // Show what was running at peak
    const queryBreakdown = new Map<string, number>();
    for (const r of peakRequests) {
      queryBreakdown.set(r.sql, (queryBreakdown.get(r.sql) || 0) + 1);
    }
    for (const [sql, count] of [...queryBreakdown.entries()].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${String(count).padStart(3)}x ${sql}`);
    }
  }
}

// --- Tests ---

test.describe("Console Scalability", () => {
  test.setTimeout(5 * 60_000);

  // Auth state path — populated by authenticateIfNeeded(), reused across tabs.
  let authStatePath: string | undefined;

  /**
   * If targeting staging/cloud, sign in via Frontegg before the first test.
   * The stored browser state (cookies, localStorage) is then passed to
   * every new browser context so all tabs are pre-authenticated.
   */
  async function createAuthenticatedContext(
    browser: { newContext: (opts?: object) => Promise<BrowserContext> },
  ): Promise<BrowserContext> {
    if (!NEEDS_AUTH) {
      return browser.newContext();
    }

    // First call: sign in and save state.
    if (!authStatePath) {
      const setupContext = await browser.newContext();
      const setupPage = await setupContext.newPage();
      authStatePath = await signIn(setupPage);
      await setupContext.close();
    }

    // All subsequent contexts reuse the saved auth state.
    return browser.newContext({ storageState: authStatePath });
  }

  /**
   * Test 1: Progressive Tab Stress
   *
   * Opens tabs one at a time on the Cluster Detail page. After each tab,
   * checks that no existing tab shows an error. Records query timings
   * across all tabs to identify the breaking point.
   */
  test("progressive tab stress on cluster detail", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];
    const pages: Page[] = [];
    const url = clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME);

    console.log(`Opening up to ${MAX_TABS} tabs on: ${url}`);

    for (let i = 0; i < MAX_TABS; i++) {
      console.log(`\n--- Opening tab ${i + 1} ---`);
      const page = await openTab(context, url, records, i);
      pages.push(page);

      await page.waitForTimeout(5_000);

      for (let j = 0; j <= i; j++) {
        await assertNoErrors(pages[j], `tab ${j + 1} (after opening tab ${i + 1})`);
      }
      console.log(`Tab ${i + 1}: OK — ${records.length} total queries so far`);
    }

    console.log(`\nHolding ${MAX_TABS} tabs open for ${HOLD_DURATION_MS / 1000}s...`);
    const holdStart = Date.now();
    while (Date.now() - holdStart < HOLD_DURATION_MS) {
      await pages[0].waitForTimeout(10_000);
      for (let j = 0; j < pages.length; j++) {
        await assertNoErrors(pages[j], `tab ${j + 1} (during hold)`);
      }
    }

    printQuerySummary(records);
    await context.close();
  });

  /**
   * Test 2: Query Timing Audit
   *
   * Opens a single Cluster Detail tab and observes every /api/sql request
   * for 2 minutes. Produces a timing report showing which queries are slowest,
   * how often each fires, and whether any fail.
   */
  test("query timing audit on cluster detail", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];
    const url = clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME);

    console.log(`Auditing queries on: ${url}`);
    const page = await openTab(context, url, records, 0);

    console.log("Recording queries for 2 minutes...");
    await page.waitForTimeout(120_000);

    await assertNoErrors(page, "single tab audit");
    printQuerySummary(records);

    const slowQueries = records.filter((r) => r.durationMs > 10_000);
    if (slowQueries.length > 0) {
      console.warn(
        `\n⚠ ${slowQueries.length} queries took >10s — this will cause health check starvation with multiple tabs.`
      );
    }

    const healthChecks = records.filter((r) => r.sql.includes("healthCheck"));
    const failedHealthChecks = healthChecks.filter(
      (r) => r.failed || r.status !== 200
    );
    expect(
      failedHealthChecks.length,
      `${failedHealthChecks.length} health checks failed on a single tab`
    ).toBe(0);

    await context.close();
  });

  /**
   * Test 3: Mixed Page Multi-Tab
   *
   * Opens 6 tabs across different page types — cluster detail, cluster list,
   * sources list, sinks list. Holds them open for 3 minutes.
   */
  test("mixed page multi-tab", async ({ browser }) => {
    const context = await createAuthenticatedContext(browser);
    const records: QueryRecord[] = [];

    const urls = [
      { url: clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME), label: "Cluster Detail" },
      { url: clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME), label: "Cluster Detail 2" },
      { url: clustersListUrl(), label: "Clusters List" },
      { url: sourcesListUrl(), label: "Sources List" },
      { url: sinksListUrl(), label: "Sinks List" },
      { url: clustersListUrl(), label: "Clusters List 2" },
    ];

    const pages: Page[] = [];

    for (let i = 0; i < urls.length; i++) {
      console.log(`Opening tab ${i + 1}: ${urls[i].label}`);
      const page = await openTab(context, urls[i].url, records, i);
      pages.push(page);
      await page.waitForTimeout(3_000);
    }

    console.log("\nHolding 6 tabs open for 3 minutes...");
    for (let check = 0; check < 6; check++) {
      await pages[0].waitForTimeout(30_000);
      for (let j = 0; j < pages.length; j++) {
        await assertNoErrors(pages[j], `${urls[j].label} (check ${check + 1}/6)`);
      }
      console.log(`Check ${check + 1}/6: all tabs OK`);
    }

    printQuerySummary(records);

    console.log("\n=== Per-Tab Query Count ===");
    for (let i = 0; i < urls.length; i++) {
      const tabRecords = records.filter((r) => r.tabIndex === i);
      const failed = tabRecords.filter((r) => r.failed || r.status !== 200).length;
      console.log(
        `Tab ${i + 1} (${urls[i].label}): ${tabRecords.length} queries, ${failed} failures`
      );
    }

    await context.close();
  });
});
