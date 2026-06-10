// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BrowserContext, expect, Page, Request } from "@playwright/test";

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
  /** In-flight at audit end — distinct from `failed`. */
  incomplete: boolean;
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

const ERROR_TEXT_PATTERNS = [
  /trouble reaching your environment/i,
  /an error occurred/i,
  /health check failed/i,
  /try again/i,
] as const;

export async function assertNoErrors(page: Page, label: string) {
  const errorBox = page.locator("[data-testid=error-box]");
  if ((await errorBox.count()) > 0) {
    const text = (await errorBox.first().textContent()) ?? "(no text)";
    throw new Error(`[${label}] ErrorBox visible on page: "${text.trim()}"`);
  }

  for (const pattern of ERROR_TEXT_PATTERNS) {
    const matches = page.getByText(pattern);
    if ((await matches.count()) > 0) {
      const text = (await matches.first().textContent()) ?? "(no text)";
      throw new Error(
        `[${label}] Error text visible on page: "${text.trim()}"`,
      );
    }
  }
}

/**
 * Regex labels for V1 useSqlTyped queries (which don't carry a `query_key`
 * URL param). Matched in order — list more specific patterns before less
 * specific ones, since `hasSuperUserPrivileges()` injects `mz_is_superuser`
 * into many auth queries.
 */
const SQL_LABELS: [string, RegExp][] = [
  ["healthCheck", /mz_version/i],
  ["canCreateObjects", /mz_show_my_schema_privileges/i],
  ["canCreateCluster", /'CREATECLUSTER'/i],
  ["rbacCheck", /mz_is_superuser/i],
];

/**
 * `hashKey` from @tanstack/react-query JSON-stringifies (not hashes), so
 * `query_key` is a readable array of `{scope, ...}` parts. Joined: `clusters.list`.
 */
function labelFromQueryKey(request: Request): string | undefined {
  try {
    const url = new URL(request.url());
    const raw = url.searchParams.get("query_key");
    if (!raw) return undefined;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return undefined;
    const scopes = parsed
      .map((part) =>
        part && typeof part === "object" && typeof part.scope === "string"
          ? part.scope
          : undefined,
      )
      .filter((s): s is string => !!s);
    if (scopes.length === 0) return undefined;
    return scopes.join(".");
  } catch {
    return undefined;
  }
}

function labelFromSql(sql: string): string | undefined {
  for (const [label, pattern] of SQL_LABELS) {
    if (pattern.test(sql)) return label;
  }
  return undefined;
}

/**
 * Labeling order:
 *   1. `query_key` URL param (V2 executeSqlV2 callers)
 *   2. SQL body regex match (V1 useSqlTyped)
 *   3. Truncated SQL
 */
export function labelRequest(request: Request): string {
  const fromKey = labelFromQueryKey(request);
  if (fromKey) return fromKey;

  let sql = "";
  try {
    const body = JSON.parse(request.postData() || "{}");
    const queries: string[] = body.queries
      ? body.queries.map((q: { query: string }) => q.query)
      : body.query
        ? [body.query]
        : [];
    if (queries.length === 0) return "unknown";

    const labels = queries.map((q) => labelFromSql(q) ?? q.substring(0, 80));
    const unique = labels.filter((l, i) => i === 0 || l !== labels[i - 1]);
    sql = unique.join(" + ");
  } catch {
    return "parse-error";
  }
  return sql;
}

export interface RegisteredListeners {
  records: QueryRecord[];
  /** Emits `incomplete: true` records for still-pending requests. Call before page close. */
  finalize: () => void;
}

export function registerQueryListeners(
  page: Page,
  tabIndex: number,
): RegisteredListeners {
  const records: QueryRecord[] = [];
  const pending = new Map<Request, { sql: string; startTime: number }>();

  page.on("request", (request) => {
    if (!request.url().includes("/api/sql")) return;
    if (request.method() !== "POST") return;
    pending.set(request, {
      sql: labelRequest(request),
      startTime: performance.now(),
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
      durationMs: performance.now() - entry.startTime,
      queuedMs,
      serverMs,
      downloadMs,
      responseBytes,
      status: response?.status() ?? 0,
      failed: false,
      incomplete: false,
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
      durationMs: performance.now() - entry.startTime,
      queuedMs: 0,
      serverMs: 0,
      downloadMs: 0,
      responseBytes: 0,
      status: 0,
      failed: true,
      incomplete: false,
      tabIndex,
    });
  });

  const finalize = () => {
    const now = performance.now();
    const nowIso = new Date().toISOString();
    for (const [, entry] of pending) {
      records.push({
        timestamp: nowIso,
        sql: entry.sql,
        durationMs: now - entry.startTime,
        queuedMs: 0,
        serverMs: 0,
        downloadMs: 0,
        responseBytes: 0,
        status: 0,
        failed: false,
        incomplete: true,
        tabIndex,
      });
    }
    pending.clear();
  };

  return { records, finalize };
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

export interface OpenedTab extends RegisteredListeners {
  page: Page;
}

/** Caller must invoke `finalize()` and merge `records` at the end of the test. */
export async function openTab(
  browser: BrowserContext,
  url: string,
  tabIndex: number,
): Promise<OpenedTab> {
  const page = await browser.newPage();
  const listeners = registerQueryListeners(page, tabIndex);
  await page.goto(url);
  console.log(`  [tab ${tabIndex}] final URL: ${page.url()}`);
  await waitForPageLoad(page);
  return { page, ...listeners };
}

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
  // First-time logins on a localhost origin trigger the Quickstart welcome
  // modal, which routes the user to the SQL Shell and prevents any other
  // page from rendering. Dismiss it before saving storage state so the
  // dismissal sticks for every test in the same context.
  const closeQuickstart = page.getByRole("button", {
    name: /close quickstart/i,
  });
  if (await closeQuickstart.count()) {
    await closeQuickstart
      .first()
      .click()
      .catch(() => {});
    console.log("  Dismissed Quickstart welcome modal.");
  }
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

interface GroupedStats {
  count: number;
  avgMs: number;
  maxMs: number;
  avgQueue: number;
  maxQueue: number;
  avgServer: number;
  maxServer: number;
  avgDownload: number;
  maxDownload: number;
  totalBytes: number;
  failures: number;
  incomplete: number;
}

function groupRecords(records: QueryRecord[]): Map<string, GroupedStats> {
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
      incomplete: number;
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
      incomplete: 0,
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
    if (r.incomplete) entry.incomplete++;
    else if (r.failed || r.status !== 200) entry.failures++;
    grouped.set(key, entry);
  }

  const result = new Map<string, GroupedStats>();
  for (const [label, s] of grouped) {
    result.set(label, {
      count: s.count,
      avgMs: Math.round(s.totalMs / s.count),
      maxMs: Math.round(s.maxMs),
      avgQueue: Math.round(s.totalQueueMs / s.count),
      maxQueue: Math.round(s.maxQueueMs),
      avgServer: Math.round(s.totalServerMs / s.count),
      maxServer: Math.round(s.maxServerMs),
      avgDownload: Math.round(s.totalDownloadMs / s.count),
      maxDownload: Math.round(s.maxDownloadMs),
      totalBytes: s.totalBytes,
      failures: s.failures,
      incomplete: s.incomplete,
    });
  }
  return result;
}

export function printQuerySummary(records: QueryRecord[]) {
  if (records.length === 0) {
    console.log("No queries recorded.");
    return;
  }

  const grouped = groupRecords(records);
  const sorted = [...grouped.entries()].sort((a, b) => b[1].maxMs - a[1].maxMs);

  console.log("\n=== Query Summary ===");
  console.table(
    Object.fromEntries(
      sorted.map(([label, s]) => [
        label,
        {
          count: s.count,
          avgMs: s.avgMs,
          maxMs: s.maxMs,
          errors: s.failures,
          incomplete: s.incomplete,
        },
      ]),
    ),
  );

  console.log("\n=== Timing Breakdown (avg/max ms) ===");
  console.table(
    Object.fromEntries(
      sorted.map(([label, s]) => [
        label,
        {
          queue: `${s.avgQueue}/${s.maxQueue}`,
          server: `${s.avgServer}/${s.maxServer}`,
          download: `${s.avgDownload}/${s.maxDownload}`,
        },
      ]),
    ),
  );

  const healthChecks = records.filter((r) => r.sql.includes("healthCheck"));
  if (healthChecks.length > 0) {
    const maxHc = healthChecks.reduce((m, r) => Math.max(m, r.durationMs), 0);
    const maxHcQueue = healthChecks.reduce(
      (m, r) => Math.max(m, r.queuedMs),
      0,
    );
    const maxHcServer = healthChecks.reduce(
      (m, r) => Math.max(m, r.serverMs),
      0,
    );
    const failedHc = healthChecks.filter(
      (r) => !r.incomplete && (r.failed || r.status !== 200),
    ).length;
    const incompleteHc = healthChecks.filter((r) => r.incomplete).length;
    console.log(
      `\nHealth check: ${healthChecks.length} calls, max ${Math.round(maxHc)}ms (queue: ${Math.round(maxHcQueue)}ms, server: ${Math.round(maxHcServer)}ms), ${failedHc} failures, ${incompleteHc} incomplete`,
    );
    if (maxHcQueue > 2000) {
      console.log(
        `  !! Health checks queued up to ${Math.round(maxHcQueue)}ms — browser connection pool starvation`,
      );
    }
    if (maxHcServer > 5000) {
      console.log(
        `  !! Health checks waited ${Math.round(maxHcServer)}ms for server response — mz_catalog_server saturated`,
      );
    }
  }

  printConcurrencySummary(records);
  printConcurrencyHistogram(records);

  const failed = records.filter(
    (r) => !r.incomplete && (r.failed || r.status !== 200),
  ).length;
  const incomplete = records.filter((r) => r.incomplete).length;
  console.log(`\nTotal requests: ${records.length}`);
  console.log(`Failed requests: ${failed}`);
  console.log(`Incomplete (still in flight at end): ${incomplete}`);
}

/**
 * Sweep-line over start/end events to find the peak in-flight request count.
 * Each request emits a +1 event at start and -1 at end; running sum of the
 * sorted event stream gives concurrency at any moment.
 */
function printConcurrencySummary(records: QueryRecord[]) {
  if (records.length === 0) return;

  const events: { time: number; delta: 1 | -1; sql: string }[] = [];
  for (const r of records) {
    if (r.incomplete) continue;
    const start = new Date(r.timestamp).getTime() - r.durationMs;
    const end = new Date(r.timestamp).getTime();
    events.push({ time: start, delta: 1, sql: r.sql });
    events.push({ time: end, delta: -1, sql: r.sql });
  }
  // Tiebreak by delta so ends (-1) process before starts (+1) at the same
  // timestamp — otherwise two sequential requests sharing an end/start ms
  // would briefly look like 2x concurrency.
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
    if (r.incomplete) return false;
    const start = new Date(r.timestamp).getTime() - r.durationMs;
    const end = new Date(r.timestamp).getTime();
    return start <= peakTime && end > peakTime;
  });

  console.log(`\n=== Concurrency (peak) ===`);
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

const HISTOGRAM_BUCKET_MS = 10_000;

interface ConcurrencyBucket {
  /** Highest number of requests in flight at any instant in the bucket. */
  peak: number;
  /** SQL labels of the requests active at the moment of the peak. */
  activeQueries: string[];
}

/** Sweep-line: walk +1/-1 events in time order, tracking peak concurrency
 * per fixed-size bucket along with the queries active at each peak. */
function calculateConcurrencyHistogram(
  records: QueryRecord[],
  bucketSizeMs: number = HISTOGRAM_BUCKET_MS,
): ConcurrencyBucket[] {
  // Incomplete requests have no end time; can't place them on the curve.
  const completed = records.filter((r) => !r.incomplete);
  if (completed.length === 0) return [];

  // Build a timeline of every moment concurrency changes. Each request
  // contributes two: one "start" (a request joins the in-flight set) and
  // one "end" (a request leaves it).
  type Event = { time: number; type: "start" | "end"; index: number };
  const events: Event[] = [];
  for (let i = 0; i < completed.length; i++) {
    const end = new Date(completed[i].timestamp).getTime();
    const start = end - completed[i].durationMs;
    events.push({ time: start, type: "start", index: i });
    events.push({ time: end, type: "end", index: i });
  }
  events.sort((a, b) =>
    a.time !== b.time ? a.time - b.time : a.type === "end" ? -1 : 1,
  );

  const windowStart = events[0].time;
  const windowEnd = events[events.length - 1].time;
  const bucketCount = Math.ceil((windowEnd - windowStart) / bucketSizeMs);
  if (bucketCount <= 1) return [];

  const bucketIndexFor = (t: number) =>
    Math.min(Math.floor((t - windowStart) / bucketSizeMs), bucketCount - 1);

  const buckets: ConcurrencyBucket[] = Array.from(
    { length: bucketCount },
    () => ({ peak: 0, activeQueries: [] }),
  );

  const currentlyActive = new Set<number>();
  const recordPeakAt = (bucketIndex: number) => {
    const bucket = buckets[bucketIndex];
    if (currentlyActive.size > bucket.peak) {
      bucket.peak = currentlyActive.size;
      bucket.activeQueries = [...currentlyActive].map((i) => completed[i].sql);
    }
  };

  let currentBucketIndex = bucketIndexFor(events[0].time);
  const advanceBucketTo = (target: number) => {
    while (currentBucketIndex < target) {
      currentBucketIndex++;
      recordPeakAt(currentBucketIndex);
    }
  };

  for (const event of events) {
    advanceBucketTo(bucketIndexFor(event.time));
    if (event.type === "start") {
      currentlyActive.add(event.index);
    } else {
      currentlyActive.delete(event.index);
    }
    recordPeakAt(currentBucketIndex);
  }

  return buckets;
}

/** Group active queries by sql label and format as `Nx label, Mx label, ...`
 * sorted by frequency. */
function summarizeActiveQueries(queries: string[]): string {
  const counts = new Map<string, number>();
  for (const sql of queries) counts.set(sql, (counts.get(sql) ?? 0) + 1);
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([sql, n]) => `${n}x ${sql}`)
    .join(", ");
}

/** Render a precomputed concurrency histogram as vertical ASCII bars (time
 * on the X axis, peak count on the Y axis), followed by per-bucket query
 * breakdowns for any window with peak > 1. */
function renderConcurrencyHistogram(
  buckets: ConcurrencyBucket[],
  bucketSizeMs: number = HISTOGRAM_BUCKET_MS,
) {
  if (buckets.length === 0) return;

  const maxPeak = Math.max(1, ...buckets.map((b) => b.peak));
  const yLabelWidth = String(maxPeak).length;
  const colWidth = 4;
  const padCol = (s: string) => s.padEnd(colWidth);
  const yIndent = " ".repeat(yLabelWidth);

  console.log(
    `\n=== Peak concurrent requests per ${bucketSizeMs / 1000}s window ===`,
  );

  // Vertical bars, one row per peak level (top to bottom).
  for (let row = maxPeak; row >= 1; row--) {
    const bars = buckets.map((b) => padCol(b.peak >= row ? "█" : " ")).join("");
    console.log(`${String(row).padStart(yLabelWidth)} │${bars}`);
  }

  // X-axis line and time labels.
  console.log(`${yIndent} └${"─".repeat(buckets.length * colWidth + 1)}`);
  const xLabels = buckets
    .map((_, i) => padCol(String((i * bucketSizeMs) / 1000)))
    .join("");
  console.log(`${yIndent}  ${xLabels}s`);

  // Breakdowns for buckets where multiple queries collided.
  if (!buckets.some((b) => b.peak > 1)) return;
  console.log("\nBucket breakdowns (peak > 1):");
  for (let i = 0; i < buckets.length; i++) {
    const b = buckets[i];
    if (b.peak <= 1) continue;
    const offsetSec = (i * bucketSizeMs) / 1000;
    console.log(
      `  +${String(offsetSec).padStart(4)}s (peak ${b.peak}):  ${summarizeActiveQueries(b.activeQueries)}`,
    );
  }
}

/** Peak concurrent requests per fixed-interval bucket — calculate from
 * records and render as vertical bars. */
function printConcurrencyHistogram(records: QueryRecord[]) {
  renderConcurrencyHistogram(calculateConcurrencyHistogram(records));
}

export function printPerTabSummary(
  records: QueryRecord[],
  tabLabels: string[],
) {
  console.log("\n=== Per-Tab Query Count ===");
  const rows: Record<
    string,
    { queries: number; failures: number; incomplete: number }
  > = {};
  for (let i = 0; i < tabLabels.length; i++) {
    const tabRecords = records.filter((r) => r.tabIndex === i);
    const failed = tabRecords.filter(
      (r) => !r.incomplete && (r.failed || r.status !== 200),
    ).length;
    const incomplete = tabRecords.filter((r) => r.incomplete).length;
    rows[`Tab ${i + 1} (${tabLabels[i]})`] = {
      queries: tabRecords.length,
      failures: failed,
      incomplete,
    };
  }
  console.table(rows);
}

/** Health check starvation is the multi-tab regression we guard against. */
export function expectNoFailedHealthChecks(records: QueryRecord[]) {
  const healthChecks = records.filter((r) => r.sql.includes("healthCheck"));
  const failedHealthChecks = healthChecks.filter(
    (r) => !r.incomplete && (r.failed || r.status !== 200),
  );
  expect(
    failedHealthChecks.length,
    `${failedHealthChecks.length} health checks failed`,
  ).toBe(0);
}
