// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Captures one console page's /api/sql traffic into a JSON file that the k6
 * harness in console/e2e-tests/k6/ replays under load. One entry per query
 * label (clusters.list, healthCheck, ...) using the helpers.ts label scheme.
 *
 * Output: e2e-tests/k6/queries.json (gitignored — environment-specific).
 *
 * Run from console/:
 *   npx playwright test --project=scalability dump-queries
 */

import { Request, test } from "@playwright/test";

import {
  assertNoErrors,
  CLUSTER_ID,
  CLUSTER_NAME,
  clusterDetailUrl,
  createAuthenticatedContext,
  labelRequest,
  waitForPageLoad,
} from "./helpers";

// Resolved against process.cwd() — script must be invoked from console/.
const OUTPUT_PATH = "e2e-tests/k6/queries.json";

// Long enough to capture 60s-cadence queries at least once after cold load.
const CAPTURE_DURATION_MS = 75_000;

interface CapturedQuery {
  urlPath: string;
  body: unknown;
}

type CapturedQueries = Record<string, CapturedQuery>;

/** Opens a page, watches every /api/sql POST, returns the latest one per label. */
async function dumpPage(
  pageName: string,
  url: string,
  browser: import("@playwright/test").Browser,
): Promise<CapturedQueries> {
  const context = await createAuthenticatedContext(browser);
  const page = await context.newPage();
  const captured = new Map<string, CapturedQuery>();

  page.on("request", (request) => {
    const entry = parseSqlRequest(request);
    if (entry) captured.set(entry.label, entry.query);
  });

  console.log(`Dumping ${pageName}: ${url}`);
  await page.goto(url);
  await waitForPageLoad(page);
  await assertNoErrors(page, `dump ${pageName}`);
  await page.waitForTimeout(CAPTURE_DURATION_MS);

  console.log(`  Captured ${captured.size} labels:`);
  for (const label of [...captured.keys()].sort()) {
    console.log(`    ${label}`);
  }

  await context.close();
  return Object.fromEntries(captured);
}

/** Returns {label, query} for a /api/sql POST; undefined for anything else. */
function parseSqlRequest(
  request: Request,
): { label: string; query: CapturedQuery } | undefined {
  if (!request.url().includes("/api/sql")) return;
  if (request.method() !== "POST") return;

  let body: unknown;
  try {
    body = JSON.parse(request.postData() || "{}");
  } catch {
    return;
  }

  const url = new URL(request.url());
  return {
    label: labelRequest(request),
    query: { urlPath: url.pathname + url.search, body },
  };
}

async function writeQueriesJson(queries: CapturedQueries) {
  // Dynamic imports — Playwright's runtime mismatches ESM/CJS on static ones.
  const fs = await import("node:fs");
  const path = await import("node:path");
  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(queries, null, 2));
  console.log(
    `\nWrote ${Object.keys(queries).length} queries to ${OUTPUT_PATH}`,
  );
}

test("dump cluster detail queries", async ({ browser }) => {
  test.setTimeout(3 * 60_000);

  const queries = await dumpPage(
    "Cluster detail",
    clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME),
    browser,
  );
  await writeQueriesJson(queries);
});
