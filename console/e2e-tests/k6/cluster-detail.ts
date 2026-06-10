// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// File-local declarations for k6-injected runtime globals (not modules).
// k6 module shapes (`k6`, `k6/http`) live in ./k6.d.ts.
declare const __ENV: Record<string, string | undefined>;
declare const __ITER: number;
declare function open(path: string): string;

/**
 * k6 load test for the cluster-detail page.
 *
 * Replays the SQL queries captured by `dump-queries.spec.ts` against
 * Materialize's HTTP SQL endpoint at increasing virtual-user counts. Each VU
 * simulates one browser tab; each iteration is one ~5s polling tick. Used to
 * characterize how cluster-detail queries behave on `mz_catalog_server`
 * under multi-user load.
 *
 * See `console/doc/guide-testing.md` ("Running Load Tests (k6)") for setup,
 * dumping `queries.json`, and run examples.
 */

import { check, sleep } from "k6";
import http from "k6/http";

interface CapturedQuery {
  urlPath: string;
  body: unknown;
}

type CapturedQueries = Record<string, CapturedQuery>;

/**
 * Reads queries.json or throws a guided error if it's missing.
 * `open()` is k6's sync init-time file read; it surfaces an unhelpful "no
 * such file or directory" by default. We catch that to point users at the
 * dump step.
 */
function loadQueries(): CapturedQueries {
  try {
    return JSON.parse(open("./queries.json")) as CapturedQueries;
  } catch {
    throw new Error(
      "queries.json not found in console/e2e-tests/k6/. " +
        "Generate it first by running:\n" +
        "  cd console && npx playwright test --project=scalability dump-queries",
    );
  }
}

/** Captured `/api/sql` POSTs keyed by query label (see ../scalability/helpers.ts). */
const QUERIES: CapturedQueries = loadQueries();

/**
 * How often each query polls (ms), matching `refetchInterval` in
 * `src/platform/clusters/queries.ts`. `Infinity` = fire once on cold load.
 */
const POLL_INTERVAL_MS: Record<string, number> = {
  "clusters.largestClusterReplica": 60_000,
  "clusters.largestMaintainedQueries": 60_000,
  "clusters.replicaUtilizationHistory": 20_000,
  canCreateCluster: Infinity,
  canCreateObjects: Infinity,
  currentUser: Infinity,
  isSuperUser: Infinity,
  privileges: Infinity,
};

/** Polling interval used when a label isn't listed in `POLL_INTERVAL_MS`. */
const DEFAULT_POLL_INTERVAL_MS = 5_000;

/** One VU iteration = one polling tick. Drives sleep and interval math. */
const TICK_MS = 5_000;

const MZ_URL = (__ENV.MZ_HTTP_URL || "http://localhost:6876").replace(
  /\/$/,
  "",
);

/**
 * k6 scenario, threshold, and summary configuration.
 *
 * - Ramps 0→100 VUs over 6 minutes via 5 stages. Each stage's target is
 *   overridable through `VUS_STAGE_N` env vars (handy for narrower sweeps).
 * - Each threshold doubles as an SLA gate AND forces k6 to render that
 *   label's `http_req_duration` breakdown in the end-of-run summary —
 *   without a threshold the metric is collected but hidden.
 */
export const options = {
  scenarios: {
    cluster_detail_ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: "30s", target: parseInt(__ENV.VUS_STAGE_1 || "5", 10) },
        { duration: "1m", target: parseInt(__ENV.VUS_STAGE_2 || "20", 10) },
        { duration: "2m", target: parseInt(__ENV.VUS_STAGE_3 || "50", 10) },
        { duration: "2m", target: parseInt(__ENV.VUS_STAGE_4 || "100", 10) },
        { duration: "30s", target: 0 },
      ],
      gracefulRampDown: "30s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    "http_req_duration{label:healthCheck}": ["p(95)<500"],
    "http_req_duration{label:clusters.list}": ["p(95)<2000"],
    "http_req_duration{label:clusters.materializationLag}": ["p(95)<2000"],
    "http_req_duration{label:clusters.clusterFreshness}": ["p(95)<2000"],
    "http_req_duration{label:clusters.replicaUtilizationHistory}": [
      "p(95)<2000",
    ],
    "http_req_duration{label:clusters.largestClusterReplica}": ["p(95)<5000"],
    "http_req_duration{label:clusters.largestMaintainedQueries}": [
      "p(95)<5000",
    ],
  },
  summaryTrendStats: ["min", "med", "avg", "p(95)", "p(99)", "max"],
};

/**
 * POSTs one captured query and records pass/fail checks. Silently skips
 * labels missing from `queries.json`.
 */
function fireQuery(label: string) {
  const entry = QUERIES[label];
  if (!entry) return;

  const res = http.post(
    `${MZ_URL}${entry.urlPath}`,
    JSON.stringify(entry.body),
    {
      headers: { "Content-Type": "application/json" },
      tags: { label },
    },
  );

  check(
    res,
    {
      "status 200": (r) => r.status === 200,
      "no sql error": (r) => {
        try {
          const body = JSON.parse(r.body as string) as {
            results?: Array<unknown>;
          };
          return !(body.results ?? []).some(
            (x) => x && typeof x === "object" && "error" in x,
          );
        } catch {
          return false;
        }
      },
    },
    { label },
  );
}

/**
 * Default VU loop — one polling tick per iteration.
 *
 * - Iteration 0 is the cold-mount burst: fires every captured query once.
 * - Later iterations fire each query only when its poll interval has elapsed.
 *   The check `elapsedMs % intervalMs < TICK_MS` picks the first tick on or
 *   after each boundary; `Infinity` intervals never satisfy it, so one-shot
 *   queries stay out of the polling loop.
 */
export default function () {
  if (__ITER === 0) {
    for (const label of Object.keys(QUERIES)) fireQuery(label);
    sleep(TICK_MS / 1000);
    return;
  }

  const elapsedMs = __ITER * TICK_MS;
  for (const label of Object.keys(QUERIES)) {
    const intervalMs = POLL_INTERVAL_MS[label] ?? DEFAULT_POLL_INTERVAL_MS;
    if (elapsedMs % intervalMs < TICK_MS) fireQuery(label);
  }
  sleep(TICK_MS / 1000);
}
