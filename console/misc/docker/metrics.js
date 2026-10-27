// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Prometheus metrics for the console's nginx, run by the njs module.
//
// Counters live in the `mz_console_metrics` shared dictionary zone, so every
// worker process adds to the same values. They reset when nginx restarts.
// This file runs on njs, not Node, so it sticks to the language subset njs
// supports: no destructuring or spread.

/* global ngx */

// Seconds. The Prometheus client libraries' default buckets.
const BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

const CONNECTION_STATES = ["active", "reading", "writing", "waiting"];

// The values `$mz_metrics_location` takes in nginx.conf.template, and `other`
// for a request that reached none of its locations.
const LOCATIONS = ["app", "static", "api", "oauth_metadata", "other"];

// The error statuses nginx produces itself, exported at 0 for every location.
const SEEDED_STATUSES = ["500", "502", "503", "504"];

// Adds rare series at 0, so that `rate()` counts their first request. A
// counter that first appears at 1 never contributes that 1. `add` leaves a
// key that already exists alone.
function seed(metrics) {
  LOCATIONS.forEach(function (location) {
    SEEDED_STATUSES.forEach(function (status) {
      metrics.add("requests|" + location + "|" + status, 0);
    });
    for (let i = 0; i <= BUCKETS.length; i++) {
      metrics.add("bucket|" + location + "|" + i, 0);
    }
    metrics.add("sum_ms|" + location, 0);
  });
  // WebSocket upgrades, which `api` proxies.
  metrics.add("requests|api|101", 0);
}

// Records a finished request. Evaluated as the `if=` condition of an access
// log, so it runs in the log phase, once the status and request time are
// final, and returns an empty string so that the log writes nothing.
function record(r) {
  const metrics = ngx.shared.mz_console_metrics;
  const location = r.variables.mz_metrics_location || "other";
  const status = r.variables.status;
  metrics.incr("requests|" + location + "|" + status, 1);

  // A WebSocket upgrade lasts as long as its session, which is not a latency.
  if (status !== "101") {
    const seconds = parseFloat(r.variables.request_time);
    let bucket = BUCKETS.findIndex(function (le) {
      return seconds <= le;
    });
    if (bucket === -1) {
      bucket = BUCKETS.length;
    }
    metrics.incr("bucket|" + location + "|" + bucket, 1);
    // `$request_time` has millisecond resolution, so the sum stays an integer.
    metrics.incr("sum_ms|" + location, Math.round(seconds * 1000));
  }
  return "";
}

// Seeding here, rather than when recording, is enough: a request recorded
// before the first scrape is invisible to `rate()` either way.
function render(r) {
  const requests = {};
  const buckets = {};
  const sums = {};
  const metrics = ngx.shared.mz_console_metrics;
  seed(metrics);
  const items = metrics.items();
  for (let i = 0; i < items.length; i++) {
    const parts = items[i][0].split("|");
    const value = items[i][1];
    if (parts[0] === "requests") {
      requests[parts[1]] = requests[parts[1]] || {};
      requests[parts[1]][parts[2]] = value;
    } else if (parts[0] === "bucket") {
      buckets[parts[1]] = buckets[parts[1]] || [];
      buckets[parts[1]][Number(parts[2])] = value;
    } else if (parts[0] === "sum_ms") {
      sums[parts[1]] = value;
    }
  }

  const lines = [
    "# HELP mz_console_http_requests_total Count of HTTP requests the console served, by location and status.",
    "# TYPE mz_console_http_requests_total counter",
  ];
  Object.keys(requests)
    .sort()
    .forEach(function (location) {
      Object.keys(requests[location])
        .sort()
        .forEach(function (status) {
          lines.push(
            'mz_console_http_requests_total{location="' +
              location +
              '",status="' +
              status +
              '"} ' +
              requests[location][status],
          );
        });
    });

  lines.push(
    "# HELP mz_console_http_request_duration_seconds Time to serve an HTTP request, by location. WebSocket upgrades are not timed.",
    "# TYPE mz_console_http_request_duration_seconds histogram",
  );
  Object.keys(buckets)
    .sort()
    .forEach(function (location) {
      const label = 'location="' + location + '"';
      let cumulative = 0;
      for (let i = 0; i <= BUCKETS.length; i++) {
        cumulative += buckets[location][i] || 0;
        const le = i < BUCKETS.length ? String(BUCKETS[i]) : "+Inf";
        lines.push(
          "mz_console_http_request_duration_seconds_bucket{" +
            label +
            ',le="' +
            le +
            '"} ' +
            cumulative,
        );
      }
      lines.push(
        "mz_console_http_request_duration_seconds_sum{" +
          label +
          "} " +
          (sums[location] || 0) / 1000,
        "mz_console_http_request_duration_seconds_count{" +
          label +
          "} " +
          cumulative,
      );
    });

  lines.push(
    "# HELP mz_console_http_connections Client connections nginx holds open, by state.",
    "# TYPE mz_console_http_connections gauge",
  );
  CONNECTION_STATES.forEach(function (state) {
    lines.push(
      'mz_console_http_connections{state="' +
        state +
        '"} ' +
        r.variables["connections_" + state],
    );
  });

  r.headersOut["Content-Type"] = "text/plain; version=0.0.4; charset=utf-8";
  r.return(200, lines.join("\n") + "\n");
}

export default { record, render };
