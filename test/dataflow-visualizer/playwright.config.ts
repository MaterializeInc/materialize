// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  // Every test shares one Materialize instance, and the introspection queries
  // behind the visualizer are unindexed scans. Two workers on a CI agent turn
  // those into multi-second waits that trip the assertion timeouts below, so
  // pin the suite to a single worker rather than only serializing each file.
  fullyParallel: false,
  workers: 1,
  retries: 1,
  reporter: 'list',
  use: {
    // In Docker, the host is 'materialized'. For local testing, use 'localhost'.
    // Use port 6878 (internal HTTP) which has all routes enabled, including /metrics.
    baseURL: process.env.MZ_HOST
      ? `http://${process.env.MZ_HOST}:6878`
      : 'http://localhost:6878',
    trace: 'on-first-retry',
  },
  expect: {
    // The visualizer pages load in stages, each rendering its own `Loading...`
    // while its introspection query is in flight. Those queries are unindexed
    // scans, so a busy CI agent can take seconds over each one. Set the budget
    // once here rather than per assertion.
    timeout: 30000,
  },
  // A test can wait out two page loads in a row (the dataflow list, then one
  // dataflow's operators), so this has to exceed the sum of the assertion
  // timeouts a single test can accumulate. Otherwise a slow page reports as an
  // opaque test timeout instead of naming the element it was waiting for.
  timeout: 90000,
});
