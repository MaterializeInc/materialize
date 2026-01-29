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
  fullyParallel: false, // Run serially since we share a single Materialize instance
  retries: 1,
  reporter: 'list',
  use: {
    // In Docker, the host is 'materialized'. For local testing, use 'localhost'.
    baseURL: process.env.MZ_HOST
      ? `http://${process.env.MZ_HOST}:6876`
      : 'http://localhost:6876',
    trace: 'on-first-retry',
  },
  timeout: 30000,
});
