// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { defineConfig, devices } from "@playwright/test";
import moduleAlias from "module-alias";

// HACK (SangJunBak):
// This is a workaround to allow Playwright to resolve the importAppConfig file to the stub implementation in /e2e-tests/importAppConfig.ts.
// This is necessary because Playwright requires CommonJS (which doesn't support top-level await), while our main appConfig uses ESM with top-level await.
// We use module-alias rather than tsconfig.json's compilerOptions.paths because I couldn't get it to resolve correctly.
moduleAlias.addAliases({
  "~/config/importAppConfig": "__mocks__/importAppConfig",
});

const config = defineConfig({
  projects: [
    /* Test against desktop browsers */
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
    // For some reason, Firefox runs into CORS issues consistently the 3rd time it loads
    // the app:
    // https://github.com/MaterializeInc/console/actions/runs/5556423929/jobs/10149604083?pr=178
    // {
    //   name: "firefox",
    //   use: { ...devices["Desktop Firefox"] },
    // },
    {
      name: "webkit",
      use: { ...devices["Desktop Safari"] },
    },
    {
      name: "teardown",
      testMatch: /global-teardown\.ts/,
      // We need all our tests to be listed here, otherwise regions might be deleted out
      // from under other tests while they are running
      dependencies: ["chromium", "webkit"],
    },
  ],
  testDir: "e2e-tests",
  // Per test timeout
  timeout: 30 * 1000, // 30 seconds
  use: {
    acceptDownloads: true,
    // Actions such as clicks, also waitForSelector calls
    actionTimeout: 5 * 1000, // 5 seconds
    trace: "on",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    // In kind, we use self-signed certs
    ignoreHTTPSErrors: true,
  },
  // If you change this, also update the value of `NUM_PLAYWRIGHT_WORKERS` in `./e2e-tests/util.ts`
  workers: 5,
  fullyParallel: true,
});

export default config;
