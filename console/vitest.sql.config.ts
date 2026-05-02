// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { defineConfig } from "vitest/config";

import baseConfig from "./vitest.config";

export default defineConfig({
  ...baseConfig,
  test: {
    ...baseConfig.test,
    globalSetup: ["src/test/sql/sql.setup.ts"],
    setupFiles: ["src/__mocks__/globalMocks.js", "src/__mocks__/jsDom.ts"],
    // These tests do not use the dom or any browser state
    // Disabling isolation improves performance
    isolate: false,
    pool: "forks",
    poolOptions: {
      forks: {
        // Because we use a shared mzcompose project, tests must be run sequentially
        maxForks: 1,
        minForks: 1,
      },
    },
    testTimeout: 10_000,
    include: ["**/*.test.sql.ts"],
  },
});
