// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { execSync } from "node:child_process";
import { appendFile } from "node:fs";
import path from "node:path";

import { defineConfig, mergeConfig } from "vitest/config";

import { TEST_LOG_PATH } from "./src/test/vitest.globalSetup";
import viteConfig from "./vite.config";

function physicalCpuCount() {
  const result = execSync(
    "lscpu -p | egrep -v '^#' | sort -u -t, -k 2,4 | wc -l",
    {
      encoding: "utf8",
    },
  );
  return Number.parseInt(result.trimEnd());
}

const vitestConfig = defineConfig({
  test: {
    environment: "jsdom",
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      "**/e2e-tests/**",
      "**/.{idea,git,cache,output,temp}/**",
      "**/{webpack,vite,vitest,jest}.config.*",
    ],
    globals: true,
    globalSetup: ["src/test/vitest.globalSetup.ts"],
    setupFiles: [
      "src/__mocks__/globalMocks.js",
      "src/__mocks__/jsDom.ts",
      "src/vitest.setup.ts",
    ],
    snapshotFormat: {
      escapeString: false,
    },
    // We need to revert to the old fakeTimers behavior in v5 otherwise tests will run out of memory.
    // https://vitest.dev/guide/migration.html#fake-timers-defaults
    fakeTimers: {
      toFake: ["Date", "setTimeout", "clearTimeout"],
    },
    pool: "forks",
    poolOptions: {
      forks: {
        // By default vitest sets these based on the logical processor count.
        // Our CI runners have hyperthreading, but using the virtual cores seems to make
        // the tests more flaky, and only slightly faster.
        maxForks: process.env.CI ? physicalCpuCount() : undefined,
        minForks: process.env.CI ? physicalCpuCount() : undefined,
      },
    },
    onConsoleLog(log: string, _type: "stdout" | "stderr"): boolean | void {
      if (process.env.PRINT_TEST_CONSOLE_LOGS === "true") {
        return true;
      }
      // Redirect all logs to a file to keep test output clean
      appendFile(TEST_LOG_PATH, log, (err) => {
        if (err) {
          console.error("Error appending to test log:", err);
        }
      });
      return false;
    },
  },
  resolve: {
    alias: [
      {
        find: "@materializeinc/sql-pretty",
        replacement: path.resolve("__mocks__/@materializeinc/sql-pretty.ts"),
      },
      {
        find: "@materializeinc/sql-lexer",
        replacement: path.resolve("__mocks__/@materializeinc/sql-lexer.js"),
      },
      {
        find: "~/config/importAppConfig",
        // HACK (SangJunBak):
        // This is a workaround to allow Vitest to resolve the importAppConfig file to the stub implementation in /__mocks__/importAppConfig.ts.
        // This is necessary because Vitest doesn't allow imports from the public directory.
        replacement: path.resolve("__mocks__/importAppConfig"),
      },
    ],
  },
});

export default mergeConfig(viteConfig, vitestConfig);
