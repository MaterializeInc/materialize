// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Console Viewers
 *
 * Holds VIEWERS console viewers open, each in its own browser context so it
 * has its own sessions like a separate user, while the workload-replay
 * console-scalability workflow measures mz_catalog_server. Creates
 * VIEWERS_READY_FILE once every viewer has loaded, then holds until
 * VIEWERS_STOP_FILE exists. Skipped unless VIEWERS is set.
 */

import fs from "node:fs";

import { BrowserContext, test } from "@playwright/test";

import {
  CLUSTER_ID,
  CLUSTER_NAME,
  clusterDetailUrl,
  createAuthenticatedContext,
  waitForPageLoad,
} from "./helpers";

const VIEWERS = parseInt(process.env.VIEWERS || "0");
const VIEWER_URL =
  process.env.VIEWER_URL || clusterDetailUrl(CLUSTER_ID, CLUSTER_NAME);
const READY_FILE = process.env.VIEWERS_READY_FILE;
const STOP_FILE = process.env.VIEWERS_STOP_FILE;
const MAX_HOLD_MS = parseInt(process.env.VIEWERS_MAX_HOLD_MS || "1800000");
// Opening every viewer at once would measure a cold-load burst, not viewers.
const OPEN_BATCH_SIZE = 10;

test.describe("Console Viewers", () => {
  test.skip(VIEWERS === 0, "set VIEWERS to hold viewers open");

  test("hold viewers open", async ({ browser }) => {
    test.setTimeout(MAX_HOLD_MS + 10 * 60_000);

    const contexts: BrowserContext[] = [];

    for (let opened = 0; opened < VIEWERS; opened += OPEN_BATCH_SIZE) {
      const batchSize = Math.min(OPEN_BATCH_SIZE, VIEWERS - opened);
      const batch = await Promise.all(
        Array.from({ length: batchSize }, async () => {
          const context = await createAuthenticatedContext(browser);
          const page = await context.newPage();
          await page.goto(VIEWER_URL);
          await waitForPageLoad(page, 60_000);
          return context;
        }),
      );
      contexts.push(...batch);
      console.log(`  ${contexts.length}/${VIEWERS} viewers open`);
    }

    console.log(`Holding ${VIEWERS} viewers on ${VIEWER_URL}`);
    if (READY_FILE) fs.writeFileSync(READY_FILE, "");
    const holdStart = Date.now();
    while (
      !(STOP_FILE && fs.existsSync(STOP_FILE)) &&
      Date.now() - holdStart < MAX_HOLD_MS
    ) {
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
    await Promise.all(contexts.map((context) => context.close()));
  });
});
