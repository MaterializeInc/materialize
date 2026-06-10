// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { test } from "@playwright/test";

import { IS_LOCAL_STACK, TestContext } from "./util";

// For testing running kind, we don't care about cleanup, since the state is ephemeral anyway.
if (!IS_LOCAL_STACK) {
  test("Disable all regions", async ({ page, request }) => {
    // Disabling regions is syncronous and can take quite a while.
    // This timeout is set based on:
    // 60s ALB timeout * 5 retries + 1 minute buffer
    test.setTimeout(6 * 60 * 1000);
    const context = await TestContext.start(page, request);
    await context.disableAllRegions();
  });
}
