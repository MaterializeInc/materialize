// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";

describe("sentry", () => {
  it("does not enable Sentry in unit tests", async () => {
    // vitest.setup.ts replaces the client in beforeAll, after test files have
    // imported ~/sentry. Re-evaluate the module to see the client it creates.
    vi.resetModules();
    await import("~/sentry");

    expect(Sentry.isEnabled()).toBe(false);
  });
});
