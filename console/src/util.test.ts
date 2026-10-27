// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { addUtcDays } from "./util";

describe("addUtcDays", () => {
  it("adds days across a UTC calendar boundary regardless of local time zone", () => {
    // 2026-01-01T23:30:00Z: late in the UTC day, so a naive local-time
    // implementation could roll to the wrong day depending on the runner's
    // time zone.
    const date = new Date("2026-01-01T23:30:00Z");
    expect(addUtcDays(date, 1).toISOString()).toBe("2026-01-02T00:00:00.000Z");
  });

  it("subtracts days with a negative argument", () => {
    const date = new Date("2026-03-01T00:00:00Z");
    expect(addUtcDays(date, -1).toISOString()).toBe("2026-02-28T00:00:00.000Z");
  });

  it("rolls across a month/year boundary", () => {
    const date = new Date("2025-12-31T12:00:00Z");
    expect(addUtcDays(date, 1).toISOString()).toBe("2026-01-01T00:00:00.000Z");
  });
});
