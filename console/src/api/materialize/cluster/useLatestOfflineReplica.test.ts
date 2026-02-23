// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { shouldSurfaceOom } from "./useLatestOfflineReplica";

describe("shouldSurfaceOom", () => {
  it("should return true when lastOomAt is within 15 minutes", () => {
    const currentTime = new Date("2023-01-01T12:00:00");
    const lastOomAt = new Date("2023-01-01T11:47:00");

    const result = shouldSurfaceOom(currentTime, lastOomAt);

    expect(result).toBe(true);
  });

  it("should return false when lastOomAt is outside 15 minutes", () => {
    const currentTime = new Date("2023-01-01T12:00:00");
    const lastOomAt = new Date("2023-01-01T11:30:00");

    const result = shouldSurfaceOom(currentTime, lastOomAt);

    expect(result).toBe(false);
  });

  it("should return false when lastOomAt is exactly at the end of 15 minutes", () => {
    const currentTime = new Date("2023-01-01T12:00:00");
    const lastOomAt = new Date("2023-01-01T11:45:00");

    const result = shouldSurfaceOom(currentTime, lastOomAt);

    expect(result).toBe(false);
  });
});
