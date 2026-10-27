// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { subHours } from "date-fns";

import {
  DEFAULT_SCHEMA_VALUES,
  MAX_TIME_SPAN_HOURS,
  queryHistoryListSchema,
} from "./queryHistoryList";

const strictQueryHistoryListSchema = queryHistoryListSchema.strict();

describe("queryHistoryListSchema", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime("1999-09-03");
  });

  it("should throw if start date is after end date in dateRange", () => {
    const dateRange = [
      new Date("2021-01-02").toISOString(),
      new Date("2021-01-01").toISOString(),
    ];
    expect(() =>
      strictQueryHistoryListSchema.parse({
        ...DEFAULT_SCHEMA_VALUES,
        dateRange,
      }),
    ).toThrow();
  });

  it("should not throw if time between start date and end date is less than or equal to 3 days", () => {
    const dateRange = [
      new Date("2024-01-04T19:16:55.537Z").toISOString(),
      new Date("2024-01-07T19:16:55.537Z").toISOString(),
    ];
    expect(() =>
      strictQueryHistoryListSchema.parse({
        ...DEFAULT_SCHEMA_VALUES,
        dateRange,
      }),
    ).not.toThrow();
  });

  it("should throw if time between start date is before MAX_TIME_SPAN_HOURS hours ago", () => {
    const currentTime = new Date();

    const dateRange = [
      subHours(currentTime, MAX_TIME_SPAN_HOURS + 1).toISOString(),
      currentTime.toISOString(),
    ];
    expect(() =>
      strictQueryHistoryListSchema.parse({
        ...DEFAULT_SCHEMA_VALUES,
        dateRange,
      }),
    ).toThrow();
  });

  it("should not throw when lower bound is 0 in durationRange", () => {
    expect(() =>
      strictQueryHistoryListSchema.parse({
        ...DEFAULT_SCHEMA_VALUES,
        durationRange: { minDuration: "0", maxDuration: "100" },
      }),
    ).not.toThrow();
  });
});
