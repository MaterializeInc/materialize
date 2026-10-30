// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { IPostgresInterval } from "~/api/materialize";
import { DataPoint } from "~/components/FreshnessGraph/types";

import { fitToRange } from "./graphWindow";

const HOUR = 3_600_000;
// One hour is binned by the minute.
const BIN = 60_000;
// Twenty seconds into a bin, so "now" is not on a bin boundary.
const NOW = Date.UTC(2026, 9, 28, 14, 30, 20);
const LAST_BIN = Math.floor(NOW / BIN) * BIN;

const reading = (timestamp: number, totalMs: number): DataPoint => ({
  timestamp,
  lag: {
    u1: {
      queryable: true,
      totalMs,
      interval: {} as IPostgresInterval,
      schemaName: "public",
      objectName: "orders_mv",
    },
  },
});

describe("fitToRange", () => {
  it("spans the selected range ending now, whatever the readings cover", () => {
    // Ten minutes of readings in a one hour range.
    const fitted = fitToRange(
      {
        historicalData: [
          reading(LAST_BIN - 10 * BIN, 1_000),
          reading(LAST_BIN, 2_000),
        ],
        startTime: LAST_BIN - 10 * BIN,
        endTime: LAST_BIN,
      },
      HOUR,
      NOW,
    );

    expect(fitted.startTime).toBe(NOW - HOUR);
    expect(fitted.endTime).toBe(NOW);
  });

  it("has a point for every bin, so a line breaks where its object has none", () => {
    const fitted = fitToRange(
      {
        historicalData: [
          reading(LAST_BIN - 10 * BIN, 1_000),
          reading(LAST_BIN - 2 * BIN, 2_000),
        ],
        startTime: 0,
        endTime: 0,
      },
      HOUR,
      NOW,
    );

    const firstBin = Math.floor((NOW - HOUR) / BIN) * BIN;
    expect(fitted.historicalData.map((d) => d.timestamp)).toEqual(
      Array.from(
        { length: (LAST_BIN - firstBin) / BIN + 1 },
        (_, i) => firstBin + i * BIN,
      ),
    );
    // Between the two readings `u1` has no entry, which the graph draws as a
    // break rather than a line joining them.
    expect(
      fitted.historicalData.find((d) => d.timestamp === LAST_BIN - 5 * BIN)
        ?.lag,
    ).toEqual({});
    expect(
      fitted.historicalData.find((d) => d.timestamp === LAST_BIN - 2 * BIN)?.lag
        .u1,
    ).toMatchObject({ totalMs: 2_000 });
  });

  it("keeps everything else the data carries", () => {
    const fitted = fitToRange(
      { historicalData: [], startTime: 0, endTime: 0, extra: "kept" },
      HOUR,
      NOW,
    );

    expect(fitted.extra).toBe("kept");
  });
});
