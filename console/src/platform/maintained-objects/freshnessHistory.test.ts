// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parse from "postgres-interval";

import {
  buildObjectFreshnessHistory,
  LagHistoryRow,
  pMaxFromHistory,
} from "./freshnessHistory";

const buildRow = (overrides: Partial<LagHistoryRow> = {}): LagHistoryRow => ({
  bucketStart: new Date("2026-05-05T00:00:00Z"),
  objectId: "u101",
  lag: parse("00:00:00"),
  schemaName: "public",
  objectName: "orders_mv",
  ...overrides,
});

describe("buildObjectFreshnessHistory", () => {
  it("transforms rows into a chart-ready shape", () => {
    const rows = [
      buildRow({
        bucketStart: new Date("2026-05-05T00:00:00Z"),
        lag: parse("00:00:30"),
      }),
      buildRow({
        bucketStart: new Date("2026-05-05T00:30:00Z"),
        lag: null,
      }),
    ];

    const { historicalData, lines, startTime, endTime } =
      buildObjectFreshnessHistory(rows, 60 * 60 * 1000);

    expect(historicalData[0]!.lag.u101).toMatchObject({
      queryable: true,
      totalMs: 30_000,
    });
    expect(historicalData[1]!.lag.u101).toMatchObject({ queryable: false });
    expect(lines[0]!.label).toBe("public.orders_mv");
    expect(startTime).toBe(new Date("2026-05-05T00:00:00Z").getTime());
    expect(endTime).toBe(new Date("2026-05-05T00:30:00Z").getTime());
  });

  it("returns an empty shape when there are no rows", () => {
    const result = buildObjectFreshnessHistory([], 60 * 60 * 1000);
    expect(result.historicalData).toEqual([]);
    expect(result.lines).toEqual([]);
  });
});

describe("pMaxFromHistory", () => {
  it("returns the largest queryable lag, ignoring non-queryable buckets", () => {
    const { historicalData } = buildObjectFreshnessHistory(
      [
        buildRow({ lag: parse("00:00:30") }),
        buildRow({ lag: null }),
        buildRow({ lag: parse("00:02:00") }),
      ],
      60 * 60 * 1000,
    );
    expect(pMaxFromHistory(historicalData, "u101")?.ms).toBe(120_000);
  });

  it("returns null when there are no queryable buckets", () => {
    const { historicalData } = buildObjectFreshnessHistory(
      [buildRow({ lag: null })],
      60 * 60 * 1000,
    );
    expect(pMaxFromHistory(historicalData, "u101")).toBeNull();
  });
});
