// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { DataPoint } from "~/components/FreshnessGraph/types";
import { FreshnessObject } from "~/platform/clusters/queries";

import {
  buildFreshnessRows,
  buildStats,
  computeStats,
  judgeLines,
  Predicate,
  sortRows,
} from "./freshnessRows";

// `spiky` is healthy except for one reading. `steady` is always slightly slow.
// `absent` never reports. Together they separate the three predicates.
const SERIES: Record<string, (number | null)[]> = {
  spiky: [400, 9_000, 420, 410, 430],
  steady: [3_000, 3_100, 2_900, 3_050, 3_000],
  absent: [null, null, null, null, null],
};

const data: DataPoint[] = [0, 1, 2, 3, 4].map((i) => ({
  timestamp: i,
  lag: {},
}));

const accessorFor = (key: string) => (d: DataPoint) =>
  SERIES[key][d.timestamp] ?? null;

const lines = Object.keys(SERIES).map((key) => ({
  key,
  label: `public.${key}`,
  yAccessor: accessorFor(key),
}));

const objectsById = new Map<string, FreshnessObject>(
  Object.keys(SERIES).map((key) => [
    key,
    {
      objectId: key,
      objectName: key,
      schemaName: "public",
      databaseName: "materialize",
      objectType: key === "spiky" ? "index" : "materialized-view",
      hydratedReplicas: key === "absent" ? 0 : 2,
      totalReplicas: 2,
    },
  ]),
);

const rowsFor = (
  predicate: Predicate,
  threshold: number,
  selected: ReadonlySet<string> = new Set(),
) => {
  const stats = buildStats(lines, data);
  const judged = judgeLines(lines, data, predicate, stats);
  return buildFreshnessRows(judged, stats, objectsById, threshold, selected);
};

describe("computeStats", () => {
  it("separates the latest reading from the worst", () => {
    expect(computeStats(accessorFor("spiky"), data)).toMatchObject({
      current: 430,
      peak: 9_000,
    });
  });

  it("is all null for a line that never reported", () => {
    expect(computeStats(accessorFor("absent"), data)).toEqual({
      current: null,
      peak: null,
      p90: null,
    });
  });

  it("discards a short spike that the peak keeps", () => {
    // The query hands us 60 readings whatever range is selected, so this is the
    // shape p90 actually sees. Six bad readings sit above the 90th percentile;
    // the 54th of 60 does not.
    const sixty: DataPoint[] = Array.from({ length: 60 }, (_, i) => ({
      timestamp: i,
      lag: {},
    }));
    const stats = computeStats((d) => (d.timestamp < 3 ? 9_000 : 400), sixty);
    expect(stats.peak).toBe(9_000);
    expect(stats.p90).toBe(400);
  });

  it("keeps a spike that is wide enough to clear the percentile", () => {
    const sixty: DataPoint[] = Array.from({ length: 60 }, (_, i) => ({
      timestamp: i,
      lag: {},
    }));
    // Seven of sixty readings is over 10%, so p90 lands inside the spike.
    const stats = computeStats((d) => (d.timestamp < 7 ? 9_000 : 400), sixty);
    expect(stats.p90).toBe(9_000);
  });
});

describe("buildFreshnessRows", () => {
  it("judges by the statistic the predicate names", () => {
    const breaching = (predicate: Predicate) =>
      rowsFor(predicate, 2_000)
        .filter((r) => r.breaching)
        .map((r) => r.key);

    // spiky is currently fine, so only the persistently slow one is bad "now".
    expect(breaching("current")).toEqual(["steady"]);
    // Both went over at some point.
    expect(breaching("peak")).toEqual(["spiky", "steady"]);
  });

  it("carries the object's identity and hydration", () => {
    const spiky = rowsFor("peak", 2_000).find((r) => r.key === "spiky");
    expect(spiky).toMatchObject({
      objectName: "spiky",
      namespace: "materialize.public",
      objectType: "index",
      hydratedReplicas: 2,
      totalReplicas: 2,
    });
  });

  it("colors exactly the rows that are drawn", () => {
    const colored = rowsFor("peak", 5_000).filter((r) => r.color !== undefined);
    expect(colored.map((r) => r.key)).toEqual(["spiky"]);
  });

  it("adds a hand-picked row without removing the breaching ones", () => {
    // Union, not replacement. A selection that replaced the threshold's set is
    // what forced modes and an ownership flip in an earlier design.
    const colored = rowsFor("peak", 5_000, new Set(["steady"]))
      .filter((r) => r.color !== undefined)
      .map((r) => r.key);
    expect(colored.sort()).toEqual(["spiky", "steady"]);
  });

  it("never breaches on a line with nothing to judge", () => {
    const absent = rowsFor("peak", 0).find((r) => r.key === "absent");
    expect(absent).toMatchObject({ breachValue: null, breaching: false });
  });

  it("lists every object whatever the threshold", () => {
    expect(rowsFor("peak", 60_000)).toHaveLength(3);
  });
});

describe("sortRows", () => {
  it("keeps nulls last in both directions", () => {
    const rows = rowsFor("peak", 2_000);
    expect(sortRows(rows, "peak", -1).at(-1)?.key).toBe("absent");
    expect(sortRows(rows, "peak", 1).at(-1)?.key).toBe("absent");
  });

  it("sorts names alphabetically", () => {
    const rows = rowsFor("peak", 2_000);
    expect(sortRows(rows, "objectName", 1).map((r) => r.key)).toEqual([
      "absent",
      "spiky",
      "steady",
    ]);
  });
});
