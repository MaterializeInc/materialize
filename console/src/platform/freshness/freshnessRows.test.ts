// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parse from "postgres-interval";
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

/**
 * A reading as the page receives it: a number is a measured lag in
 * milliseconds, `unreadable` is a reading whose lag came back NULL, and
 * `missing` is no reading at all.
 */
type Reading = number | "unreadable" | "missing";

// `spiky` is healthy except for one reading. `steady` is always slightly slow.
// `absent` never reports. `unreadable` reports, and its answer is that it
// cannot be read.
const SERIES: Record<string, Reading[]> = {
  spiky: [400, 9_000, 420, 410, 430],
  steady: [3_000, 3_100, 2_900, 3_050, 3_000],
  absent: ["missing", "missing", "missing", "missing", "missing"],
  unreadable: [400, 420, "unreadable", "unreadable", "unreadable"],
};

const readingFor = (key: string, reading: Reading) =>
  reading === "unreadable"
    ? { queryable: false as const, schemaName: "public", objectName: key }
    : {
        queryable: true as const,
        totalMs: reading as number,
        interval: parse("00:00:01"),
        schemaName: "public",
        objectName: key,
      };

const buildData = (series: Record<string, Reading[]>, length: number) =>
  Array.from({ length }, (_, i) => ({
    timestamp: i,
    lag: Object.fromEntries(
      Object.entries(series)
        .filter(([, readings]) => readings[i] !== "missing")
        .map(([key, readings]) => [key, readingFor(key, readings[i])]),
    ),
  })) as DataPoint[];

const data = buildData(SERIES, 5);

// Mirrors the accessor `useClusterFreshness` builds, including its mapping of
// an unreadable reading to 0 so the line draws at the bottom of the graph. The
// statistics must not inherit that 0.
const accessorFor = (key: string) => (d: DataPoint) => {
  const reading = d.lag[key];
  if (!reading) return null;
  return reading.queryable ? reading.totalMs : 0;
};

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
    expect(computeStats("spiky", data)).toMatchObject({
      current: 430,
      peak: 9_000,
    });
  });

  it("is all null for a line that never reported", () => {
    expect(computeStats("absent", data)).toEqual({
      current: null,
      peak: null,
      p90: null,
      notQueryable: false,
    });
  });

  it("does not let an unreadable reading score as zero lag", () => {
    const stats = computeStats("unreadable", data);
    // The graph's accessor returns 0 for these readings so the line draws at
    // the bottom. Inheriting that would score the worst state as the best.
    expect(accessorFor("unreadable")(data[4]!)).toBe(0);
    expect(stats.notQueryable).toBe(true);
    expect(stats.peak).toBe(420);
    expect(stats.current).toBeNull();
  });

  it("discards a short spike that the peak keeps", () => {
    // The query hands us 60 readings whatever range is selected, so this is the
    // shape p90 actually sees. Six bad readings sit above the 90th percentile;
    // the 54th of 60 does not.
    const sixty = buildData(
      { spike: Array.from({ length: 60 }, (_, i) => (i < 3 ? 9_000 : 400)) },
      60,
    );
    const stats = computeStats("spike", sixty);
    expect(stats.peak).toBe(9_000);
    expect(stats.p90).toBe(400);
  });

  it("keeps a spike that is wide enough to clear the percentile", () => {
    // Seven of sixty readings is over 10%, so p90 lands inside the spike.
    const sixty = buildData(
      { spike: Array.from({ length: 60 }, (_, i) => (i < 7 ? 9_000 : 400)) },
      60,
    );
    expect(computeStats("spike", sixty).p90).toBe(9_000);
  });
});

describe("buildFreshnessRows", () => {
  it("judges by the statistic the predicate names", () => {
    const breaching = (predicate: Predicate) =>
      rowsFor(predicate, 2_000)
        .filter((r) => r.breaching)
        .map((r) => r.key);

    // spiky is currently fine, so only the persistently slow one is bad "now".
    // The unreadable object leads whatever the predicate: it is not slow, it
    // cannot be read, which no threshold forgives.
    expect(breaching("current")).toEqual(["unreadable", "steady"]);
    // Both of the measurable ones went over at some point.
    expect(breaching("peak")).toEqual(["unreadable", "spiky", "steady"]);
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
    expect(colored.map((r) => r.key)).toEqual(["unreadable", "spiky"]);
  });

  it("adds a hand-picked row without removing the breaching ones", () => {
    // Union, not replacement. A selection that replaced the threshold's set is
    // what forced modes and an ownership flip in an earlier design.
    const colored = rowsFor("peak", 5_000, new Set(["steady"]))
      .filter((r) => r.color !== undefined)
      .map((r) => r.key);
    expect(colored.sort()).toEqual(["spiky", "steady", "unreadable"]);
  });

  it("never breaches on a line with nothing to judge", () => {
    const absent = rowsFor("peak", 0).find((r) => r.key === "absent");
    expect(absent).toMatchObject({ breachValue: null, breaching: false });
  });

  it("breaches an unreadable object at a threshold nothing else clears", () => {
    const rows = rowsFor("peak", 60_000);
    expect(rows.filter((r) => r.breaching).map((r) => r.key)).toEqual([
      "unreadable",
    ]);
    expect(rows.find((r) => r.key === "unreadable")).toMatchObject({
      notQueryable: true,
    });
  });

  it("lists every object whatever the threshold", () => {
    expect(rowsFor("peak", 60_000)).toHaveLength(4);
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
      "unreadable",
    ]);
  });
});
