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
  describeThreshold,
  judgeLines,
  latestPeak,
  Predicate,
  UNREADABLE,
} from "./freshnessRows";
import { HydrationCounts, LagPercentiles } from "./queries";

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

// Mirrors the accessor `useClusterFreshness` builds. `null` for an unreadable
// reading breaks the line rather than drawing it at the bottom of the plot.
const accessorFor = (key: string) => (d: DataPoint) => {
  const reading = d.lag[key];
  if (!reading) return null;
  return reading.queryable ? reading.totalMs : null;
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
      schemaId: "u3",
      schemaName: "public",
      databaseName: "materialize",
      clusterId: "u1",
      clusterName: "quickstart",
      objectType: key === "spiky" ? "index" : "materialized-view",
    },
  ]),
);

/**
 * What the latest-reading query returns: each object's newest reading, which
 * the binned series cannot supply once a bin spans more than a minute.
 *
 * An object with no reading is absent from the map. One whose newest reading
 * came back NULL maps to `null`. The two mean opposite things, and only the
 * second says the object could not be read.
 */
const latestByObjectId = new Map<string, number | null>(
  Object.entries(SERIES).flatMap(([key, readings]) => {
    const last = readings.at(-1);
    if (last === "missing") return [];
    return [
      [key, last === "unreadable" ? null : last] as [string, number | null],
    ];
  }),
);

const hydrationByObjectId = new Map<string, HydrationCounts>(
  Object.keys(SERIES).map((key) => [
    key,
    { hydratedReplicas: key === "absent" ? 0 : 2, totalReplicas: 2 },
  ]),
);

/**
 * What the percentile query returns, null where a reading could not be taken.
 * `absent` has no readings in the range, so it has no entry.
 */
const percentilesByObjectId = new Map<string, LagPercentiles>([
  ["spiky", { peak: 9_000, p90: 430, p99: 9_000, p99_9: 9_000 }],
  ["steady", { peak: 3_100, p90: 3_050, p99: 3_100, p99_9: 3_100 }],
  ["unreadable", { peak: null, p90: null, p99: null, p99_9: null }],
]);

const rowsFor = (predicate: Predicate, threshold: number) => {
  const stats = buildStats(lines, latestByObjectId, percentilesByObjectId);
  const judged = judgeLines(lines, predicate, stats);
  return buildFreshnessRows({
    judged,
    statsByKey: stats,
    objectsById,
    hydrationByObjectId,
    threshold,
  });
};

describe("computeStats", () => {
  it("takes the percentiles from the query and Now from the latest reading", () => {
    expect(computeStats(430, percentilesByObjectId.get("spiky"))).toEqual({
      current: 430,
      peak: 9_000,
      p90: 430,
      p99: 9_000,
      p99_9: 9_000,
    });
  });

  it("is all null for an object with no readings", () => {
    expect(computeStats(undefined, undefined)).toEqual({
      current: null,
      peak: null,
      p90: null,
      p99: null,
      p99_9: null,
    });
  });

  it("counts an unreadable reading as worse than any lag", () => {
    const stats = computeStats(null, percentilesByObjectId.get("unreadable"));
    // The graph breaks its line at these readings rather than drawing them.
    expect(accessorFor("unreadable")(data[4]!)).toBeNull();
    expect(stats).toEqual({
      current: UNREADABLE,
      peak: UNREADABLE,
      p90: UNREADABLE,
      p99: UNREADABLE,
      p99_9: UNREADABLE,
    });
  });

  it("counts a lag that started after the percentiles were fetched in pMAX", () => {
    const stats = computeStats(600_000, percentilesByObjectId.get("spiky"));
    expect(stats.peak).toBe(600_000);
    expect(stats.p90).toBe(430);
  });

  it("takes pMAX from the latest reading before the range has any", () => {
    expect(computeStats(5_000, undefined)).toMatchObject({
      current: 5_000,
      peak: 5_000,
      p90: null,
    });
  });

  it("judges each predicate by its own statistic", () => {
    // Unreadable once, hours ago, and healthy since: the worst thing in the
    // window, but not now and not one reading in ten.
    const stats = computeStats(400, {
      peak: null,
      p90: 400,
      p99: null,
      p99_9: null,
    });
    const line = { key: "mv", yAccessor: () => null };
    const byKey = new Map([["mv", stats]]);
    const breaches = (predicate: Predicate) => {
      const value = judgeLines([line], predicate, byKey)[0].breachValue;
      return value !== null && value > 2_000;
    };

    expect(breaches("current")).toBe(false);
    expect(breaches("p90")).toBe(false);
    expect(breaches("p99")).toBe(true);
    expect(breaches("peak")).toBe(true);
  });
});

describe("latestPeak", () => {
  const latest = new Map<string, number | null>([
    ["u1", 400],
    ["u2", 9_000],
    ["u3", null],
  ]);

  it("takes the worst newest minute among the objects asked for", () => {
    expect(latestPeak(["u1", "u2"], latest)).toBe(9_000);
  });

  it("counts an unreadable minute as the worst", () => {
    expect(latestPeak(["u1", "u2", "u3"], latest)).toBe(UNREADABLE);
  });

  it("is undefined when none of the objects has a minute", () => {
    expect(latestPeak(["u9"], latest)).toBeUndefined();
    expect(latestPeak(["u1"], undefined)).toBeUndefined();
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
      databaseName: "materialize",
      schemaId: "u3",
      schemaName: "public",
      clusterId: "u1",
      clusterName: "quickstart",
      objectType: "index",
      hydratedReplicas: 2,
      totalReplicas: 2,
    });
  });

  it("colors exactly the rows that are drawn", () => {
    const colored = rowsFor("peak", 5_000).filter((r) => r.color !== undefined);
    expect(colored.map((r) => r.key)).toEqual(["unreadable", "spiky"]);
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
      peak: UNREADABLE,
    });
  });

  it("lists every object whatever the threshold", () => {
    expect(rowsFor("peak", 60_000)).toHaveLength(4);
  });
});

describe("describeThreshold", () => {
  // The range labels are bare nouns, so the sentence supplies "over the last"
  // and reads the way the control above it does.
  it("reads as prose for each predicate", () => {
    expect(describeThreshold("2s", "peak", "1 hour")).toBe(
      "2s at any moment over the last 1 hour",
    );
    expect(describeThreshold("1m 30s", "p90", "24 hours")).toBe(
      "1m 30s for more than 10% of minutes over the last 24 hours",
    );
  });

  it("leaves the range off 'right now'", () => {
    // It describes this moment whatever range is graphed.
    expect(describeThreshold("2s", "current", "24 hours")).toBe("2s right now");
  });
});
