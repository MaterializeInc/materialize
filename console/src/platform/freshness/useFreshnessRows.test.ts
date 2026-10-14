// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook } from "@testing-library/react";
import parse from "postgres-interval";
import { describe, expect, it } from "vitest";

import { DataPoint, GraphLineSeries } from "~/components/FreshnessGraph/types";
import { FreshnessObject } from "~/platform/clusters/queries";

import { HydrationCounts } from "./queries";
import { FreshnessRowsParams, useFreshnessRows } from "./useFreshnessRows";

const KEYS = ["u1", "u2", "u3"];

const historicalData: DataPoint[] = Array.from({ length: 60 }, (_, bin) => ({
  timestamp: bin,
  lag: Object.fromEntries(
    KEYS.map((key, i) => [
      key,
      {
        queryable: true as const,
        totalMs: 400 + i * 1_000 + (bin % 5) * 10,
        interval: parse("00:00:01"),
        schemaName: "public",
        objectName: key,
      },
    ]),
  ),
}));

const lines: GraphLineSeries[] = KEYS.map((key) => ({
  key,
  label: `public.${key}`,
  yAccessor: (d: DataPoint) => {
    const reading = d.lag[key];
    if (!reading) return null;
    return reading.queryable ? reading.totalMs : null;
  },
}));

const objectsById = new Map<string, FreshnessObject>(
  KEYS.map((key) => [
    key,
    {
      objectId: key,
      objectName: key,
      schemaName: "public",
      databaseName: "materialize",
      objectType: "materialized-view",
    },
  ]),
);

const hydrationByObjectId = new Map<string, HydrationCounts>(
  KEYS.map((key) => [key, { hydratedReplicas: 2, totalReplicas: 2 }]),
);

const latestByObjectId = new Map<string, number | null>(
  KEYS.map((key, i) => [key, 400 + i * 1_000]),
);

const baseParams: FreshnessRowsParams = {
  lines,
  historicalData,
  latestByObjectId,
  objectsById,
  hydrationByObjectId,
  typeFilters: [],
  predicate: "peak",
  threshold: 2_000,
};

const render = (params: FreshnessRowsParams = baseParams) =>
  renderHook((p: FreshnessRowsParams) => useFreshnessRows(p), {
    initialProps: params,
  });

describe("useFreshnessRows", () => {
  it("builds a row per line, worst first", () => {
    const { result } = render();
    expect(result.current.rows).toHaveLength(3);
    expect(result.current.rows[0].peak).toBeGreaterThan(
      result.current.rows[2].peak ?? 0,
    );
  });

  it("holds every cached value when nothing changes", () => {
    const { result, rerender } = render();
    const { judged, rows, breaching } = result.current;

    rerender(baseParams);

    expect(result.current.judged).toBe(judged);
    expect(result.current.rows).toBe(rows);
    expect(result.current.breaching).toBe(breaching);
  });

  /**
   * A drag emits a value per pointer move, and only the settled one reaches
   * here. Between settles the page still re-renders, so these caches are what
   * keep a row per object from being rebuilt at that rate.
   */
  it("holds across a render that changes nothing it reads", () => {
    const { result, rerender } = render();
    const { rows } = result.current;

    // The same inputs, rebuilt the way a parent re-render hands them over.
    rerender({ ...baseParams });

    expect(result.current.rows).toBe(rows);
  });

  it("rebuilds the rows when the threshold settles somewhere new", () => {
    const { result, rerender } = render();
    const { rows, judged } = result.current;

    rerender({ ...baseParams, threshold: 1_500 });

    expect(result.current.rows).not.toBe(rows);
    // The judging is upstream of the threshold, so it holds.
    expect(result.current.judged).toBe(judged);
  });

  it("rebuilds everything when the predicate changes", () => {
    const { result, rerender } = render();
    const { judged, rows } = result.current;

    rerender({ ...baseParams, predicate: "current" });

    expect(result.current.judged).not.toBe(judged);
    expect(result.current.rows).not.toBe(rows);
  });

  it("narrows to the filtered types", () => {
    const { result } = render({ ...baseParams, typeFilters: ["index"] });
    expect(result.current.rows).toHaveLength(0);
  });
});
