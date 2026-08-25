// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

import {
  utilizationFilterFn,
  utilizationFilterFromUrl,
  utilizationFilterToUrl,
  UtilizationFilterValue,
} from "./utilizationFilters";

/**
 * A row reporting `fraction` in the column under test. The filter reads nothing
 * else off the row, so the rest of a `Row` is left out.
 */
const rowReporting = (fraction: number | null | undefined) =>
  ({
    getValue: () => fraction,
  }) as unknown as Row<unknown>;

const keeps = (
  fraction: number | null | undefined,
  filter: UtilizationFilterValue,
) => utilizationFilterFn(rowReporting(fraction), "cpuPercent", filter);

describe("utilizationFilterFn", () => {
  const above50: UtilizationFilterValue = { comparison: ">", percent: 50 };
  const below50: UtilizationFilterValue = { comparison: "<", percent: 50 };

  it("reads the column as a fraction and the threshold as a percentage", () => {
    expect(keeps(0.9, above50)).toBe(true);
    expect(keeps(0.1, above50)).toBe(false);
  });

  it("keeps readings below the threshold when comparing with <", () => {
    expect(keeps(0.1, below50)).toBe(true);
    expect(keeps(0.9, below50)).toBe(false);
  });

  it("excludes a reading exactly on the threshold, either direction", () => {
    // Both comparisons are strict, so 50% satisfies neither "> 50" nor "< 50".
    expect(keeps(0.5, above50)).toBe(false);
    expect(keeps(0.5, below50)).toBe(false);
  });

  it("compares the unrounded reading, not its rounded display value", () => {
    const above80: UtilizationFilterValue = { comparison: ">", percent: 80 };
    // Both render as "80.0%" through PercentBar's one decimal place.
    expect(keeps(0.7996, above80)).toBe(false);
    expect(keeps(0.8004, above80)).toBe(true);
  });

  it("excludes a replica with no sample, whichever way the filter points", () => {
    expect(keeps(null, above50)).toBe(false);
    expect(keeps(null, below50)).toBe(false);
    expect(keeps(undefined, above50)).toBe(false);
  });

  it("keeps an idle replica reporting zero when the filter allows it", () => {
    // 0 is a real reading, not a missing one.
    expect(keeps(0, below50)).toBe(true);
    expect(keeps(0, above50)).toBe(false);
  });

  it("handles a reading above the allocation", () => {
    // `heap_percent` counts RAM plus swap against the heap limit, so it can
    // exceed 100%.
    expect(keeps(1.4, { comparison: ">", percent: 100 })).toBe(true);
    expect(keeps(1.4, { comparison: "<", percent: 100 })).toBe(false);
  });

  it("accepts a fractional threshold", () => {
    expect(keeps(0.08, { comparison: ">", percent: 7.5 })).toBe(true);
    expect(keeps(0.07, { comparison: ">", percent: 7.5 })).toBe(false);
  });
});

describe("utilizationFilterToUrl", () => {
  it("spells the comparison as a word", () => {
    // A raw ">" percent-encodes to "%3E", which makes a bookmark unreadable.
    expect(utilizationFilterToUrl({ comparison: ">", percent: 80 })).toBe(
      "gt.80",
    );
    expect(utilizationFilterToUrl({ comparison: "<", percent: 80 })).toBe(
      "lt.80",
    );
  });

  it("survives a round trip, fractions included", () => {
    for (const value of [
      { comparison: ">", percent: 0 },
      { comparison: "<", percent: 100 },
      { comparison: ">", percent: 7.5 },
    ] satisfies UtilizationFilterValue[]) {
      expect(utilizationFilterFromUrl(utilizationFilterToUrl(value))).toEqual(
        value,
      );
    }
  });
});

describe("utilizationFilterFromUrl", () => {
  it("reads a well-formed parameter", () => {
    expect(utilizationFilterFromUrl("gt.80")).toEqual({
      comparison: ">",
      percent: 80,
    });
    expect(utilizationFilterFromUrl("lt.5")).toEqual({
      comparison: "<",
      percent: 5,
    });
  });

  it("reads a fractional threshold", () => {
    expect(utilizationFilterFromUrl("gt.7.5")).toEqual({
      comparison: ">",
      percent: 7.5,
    });
  });

  // A hand-edited or stale link must leave the table unfiltered rather than
  // install a filter the control cannot display or clear.
  it.each([
    ["absent", null],
    ["empty", ""],
    ["an unknown comparison", "ge.40"],
    ["no comparison", "40"],
    ["no threshold", "gt."],
    ["a non-numeric threshold", "gt.abc"],
    ["a negative threshold", "gt.-10"],
    ["trailing junk", "gt.40x"],
    ["leading junk", "xgt.40"],
    ["a comparison alone", "gt"],
  ])("rejects a parameter that is %s", (_label, raw) => {
    expect(utilizationFilterFromUrl(raw)).toBeUndefined();
  });
});
