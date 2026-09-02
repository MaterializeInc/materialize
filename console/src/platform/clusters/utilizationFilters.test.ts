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
  utilizationFilterLabel,
  utilizationFilterToUrl,
} from "./utilizationFilters";

/**
 * A row reporting `fraction` in the column under test. The filter reads nothing
 * else off the row, so the rest of a `Row` is left out.
 */
const rowReporting = (fraction: number | null | undefined) =>
  ({
    getValue: () => fraction,
  }) as unknown as Row<unknown>;

const keeps = (fraction: number | null | undefined, percent: number) =>
  utilizationFilterFn(rowReporting(fraction), "cpuPercent", percent);

describe("utilizationFilterFn", () => {
  it("reads the column as a fraction and the threshold as a percentage", () => {
    expect(keeps(0.9, 50)).toBe(true);
    expect(keeps(0.1, 50)).toBe(false);
  });

  it("keeps a reading exactly on the threshold", () => {
    // The threshold is a floor, so 50% satisfies "at least 50".
    expect(keeps(0.5, 50)).toBe(true);
  });

  it("compares the unrounded reading, not its rounded display value", () => {
    // Both render as "80.0%" through PercentBar's one decimal place.
    expect(keeps(0.7996, 80)).toBe(false);
    expect(keeps(0.8004, 80)).toBe(true);
  });

  it("excludes a replica with no sample", () => {
    expect(keeps(null, 50)).toBe(false);
    expect(keeps(undefined, 50)).toBe(false);
  });

  it("excludes an idle replica reporting zero", () => {
    // 0 is a real reading, and it does not reach any threshold the panel can
    // apply, since a threshold of 0 clears the filter instead.
    expect(keeps(0, 50)).toBe(false);
  });

  it("handles a reading above the allocation", () => {
    // `heap_percent` counts RAM plus swap against the heap limit, so it can
    // exceed 100%.
    expect(keeps(1.4, 100)).toBe(true);
    expect(keeps(0.99, 100)).toBe(false);
  });

  it("accepts a fractional threshold", () => {
    expect(keeps(0.08, 7.5)).toBe(true);
    expect(keeps(0.07, 7.5)).toBe(false);
  });
});

describe("utilizationFilterToUrl", () => {
  it("writes the threshold on its own", () => {
    expect(utilizationFilterToUrl(80)).toBe("80");
    expect(utilizationFilterToUrl(7.5)).toBe("7.5");
  });

  it("survives a round trip, fractions included", () => {
    for (const percent of [1, 100, 7.5, 250]) {
      expect(utilizationFilterFromUrl(utilizationFilterToUrl(percent))).toBe(
        percent,
      );
    }
  });
});

describe("utilizationFilterFromUrl", () => {
  it("reads a well-formed parameter", () => {
    expect(utilizationFilterFromUrl("80")).toBe(80);
    expect(utilizationFilterFromUrl("7.5")).toBe(7.5);
  });

  // A hand-edited or stale link must leave the table unfiltered rather than
  // install a filter the panel cannot show or clear.
  it.each([
    ["absent", null],
    ["empty", ""],
    ["zero, which is every sampled replica", "0"],
    ["negative", "-10"],
    ["non-numeric", "abc"],
    ["trailing junk", "40x"],
    ["a comparison prefix", "gt.40"],
    ["a percent sign", "40%"],
    ["whitespace", " 40"],
  ])("rejects a parameter that is %s", (_label, raw) => {
    expect(utilizationFilterFromUrl(raw)).toBeUndefined();
  });
});

describe("utilizationFilterLabel", () => {
  it("reads as the condition it applies", () => {
    expect(utilizationFilterLabel("CPU", 40)).toBe("CPU ≥ 40%");
    expect(utilizationFilterLabel("Memory", 7.5)).toBe("Memory ≥ 7.5%");
  });
});
