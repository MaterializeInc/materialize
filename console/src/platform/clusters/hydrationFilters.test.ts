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
  HYDRATION_BUCKETS,
  HydrationBucket,
} from "~/platform/maintained-objects/filters";

import {
  HYDRATION_COLUMN_ID,
  hydrationFilterFn,
  hydrationFilterFromUrl,
  hydrationFilterLabel,
} from "./hydrationFilters";

/**
 * A row reporting `bucket` in the hydration column. The filter reads nothing
 * else off the row, so the rest of a `Row` is left out.
 */
const rowReporting = (bucket: HydrationBucket | undefined) =>
  ({
    getValue: () => bucket,
  }) as unknown as Row<unknown>;

const keeps = (
  bucket: HydrationBucket | undefined,
  selected: HydrationBucket[],
) => hydrationFilterFn(rowReporting(bucket), HYDRATION_COLUMN_ID, selected);

describe("hydrationFilterFn", () => {
  it("keeps a row whose bucket is selected", () => {
    expect(keeps("hydrating", ["hydrating"])).toBe(true);
  });

  it("drops a row whose bucket is not selected", () => {
    expect(keeps("hydrated", ["hydrating"])).toBe(false);
  });

  it("keeps a row matching any one of several selected buckets", () => {
    expect(keeps("not_hydrated", ["hydrating", "not_hydrated"])).toBe(true);
    expect(keeps("hydrated", ["hydrating", "not_hydrated"])).toBe(false);
  });

  it("filters nothing when the selection is empty", () => {
    // Unchecking the last box clears the column filter rather than selecting
    // nothing, but an empty array must behave the same way either way.
    expect(keeps("hydrated", [])).toBe(true);
    expect(keeps(undefined, [])).toBe(true);
  });

  it("filters nothing when the column carries no filter value", () => {
    expect(
      hydrationFilterFn(
        rowReporting("hydrated"),
        HYDRATION_COLUMN_ID,
        undefined as unknown as HydrationBucket[],
      ),
    ).toBe(true);
  });

  it("drops a replica whose hydration is unknown from any active selection", () => {
    // It cannot be placed in a bucket, so no selection describes it. This
    // matches how `utilizationFilterFn` treats a replica with no reading.
    for (const bucket of HYDRATION_BUCKETS) {
      expect(keeps(undefined, [bucket])).toBe(false);
    }
    expect(keeps(undefined, [...HYDRATION_BUCKETS])).toBe(false);
  });
});

describe("hydrationFilterFromUrl", () => {
  it("reads a single well-formed bucket", () => {
    expect(hydrationFilterFromUrl(["hydrating"])).toEqual(["hydrating"]);
  });

  it("reads several buckets at once", () => {
    expect(hydrationFilterFromUrl(["hydrated", "not_hydrated"])).toEqual([
      "hydrated",
      "not_hydrated",
    ]);
  });

  it("orders the result by bucket, not by the order the URL names them", () => {
    // Two links naming the same set produce the same filter, so the chips and
    // the parameter a round trip writes back do not depend on click order.
    expect(hydrationFilterFromUrl(["not_hydrated", "hydrated"])).toEqual([
      "hydrated",
      "not_hydrated",
    ]);
  });

  it("collapses a bucket the URL repeats", () => {
    expect(hydrationFilterFromUrl(["hydrating", "hydrating"])).toEqual([
      "hydrating",
    ]);
  });

  it("keeps the buckets it recognises alongside one it does not", () => {
    expect(hydrationFilterFromUrl(["lukewarm", "hydrated"])).toEqual([
      "hydrated",
    ]);
  });

  it("returns a fresh array rather than the shared bucket list", () => {
    // The result becomes TanStack filter state, which callers are free to
    // replace; it must not alias the module-level constant.
    const result = hydrationFilterFromUrl([...HYDRATION_BUCKETS]);

    expect(result).toEqual([...HYDRATION_BUCKETS]);
    expect(result).not.toBe(HYDRATION_BUCKETS);
  });

  // A hand-edited or stale link must leave the table unfiltered rather than
  // install a filter the panel cannot show or clear.
  it.each([
    ["absent", []],
    ["empty", [""]],
    ["an unknown bucket", ["lukewarm"]],
    ["only unknown buckets", ["lukewarm", "boiling"]],
    ["a label rather than a bucket id", ["Not Hydrated"]],
    ["a bucket id in the wrong case", ["HYDRATED"]],
    ["a bucket id with whitespace", [" hydrated"]],
    ["a comma-joined list", ["hydrated,hydrating"]],
  ])("rejects a parameter that is %s", (_label, raw) => {
    expect(hydrationFilterFromUrl(raw)).toBeUndefined();
  });
});

describe("hydrationFilterLabel", () => {
  it.each([
    ["hydrated", "Hydration: Hydrated"],
    ["hydrating", "Hydration: Hydrating"],
    ["not_hydrated", "Hydration: Not Hydrated"],
  ] as const)("states %s as the condition it applies", (bucket, expected) => {
    expect(hydrationFilterLabel(bucket)).toBe(expected);
  });

  it("names a condition for every bucket the panel offers", () => {
    // A bucket added without a label would otherwise reach a chip as
    // "Hydration: undefined".
    for (const bucket of HYDRATION_BUCKETS) {
      expect(hydrationFilterLabel(bucket)).toMatch(/^Hydration: \S/);
    }
  });
});
