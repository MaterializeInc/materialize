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
  DEFAULT_MINIMUM_REPLICAS,
  replicaCountFilterFn,
  replicaCountFilterFromUrl,
  replicaCountFilterLabel,
  replicaCountFilterToUrl,
} from "./replicaCountFilters";

type ReplicaCountRow = { cluster: { replicas: unknown[] } };

/**
 * A row belonging to a cluster running `count` replicas. The filter reads
 * nothing else off the row, and nothing about the replicas themselves, so the
 * rest of a `Row` is left out.
 */
const rowInClusterOf = (count: number) =>
  ({
    original: { cluster: { replicas: new Array(count).fill(null) } },
  }) as Row<ReplicaCountRow>;

const keeps = (clusterReplicas: number, minimum: number) =>
  replicaCountFilterFn(rowInClusterOf(clusterReplicas), "replica", minimum);

describe("replicaCountFilterFn", () => {
  it("keeps a cluster with more replicas than the minimum", () => {
    expect(keeps(3, 2)).toBe(true);
  });

  it("keeps a cluster sitting exactly on the minimum", () => {
    // The minimum is a floor, so 2 satisfies "at least 2".
    expect(keeps(2, 2)).toBe(true);
  });

  it("drops a cluster with fewer replicas than the minimum", () => {
    expect(keeps(1, 2)).toBe(false);
  });

  it("drops a cluster running no replicas at the default minimum", () => {
    // The replica-less cluster still gets a row, so the filter is what hides
    // it rather than the row never being built.
    expect(keeps(0, DEFAULT_MINIMUM_REPLICAS)).toBe(false);
    expect(keeps(1, DEFAULT_MINIMUM_REPLICAS)).toBe(true);
  });

  it("counts the row's cluster, not the row's own replica", () => {
    // The table shows one row per replica, so a row that has a replica always
    // counts as one on its own. Reading the row instead of its cluster would
    // make every minimum above 1 match nothing.
    expect(keeps(2, 2)).toBe(true);
    expect(keeps(5, 4)).toBe(true);
  });

  it("keeps every row at a minimum of zero", () => {
    // Documents the contract rather than a reachable state: the panel and the
    // URL both turn "no minimum" into undefined, which removes the filter.
    expect(keeps(0, 0)).toBe(true);
    expect(keeps(3, 0)).toBe(true);
  });
});

describe("replicaCountFilterFromUrl", () => {
  it("reads a well-formed minimum", () => {
    expect(replicaCountFilterFromUrl("2")).toBe(2);
    expect(replicaCountFilterFromUrl("10")).toBe(10);
  });

  it("reads the default stated explicitly", () => {
    expect(replicaCountFilterFromUrl("1")).toBe(DEFAULT_MINIMUM_REPLICAS);
  });

  it("reads zero as no minimum at all", () => {
    // The only way a URL can ask for every cluster, which is why clearing the
    // filter writes the parameter rather than dropping it.
    expect(replicaCountFilterFromUrl("0")).toBeUndefined();
  });

  it("ignores leading zeros", () => {
    expect(replicaCountFilterFromUrl("007")).toBe(7);
  });

  // Absent or malformed means the *default*, not "unfiltered". A plain visit
  // carries no parameter and has to land on the default, so there is no value
  // left for a malformed parameter to mean. NOTE: this inverts how
  // `utilizationFilterFromUrl` treats bad input, which is to filter nothing.
  it.each([
    ["absent", null],
    ["empty", ""],
    ["negative", "-1"],
    ["fractional", "1.5"],
    ["non-numeric", "abc"],
    ["trailing junk", "2x"],
    ["leading whitespace", " 2"],
    ["a comparison prefix", "gte.2"],
    ["a comma-joined list", "1,2"],
  ])("falls back to the default when the parameter is %s", (_label, raw) => {
    expect(replicaCountFilterFromUrl(raw)).toBe(DEFAULT_MINIMUM_REPLICAS);
  });
});

describe("replicaCountFilterToUrl", () => {
  it("leaves the default out of the URL", () => {
    expect(replicaCountFilterToUrl(DEFAULT_MINIMUM_REPLICAS)).toBeUndefined();
  });

  it("writes a minimum that differs from the default", () => {
    expect(replicaCountFilterToUrl(2)).toBe(2);
    expect(replicaCountFilterToUrl(10)).toBe(10);
  });

  it("states no minimum as zero", () => {
    // Dropping the parameter would read back as the default on the next visit,
    // so "every cluster" has to be written down.
    expect(replicaCountFilterToUrl(undefined)).toBe(0);
    expect(replicaCountFilterToUrl(0)).toBe(0);
  });
});

describe("a replica count round trip", () => {
  /**
   * The trip the table actually performs: write the minimum to the URL, then
   * read it back. An undefined parameter is not written at all, so the read
   * side sees `null`, which is what makes the default reachable.
   */
  const roundTrip = (minimum: number | undefined) => {
    const written = replicaCountFilterToUrl(minimum);
    return replicaCountFilterFromUrl(
      written === undefined ? null : String(written),
    );
  };

  it("survives for the default, which travels as no parameter", () => {
    expect(roundTrip(DEFAULT_MINIMUM_REPLICAS)).toBe(DEFAULT_MINIMUM_REPLICAS);
  });

  it("survives for a minimum above the default", () => {
    for (const minimum of [2, 3, 10, 99]) {
      expect(roundTrip(minimum)).toBe(minimum);
    }
  });

  it("survives for no minimum", () => {
    expect(roundTrip(undefined)).toBeUndefined();
  });

  it("treats zero and no minimum as the same state", () => {
    expect(roundTrip(0)).toBe(roundTrip(undefined));
  });
});

describe("replicaCountFilterLabel", () => {
  it("reads as the condition it applies", () => {
    expect(replicaCountFilterLabel(1)).toBe("Replicas ≥ 1");
    expect(replicaCountFilterLabel(2)).toBe("Replicas ≥ 2");
    expect(replicaCountFilterLabel(10)).toBe("Replicas ≥ 10");
  });
});
