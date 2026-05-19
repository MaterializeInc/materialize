// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { rankClusters } from "./ClusterDropdown";

describe("rankClusters", () => {
  const clusters = [
    { name: "quickstart" },
    { name: "prod" },
    { name: "production-analytics" },
    { name: "production-analytics-prod" },
    { name: "staging-cluster" },
    { name: "mz_introspection" },
  ];

  it("returns the original options reference when input is empty", () => {
    // Identity check: empty input must not reorder or copy the list, so the
    // dropdown's first render shows the caller's order (alphabetized by the
    // ShellHeader).
    expect(rankClusters(clusters, "")).toBe(clusters);
  });

  it("ranks exact match above starts-with above contains", () => {
    expect(rankClusters(clusters, "prod").map((c) => c.name)).toEqual([
      "prod",
      "production-analytics",
      "production-analytics-prod",
    ]);
  });

  it("is case-insensitive", () => {
    expect(rankClusters(clusters, "PROD").map((c) => c.name)).toEqual([
      "prod",
      "production-analytics",
      "production-analytics-prod",
    ]);
  });

  it("matches acronyms (e.g. `qs` -> `quickstart`)", () => {
    expect(rankClusters(clusters, "qs").map((c) => c.name)).toEqual([
      "quickstart",
    ]);
  });

  it("ranks substring matches above subsequence-only matches", () => {
    // `mz_introspection` contains the substring "intro";
    // `production-analytics-prod` only matches "intro" as a non-contiguous
    // subsequence (i…n…t…r…o), so it should rank lower.
    expect(rankClusters(clusters, "intro").map((c) => c.name)).toEqual([
      "mz_introspection",
      "production-analytics-prod",
    ]);
  });

  it("returns an empty list when nothing matches", () => {
    expect(rankClusters(clusters, "xyz")).toEqual([]);
  });

  it("filters single-match queries down to just the matching option", () => {
    expect(rankClusters(clusters, "stag").map((c) => c.name)).toEqual([
      "staging-cluster",
    ]);
  });
});
