// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parse from "postgres-interval";

import { CriticalPathRow } from "~/api/materialize/maintained-objects/criticalPath";

import { computeFreshnessBreakdown } from "./freshnessBreakdown";

const buildRow = (
  overrides: Partial<CriticalPathRow> = {},
): CriticalPathRow => ({
  id: "u1",
  childId: "probe",
  name: "input1",
  objectType: "materialized-view",
  schemaName: "public",
  databaseName: "materialize",
  clusterName: "default",
  lag: parse("00:00:01"),
  targetLag: parse("00:00:10"),
  isBottleneck: false,
  parentSourceId: null,
  ...overrides,
});

describe("computeFreshnessBreakdown", () => {
  it("returns null when there are no upstream rows", () => {
    expect(computeFreshnessBreakdown([])).toBeNull();
  });

  it("returns null when targetLag is missing", () => {
    expect(
      computeFreshnessBreakdown([buildRow({ targetLag: null })]),
    ).toBeNull();
  });

  it("returns null when target lag is below the noise floor", () => {
    // targetLag of 500ms is below `BREAKDOWN_MIN_LAG_MS` (1s).
    expect(
      computeFreshnessBreakdown([buildRow({ targetLag: parse("00:00:00.5") })]),
    ).toBeNull();
  });

  it("returns null when self-delay is non-positive (max input ≥ target)", () => {
    // Sometimes the input lag pMAX exceeds the target's pMAX in different
    // windows — there's nothing useful to attribute as self-delay.
    expect(
      computeFreshnessBreakdown([
        buildRow({
          targetLag: parse("00:00:10"),
          lag: parse("00:00:11"),
        }),
      ]),
    ).toBeNull();
  });

  it("splits target lag into max-upstream + self-delay", () => {
    const breakdown = computeFreshnessBreakdown([
      buildRow({
        id: "u1",
        name: "fast_input",
        lag: parse("00:00:02"),
        targetLag: parse("00:00:10"),
      }),
      buildRow({
        id: "u2",
        name: "slow_input",
        lag: parse("00:00:06"),
        targetLag: parse("00:00:10"),
      }),
    ]);
    expect(breakdown).toMatchObject({
      maxInputMs: 6_000,
      selfDelayMs: 4_000,
      isSelfBottleneck: false,
      dominantUpstreamIds: ["u2"],
      dominantUpstreamNames: ["slow_input"],
    });
  });

  it("flags self-bottleneck when self-delay exceeds max upstream", () => {
    const breakdown = computeFreshnessBreakdown([
      buildRow({
        lag: parse("00:00:02"),
        targetLag: parse("00:00:11"),
      }),
    ]);
    // 11s target − 2s upstream = 9s self, which exceeds 2s max upstream.
    expect(breakdown).toMatchObject({
      maxInputMs: 2_000,
      selfDelayMs: 9_000,
      isSelfBottleneck: true,
    });
  });

  it("lists every input tied for max as a dominant upstream", () => {
    const breakdown = computeFreshnessBreakdown([
      buildRow({ id: "a", name: "a", lag: parse("00:00:05") }),
      buildRow({ id: "b", name: "b", lag: parse("00:00:05") }),
      buildRow({ id: "c", name: "c", lag: parse("00:00:01") }),
    ]);
    expect(breakdown?.dominantUpstreamIds).toEqual(["a", "b"]);
    expect(breakdown?.dominantUpstreamNames).toEqual(["a", "b"]);
  });

  it("treats null input lags as zero when computing max upstream", () => {
    // A row that hasn't reported lag yet shouldn't pin maxInputMs to 0 if
    // another sibling has a real measurement.
    const breakdown = computeFreshnessBreakdown([
      buildRow({ id: "a", name: "a", lag: null }),
      buildRow({ id: "b", name: "b", lag: parse("00:00:04") }),
    ]);
    expect(breakdown?.maxInputMs).toBe(4_000);
    expect(breakdown?.dominantUpstreamIds).toEqual(["b"]);
  });

  it("returns no dominant upstreams when every input has null lag", () => {
    // No measured input → maxInputMs is 0, so there's no chain bottleneck to
    // name; we still surface the breakdown so the chip can show 0s upstream.
    const breakdown = computeFreshnessBreakdown([
      buildRow({ id: "a", lag: null }),
      buildRow({ id: "b", lag: null }),
    ]);
    expect(breakdown).toMatchObject({
      maxInputMs: 0,
      isSelfBottleneck: true,
      dominantUpstreamIds: [],
      dominantUpstreamNames: [],
    });
  });
});
