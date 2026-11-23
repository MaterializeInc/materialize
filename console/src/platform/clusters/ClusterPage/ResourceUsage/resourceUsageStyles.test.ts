// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  percentAxisTicks,
  percentAxisTop,
  percentChart,
} from "./resourceUsageStyles";

describe("percentAxisTop", () => {
  it.each([
    [0, 125],
    [90, 125],
    [100, 125],
    [100.1, 150],
    [120, 150],
    [125, 150],
    [140, 175],
    [250, 275],
  ])("tops a peak of %d%% at %d%%", (peak, top) => {
    expect(percentAxisTop(peak)).toBe(top);
  });
});

describe("percentAxisTicks", () => {
  it("labels every 25% below the top", () => {
    expect(percentAxisTicks(125)).toEqual([25, 50, 75, 100]);
    expect(percentAxisTicks(200)).toEqual([25, 50, 75, 100, 125, 150, 175]);
  });
});

describe("percentChart", () => {
  it("grows 42px per 25% step so every chart shares one scale", () => {
    expect(percentChart(90, 500).yScale.range()).toEqual([210, 0]);
    expect(percentChart(175, 500).yScale.range()).toEqual([336, 0]);
  });
});
