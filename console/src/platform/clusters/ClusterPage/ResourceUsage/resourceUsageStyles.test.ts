// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { percentAxisTicks, percentAxisTop } from "./resourceUsageStyles";

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
  it("labels every 25% up to seven steps", () => {
    expect(percentAxisTicks(125)).toEqual([25, 50, 75, 100]);
    expect(percentAxisTicks(175)).toEqual([25, 50, 75, 100, 125, 150]);
  });

  it("thins the labels on a taller axis", () => {
    expect(percentAxisTicks(200)).toEqual([50, 100, 150]);
    expect(percentAxisTicks(400)).toEqual([75, 150, 225, 300, 375]);
  });
});
