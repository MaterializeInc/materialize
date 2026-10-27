// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { calculateAxisTicks } from "./utils";

const oneThird = 1 / 3;
const twoThirds = 2 / 3;

describe("calculateAxisTicks", () => {
  it("empty domain results in a single tick", () => {
    expect(calculateAxisTicks({ tickCount: 4, min: 0, max: 0 })).toEqual({
      alignedMin: 0,
      alignedMax: 0,
      stepSize: 0,
      ticks: [0],
    });
  });

  it("the largest tick is greater or equal to the max value on the graph", () => {
    expect(calculateAxisTicks({ tickCount: 4, min: 0, max: 1 })).toEqual({
      alignedMin: 0,
      alignedMax: 1,
      stepSize: oneThird,
      ticks: [0, oneThird, twoThirds, 1.0],
    });
  });

  it("max of 3 results in a step size of 1", () => {
    expect(calculateAxisTicks({ tickCount: 4, min: 0, max: 3 })).toEqual({
      alignedMin: 0,
      alignedMax: 3,
      stepSize: 1,
      ticks: [0, 1, 2, 3],
    });
  });
});
