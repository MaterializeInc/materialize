// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/** One line on the graph, plus the single value the threshold judges it by. */
export interface ThresholdLineSeries<Datum> {
  /** Identifies the line across renders and in the highlight map. Must be unique. */
  key: string;
  label?: string;
  /**
   * This line's value at `datum`, or null where it has no reading. A null
   * breaks the line rather than bridging the gap.
   */
  yAccessor: (d: Datum) => number | null;
  /**
   * The value the threshold compares this line against, or null when the line
   * has nothing to judge and so can never breach.
   *
   * The caller supplies it rather than the graph deriving it, because which
   * statistic deserves judging is the caller's policy: a peak, a p99, or the
   * reading right now are all reasonable, and only the last of them can be read
   * off the points that happen to be drawn.
   */
  breachValue: number | null;
}
