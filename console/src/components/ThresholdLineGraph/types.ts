// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/** One line on the graph. */
export interface ThresholdLineSeries<Datum> {
  /** Unique; identifies the line across renders. */
  key: string;
  label?: string;
  /** The value at `datum`, or null for a gap that breaks the line. */
  yAccessor: (d: Datum) => number | null;
  /**
   * What the threshold judges this line by, or null when there is nothing to
   * judge. The caller supplies it because which statistic counts, a peak, a
   * p99 or the reading now, is its policy.
   */
  breachValue: number | null;
}
