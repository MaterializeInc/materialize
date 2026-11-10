// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { calculateBucketSizeFromLookback } from "~/api/materialize/freshness/lagHistory";
import { DataPoint } from "~/components/FreshnessGraph/types";

/**
 * The graph's data over exactly the selected range, ending at `nowMs`.
 *
 * Every bin in the range gets a point, empty where no object has a reading, so
 * a line breaks across a gap instead of joining the readings either side. The
 * newest bin starts before `nowMs`, which leaves a gap at the right edge: the
 * end of a line is as old as its bin, not now.
 *
 * NOTE: the oldest bin can start before the range does, putting its point left
 * of the plot. The graph clips lines to the plot for that.
 */
export function fitToRange<
  T extends { historicalData: DataPoint[]; startTime: number; endTime: number },
>(data: T, lookbackMs: number, nowMs: number): T {
  const binMs = calculateBucketSizeFromLookback(lookbackMs);
  const startTime = nowMs - lookbackMs;
  const byTimestamp = new Map(
    data.historicalData.map((point) => [point.timestamp, point]),
  );
  const historicalData: DataPoint[] = [];
  for (
    let timestamp = Math.floor(startTime / binMs) * binMs;
    timestamp <= nowMs;
    timestamp += binMs
  ) {
    historicalData.push(byTimestamp.get(timestamp) ?? { timestamp, lag: {} });
  }
  return { ...data, historicalData, startTime, endTime: nowMs };
}
