// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CriticalPathRow } from "~/api/materialize/maintained-objects/criticalPath";
import { sumPostgresIntervalMs } from "~/util";

export interface FreshnessBreakdown {
  selfDelayMs: number;
  maxInputMs: number;
  isSelfBottleneck: boolean;
  /** Object ids of immediate upstreams tied for max input lag (the chain
   *  bottleneck at the first hop). Use these to color the corresponding nodes. */
  dominantUpstreamIds: string[];
  /** Display names of those same upstreams, used by the breakdown strip. */
  dominantUpstreamNames: string[];
}

/** Don't show breakdown for objects that are essentially fresh — tiny
 *  rounding-level numbers just add noise. */
const BREAKDOWN_MIN_LAG_MS = 1000;

export const computeFreshnessBreakdown = (
  data: CriticalPathRow[],
): FreshnessBreakdown | null => {
  if (data.length === 0) return null;
  const targetLag = data[0]?.targetLag;
  if (!targetLag) return null;
  const targetMs = sumPostgresIntervalMs(targetLag);
  if (targetMs < BREAKDOWN_MIN_LAG_MS) return null;
  const lagsMs = data.map((r) => (r.lag ? sumPostgresIntervalMs(r.lag) : 0));
  const maxInputMs = Math.max(0, ...lagsMs);
  const selfDelayMs = targetMs - maxInputMs;
  if (selfDelayMs <= 0) return null;
  const dominant = data.filter(
    (r, i) => maxInputMs > 0 && lagsMs[i] === maxInputMs,
  );
  return {
    selfDelayMs,
    maxInputMs,
    isSelfBottleneck: selfDelayMs > maxInputMs,
    dominantUpstreamIds: dominant.map((r) => r.id),
    dominantUpstreamNames: dominant.map((r) => r.name),
  };
};
