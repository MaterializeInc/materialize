// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { ThemeColors } from "~/theme";

import type { RelationVitals } from "./queries";

/** Below this lag a relation reads as current. */
export const FRESH_LAG_MS = 2_000;
/** Above this lag a relation reads as stale. */
export const STALE_LAG_MS = 30_000;

export type FreshnessState =
  "fresh" | "lagging" | "stale" | "hydrating" | "computedOnRead" | "unknown";

export interface Freshness {
  state: FreshnessState;
  lagMs: number | null;
}

/**
 * How current a relation is, from its write frontier. A plain view has no
 * frontier of its own: it is computed when read, so it is as fresh as its
 * inputs and is not flagged.
 */
export function freshnessOf(
  vitals: RelationVitals | undefined,
  kind: string,
  now: number,
): Freshness {
  if (!vitals || vitals.writeFrontier === null) {
    return {
      state:
        vitals?.hydrated === false
          ? "hydrating"
          : kind === "view"
            ? "computedOnRead"
            : "unknown",
      lagMs: null,
    };
  }
  if (vitals.hydrated === false) return { state: "hydrating", lagMs: null };
  const lagMs = Math.max(0, now - vitals.writeFrontier);
  if (lagMs < FRESH_LAG_MS) return { state: "fresh", lagMs };
  if (lagMs < STALE_LAG_MS) return { state: "lagging", lagMs };
  return { state: "stale", lagMs };
}

export function formatLag(lagMs: number) {
  if (lagMs < 10_000) return `${(lagMs / 1000).toFixed(1)}s`;
  if (lagMs < 60_000) return `${Math.round(lagMs / 1000)}s`;
  if (lagMs < 3_600_000) return `${Math.round(lagMs / 60_000)}m`;
  return `${Math.round(lagMs / 3_600_000)}h`;
}

export function freshnessText(freshness: Freshness) {
  switch (freshness.state) {
    case "hydrating":
      return "Hydrating";
    case "computedOnRead":
      return "Computed on read";
    case "unknown":
      return null;
    default:
      return freshness.lagMs === null
        ? null
        : `${formatLag(freshness.lagMs)} behind`;
  }
}

export function freshnessColor(freshness: Freshness, colors: ThemeColors) {
  switch (freshness.state) {
    case "fresh":
      return colors.accent.green;
    case "lagging":
      return colors.accent.darkYellow;
    case "stale":
      return colors.accent.red;
    case "hydrating":
      return colors.accent.blue;
    default:
      return colors.foreground.tertiary;
  }
}
