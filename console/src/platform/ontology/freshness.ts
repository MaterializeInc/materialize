// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { ThemeColors } from "~/theme";

import { LINEAGE_EDGE_KIND, OntologyEdge, OntologyNode } from "./model";
import type { RelationVitals } from "./queries";

/** Below this lag a relation reads as current. */
export const FRESH_LAG_MS = 2_000;
/** Above this lag a relation reads as stale. */
export const STALE_LAG_MS = 30_000;
/**
 * How long a relation must stay past `FRESH_LAG_MS` before it reads as
 * lagging. Frontiers advance about once a second, so the lag measured
 * between two advances swings by a second, and a relation near the threshold
 * would otherwise flip between fresh and lagging on every tick.
 */
export const LAGGING_HOLD_MS = 2_000;

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

/**
 * `freshness`, reading as fresh until the relation has been lagging for
 * `LAGGING_HOLD_MS`. `laggingSince` holds, by relation, when its current
 * stretch of lagging began, and is updated here.
 */
export function steadyFreshness(
  freshness: Freshness,
  id: string,
  now: number,
  laggingSince: Map<string, number>,
): Freshness {
  if (freshness.state !== "lagging") {
    laggingSince.delete(id);
    return freshness;
  }
  const since = laggingSince.get(id) ?? now;
  laggingSince.set(id, since);
  return now - since < LAGGING_HOLD_MS
    ? { ...freshness, state: "fresh" }
    : freshness;
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

export type FlowSpeed = "fast" | "slow";

/**
 * How fast changes visibly flow into each relation along its lineage edges,
 * by node id, for the relations they flow into at all. A maintained
 * relation takes changes in as they happen, unless it is stuck past
 * `STALE_LAG_MS`. A plain view maintains nothing, but a maintained relation
 * reading it runs it inline, so changes flow through it at the fastest speed
 * of the maintained relations reading it, directly or through other plain
 * views.
 */
export function flowSpeeds(
  nodes: OntologyNode[],
  edges: OntologyEdge[],
  getFreshness: (node: OntologyNode) => Freshness,
): Map<string, FlowSpeed> {
  const flow = new Map<string, FlowSpeed>();
  const plainViews = new Set<string>();
  for (const node of nodes) {
    const freshness = getFreshness(node);
    if (freshness.state === "computedOnRead") plainViews.add(node.id);
    if (freshness.state === "fresh") flow.set(node.id, "fast");
    if (freshness.state === "lagging") flow.set(node.id, "slow");
  }
  const lineage = edges.filter((edge) => edge.kind === LINEAGE_EDGE_KIND);
  for (let changed = true; changed;) {
    changed = false;
    for (const { source, target } of lineage) {
      const speed = flow.get(source);
      const current = flow.get(target);
      if (!speed || !plainViews.has(target) || current === "fast") continue;
      if (current !== speed) {
        flow.set(target, speed);
        changed = true;
      }
    }
  }
  return flow;
}
