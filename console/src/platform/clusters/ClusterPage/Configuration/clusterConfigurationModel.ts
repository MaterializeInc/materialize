// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  escapedLiteral,
  queryBuilder,
  quoteIdentifier,
} from "~/api/materialize";
import { Cluster } from "~/api/materialize/cluster/clusterList";
import { formatDurationExact } from "~/utils/format";

export type ConfigurationSection = "overview" | "replicas" | "auto-scaling";

export const CONFIGURATION_SECTIONS: readonly ConfigurationSection[] = [
  "overview",
  "replicas",
  "auto-scaling",
];

/** A cluster's autoscaling policy and in-flight burst, from its JSON. */
export interface AutoScaling {
  /** Null when no strategy is configured. */
  policy: {
    /** The size of the replica added while objects hydrate. */
    hydrationSize: string;
    /** Undefined when the strategy leaves it to the default. */
    lingerMs: number | undefined;
  } | null;
  /** The burst running now, which can outlast a policy just removed. */
  burst: { size: string; lingerMs: number | undefined } | undefined;
}

const asObject = (value: unknown) =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

// Durations serialize as `{ secs, nanos }`.
const durationMs = (value: unknown) => {
  const duration = asObject(value);
  if (typeof duration?.secs !== "number") return undefined;
  const nanos = typeof duration.nanos === "number" ? duration.nanos : 0;
  return duration.secs * 1000 + nanos / 1e6;
};

/**
 * A `mz_cluster_auto_scaling_strategies` row, or null for a cluster without
 * one. `on_hydration` is the only sub-policy there is.
 */
export function parseAutoScaling(
  row: { strategy: unknown; state: unknown } | null,
): AutoScaling {
  const onHydration = asObject(asObject(row?.strategy)?.on_hydration);
  const burst = asObject(asObject(row?.state)?.burst);
  return {
    policy:
      typeof onHydration?.hydration_size === "string"
        ? {
            hydrationSize: onHydration.hydration_size,
            lingerMs: durationMs(onHydration.linger_duration),
          }
        : null,
    burst:
      typeof burst?.burst_size === "string"
        ? {
            size: burst.burst_size,
            lingerMs: durationMs(burst.linger_duration),
          }
        : undefined,
  };
}

export type ReplicaStatus =
  | { kind: "online" }
  | { kind: "offline"; reason: string | null }
  | { kind: "unknown" };

/** A replica is online once every one of its processes is. */
export function replicaStatus(
  statuses: Cluster["replicas"][number]["statuses"],
): ReplicaStatus {
  const offline = statuses.find(({ status }) => status === "offline");
  if (offline) return { kind: "offline", reason: offline.reason };
  if (
    statuses.length > 0 &&
    statuses.every(({ status }) => status === "online")
  ) {
    return { kind: "online" };
  }
  return { kind: "unknown" };
}

// Rounded to microseconds, an interval's precision, so float noise from the
// nanos can't reach the literal.
const lingerLiteral = (lingerMs: number) => {
  const micros = Math.round(lingerMs * 1000);
  return micros % 1_000_000 === 0
    ? `'${micros / 1_000_000}s'`
    : `'${micros / 1000}ms'`;
};

/** A linger for display. Zero reads `0s`, as the docs write the default. */
export const formatLinger = (lingerMs: number) =>
  lingerMs === 0 ? "0s" : formatDurationExact(lingerMs);

/** Sets an `ON HYDRATION` autoscaling strategy. */
export const setAutoScalingStatement = (
  clusterName: string,
  hydrationSize: string,
  lingerMs: number | undefined,
) =>
  [
    `ALTER CLUSTER ${quoteIdentifier(clusterName)} SET (`,
    "  AUTO SCALING STRATEGY = (",
    `    ON HYDRATION (HYDRATION SIZE = ${
      escapedLiteral(hydrationSize).compile(queryBuilder).sql
    }${
      lingerMs === undefined
        ? ""
        : `, LINGER DURATION = ${lingerLiteral(lingerMs)}`
    })`,
    "  )",
    ");",
  ].join("\n");

/** Removes a cluster's autoscaling strategy. */
export const resetAutoScalingStatement = (clusterName: string) =>
  `ALTER CLUSTER ${quoteIdentifier(clusterName)} RESET (AUTO SCALING STRATEGY);`;

/**
 * Says what an in-progress resize changes about the size and replication
 * factor, or undefined when it changes neither. Until it finishes,
 * `mz_clusters` keeps the current values.
 */
export function resizeNote(
  cluster: Pick<Cluster, "size" | "replicationFactor">,
  target: { size: string | null; replicationFactor: number | null },
) {
  const size =
    target.size !== null && target.size !== cluster.size
      ? target.size
      : undefined;
  const factor =
    target.replicationFactor !== null &&
    target.replicationFactor !== cluster.replicationFactor
      ? target.replicationFactor
      : undefined;
  if (size !== undefined && factor !== undefined) {
    return `A resize to ${size} with replication factor ${factor} is in progress. Size and replication factor show the current values until it finishes.`;
  }
  if (size !== undefined) {
    return `A resize to ${size} is in progress. Size shows the current size until it finishes.`;
  }
  if (factor !== undefined) {
    return `A change to replication factor ${factor} is in progress. Replication factor shows the current factor until it finishes.`;
  }
  return undefined;
}
