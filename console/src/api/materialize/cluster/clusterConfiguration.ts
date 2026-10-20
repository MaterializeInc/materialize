// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";

import { buildObjectCreationTimesQuery } from "./objectCreationTimes";

/**
 * First version with `mz_internal.mz_cluster_auto_scaling_strategies` and
 * `mz_internal.mz_cluster_reconfigurations`.
 */
export const AUTO_SCALING_MIN_VERSION = "26.34.0";

/** A cluster's autoscaling strategy and any in-flight burst, as JSON. */
export function buildClusterAutoScalingQuery(clusterId: string) {
  return queryBuilder
    .selectFrom("mz_cluster_auto_scaling_strategies")
    .select(["strategy", "state"])
    .where("cluster_id", "=", clusterId);
}

/**
 * The size and replication factor an in-progress resize of the cluster is
 * heading to.
 */
export function buildClusterResizeTargetQuery(clusterId: string) {
  return queryBuilder
    .selectFrom("mz_cluster_reconfigurations")
    .select([
      sql<string | null>`target->>'size'`.as("size"),
      sql<number | null>`(target->>'replication_factor')::int4`.as(
        "replicationFactor",
      ),
    ])
    .where("cluster_id", "=", clusterId)
    .where("status", "=", "in-progress");
}

/**
 * A cluster's autoscaling strategy row, null for a cluster without one, and
 * the size its in-progress resize is heading to, undefined for none.
 */
export async function fetchClusterAutoScaling({
  clusterId,
  queryKey,
  requestOptions,
}: {
  clusterId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  // Strict serializable, as in `fetchClusterConfiguration`: the form reads
  // this right after an ALTER.
  const [autoScalingRes, resizeTargetRes] = await executeSqlV2({
    queries: [
      buildClusterAutoScalingQuery(clusterId).compile(),
      buildClusterResizeTargetQuery(clusterId).compile(),
    ] as const,
    queryKey,
    requestOptions,
  });
  return {
    autoScaling: autoScalingRes.rows.at(0) ?? null,
    resizeTargetSize: resizeTargetRes.rows.at(0)?.size ?? undefined,
  };
}

/** The configuration the cluster list doesn't carry, read in one request. */
export async function fetchClusterConfiguration({
  params: { clusterId, replicaIds, includeAutoScaling },
  queryKey,
  requestOptions,
}: {
  params: {
    clusterId: string;
    replicaIds: string[];
    /**
     * The autoscaling and reconfiguration relations only exist from
     * `AUTO_SCALING_MIN_VERSION`.
     */
    includeAutoScaling: boolean;
  };
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const creationTimes = buildObjectCreationTimesQuery({
    objectIds: [clusterId, ...replicaIds],
    objectTypes: ["cluster", "cluster-replica"],
  }).compile();
  // NOTE: Strict serializable, the default, so a read right after an ALTER or
  // a new replica sees it. A serializable read can pick a timestamp from
  // before the write, and this query has no refetch interval to recover.
  if (!includeAutoScaling) {
    const [creationTimesRes] = await executeSqlV2({
      queries: [creationTimes] as const,
      queryKey,
      requestOptions,
    });
    return {
      creationTimes: creationTimesRes.rows,
      autoScaling: undefined,
      resizeTarget: undefined,
    };
  }
  const [creationTimesRes, autoScalingRes, resizeTargetRes] =
    await executeSqlV2({
      queries: [
        creationTimes,
        buildClusterAutoScalingQuery(clusterId).compile(),
        buildClusterResizeTargetQuery(clusterId).compile(),
      ] as const,
      queryKey,
      requestOptions,
    });
  return {
    creationTimes: creationTimesRes.rows,
    // No row means no strategy and no burst.
    autoScaling: autoScalingRes.rows.at(0) ?? null,
    // No row means no resize in progress.
    resizeTarget: resizeTargetRes.rows.at(0),
  };
}
