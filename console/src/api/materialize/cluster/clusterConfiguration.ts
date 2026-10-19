// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";

import { executeSqlV2, queryBuilder } from "~/api/materialize";

import { buildObjectCreationTimesQuery } from "./objectCreationTimes";

/** First version with `mz_internal.mz_cluster_auto_scaling_strategies`. */
export const AUTO_SCALING_MIN_VERSION = "26.34.0";

/** A cluster's autoscaling strategy and any in-flight burst, as JSON. */
export function buildClusterAutoScalingQuery(clusterId: string) {
  return queryBuilder
    .selectFrom("mz_cluster_auto_scaling_strategies")
    .select(["strategy", "state"])
    .where("cluster_id", "=", clusterId);
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
    /** The autoscaling relation only exists from `AUTO_SCALING_MIN_VERSION`. */
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
    return { creationTimes: creationTimesRes.rows, autoScaling: undefined };
  }
  const [creationTimesRes, autoScalingRes] = await executeSqlV2({
    queries: [
      creationTimes,
      buildClusterAutoScalingQuery(clusterId).compile(),
    ] as const,
    queryKey,
    requestOptions,
  });
  return {
    creationTimes: creationTimesRes.rows,
    // No row means no strategy and no burst.
    autoScaling: autoScalingRes.rows.at(0) ?? null,
  };
}
