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
import { buildClusterReplicaHeapMetricsTable } from "~/api/materialize/expressionBuilders";

export function buildLargestClusterReplicaQuery(clusterId: string) {
  return (
    queryBuilder
      .selectFrom("mz_cluster_replicas as cr")
      .leftJoin(
        buildClusterReplicaHeapMetricsTable().as("crhm"),
        "crhm.replica_id",
        "cr.id",
      )
      // Prefer a fully-hydrated replica, whose reported sizes are stable.
      // Joining mz_compute_hydration_times before aggregating lets its
      // replica_id index bound the scan to this cluster's replicas, unlike an
      // aggregation over the whole environment's mz_hydration_statuses.
      .leftJoin("mz_compute_hydration_times as ht", (join) =>
        join
          .onRef("ht.replica_id", "=", "cr.id")
          // System introspection indexes may never report hydrated.
          .on("ht.object_id", "not like", "s%"),
      )
      .select([
        "cr.name",
        "cr.size",
        sql<string | null>`crhm.heap_limit::text`.as("heapLimit"),
      ])
      .where("cr.cluster_id", "=", clusterId)
      .groupBy(["cr.id", "cr.name", "cr.size", "crhm.heap_limit"])
      // Prefer a replica whose compute objects are all hydrated, so its
      // reported sizes are stable. time_ns is NULL until an object hydrates. A
      // replica with no hydration rows, such as one just added, has bool_and
      // over an all-NULL row and so sorts last. Then order by heap limit.
      .orderBy(sql`bool_and(ht.time_ns IS NOT NULL) DESC`)
      .orderBy(sql`crhm.heap_limit DESC NULLS LAST`)
      .limit(1)
  );
}

export type LargestClusterReplicaParams = {
  clusterId: string;
};

/**
 * Fetches the largest cluster replica for a given cluster.
 */
export async function fetchLargestClusterReplica({
  params,
  queryKey,
  requestOptions,
}: {
  params: LargestClusterReplicaParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildLargestClusterReplicaQuery(
    params.clusterId,
  ).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
