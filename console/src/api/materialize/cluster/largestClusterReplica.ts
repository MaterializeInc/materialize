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
      .innerJoin("mz_cluster_replica_sizes as crs", "cr.size", "crs.size")
      .leftJoin(
        // Sometimes system indexes on user clusters (e.g. introspection indexes)
        // do not hydrate. Thus we filter them out first.
        queryBuilder
          .selectFrom("mz_hydration_statuses")
          .select(["replica_id", "hydrated"])
          .where("object_id", "not like", "s%")
          .as("hs"),
        "cr.id",
        "hs.replica_id",
      )
      .leftJoin(
        buildClusterReplicaHeapMetricsTable().as("crhm"),
        "crhm.replica_id",
        "cr.id",
      )
      .select([
        "cr.name",
        "cr.size",
        sql<string | null>`crhm.heap_limit::text`.as("heapLimit"),
        sql<boolean>`bool_and(hs.hydrated)`.as("isHydrated"),
      ])
      .where("cr.cluster_id", "=", clusterId)
      .groupBy([
        "cr.name",
        "cr.size",
        "crs.memory_bytes",
        "crs.disk_bytes",
        "crs.processes",
        "crhm.heap_limit",
      ])
      // Prioritize fully hydrated clusters first, then order by memory
      .orderBy("isHydrated", sql`desc NULLS LAST`)
      .orderBy("heapLimit", sql`desc NULLS LAST`)
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
