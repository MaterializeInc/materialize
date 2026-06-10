// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql } from "kysely";

import { extractEnvironmentVersion } from "~/api/buildQueryKeySchema";
import { queryBuilder } from "~/api/materialize";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";
import {
  buildClusterReplicaHeapMetricsTable,
  buildClusterReplicaUtilizationTable,
} from "~/api/materialize/expressionBuilders";

export function buildClusterReplicaMetricsQuery({
  clusterId,
  environmentVersion,
}: ClusterReplicaMetricsParameters) {
  return (
    queryBuilder
      .selectFrom("mz_cluster_replicas as cr")
      .innerJoin("mz_cluster_replica_sizes as crs", "crs.size", "cr.size")
      .innerJoin(
        buildClusterReplicaUtilizationTable({ environmentVersion }).as("cru"),
        (join) => join.onRef("cr.id", "=", "cru.replica_id"),
      )
      // heap bytes are stored in cluster replica metrics because replicas of same sizes can have different swap limits
      .leftJoin(
        buildClusterReplicaHeapMetricsTable().as("crhm"),
        "crhm.replica_id",
        "cr.id",
      )
      .select((eb) => [
        "cr.id",
        "cr.name",
        "cr.size",
        sql<bigint>`${eb.ref("crs.cpu_nano_cores")} * ${eb.ref("crs.processes")}`.as(
          "cpuNanoCores",
        ),
        sql<bigint>`${eb.ref("crs.memory_bytes")} * ${eb.ref("crs.processes")}`.as(
          "memoryBytes",
        ),
        sql<bigint>`${eb.ref("crs.disk_bytes")} * ${eb.ref("crs.processes")}`.as(
          "diskBytes",
        ),
        sql<bigint>`COALESCE(crhm.heap_bytes::bigint, 0)`.as("heapBytes"),

        "cru.cpu_percent as cpuPercent",
        "cru.memory_percent as memoryPercent",
        "cru.disk_percent as diskPercent",
        "cru.heap_percent as heapPercent",
      ])
      .where("cr.cluster_id", "=", clusterId)

      .orderBy("cr.id")
  );
}

export type ClusterReplicaMetricsResult = InferResult<
  ReturnType<typeof buildClusterReplicaMetricsQuery>
>;

export type ClusterReplicaMetricsParameters = {
  clusterId: string;
  environmentVersion?: string;
};

/**
 * Fetches metrics for the replicas of a given cluster
 */
export async function fetchClusterReplicaMetrics({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: ClusterReplicaMetricsParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  // Environment version is nested in the clusterMetricsQueryKey from buildRegionQueryKey.
  const environmentVersion = extractEnvironmentVersion(
    queryKey[0] as readonly unknown[],
  );

  const compiledQuery = buildClusterReplicaMetricsQuery({
    ...parameters,
    environmentVersion,
  }).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
