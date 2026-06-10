// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql } from "kysely";

import { executeSqlV2, queryBuilder } from "../";
import { buildClusterReplicaHeapMetricsTable } from "../expressionBuilders";

export type ClusterReplicasParams = {
  clusterId: string;
  orderBy: "memoryBytes" | "name" | "heapLimit";
};

export function buildClusterReplicasQuery({
  clusterId,
  orderBy,
}: ClusterReplicasParams) {
  return (
    queryBuilder
      .selectFrom("mz_cluster_replicas as cr")
      .innerJoin("mz_cluster_replica_sizes as crs", "cr.size", "crs.size")
      .leftJoin(
        buildClusterReplicaHeapMetricsTable().as("crhm"),
        "crhm.replica_id",
        "cr.id",
      )
      .select((eb) => [
        "cr.name",
        "cr.size",
        sql<string>`(${eb.ref("crs.memory_bytes")} * ${eb.ref(
          "crs.processes",
        )})::text`.as("memoryBytes"),
        sql<string>`(${eb.ref("crs.disk_bytes")} * ${eb.ref(
          "crs.processes",
        )})::text`.as("diskBytes"),
        sql<string | null>`crhm.heap_limit::text`.as("heapLimit"),
      ])
      .where("cr.cluster_id", "=", clusterId)
      // Order by smallest replicas first
      .orderBy(orderBy)
  );
}

const spanContext = {
  name: "cluster-replicas-query",
  op: "http.client",
};

/**
 * Fetches all replicas for a given cluster.
 */
export async function fetchClusterReplicas(
  params: ClusterReplicasParams,
  queryKey: QueryKey,
  requestOptions?: RequestInit,
) {
  const compiledQuery = buildClusterReplicasQuery(params).compile();
  return Sentry.startSpan(spanContext, async () =>
    executeSqlV2({
      queries: compiledQuery,
      queryKey: queryKey,
      requestOptions,
    }),
  );
}

export type ClusterReplica = InferResult<
  ReturnType<typeof buildClusterReplicasQuery>
>[0];
