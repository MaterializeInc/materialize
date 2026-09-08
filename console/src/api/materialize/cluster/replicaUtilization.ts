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

import { executeSqlV2, queryBuilder } from "~/api/materialize";

/**
 * Peak utilization per replica over the last hour.
 *
 * Reads the maintained `_3h` view that already backs the cluster detail page's
 * utilization charts, so the list and the charts agree and no new arrangement
 * is built for the list.
 *
 * NOTE: the view reports fractions of the replica's allocation, not
 * percentages, matching the rest of the utilization-history path. The column
 * names carry `percent` because the view's own columns do.
 */
export function buildReplicaUtilizationQuery() {
  return (
    queryBuilder
      .selectFrom("mz_console_cluster_utilization_overview_3h")
      // The view retains three hours; the list reports the last hour of it. The
      // `mz_now()` form is what lets the filter bound the read rather than
      // filtering after the fact.
      .where(sql<boolean>`mz_now() <= occurred_at + INTERVAL '1 hour'`)
      .groupBy("replica_id")
      .select([
        "replica_id as replicaId",
        sql<number | null>`MAX(cpu_percent)`.as("cpuPercent"),
        sql<number | null>`MAX(memory_percent)`.as("memoryPercent"),
        sql<number | null>`MAX(disk_percent)`.as("diskPercent"),
        sql<number | null>`MAX(heap_percent)`.as("heapPercent"),
      ])
  );
}

export type ReplicaUtilization = InferResult<
  ReturnType<typeof buildReplicaUtilizationQuery>
>[0];

/** Fetches last-hour peak utilization for every replica in the environment. */
export async function fetchReplicaUtilization({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildReplicaUtilizationQuery().compile();
  return executeSqlV2({ queries: compiledQuery, queryKey, requestOptions });
}
