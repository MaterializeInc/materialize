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
import { buildClusterReplicaUtilizationTable } from "~/api/materialize/expressionBuilders";

export const MAINTAINED_OBJECT_TYPES = [
  "source",
  "materialized-view",
  "index",
  "sink",
  "table",
] as const;

export type MaintainedObjectType = (typeof MAINTAINED_OBJECT_TYPES)[number];

export interface MaintainedObjectsListParams {
  /** Lookback window in minutes for the lag temporal filter. */
  lookbackMinutes: number;
}

/**
 * Fetches the latest lag for all user-owned maintained objects.
 * Uses mz_wallclock_global_lag_recent_history with a configurable temporal filter
 * and DISTINCT ON to get the max lag per object within the lookback window.
 */
export function buildMaintainedObjectsListQuery({
  lookbackMinutes,
}: MaintainedObjectsListParams) {
  return queryBuilder
    .with("latest_lag", (cte) =>
      cte
        .selectFrom("mz_wallclock_global_lag_recent_history")
        .select(["object_id", sql`max(lag)`.as("lag")])
        .where(
          (eb) =>
            sql`${eb.ref("occurred_at")} + INTERVAL '${sql.raw(`${lookbackMinutes}`)} MINUTES'`,
          ">=",
          sql<Date>`mz_now()`,
        )
        .groupBy("object_id"),
    )
    .selectFrom("mz_objects as o")
    .innerJoin(
      "mz_object_fully_qualified_names as fqn",
      "fqn.id",
      "o.id",
    )
    .leftJoin("mz_clusters as c", "c.id", "o.cluster_id")
    .leftJoin("mz_sources as s", "s.id", "o.id")
    .leftJoin("mz_hydration_statuses as hs", "hs.object_id", "o.id")
    .leftJoin("latest_lag as ll", "ll.object_id", "o.id")
    .select([
      "o.id",
      "fqn.name",
      "fqn.schema_name as schemaName",
      "fqn.database_name as databaseName",
      sql<MaintainedObjectType>`o.type`.as("objectType"),
      "c.id as clusterId",
      "c.name as clusterName",
      "s.type as sourceType",
      "hs.hydrated",
      "ll.lag",
    ])
    .where("o.id", "like", "u%")
    .where(
      "o.type",
      "in",
      MAINTAINED_OBJECT_TYPES as unknown as string[],
    )
    // Filter out subsources and progress sources — only show top-level sources
    .where((eb) =>
      eb.or([
        eb("o.type", "!=", "source"),
        eb.and([
          eb("s.type", "!=", "subsource"),
          eb("s.type", "!=", "progress"),
        ]),
      ]),
    )
    .orderBy("fqn.name");
}

export type MaintainedObjectRow = InferResult<
  ReturnType<typeof buildMaintainedObjectsListQuery>
>[0];

export async function fetchMaintainedObjectsList({
  params,
  queryKey,
  requestOptions,
}: {
  params: MaintainedObjectsListParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildMaintainedObjectsListQuery(params).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
    sessionVariables: {
      transaction_isolation: "serializable",
    },
  });
}

/**
 * Fetches current CPU and memory utilization per cluster replica.
 * Returns cluster-level utilization (same value for all objects on a cluster).
 */
export function buildClusterUtilizationQuery() {
  return queryBuilder
    .selectFrom(buildClusterReplicaUtilizationTable().as("cru"))
    .innerJoin("mz_cluster_replicas as cr", "cr.id", "cru.replica_id")
    .innerJoin("mz_clusters as c", "c.id", "cr.cluster_id")
    .select([
      "c.id as clusterId",
      "c.name as clusterName",
      "cr.id as replicaId",
      "cr.name as replicaName",
      "cru.cpu_percent as cpuPercent",
      "cru.memory_percent as memoryPercent",
    ])
    .where("c.id", "like", "u%");
}

export type ClusterUtilizationRow = InferResult<
  ReturnType<typeof buildClusterUtilizationQuery>
>[0];

export async function fetchClusterUtilization({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildClusterUtilizationQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}
