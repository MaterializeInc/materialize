// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { InferResult, RawBuilder, sql } from "kysely";

import { queryBuilder } from "./db";
import {
  getQueryBuilderForVersion,
  semverParseGte,
  VersionMap,
} from "./versioning";

export { semverParseGte };

export function getOwners() {
  return queryBuilder.selectFrom("mz_roles as r").select((eb) => [
    "r.id",
    "r.name",
    eb
      .or([
        sql<boolean>`(${hasSuperUserPrivileges()})`,
        eb.fn<boolean>("has_role", [
          sql.id("current_user"),
          "r.oid",
          sql.lit("USAGE"),
        ]),
      ])
      .$castTo<boolean>()
      .as("isOwner"),
  ]);
}

// Note(SangJunBak): This is a helper function to convert an expression to a JSON array.
// We need to explicitly type the output and cannot generically use the type of the
// input expression. This is because the expression becomes converted into a jsonb and we cannot
// implicitly convert the jsonb back to its original types on runtime.
export function jsonArrayFrom<T, R = unknown>(expr: R): RawBuilder<T[]> {
  return sql`(select coalesce(jsonb_agg(agg), '[]') from ${expr} as agg)`;
}

type ClusterReplicaUtilizationArgs = {
  mzClusterReplicaUtilization?: "mz_cluster_replica_utilization";
};

const clusterReplicaUtilizationQuery = ({
  mzClusterReplicaUtilization = "mz_cluster_replica_utilization",
}: ClusterReplicaUtilizationArgs = {}) => {
  return queryBuilder
    .selectFrom(`${mzClusterReplicaUtilization} as cru`)
    .groupBy(["replica_id", "process_id"])
    .select([
      "replica_id",
      sql<number | null>`SUM(cru.cpu_percent) / COUNT(process_id)`.as(
        "cpu_percent",
      ),
      // Because processes have the same total memory size,
      // we're able to sum all memory percentages and divide by the number of processes. Assuming two processes,
      // this is equivalent to (process_1_memory + process_2_memory) / (process_1_total_memory + process_2_total).
      // We can assume if a process is offline, we'll still have a row for it that contains null values.
      sql<number | null>`SUM(cru.memory_percent) / COUNT(process_id)`.as(
        "memory_percent",
      ),
      sql<number | null>`SUM(cru.disk_percent) / COUNT(process_id)`.as(
        "disk_percent",
      ),
      // For multi-process clusters, we take the max heap percentage of all processes.
      sql<number | null>`MAX(cru.heap_percent)`.as("heap_percent"),
    ]);
};

const legacyClusterReplicaUtilizationQuery = ({
  mzClusterReplicaUtilization = "mz_cluster_replica_utilization",
}: ClusterReplicaUtilizationArgs = {}) => {
  return queryBuilder
    .selectFrom(`${mzClusterReplicaUtilization} as cru`)
    .groupBy(["replica_id", "process_id"])
    .select([
      "replica_id",
      sql<number | null>`SUM(cru.cpu_percent) / COUNT(process_id)`.as(
        "cpu_percent",
      ),
      // Because processes have the same total memory size,
      // we're able to sum all memory percentages and divide by the number of processes. Assuming two processes,
      // this is equivalent to (process_1_memory + process_2_memory) / (process_1_total_memory + process_2_total).
      // We can assume if a process is offline, we'll still have a row for it that contains null values.
      sql<number | null>`SUM(cru.memory_percent) / COUNT(process_id)`.as(
        "memory_percent",
      ),
      sql<number | null>`SUM(cru.disk_percent) / COUNT(process_id)`.as(
        "disk_percent",
      ),
      sql.lit(null).as("heap_percent"),
    ]);
};

type ClusterReplicaUtilizationResult = InferResult<
  ReturnType<typeof clusterReplicaUtilizationQuery>
>[0];

const CLUSTER_REPLICA_UTILIZATION_QUERIES: VersionMap<
  ClusterReplicaUtilizationArgs,
  ClusterReplicaUtilizationResult
> = {
  // For versions >= v0.161.0, heap_percent is available in the cluster replica utilization table.
  "0.161.0": clusterReplicaUtilizationQuery,
  "0.0.0": legacyClusterReplicaUtilizationQuery,
};

// NOTE(benesch): We do not have ideal handling for
// multiprocess clusters (i.e., 2xlarge+ clusters at the time of writing) in
// the Console. Specifically, we merge metrics across processes as if they were a single process.
// Ideally it would return data for each process in
// the replica separately, but the downstream consumers (e.g., the replica
// table) are not yet equipped to handle that.
// A better fix should be handled here (https://github.com/MaterializeInc/console/issues/1041)
export function buildClusterReplicaUtilizationTable(
  {
    mzClusterReplicaUtilization = "mz_cluster_replica_utilization",
    environmentVersion,
  }: {
    mzClusterReplicaUtilization?: "mz_cluster_replica_utilization";
    environmentVersion?: string;
  } = {
    mzClusterReplicaUtilization: "mz_cluster_replica_utilization",
  },
) {
  const queryBuilderFactory = getQueryBuilderForVersion(
    environmentVersion,
    CLUSTER_REPLICA_UTILIZATION_QUERIES,
  );
  return queryBuilderFactory({ mzClusterReplicaUtilization });
}

/**
 * Useful since Kysely's count function returns a union of all number types in TypeScript
 */
export function countAll() {
  return sql<bigint>`count(*)`.as("count");
}

/**
 * This is important for flexible deployment mode where users aren't superusers by default
 * but need superuser privileges. RBAC is disabled by default in flexible deployment mode
 * so we can identify flexible deployment mode by checking the system variable `enable_rbac_checks`
 */
export function hasSuperUserPrivileges() {
  return sql.raw<boolean>(
    `SELECT mz_is_superuser() OR current_setting('enable_rbac_checks') = 'off'`,
  );
}

/**
 *
 * Represents Postgres and MySQL tables for pre and post source versioning. Also represents Kafka and Webhook sources post source versioning.
 */
export function buildSourceDependenciesQuery(sourceId: string) {
  return (
    queryBuilder
      // We use mz_object_dependencies instead of mz_tables and mz_sources since mz_tables is not a retained metrics object yet (https://github.com/MaterializeInc/materialize/pull/30788).
      .selectFrom("mz_object_dependencies as od")
      .leftJoin("mz_sources as subsources", "object_id", "subsources.id")
      .where("referenced_object_id", "=", sourceId)
      .where((eb) =>
        eb.or([
          eb("subsources.type", "<>", "progress"),
          // When the source dependency is a table, subsources.type is null.
          eb("subsources.type", "is", null),
        ]),
      )
      .select(["od.object_id as id", "od.referenced_object_id as sourceId"])
  );
}

const FIRST_REPLICA_SOURCE_STATISTICS_QUERIES = {
  // This is the query for modern versions of Materialize (>= v0.148.0).
  "0.148.0": () =>
    queryBuilder
      .with("latest_replica_per_source", (qb) =>
        qb
          .selectFrom("mz_cluster_replica_history as crh")
          // Filter to only extant cluster replicas - mz_cluster_replica_history contains
          // records for dropped replicas, but we only want statistics from active ones
          .innerJoin("mz_cluster_replicas as cr", "cr.id", "crh.replica_id")
          .innerJoin("mz_sources as s", "s.cluster_id", "cr.cluster_id")
          .select(["s.id as source_id", "crh.replica_id", "crh.created_at"])
          .distinctOn("s.id")
          .orderBy("s.id", "asc")
          .orderBy("crh.created_at", sql`desc NULLS LAST`),
      )
      .selectFrom("mz_source_statistics as stat")
      .innerJoin("latest_replica_per_source as lr", "lr.source_id", "stat.id")
      .whereRef("lr.replica_id", "=", "stat.replica_id")
      .selectAll("stat"),

  // The '0.0.0' query is a fallback for any version older than the ones specified above.
  "0.0.0": () =>
    queryBuilder
      .selectFrom("mz_source_statistics as stat")
      .distinctOn("stat.id")
      .selectAll("stat")
      .orderBy("stat.id"),
};

/**
 * Builds a subquery that selects the first replica's source statistics for each source.
 * This is useful for avoiding duplicate rows when joining with mz_source_statistics
 * in queries that don't need per-replica granularity.
 *
 * @param environmentVersion - The environment version string (e.g., "0.148.0").
 * If not provided, defaults to the latest query variant.
 */
export function buildFirstReplicaSourceStatisticsTable(
  environmentVersion?: string,
) {
  const queryBuilderFactory = getQueryBuilderForVersion(
    environmentVersion,
    FIRST_REPLICA_SOURCE_STATISTICS_QUERIES,
  );
  return queryBuilderFactory({});
}

export type FirstReplicaSourceStatistics = InferResult<
  ReturnType<typeof buildFirstReplicaSourceStatisticsTable>
>[0];

/**
 * Builds a subquery that aggregates heap metrics for each cluster replica.
 * This eliminates the need for groupBy in queries that only need heap metrics,
 * making the queries more readable and maintainable.
 */
export function buildClusterReplicaHeapMetricsTable() {
  return queryBuilder
    .selectFrom("mz_cluster_replica_metrics as crm")
    .groupBy("crm.replica_id")
    .select([
      "crm.replica_id",
      sql<bigint | null>`MAX(crm.heap_limit)`.as("heap_limit"),
      sql<bigint | null>`MAX(crm.heap_bytes)`.as("heap_bytes"),
    ]);
}

export type ClusterReplicaHeapMetrics = InferResult<
  ReturnType<typeof buildClusterReplicaHeapMetricsTable>
>[0];
