// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql, SqlBool } from "kysely";

const lit = sql.lit;

import {
  buildSessionVariables,
  executeSqlV2,
  queryBuilder,
} from "~/api/materialize";
import { queryBuilder as rawQueryBuilder } from "~/api/materialize/db";

/**
 * Fetches detailed metadata for a single maintained object,
 * including cluster replica info and SQL definition.
 */
export function buildObjectDetailQuery(objectId: string) {
  return queryBuilder
    .selectFrom("mz_objects as o")
    .innerJoin(
      "mz_object_fully_qualified_names as fqn",
      "fqn.id",
      "o.id",
    )
    .leftJoin("mz_clusters as c", "c.id", "o.cluster_id")
    .leftJoin("mz_sources as s", "s.id", "o.id")
    .leftJoin("mz_hydration_statuses as hs", "hs.object_id", "o.id")
    .leftJoin(
      "mz_cluster_replicas as cr",
      "cr.cluster_id",
      "c.id",
    )
    .leftJoin(
      "mz_cluster_replica_sizes as crs",
      "crs.size",
      "cr.size",
    )
    .leftJoin(
      queryBuilder
        .selectFrom("mz_wallclock_global_lag_recent_history")
        .select(["object_id", "lag"])
        .distinctOn(["object_id"])
        .orderBy("object_id")
        .orderBy("occurred_at", "desc")
        .where(
          (eb) => sql`${eb.ref("occurred_at")} + INTERVAL '5 MINUTES'`,
          ">=",
          sql<Date>`mz_now()`,
        )
        .as("ll"),
      "ll.object_id",
      "o.id",
    )
    .select([
      "o.id",
      "fqn.name",
      "fqn.schema_name as schemaName",
      "fqn.database_name as databaseName",
      sql<string>`o.type`.as("objectType"),
      "o.owner_id as ownerId",
      "c.id as clusterId",
      "c.name as clusterName",
      sql<boolean | null>`c.managed`.as("clusterManaged"),
      "cr.id as replicaId",
      "cr.name as replicaName",
      "cr.size as replicaSize",
      sql<string | null>`(crs.memory_bytes * crs.processes)::text`.as(
        "replicaTotalMemoryBytes",
      ),
      "s.type as sourceType",
      "hs.hydrated",
      "ll.lag",
    ])
    .where("o.id", "=", objectId);
}

export type ObjectDetailRow = InferResult<
  ReturnType<typeof buildObjectDetailQuery>
>[0];

export async function fetchObjectDetail({
  objectId,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectDetailQuery(objectId).compile();
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
 * Fetches per-object memory usage using optimized raw tables.
 *
 * Reads only mz_arrangement_heap_size_raw + mz_arrangement_batcher_size_raw
 * instead of the full mz_dataflow_arrangement_sizes (10-way CTE).
 * Returns the same `size` value but skips records, batches, capacity, allocations.
 *
 * Requires cluster + cluster_replica session variables.
 * Only works for compute objects (indexes, MVs).
 */
export function buildObjectMemoryQuery(objectId: string) {
  return sql<{
    objectId: string;
    memoryBytes: string;
  }>`
    SELECT
      ce.export_id AS "objectId",
      (COALESCE(SUM(hs.size), 0) + COALESCE(SUM(bs.size), 0))::text AS "memoryBytes"
    FROM mz_compute_exports AS ce
    JOIN mz_dataflow_operator_dataflows AS dod
      ON dod.dataflow_id = ce.dataflow_id
    LEFT JOIN (
      SELECT operator_id, COUNT(*) AS size
      FROM mz_arrangement_heap_size_raw
      GROUP BY operator_id
    ) AS hs ON hs.operator_id = dod.id
    LEFT JOIN (
      SELECT operator_id, COUNT(*) AS size
      FROM mz_arrangement_batcher_size_raw
      GROUP BY operator_id
    ) AS bs ON bs.operator_id = dod.id
    WHERE ce.export_id = ${lit(objectId)}
    GROUP BY ce.export_id
  `;
}

export type ObjectMemoryRow = {
  objectId: string;
  memoryBytes: string;
};

export async function fetchObjectMemory({
  objectId,
  clusterName,
  replicaName,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  clusterName: string;
  replicaName: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectMemoryQuery(objectId).compile(rawQueryBuilder);
  return executeSqlV2({
    sessionVariables: {
      ...buildSessionVariables({
        cluster: clusterName,
        cluster_replica: replicaName,
      }),
      transaction_isolation: "serializable",
    },
    queries: compiledQuery,
    queryKey,
    requestOptions,
    requestTimeoutMs: 60_000,
  });
}

/**
 * Fetches the latest lag for a single object.
 * Lightweight query — used for live polling in the detail panel.
 */
export function buildObjectLagQuery(objectId: string) {
  return queryBuilder
    .selectFrom("mz_wallclock_global_lag_recent_history")
    .select(["object_id", "lag"])
    .distinctOn(["object_id"])
    .orderBy("object_id")
    .orderBy("occurred_at", "desc")
    .where("object_id", "=", objectId)
    .where(
      (eb) => sql`${eb.ref("occurred_at")} + INTERVAL '5 MINUTES'`,
      ">=",
      sql<Date>`mz_now()`,
    );
}

export async function fetchObjectLag({
  objectId,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectLagQuery(objectId).compile();
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
 * Fetches column definitions for an object.
 */
export function buildObjectColumnsQuery(objectId: string) {
  return queryBuilder
    .selectFrom("mz_columns")
    .select(["name", "type", "nullable"])
    .where("id", "=", objectId)
    .orderBy("position");
}

export type ObjectColumnRow = InferResult<
  ReturnType<typeof buildObjectColumnsQuery>
>[0];

export async function fetchObjectColumns({
  objectId,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectColumnsQuery(objectId).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}
