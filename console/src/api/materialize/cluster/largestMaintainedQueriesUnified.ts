// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Alternative largest maintained queries using mz_internal.mz_object_arrangement_sizes.
 *
 * Uses the unified introspection collection instead of per-cluster session variables.
 * No SET cluster / SET cluster_replica needed — queries mz_catalog_server directly.
 *
 * Drop-in replacement for largestMaintainedQueries.ts once validated.
 */

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { executeSqlV2 } from "~/api/materialize";
import { queryBuilder as rawQueryBuilder } from "~/api/materialize/db";

export type LargestMaintainedQueriesUnifiedParams = {
  replicaId: string;
  replicaHeapLimit: number;
  limit: number;
};

export type LargestMaintainedQueriesUnifiedRow = {
  id: string;
  name: string;
  size: bigint | null;
  memoryPercentage: number | null;
  type: "materialized-view" | "index";
  schemaName: string;
  databaseName: string;
};

export function buildLargestMaintainedQueriesUnifiedQuery({
  replicaId,
  replicaHeapLimit,
  limit,
}: LargestMaintainedQueriesUnifiedParams) {
  return sql<LargestMaintainedQueriesUnifiedRow>`
    SELECT
      o.id,
      o.name,
      oas.size,
      ${
        replicaHeapLimit
          ? sql`(oas.size::float8 / ${sql.raw(replicaHeapLimit.toString())}) * 100`
          : sql`null`
      } AS "memoryPercentage",
      o.type,
      sc.name AS "schemaName",
      da.name AS "databaseName"
    FROM mz_internal.mz_object_arrangement_sizes AS oas
    INNER JOIN mz_objects AS o ON o.id = oas.object_id
    LEFT JOIN mz_schemas AS sc ON sc.id = o.schema_id
    LEFT JOIN mz_databases AS da ON da.id = sc.database_id
    WHERE oas.replica_id = ${sql.lit(replicaId)}
      AND oas.object_id NOT LIKE 't%'
    ORDER BY "memoryPercentage" DESC NULLS LAST
    LIMIT ${sql.raw(limit.toString())}
  `;
}

export async function fetchLargestMaintainedQueriesUnified({
  params,
  queryKey,
  requestOptions,
}: {
  params: LargestMaintainedQueriesUnifiedParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const query = buildLargestMaintainedQueriesUnifiedQuery(params);

  // No sessionVariables needed — queries mz_catalog_server directly
  return executeSqlV2({
    queries: query.compile(rawQueryBuilder),
    queryKey: queryKey,
    requestOptions,
  });
}
