// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Alternative per-object memory query using mz_internal.mz_object_arrangement_sizes.
 *
 * Uses the unified introspection collection instead of per-cluster session variables.
 * No SET cluster / SET cluster_replica needed — queries mz_catalog_server directly.
 *
 * Drop-in replacement for buildObjectMemoryQuery in objectDetail.ts once validated.
 */

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { executeSqlV2 } from "~/api/materialize";
import { queryBuilder as rawQueryBuilder } from "~/api/materialize/db";

export type ObjectMemoryUnifiedRow = {
  objectId: string;
  replicaId: string;
  replicaName: string;
  memoryBytes: string;
  memoryPercentage: number | null;
};

/**
 * Fetches per-object memory usage from the unified introspection collection.
 * Returns arrangement size for a single object across all replicas.
 * No session variables needed — queries mz_catalog_server directly.
 */
export function buildObjectMemoryUnifiedQuery(objectId: string) {
  return sql<ObjectMemoryUnifiedRow>`
    SELECT
      oas.object_id AS "objectId",
      oas.replica_id AS "replicaId",
      cr.name AS "replicaName",
      oas.size::text AS "memoryBytes",
      (oas.size::float8 / NULLIF(crs.memory_bytes * crs.processes, 0)) * 100 AS "memoryPercentage"
    FROM mz_internal.mz_object_arrangement_sizes AS oas
    INNER JOIN mz_cluster_replicas AS cr ON cr.id = oas.replica_id
    INNER JOIN mz_cluster_replica_sizes AS crs ON crs.size = cr.size
    WHERE oas.object_id = ${sql.lit(objectId)}
  `;
}

/**
 * Fetches per-object memory for a single object across all replicas.
 * Shows memory on each replica — useful for the object detail panel.
 */
export async function fetchObjectMemoryUnified({
  objectId,
  queryKey,
  requestOptions,
}: {
  objectId: string;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildObjectMemoryUnifiedQuery(objectId).compile(rawQueryBuilder);

  // No sessionVariables needed — queries mz_catalog_server directly
  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}
