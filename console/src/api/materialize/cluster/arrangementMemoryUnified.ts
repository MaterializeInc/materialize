// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Alternative arrangement memory query using mz_internal.mz_object_arrangement_sizes.
 *
 * Uses the unified introspection collection instead of per-cluster session variables.
 * No SET cluster / SET cluster_replica needed — queries mz_catalog_server directly.
 *
 * Drop-in replacement for arrangementMemory.ts once validated.
 */

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { executeSqlV2 } from "~/api/materialize";
import { queryBuilder as rawQueryBuilder } from "~/api/materialize/db";

export type ArrangementMemoryUnifiedParams = {
  arrangementIds?: string[];
  replicaId: string;
  replicaHeapLimit?: number;
};

export type MemoryUsageUnified = {
  id: string;
  size: bigint | null;
  memoryPercentage: number | null;
};

export function buildArrangementMemoryUnifiedQuery({
  replicaId,
  replicaHeapLimit,
  arrangementIds,
}: Required<
  Pick<
    ArrangementMemoryUnifiedParams,
    "replicaId" | "replicaHeapLimit" | "arrangementIds"
  >
>) {
  const idList = arrangementIds.map((id) => `'${id}'`).join(", ");

  return sql<MemoryUsageUnified>`
    SELECT
      o.id,
      oas.size,
      ${
        replicaHeapLimit
          ? sql`(oas.size::float8 / ${sql.raw(replicaHeapLimit.toString())}) * 100`
          : sql`null`
      } AS "memoryPercentage"
    FROM mz_internal.mz_object_arrangement_sizes AS oas
    INNER JOIN mz_objects AS o ON o.id = oas.object_id
    WHERE oas.replica_id = ${sql.lit(replicaId)}
      AND o.id IN (${sql.raw(idList)})
  `;
}

export async function fetchArrangementMemoryUnified({
  params: { replicaId, arrangementIds, replicaHeapLimit },
  queryKey,
  requestOptions,
}: {
  params: ArrangementMemoryUnifiedParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  if (
    !replicaId ||
    !arrangementIds ||
    !replicaHeapLimit ||
    arrangementIds?.length === 0
  ) {
    return null;
  }

  const compiledQuery = buildArrangementMemoryUnifiedQuery({
    replicaId,
    arrangementIds,
    replicaHeapLimit,
  }).compile(rawQueryBuilder);

  // No sessionVariables needed — queries mz_catalog_server directly
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
