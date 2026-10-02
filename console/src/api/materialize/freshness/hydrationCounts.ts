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

/**
 * Replica hydration counts for a known set of objects.
 *
 * Kept out of `buildLagHistoryQuery` because that builder is shared with
 * Environment Overview, Cluster Overview and the object panel's poll, none of
 * which read a hydration column. Joined there, it cost all of them a full scan
 * and reduce of `mz_hydration_statuses`.
 *
 * NOTE: scoping by object ID does not turn the read into an index lookup.
 * `mz_hydration_statuses_ind` is keyed on `(object_id, replica_id)`, and a
 * lookup needs every key column, so constraining `object_id` alone still plans
 * as a full scan. Verified on v26.45.0-dev with both an `IN` list and an
 * `unnest`ed array, at 2 and at 1000 IDs. The scoping is still worth keeping
 * for the rows it discards before the reduce, but the win here is that only
 * this page pays the scan.
 */
export function buildHydrationCountsQuery(objectIds: string[]) {
  return queryBuilder
    .selectFrom("mz_hydration_statuses as hs")
    .where("hs.object_id", "in", objectIds)
    .select(({ fn }) => [
      "hs.object_id as objectId",
      fn.countAll<bigint>().as("totalReplicas"),
      sql<bigint>`count(*) FILTER (WHERE hs.hydrated)`.as("hydratedReplicas"),
    ])
    .groupBy("hs.object_id");
}

export async function fetchHydrationCounts({
  objectIds,
  queryKey,
  requestOptions,
}: {
  objectIds: string[];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildHydrationCountsQuery(objectIds).compile(),
    queryKey,
    requestOptions,
  });
}
