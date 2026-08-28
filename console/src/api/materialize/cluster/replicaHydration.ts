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
 * Per-replica hydration: how many of the dataflow objects installed on a
 * replica report `hydrated = true`, out of the objects counted for it.
 *
 * The counted set matches `buildHydrationAggregateQuery`, which backs the
 * Maintained Objects status column, so the two tables classify a replica the
 * same way. That means subsources and progress collections are left out, and
 * everything else `mz_hydration_statuses` reports is counted.
 *
 * NOTE: at replica grain that inherits two things the object-grain feed never
 * has to face. Sinks are counted, and they are the only branch of the view that
 * can yield `hydrated = NULL`; `count(*)` counts a NULL but the FILTER does
 * not, so an unreported sink holds a replica below its total. System
 * introspection objects are counted too, and every replica carries dozens, so
 * they dominate the total on a replica with few user objects. Narrowing the
 * counted set is deferred to its own change.
 *
 * Every replica is reported, including those of system clusters, which the
 * clusters list shows when "Show system clusters" is on.
 *
 * Rows with no `replica_id` are dropped. This is not an exclusion of objects but
 * of unattributable readings: the view emits such a row for an object with no
 * hydration report at all, so it describes no replica and would otherwise group
 * under a key that matches none.
 */
export function buildReplicaHydrationQuery() {
  return queryBuilder
    .selectFrom("mz_hydration_statuses as hs")
    .leftJoin("mz_sources as s", "s.id", "hs.object_id")
    .where("hs.replica_id", "is not", null)
    .where((eb) =>
      eb.or([
        eb("s.type", "is", null),
        eb("s.type", "not in", ["subsource", "progress"]),
      ]),
    )
    .select((eb) => [
      "hs.replica_id as replicaId",
      sql<bigint>`count(*) FILTER (WHERE ${eb.ref("hs.hydrated")})`.as(
        "hydratedObjects",
      ),
      sql<bigint>`count(*)`.as("totalObjects"),
    ])
    .groupBy("hs.replica_id");
}

export type ReplicaHydrationRow = InferResult<
  ReturnType<typeof buildReplicaHydrationQuery>
>[0];

/** Fetches per-replica hydration counts for every replica in the environment. */
export async function fetchReplicaHydration({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildReplicaHydrationQuery().compile();
  return executeSqlV2({ queries: compiledQuery, queryKey, requestOptions });
}
