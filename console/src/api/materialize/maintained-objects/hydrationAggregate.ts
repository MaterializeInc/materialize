// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { queryBuilder } from "~/api/materialize";

export interface HydrationAggregateRow {
  object_id: string;
  hydratedReplicas: bigint | null;
  totalReplicas: bigint | null;
  sourceStatus: string | null;
  sourceError: string | null;
  snapshotCommitted: boolean | null;
}

/**
 * One row per object: how many of its replicas report `hydrated = true` out of
 * the total replicas recorded in `mz_hydration_statuses`, plus, for sources,
 * their ingestion status, error, and snapshot flag.
 *
 * Source status rides on this feed rather than its own SUBSCRIBE: it is the
 * same one-row-per-object grain, and `mz_hydration_statuses` already reads
 * `mz_source_statistics`, so this adds no new frontier dependency. The join
 * with `mz_source_statuses` is FULL so webhook sources, which have a status but
 * no hydration rows, still get a row.
 *
 * `snapshot_committed` is aggregated with `bool_and`, so a source only reads as
 * committed once every replica reporting statistics agrees. A replica that has
 * not reported yet contributes no row, so a scale-up cannot flip a committed
 * source back to snapshotting.
 */
export function buildHydrationAggregateQuery() {
  const hydration = queryBuilder
    .selectFrom("mz_hydration_statuses")
    .select((eb) => [
      "object_id",
      sql<bigint>`count(*) FILTER (WHERE ${eb.ref("hydrated")})`.as(
        "hydratedReplicas",
      ),
      sql<bigint>`count(*)`.as("totalReplicas"),
    ])
    .groupBy("object_id");

  const snapshotCommittedBySource = queryBuilder
    .selectFrom("mz_source_statistics")
    .select((eb) => [
      "id",
      sql<boolean | null>`bool_and(${eb.ref("snapshot_committed")})`.as(
        "snapshotCommitted",
      ),
    ])
    .groupBy("id");

  return queryBuilder
    .selectFrom(hydration.as("h"))
    .fullJoin("mz_source_statuses as ss", "ss.id", "h.object_id")
    .leftJoin(snapshotCommittedBySource.as("stats"), "stats.id", "ss.id")
    .select([
      sql<string>`coalesce(h.object_id, ss.id)`.as("object_id"),
      "h.hydratedReplicas",
      "h.totalReplicas",
      "ss.status as sourceStatus",
      "ss.error as sourceError",
      "stats.snapshotCommitted",
    ]);
}
