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
 * source back to snapshotting. Statistics rows can outlive their replica, so
 * the aggregate is restricted to live replicas (keeping rows with no
 * `replica_id`, which is how webhook sources report); a dropped replica's
 * stale row must not pin the flag.
 *
 * Subsources and progress collections are excluded: the UI hides them, and
 * they multiply the feed's cardinality several times over.
 */
export function buildHydrationAggregateQuery() {
  // Subsources have hydration rows of their own (they run on their parent's
  // cluster), so both sides of the FULL JOIN need the exclusion.
  const hydration = queryBuilder
    .selectFrom("mz_hydration_statuses as hs")
    .leftJoin("mz_sources as s", "s.id", "hs.object_id")
    .where((eb) =>
      eb.or([
        eb("s.type", "is", null),
        eb("s.type", "not in", ["subsource", "progress"]),
      ]),
    )
    .select((eb) => [
      "hs.object_id",
      sql<bigint>`count(*) FILTER (WHERE ${eb.ref("hs.hydrated")})`.as(
        "hydratedReplicas",
      ),
      sql<bigint>`count(*)`.as("totalReplicas"),
    ])
    .groupBy("hs.object_id");

  const statuses = queryBuilder
    .selectFrom("mz_source_statuses")
    .where("type", "not in", ["subsource", "progress"])
    .select(["id", "status", "error"]);

  const snapshotCommittedBySource = queryBuilder
    .selectFrom("mz_source_statistics as st")
    .leftJoin("mz_cluster_replicas as r", "r.id", "st.replica_id")
    .where((eb) =>
      eb.or([eb("r.id", "is not", null), eb("st.replica_id", "is", null)]),
    )
    .select((eb) => [
      "st.id",
      sql<boolean | null>`bool_and(${eb.ref("st.snapshot_committed")})`.as(
        "snapshotCommitted",
      ),
    ])
    .groupBy("st.id");

  return queryBuilder
    .selectFrom(hydration.as("h"))
    .fullJoin(statuses.as("ss"), "ss.id", "h.object_id")
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
