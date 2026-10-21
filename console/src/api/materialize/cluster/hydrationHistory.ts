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
import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";

const timestampLit = (date: string) => sql<Date>`${sql.lit(date)}::timestamptz`;

/**
 * A cluster's hydration episodes that finished at or after `startDate`, one
 * row per episode. Requires mz >= 26.43, which records one row per process.
 * Episodes with no objects stay in: on a replica with nothing to hydrate, such
 * as one running only sources, they're the record that a boot finished.
 *
 * Peaks are the highest any of the episode's processes reached since it
 * started, read when the episode was recorded, so they can predate it.
 * `peakHeapBytes` is the highest memory plus disk peak of a single process.
 *
 * NOTE: the history has no index, so this reads storage. It is sampled one
 * replica at a time, so an episode shows up a while after it finishes, and
 * only episodes that finish are recorded.
 */
export function buildClusterHydrationEpisodesQuery({
  clusterId,
  startDate,
}: {
  clusterId: string;
  startDate: string;
}) {
  return (
    queryBuilder
      .selectFrom("mz_replica_hydration_history")
      .select([
        "replica_id as replicaId",
        "started_at as startedAt",
        // Timing and object count repeat on each process's row.
        sql<Date>`max(finished_at)`.as("finishedAt"),
        sql<number>`max(object_count)::float8`.as("objectCount"),
        sql<number | null>`max(peak_memory_bytes)::float8`.as(
          "peakMemoryBytes",
        ),
        sql<number | null>`max(peak_disk_bytes)::float8`.as("peakDiskBytes"),
        sql<
          number | null
        >`max(peak_memory_bytes + coalesce(peak_disk_bytes, 0))::float8`.as(
          "peakHeapBytes",
        ),
      ])
      .where("cluster_id", "=", clusterId)
      // Rows recorded before 26.43 have no process id, peaks taken across
      // processes, and counts that include introspection dataflows.
      .where("process_id", "is not", null)
      .where("finished_at", ">=", timestampLit(startDate))
      .groupBy(["replica_id", "started_at"])
  );
}

export async function fetchClusterHydrationEpisodes({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildClusterHydrationEpisodesQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildClusterHydrationEpisodesQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}

/**
 * A cluster's replicas that existed at or after `startDate`, including
 * dropped ones, with each replica's name and size when it was created. System
 * clusters have none.
 */
export function buildClusterReplicaHistoryQuery({
  clusterId,
  startDate,
}: {
  clusterId: string;
  startDate: string;
}) {
  return queryBuilder
    .selectFrom("mz_cluster_replica_history")
    .select([
      "replica_id as replicaId",
      "replica_name as name",
      "size",
      "created_at as createdAt",
      "dropped_at as droppedAt",
    ])
    .where("cluster_id", "=", clusterId)
    .where((eb) =>
      eb.or([
        eb("dropped_at", "is", null),
        eb("dropped_at", ">=", timestampLit(startDate)),
      ]),
    );
}

export async function fetchClusterReplicaHistory({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildClusterReplicaHistoryQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildClusterReplicaHistoryQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}

export interface ReplicaObjectHydrationState {
  replicaId: string;
  objectId: string;
  isHydrated: boolean;
}

export const replicaObjectHydrationStateKey = ({
  replicaId,
  objectId,
}: ReplicaObjectHydrationState) => `${replicaId}|${objectId}`;

/**
 * Whether each user object on the given replicas is hydrated. The literal id
 * list makes this a lookup on `mz_compute_hydration_times_ind`.
 *
 * NOTE: for a few seconds after a replica restarts, this can still report the
 * previous process's hydrated objects.
 */
export function buildReplicaHydrationStatesSubscribe({
  replicaIds,
}: {
  replicaIds: string[];
}) {
  return buildSubscribeQuery<ReplicaObjectHydrationState>(
    queryBuilder
      .selectFrom("mz_compute_hydration_times")
      .select([
        "replica_id as replicaId",
        "object_id as objectId",
        sql<boolean>`time_ns IS NOT NULL`.as("isHydrated"),
      ])
      .where("replica_id", "in", replicaIds)
      .where("object_id", "like", "u%"),
    { upsertKey: ["replicaId", "objectId"] },
  );
}

/** One object's hydration on a replica, across the replica's workers. */
export interface LiveObjectHydration {
  globalId: string;
  /** The catalog id, null for a dropped object's dataflow. */
  objectId: string | null;
  installedAt: Date;
  /** When the first worker got a hydration slot, null while queued. */
  startedAt: Date | null;
  /** When the last worker finished, null until every worker has. */
  hydratedAt: Date | null;
}

/**
 * Each compute object's hydration on one replica, as it happens. Reads the
 * replica's introspection, so it must run in a session pinned to the replica
 * (`cluster` and `cluster_replica`), and runs on that replica rather than on
 * `mz_catalog_server`. Only user objects, which leaves out this subscribe's
 * own transient dataflow. Requires mz >= 26.39.
 */
export function buildLiveObjectHydrationsSubscribe() {
  return buildSubscribeQuery<LiveObjectHydration>(
    queryBuilder
      .selectFrom("mz_compute_hydration_times_per_worker as h")
      .leftJoin("mz_object_global_ids as g", "g.global_id", "h.export_id")
      .select([
        "h.export_id as globalId",
        "g.id as objectId",
        sql<Date>`min(h.installed_at)`.as("installedAt"),
        sql<Date | null>`min(h.started_at)`.as("startedAt"),
        sql<Date | null>`CASE WHEN count(h.hydrated_at) = count(*) THEN max(h.hydrated_at) END`.as(
          "hydratedAt",
        ),
      ])
      .where("h.export_id", "like", "u%")
      .groupBy(["h.export_id", "g.id"]),
    { upsertKey: "globalId" },
  );
}

/**
 * Each object hydration on `replicaId` whose dataflow was installed between
 * `startDate` and `endDate`, the window of one episode. `objectId` is the
 * catalog id, null once the object is dropped. Requires mz >= 26.40.
 *
 * NOTE: the history has no index, so this reads storage.
 */
export function buildEpisodeObjectHydrationsQuery({
  replicaId,
  startDate,
  endDate,
}: {
  replicaId: string;
  startDate: string;
  endDate: string;
}) {
  return queryBuilder
    .selectFrom("mz_object_hydration_history as h")
    .leftJoin("mz_object_global_ids as g", "g.global_id", "h.object_id")
    .select([
      "h.object_id as globalId",
      "g.id as objectId",
      "h.installed_at as installedAt",
      "h.started_at as startedAt",
      "h.hydrated_at as hydratedAt",
    ])
    .where("h.replica_id", "=", replicaId)
    .where("h.installed_at", ">=", timestampLit(startDate))
    .where("h.installed_at", "<=", timestampLit(endDate));
}

export async function fetchEpisodeObjectHydrations({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildEpisodeObjectHydrationsQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildEpisodeObjectHydrationsQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}

/**
 * The highest CPU any of `replicaId`'s processes reported between `startDate`
 * and `endDate`, null when no sample falls in the window. Samples are a minute
 * apart, so this can miss the true peak. A lookup on
 * `mz_cluster_replica_metrics_history_ind`.
 */
export function buildReplicaCpuPeakQuery({
  replicaId,
  startDate,
  endDate,
}: {
  replicaId: string;
  startDate: string;
  endDate: string;
}) {
  return queryBuilder
    .selectFrom("mz_cluster_replica_metrics_history")
    .select(
      sql<number | null>`max(cpu_nano_cores)::float8`.as("peakCpuNanoCores"),
    )
    .where("replica_id", "=", replicaId)
    .where("occurred_at", ">=", timestampLit(startDate))
    .where("occurred_at", "<=", timestampLit(endDate));
}

export async function fetchReplicaCpuPeak({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildReplicaCpuPeakQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildReplicaCpuPeakQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows[0]?.peakCpuNanoCores ?? null;
}
