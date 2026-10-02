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
 * Status transitions of the given replicas over their whole retained history,
 * so a caller can find the state before a window from the last earlier
 * transition. The literal id list makes this a lookup on
 * `mz_cluster_replica_status_history_ind`.
 */
export function buildReplicaStatusHistoryQuery({
  replicaIds,
}: {
  replicaIds: string[];
}) {
  return (
    queryBuilder
      .selectFrom("mz_cluster_replica_status_history")
      .select([
        "replica_id as replicaId",
        "occurred_at as occurredAt",
        "status",
        "reason",
      ])
      .where("replica_id", "in", replicaIds)
      // Processes should share the same state, so take the first process's.
      .where("process_id", "=", "0")
  );
}

export type ReplicaStatusTransition = Awaited<
  ReturnType<typeof fetchReplicaStatusHistory>
>[number];

export async function fetchReplicaStatusHistory({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildReplicaStatusHistoryQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildReplicaStatusHistoryQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}

/**
 * Compute objects the given replicas have not finished hydrating. A replica
 * with any rows here is hydrating. The literal id list makes this a lookup on
 * `mz_compute_hydration_times_ind`.
 */
export function buildUnhydratedComputeObjectsQuery({
  replicaIds,
}: {
  replicaIds: string[];
}) {
  return queryBuilder
    .selectFrom("mz_compute_hydration_times")
    .select(["replica_id as replicaId", "object_id as objectId"])
    .where("replica_id", "in", replicaIds)
    .where("time_ns", "is", null);
}

export async function fetchUnhydratedComputeObjects({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildUnhydratedComputeObjectsQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildUnhydratedComputeObjectsQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}

/**
 * Hydration episodes of the given replicas that finished at or after
 * `startDate`. Requires mz >= 26.43, which added `process_id`.
 *
 * NOTE: the history has no index, so this reads storage. The history is also
 * sampled one replica at a time, so an episode shows up a while after it
 * finishes and a hydration in progress is absent.
 */
export function buildReplicaHydrationEpisodesQuery({
  replicaIds,
  startDate,
}: {
  replicaIds: string[];
  startDate: string;
}) {
  return (
    queryBuilder
      .selectFrom("mz_replica_hydration_history")
      .select([
        "replica_id as replicaId",
        "started_at as startedAt",
        "finished_at as finishedAt",
      ])
      .where("replica_id", "in", replicaIds)
      // Episode timing is replica-wide and repeated for each process.
      .where((eb) =>
        eb.or([eb("process_id", "is", null), eb("process_id", "=", "0")]),
      )
      .where("finished_at", ">=", sql<Date>`${sql.lit(startDate)}::timestamptz`)
  );
}

export type ReplicaHydrationEpisode = Awaited<
  ReturnType<typeof fetchReplicaHydrationEpisodes>
>[number];

export async function fetchReplicaHydrationEpisodes({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildReplicaHydrationEpisodesQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildReplicaHydrationEpisodesQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}
