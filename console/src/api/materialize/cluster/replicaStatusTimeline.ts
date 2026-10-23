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

export const OOM_REASON = "oom-killed";

/**
 * Status history of the given replicas over their whole retained history, so
 * a caller can find the state before a window from the last earlier
 * transition. Selects only the rows `splitReplicaStatusRows` reads. The literal
 * id list makes this a lookup on `mz_cluster_replica_status_history_ind`.
 */
export function buildReplicaStatusHistoryQuery({
  replicaIds,
}: {
  replicaIds: string[];
}) {
  return queryBuilder
    .selectFrom("mz_cluster_replica_status_history")
    .select([
      "replica_id as replicaId",
      sql<string>`process_id::text`.as("processId"),
      "occurred_at as occurredAt",
      "status",
      "reason",
    ])
    .where("replica_id", "in", replicaIds)
    .where((eb) =>
      eb.or([eb("process_id", "=", "0"), eb("reason", "=", OOM_REASON)]),
    );
}

/**
 * Splits status history rows into replica status transitions and OOM kills.
 * A replica's status is taken to be process 0's. The Kubernetes orchestrator
 * records an OOM kill on the process (pod) it hit, so kills come from every
 * process.
 */
export function splitReplicaStatusRows<
  Row extends { processId: string; reason: string | null },
>(rows: Row[]) {
  return {
    transitions: rows.filter((row) => row.processId === "0"),
    oomKills: rows.filter((row) => row.reason === OOM_REASON),
  };
}

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
  return splitReplicaStatusRows(res.rows);
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
 * How long each of the given replicas took to hydrate each of the given
 * objects, in milliseconds, null while it is hydrating. The literal replica
 * ids make this a lookup on `mz_compute_hydration_times_ind`.
 */
export function buildObjectHydrationTimesQuery({
  replicaIds,
  objectIds,
}: {
  replicaIds: string[];
  objectIds: string[];
}) {
  return queryBuilder
    .selectFrom("mz_compute_hydration_times")
    .select([
      "replica_id as replicaId",
      "object_id as objectId",
      sql<number | null>`time_ns::float8 / 1000000`.as("hydrationMs"),
    ])
    .where("replica_id", "in", replicaIds)
    .where("object_id", "in", objectIds);
}

/** When environmentd last started, by the wall clock. */
export function buildEnvironmentStartedAtQuery() {
  return queryBuilder.selectNoFrom(
    sql<Date>`now() - mz_uptime()`.as("startedAt"),
  );
}

export async function fetchObjectHydrationTimes({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildObjectHydrationTimesQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const [hydrations, environment] = await executeSqlV2({
    queries: [
      buildObjectHydrationTimesQuery(params).compile(),
      buildEnvironmentStartedAtQuery().compile(),
    ] as const,
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return {
    hydrations: hydrations.rows,
    environmentStartedAt: environment.rows[0].startedAt,
  };
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
