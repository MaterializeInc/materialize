// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";

import { executeSqlV2, queryBuilder } from "~/api/materialize";

/**
 * Each process's heap limit (RAM plus swap) for the given replicas, from their
 * latest metrics. The literal id list makes this a lookup on
 * `mz_cluster_replica_metrics_ind`.
 *
 * NOTE: callers fold the per-process rows, because a GROUP BY here turns the
 * fast-path lookup into a dataflow.
 */
export function buildReplicaHeapLimitsQuery({
  replicaIds,
}: {
  replicaIds: string[];
}) {
  return queryBuilder
    .selectFrom("mz_cluster_replica_metrics")
    .select([
      "replica_id as replicaId",
      "process_id as processId",
      "heap_limit as heapLimit",
    ])
    .where("replica_id", "in", replicaIds);
}

export async function fetchReplicaHeapLimits({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildReplicaHeapLimitsQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildReplicaHeapLimitsQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}
