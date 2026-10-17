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

/**
 * One process's sample from `mz_cluster_replica_metrics_history`. A process
 * whose metrics couldn't be fetched still has a row, with null values.
 */
export interface ProcessMetricsSample {
  replicaId: string;
  processId: string;
  occurredAt: Date;
  cpuNanoCores: number | null;
  memoryBytes: number | null;
  diskBytes: number | null;
  heapBytes: number | null;
  heapLimit: number | null;
  swapBytes?: number | null;
}

/** A size's process count and each process's CPU, memory and disk allocation. */
export interface SizeAllocation {
  processes: number;
  /** Timely workers per process. */
  workers: number;
  cpuNanoCores: number;
  memoryBytes: number;
  diskBytes: number;
}

export const processSampleKey = (sample: ProcessMetricsSample) =>
  `${sample.replicaId}|${sample.processId}|${sample.occurredAt.getTime()}`;

/**
 * Each process's samples for the given replicas since `since`. The literal ids
 * make this a lookup on `mz_cluster_replica_metrics_history_ind`. Samples are
 * never retracted as they age, so a long-running caller moves `since` forward.
 */
export function buildReplicaProcessMetricsSubscribe({
  replicaIds,
  since,
  includeSwap,
}: {
  replicaIds: string[];
  since: Date;
  // `swap_bytes` is absent before mz 26.43.
  includeSwap: boolean;
}) {
  const query = queryBuilder
    .selectFrom("mz_cluster_replica_metrics_history as m")
    .select([
      "m.replica_id as replicaId",
      // `process_id` is a uint8, which would arrive as a BigInt.
      sql<string>`m.process_id::text`.as("processId"),
      "m.occurred_at as occurredAt",
      sql<number | null>`m.cpu_nano_cores::float8`.as("cpuNanoCores"),
      sql<number | null>`m.memory_bytes::float8`.as("memoryBytes"),
      sql<number | null>`m.disk_bytes::float8`.as("diskBytes"),
      sql<number | null>`m.heap_bytes::float8`.as("heapBytes"),
      sql<number | null>`m.heap_limit::float8`.as("heapLimit"),
    ])
    .$if(includeSwap, (qb) =>
      qb.select(sql<number | null>`m.swap_bytes::float8`.as("swapBytes")),
    )
    .where("m.replica_id", "in", replicaIds)
    // NOTE: a literal bound, not a `mz_now()` filter. A temporal filter makes
    // the subscribe hold each row's pending retraction on mz_catalog_server:
    // ~12 MiB per tab against ~2 MiB here, for 2 replicas of 16 processes, 24h.
    .where(
      "m.occurred_at",
      ">=",
      sql<Date>`${sql.lit(since.toISOString())}::timestamptz`,
    );
  return buildSubscribeQuery<ProcessMetricsSample>(query, {
    upsertKey: ["replicaId", "processId", "occurredAt"],
  });
}

/** Each size's process count and per-process allocations. */
export function buildReplicaSizeAllocationsQuery({
  sizes,
}: {
  sizes: string[];
}) {
  return queryBuilder
    .selectFrom("mz_cluster_replica_sizes")
    .select([
      "size",
      sql<number>`processes::float8`.as("processes"),
      sql<number>`workers::float8`.as("workers"),
      sql<number>`cpu_nano_cores::float8`.as("cpuNanoCores"),
      sql<number>`memory_bytes::float8`.as("memoryBytes"),
      sql<number>`disk_bytes::float8`.as("diskBytes"),
    ])
    .where("size", "in", sizes);
}

export async function fetchReplicaSizeAllocations({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildReplicaSizeAllocationsQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildReplicaSizeAllocationsQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}
