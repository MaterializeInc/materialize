// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";

import { fetchClusterDeploymentLineage } from "./clusterDeploymentLineage";

export type ReplicaUtilizationHistoryParameters = {
  // Filter per cluster
  clusterIds?: string[];
  // Filter per replica
  replicaId?: string;
  // Start date of the history. The history will start from the earliest bucket.
  startDate: string;
  // End date of the history. The history will end at the latest bucket.
  endDate?: string;
  // Size of the time buckets in milliseconds
  bucketSizeMs: number;

  // Whether to use the console cluster utilization overview view
  shouldUseConsoleClusterUtilizationOverviewView?: boolean;
};
// We have an equivalent query in `builtin.rs` in the MaterializeInc/materialize.
// This should query should be kept in sync with `mz_console_cluster_utilization_overview`.
export function buildReplicaUtilizationHistoryQuery({
  clusterIds,
  replicaId,
  startDate,
  endDate,
  bucketSizeMs,
}: ReplicaUtilizationHistoryParameters) {
  const bucketSizeMsSqlStr = sql.raw(`${bucketSizeMs}`);
  const startDateLit = sql.lit(startDate);
  const endDateLit = sql.lit(startDate);

  const dateBinOrigin = sql.lit("1970-01-01");

  let query = queryBuilder
    .with("replica_history", (qb) =>
      qb
        .selectFrom("mz_cluster_replica_history")
        .select(["replica_id", "cluster_id", "size"])
        // We need to union the current set of cluster replicas since mz_cluster_replica_history doesn't account for system clusters
        .union(
          qb
            .selectFrom("mz_cluster_replicas")
            .select(["id as replica_id", "cluster_id", "size"]),
        ),
    )
    .with("replica_name_history", (qb) =>
      qb
        .selectFrom("mz_cluster_replica_name_history")
        .select((eb) => [
          "id",
          "new_name as name",
          sql<Date>`COALESCE(${eb.ref("occurred_at")}, TIMESTAMP ${dateBinOrigin})`.as(
            "occurred_at",
          ),
        ]),
    )
    // NOTE(SangJunBak): We do not have ideal handling for
    // multiprocess clusters (i.e., 6400cc+ clusters at the time of writing) in
    // the Console. Specifically, we merge metrics across processes as if they were on a single machine.
    // Ideally it would return data for each process in
    // the replica separately, but the downstream consumers (e.g., the replica
    // graphs) are not yet equipped to handle that.
    // A better fix should be handled here (https://github.com/MaterializeInc/console/issues/1041)
    .with("replica_metrics_history", (qb) =>
      qb
        .selectFrom("replica_history as r")
        .innerJoin("mz_cluster_replica_sizes as s", "r.size", "s.size")
        .innerJoin(
          "mz_cluster_replica_metrics_history as m",
          "m.replica_id",
          "r.replica_id",
        )
        .select([
          "m.occurred_at",
          "m.replica_id",
          "r.size",
          sql<
            number | null
          >`(SUM(m.cpu_nano_cores::float8) / (NULLIF(s.cpu_nano_cores, 0) * s.processes))`.as(
            "cpu_percent",
          ),
          sql<
            number | null
          >`(SUM(m.memory_bytes::float8) / (NULLIF(s.memory_bytes, 0) * s.processes))`.as(
            "memory_percent",
          ),
          sql<
            number | null
          >`(SUM(m.disk_bytes::float8) / (NULLIF(s.disk_bytes, 0) * s.processes))`.as(
            "disk_percent",
          ),
          sql<number | null>`SUM(m.disk_bytes::float8)`.as("disk_bytes"),
          sql<number | null>`SUM(m.memory_bytes::float8)`.as("memory_bytes"),
          sql<number | null>`s.disk_bytes * s.processes`.as("total_disk_bytes"),
          sql<number>`s.memory_bytes * s.processes`.as("total_memory_bytes"),
          sql<number | null>`MAX(m.heap_bytes::float8)`.as("heap_bytes"),
          sql<number | null>`MAX(m.heap_limit)`.as("heap_limit"),
          sql<
            number | null
          >`MAX(m.heap_bytes::float8 / NULLIF(m.heap_limit, 0))`.as(
            "heap_percent",
          ),
        ])
        .groupBy([
          "m.occurred_at",
          "m.replica_id",
          "r.size",
          "s.cpu_nano_cores",
          "s.memory_bytes",
          "s.disk_bytes",
          "s.processes",
        ]),
    )
    .with("replica_utilization_history_binned", (qb) => {
      let cte = qb
        .selectFrom("replica_history as r")
        .innerJoin(
          "replica_metrics_history as m",
          "m.replica_id",
          "r.replica_id",
        )
        .select([
          "m.occurred_at",
          "m.replica_id",
          "m.cpu_percent",
          "m.memory_percent",
          "m.memory_bytes",
          "m.disk_percent",
          "m.disk_bytes",
          "m.total_disk_bytes",
          "m.total_memory_bytes",
          "m.heap_bytes",
          "m.heap_percent",
          "m.size",
          sql<Date>`date_bin(
              '${bucketSizeMsSqlStr} MILLISECONDS',
              occurred_at,
              TIMESTAMP ${dateBinOrigin}
            )`.as("bucket_start"),
        ])
        .where(
          "occurred_at",
          ">=",
          sql<Date>`
            date_bin(
              '${bucketSizeMsSqlStr} MILLISECONDS',
              TIMESTAMP ${startDateLit},
              TIMESTAMP ${dateBinOrigin}
            )`,
        );
      if (endDate) {
        cte = cte.where(
          (eb) =>
            sql<Date>`${eb.ref("occurred_at")} + INTERVAL '${bucketSizeMsSqlStr} MILLISECONDS'`,
          "<=",
          sql<Date>`
            date_bin(
              '${bucketSizeMsSqlStr} MILLISECONDS',
              TIMESTAMP ${endDateLit},
              TIMESTAMP ${dateBinOrigin}
            )`,
        );
      }
      return cte;
    })
    // For each (replica, bucket), take the (replica, bucket) with the highest memory
    .with("max_memory", (qb) =>
      /**
       * This is a TOP k=1 optimization using DISTINCT ON https://materialize.com/docs/transform-data/patterns/top-k/.
       */
      qb
        .selectFrom("replica_utilization_history_binned")
        .distinctOn(["bucket_start", "replica_id"])
        .select(["bucket_start", "replica_id", "memory_percent", "occurred_at"])
        .orderBy("bucket_start")
        .orderBy("replica_id")
        .orderBy((oeb) => sql`COALESCE(${oeb.ref("memory_bytes")}, 0)`, "desc"),
    )
    // For each (replica, bucket), take the (replica, bucket) with the highest disk
    .with("max_disk", (qb) =>
      qb
        .selectFrom("replica_utilization_history_binned")
        .distinctOn(["bucket_start", "replica_id"])
        .select(["bucket_start", "replica_id", "disk_percent", "occurred_at"])
        .orderBy("bucket_start")
        .orderBy("replica_id")
        .orderBy((oeb) => sql`COALESCE(${oeb.ref("disk_bytes")}, 0)`, "desc"),
    )
    // For each (replica, bucket), take the (replica, bucket) with the highest cpu
    .with("max_cpu", (qb) =>
      qb
        .selectFrom("replica_utilization_history_binned")
        .distinctOn(["bucket_start", "replica_id"])
        .select(["bucket_start", "replica_id", "cpu_percent", "occurred_at"])
        .orderBy("bucket_start")
        .orderBy("replica_id")
        .orderBy((oeb) => sql`COALESCE(${oeb.ref("cpu_percent")}, 0)`, "desc"),
    )
    // For each (replica, bucket), take the (replica, bucket) with the highest heap
    .with("max_heap", (qb) =>
      qb
        .selectFrom("replica_utilization_history_binned")
        .distinctOn(["bucket_start", "replica_id"])
        .select(["bucket_start", "replica_id", "heap_percent", "occurred_at"])
        .orderBy("bucket_start")
        .orderBy("replica_id")
        .orderBy((oeb) => sql`COALESCE(${oeb.ref("heap_bytes")}, 0)`, "desc"),
    )
    // For each (replica, bucket), take the (replica, bucket)
    // with the highest combined memory and disk. This is different from adding the
    // max memory and max disk per bucket because both values may not occur at the same time if
    // the bucket interval is large.
    .with("max_memory_and_disk", (qb) =>
      qb
        .selectFrom(
          qb
            .selectFrom("replica_utilization_history_binned")
            .selectAll()
            .select(({ ref }) =>
              sql<number>`
              CASE WHEN ${ref("disk_bytes")} IS NULL AND ${ref("memory_bytes")} IS NULL THEN NULL
              ELSE
                (
                  /*
                    We use the percentage of the current disk and memory relative to the total memory of a cluster.
                    If over 100%, it means the replica is spilling to disk. Too much spilling to disk is unwanted for
                    compute clusters since it means performance degradation.
                  */
                  (COALESCE(${ref("memory_bytes")}, 0) + COALESCE(${ref("disk_bytes")}, 0)) /
                  NULLIF((${ref("total_memory_bytes")} + ${ref("total_disk_bytes")}), 0)
                )
              END
          `.as("memory_and_disk_percent"),
            )
            .as("max_memory_and_disk_inner"),
        )
        .distinctOn(["bucket_start", "replica_id"])
        .select([
          "bucket_start",
          "replica_id",
          "memory_percent",
          "disk_percent",
          "memory_and_disk_percent",
          "occurred_at",
        ])
        .orderBy("bucket_start")
        .orderBy("replica_id")
        .orderBy(
          (oeb) => sql`COALESCE(${oeb.ref("memory_and_disk_percent")}, 0)`,
          "desc",
        ),
    )
    // For each (replica, bucket), get its offline events at that time
    .with("replica_offline_event_history", (qb) => {
      let cte = qb
        .selectFrom("mz_cluster_replica_status_history as rsh")
        .select([
          sql<Date>`date_bin(
              '${bucketSizeMsSqlStr} MILLISECONDS',
              occurred_at,
              TIMESTAMP ${dateBinOrigin}
            )`.as("bucket_start"),
          "replica_id",
          sql<
            {
              replicaId: string;
              occurredAt: string;
              status: string;
              reason: string;
            }[]
          >`jsonb_agg(
              jsonb_build_object(
                'replicaId', rsh.replica_id,
                'occurredAt', rsh.occurred_at,
                'status', rsh.status,
                'reason', rsh.reason
              )
          )`.as("offline_events"),
        ])
        // NOTE(SangJunBak): Given processes should share the same state, we can just take the statuses of the first process.
        .where("process_id", "=", "0")
        .where("status", "=", "offline")
        .where(
          "occurred_at",
          ">=",
          sql<Date>`
            date_bin(
              '${bucketSizeMsSqlStr} MILLISECONDS',
              TIMESTAMP ${startDateLit},
              TIMESTAMP ${dateBinOrigin}
            )`,
        )
        .groupBy(["bucket_start", "replica_id"]);

      if (endDate) {
        cte = cte.where(
          (eb) =>
            sql<Date>`${eb.ref("occurred_at")} + INTERVAL '${bucketSizeMsSqlStr} MILLISECONDS'`,
          "<=",
          sql<Date>`
            date_bin(
              '${bucketSizeMsSqlStr} MILLISECONDS',
              TIMESTAMP ${endDateLit},
              TIMESTAMP ${dateBinOrigin}
            )`,
        );
      }

      return cte;
    })
    .selectFrom("max_memory")
    .innerJoin("max_disk", (join) =>
      join
        .onRef("max_memory.bucket_start", "=", "max_disk.bucket_start")
        .onRef("max_memory.replica_id", "=", "max_disk.replica_id"),
    )
    .innerJoin("max_cpu", (join) =>
      join
        .onRef("max_memory.bucket_start", "=", "max_cpu.bucket_start")
        .onRef("max_memory.replica_id", "=", "max_cpu.replica_id"),
    )
    .innerJoin("max_heap", (join) =>
      join
        .onRef("max_memory.bucket_start", "=", "max_heap.bucket_start")
        .onRef("max_memory.replica_id", "=", "max_heap.replica_id"),
    )
    .innerJoin("max_memory_and_disk", (join) =>
      join
        .onRef(
          "max_memory.bucket_start",
          "=",
          "max_memory_and_disk.bucket_start",
        )
        .onRef("max_memory.replica_id", "=", "max_memory_and_disk.replica_id"),
    )
    .innerJoin("replica_history", (join) =>
      join.onRef("max_memory.replica_id", "=", "replica_history.replica_id"),
    )
    /**
     * This is a TOP k optimization using a LATERAL subquery and limit https://materialize.com/docs/transform-data/patterns/top-k/.
     * Because Kysely doesn't support lateral cross joins, we use innerJoinLateral and join.onTrue() to get the same behavior.
     */
    .innerJoinLateral(
      (qb) =>
        qb
          .selectFrom("replica_name_history")
          .selectAll()
          .whereRef("max_memory.replica_id", "=", "replica_name_history.id")
          // Before the end of each bucket, get the closest name to the end
          .whereRef(
            sql<Date>`max_memory.bucket_start + INTERVAL '${bucketSizeMsSqlStr} MILLISECONDS'`,
            ">=",
            "replica_name_history.occurred_at",
          )
          .orderBy("replica_name_history.occurred_at", "desc")
          .limit(1)
          .as("replica_name_history"),
      (join) => join.onTrue(),
    )
    .leftJoin("replica_offline_event_history", (join) =>
      join
        .onRef(
          "max_memory.bucket_start",
          "=",
          "replica_offline_event_history.bucket_start",
        )
        .onRef(
          "max_memory.replica_id",
          "=",
          "replica_offline_event_history.replica_id",
        ),
    )
    .select([
      "max_memory.bucket_start as bucketStart",
      "max_memory.replica_id as replicaId",
      "max_memory.memory_percent as maxMemoryPercent",
      "max_memory.occurred_at as maxMemoryAt",
      "max_disk.disk_percent as maxDiskPercent",
      "max_disk.occurred_at as maxDiskAt",
      "max_memory_and_disk.memory_and_disk_percent as maxMemoryAndDiskPercent",
      "max_memory_and_disk.memory_percent as maxMemoryAndDiskMemoryPercent",
      "max_memory_and_disk.disk_percent as maxMemoryAndDiskDiskPercent",
      "max_memory_and_disk.occurred_at as maxMemoryAndDiskAt",
      "max_cpu.cpu_percent as maxCpuPercent",
      "max_cpu.occurred_at as maxCpuAt",
      "max_heap.heap_percent as maxHeapPercent",
      "max_heap.occurred_at as maxHeapAt",
      "replica_offline_event_history.offline_events as offlineEvents",
      sql<Date>`max_memory.bucket_start + INTERVAL '${bucketSizeMsSqlStr} MILLISECONDS'`.as(
        "bucketEnd",
      ),
      "replica_name_history.name",
      "replica_history.cluster_id as clusterId",
      "replica_history.size",
    ])
    .orderBy("bucketStart");

  if (clusterIds !== undefined && clusterIds.length > 0) {
    query = query.where("replica_history.cluster_id", "in", clusterIds);
  }

  if (replicaId) {
    query = query.where("max_memory.replica_id", "=", replicaId);
  }

  return query;
}

/**
 * This is an optimized version of buildReplicaUtilizationHistoryQuery
 * that uses a time period of 14 days and a bucket of 8 hours via a built-in index+view.
 */
export function buildConsoleClusterUtilizationOverviewQuery({
  clusterIds,
  replicaId,
}: {
  clusterIds?: string[];
  replicaId?: string;
}) {
  let query = queryBuilder
    .selectFrom("mz_console_cluster_utilization_overview")
    .select([
      "bucket_start as bucketStart",
      "replica_id as replicaId",
      "memory_percent as maxMemoryPercent",
      "max_memory_at as maxMemoryAt",
      "disk_percent as maxDiskPercent",
      "max_disk_at as maxDiskAt",
      "max_cpu_percent as maxCpuPercent",
      "max_cpu_at as maxCpuAt",
      "heap_percent as maxHeapPercent",
      "max_heap_at as maxHeapAt",
      "memory_and_disk_percent as maxMemoryAndDiskPercent",
      "max_memory_and_disk_memory_percent as maxMemoryAndDiskMemoryPercent",
      "max_memory_and_disk_disk_percent as maxMemoryAndDiskDiskPercent",
      "max_memory_and_disk_at as maxMemoryAndDiskAt",
      "offline_events as offlineEvents",
      "bucket_end as bucketEnd",
      "name",
      "cluster_id as clusterId",
      "size",
    ])
    .orderBy("bucketStart");

  if (clusterIds !== undefined && clusterIds.length > 0) {
    query = query.where("cluster_id", "in", clusterIds);
  }

  if (replicaId) {
    query = query.where("replica_id", "=", replicaId);
  }

  return query;
}

export type OfflineEvent = {
  replicaId: string;
  occurredAt: string;
  status: string;
  reason: string;
};

export type Bucket = {
  size: string | null;
  bucketStart: Date;
  replicaId: string;
  bucketEnd: Date;
  name: string;
  // The cluster ID of the replica's current DBT deployment
  currentDeploymentClusterId: string;
  // The cluster ID of the replica. If the cluster was dropped,
  // this will be different from currentDeploymentClusterId
  clusterId: string;
  maxMemory: {
    percent: number | null;
    occurredAt: Date;
  };
  maxDisk: {
    percent: number | null;
    occurredAt: Date;
  };
  maxCpu: {
    percent: number | null;
    occurredAt: Date;
  };
  maxHeap: {
    percent: number | null;
    occurredAt: Date;
  };
  maxMemoryAndDisk: {
    memoryPercent: number | null;
    diskPercent: number | null;
    percent: number | null;
    occurredAt: Date;
  };

  offlineEvents: OfflineEvent[] | null;
};

export async function fetchReplicaUtilizationHistory({
  params,
  queryKey,
  requestOptions,
}: {
  params: ReplicaUtilizationHistoryParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const {
    pastDeploymentsByCurrentDeployment,
    currentDeploymentByPastDeployment,
  } = await fetchClusterDeploymentLineage({
    params: {
      clusterIds: params.clusterIds,
    },
    queryKey: [...queryKey, "deploymentLineage"],
    requestOptions,
  });

  const clusterIdsFilter = params.clusterIds?.reduce((accum, clusterId) => {
    const lineage = pastDeploymentsByCurrentDeployment.get(clusterId);
    if (lineage) {
      accum.push(...lineage.map((d) => d.clusterId));
    } else {
      /**
       * All user clusters have a lineage since at the very least, its lineage contains itself. However,
       * system clusters don't have a lineage since mz_cluster_deployment_lineage doesn't account for system clusters.
       * Thus we add just the original cluster ID for this case.
       */
      accum.push(clusterId);
    }
    return accum;
  }, [] as string[]);

  let utilizationQuery = buildReplicaUtilizationHistoryQuery({
    ...params,
    clusterIds: clusterIdsFilter,
  }).compile();

  if (params.shouldUseConsoleClusterUtilizationOverviewView) {
    utilizationQuery = buildConsoleClusterUtilizationOverviewQuery({
      clusterIds: clusterIdsFilter,
      replicaId: params.replicaId,
    }).compile();
  }

  const utilizationRes = await executeSqlV2({
    queries: utilizationQuery,
    queryKey: queryKey,
    requestOptions,
    sessionVariables: {
      // We use serializable because we don't care about strict seriailizability and to get consistent performance
      transaction_isolation: "serializable",
    },
  });

  const bucketsByReplicaId: Record<string, Bucket[]> = {};

  let minBucketStartMs = Number.POSITIVE_INFINITY;
  let maxBucketEndMs = Number.NEGATIVE_INFINITY;

  for (const row of utilizationRes.rows) {
    minBucketStartMs = Math.min(minBucketStartMs, row.bucketStart.getTime());
    maxBucketEndMs = Math.max(maxBucketEndMs, row.bucketEnd.getTime());

    const {
      replicaId,
      size,
      bucketStart,
      bucketEnd,
      name,
      clusterId,
      offlineEvents,
    } = row;

    const buckets = bucketsByReplicaId[replicaId];

    if (name === null || clusterId === null) {
      const err = new Error(
        `Expected name: ${name} and clusterId: ${clusterId} to be defined`,
      );

      Sentry.captureException(err);
      throw err;
    }

    const currentDeploymentClusterId =
      currentDeploymentByPastDeployment.get(clusterId)
        ?.currentDeploymentClusterId ?? clusterId;

    const newBucket = {
      size,
      bucketStart,
      bucketEnd,
      offlineEvents,
      name,
      currentDeploymentClusterId,
      clusterId,
      replicaId,
      maxMemory: {
        percent: row.maxMemoryPercent,
        occurredAt: row.maxMemoryAt,
      },
      maxDisk: {
        percent: row.maxDiskPercent,
        occurredAt: row.maxDiskAt,
      },
      maxCpu: {
        percent: row.maxCpuPercent,
        occurredAt: row.maxCpuAt,
      },
      maxHeap: {
        percent: row.maxHeapPercent ?? null,
        occurredAt: row.maxHeapAt ?? new Date(),
      },
      maxMemoryAndDisk: {
        percent: row.maxMemoryAndDiskPercent,
        memoryPercent: row.maxMemoryAndDiskMemoryPercent,
        diskPercent: row.maxMemoryAndDiskDiskPercent,
        occurredAt: row.maxMemoryAndDiskAt,
      },
    };

    if (buckets) {
      buckets.push(newBucket);
    } else {
      bucketsByReplicaId[replicaId] = [newBucket];
    }
  }
  return {
    minBucketStartMs,
    maxBucketEndMs,
    bucketsByReplicaId,
  };
}
