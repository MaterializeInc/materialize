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

import { fetchClusterDeploymentLineage } from "./clusterDeploymentLineage";
import {
  attachOfflineEvents,
  bucketRowsToBucketsByReplicaId,
  rebucketUtilizationSamples,
  UtilizationBucketRow,
} from "./replicaUtilizationBinning";

// Re-exported for existing importers of these data types.
export type { Bucket, OfflineEvent } from "./replicaUtilizationBinning";

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

  // Whether to use the console cluster utilization overview view (14d/1h).
  shouldUseConsoleClusterUtilizationOverviewView?: boolean;
  // Finer indexed-view tiers for shorter windows. "unbinned3h" reads the un-binned
  // 3h base and bins client-side; "overview24h" reads the 24h/5min binned view.
  // Unset (and `shouldUse...` false) falls back to the ad-hoc whole-fleet query.
  utilizationView?: "unbinned3h" | "overview24h";
};
// We have an equivalent query in `builtin.rs` in MaterializeInc/materialize.
// This query should be kept in sync with `mz_console_cluster_utilization_overview`.
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
    .with("replica_history", (qb) => {
      let history = qb
        .selectFrom("mz_cluster_replica_history")
        .select(["replica_id", "cluster_id", "size"]);
      // We need to union the current set of cluster replicas since mz_cluster_replica_history doesn't account for system clusters
      let current = qb
        .selectFrom("mz_cluster_replicas")
        .select(["id as replica_id", "cluster_id", "size"]);
      // Push the cluster filter into both UNION branches up front. Otherwise it
      // only enters at the final join and the optimizer can't push it into the
      // shared `replica_utilization_history_binned` CTE, so the metrics
      // aggregate and all five Top-1 passes run over the whole fleet and only
      // the last step discards other clusters. Filtering here scopes the heavy
      // work to the requested cluster(s).
      if (clusterIds !== undefined && clusterIds.length > 0) {
        history = history.where("cluster_id", "in", clusterIds);
        current = current.where("cluster_id", "in", clusterIds);
      }
      return history.union(current);
    })
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
          // heap_limit is NULL when clusterd isn't launched with --heap-limit
          // (e.g. the emulator's process orchestrator). Fall back to the
          // size-based memory percent so the chart still renders.
          sql<number | null>`COALESCE(
            MAX(m.heap_bytes::float8 / NULLIF(m.heap_limit, 0)),
            SUM(m.memory_bytes::float8) / (NULLIF(s.memory_bytes, 0) * s.processes)
          )`.as("heap_percent"),
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
      // Read the per-sample rollup directly. replica_metrics_history is already
      // derived from replica_history, so joining replica_history here would be
      // redundant and could fan out if a replica ever had two sizes.
      let cte = qb
        .selectFrom("replica_metrics_history as m")
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
        // Restrict the offline scan to the replicas we're charting. When a
        // cluster filter is set, replica_history is already scoped to it, so
        // this turns a full status-history scan into a lookup; when it isn't,
        // replica_history is the whole fleet and this is a no-op.
        .where("rsh.replica_id", "in", (eb) =>
          eb.selectFrom("replica_history").select("replica_id"),
        )
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

  if (replicaId) {
    query = query.where("max_memory.replica_id", "=", replicaId);
  }

  return query;
}

/**
 * Optimized version of `buildReplicaUtilizationHistoryQuery` that reads a
 * maintained overview index+view (`_overview` 14d or `_overview_24h` 24h).
 */
export function buildConsoleClusterUtilizationOverviewQuery({
  clusterIds,
  replicaId,
  startDate,
  view = "mz_console_cluster_utilization_overview",
  resolveLineage = false,
}: {
  clusterIds?: string[];
  replicaId?: string;
  // Clip to the requested window (the view retains more than shorter windows).
  startDate?: string;
  // The `_overview` (14d/1h) and `_overview_24h` (24h/5min) views share the same
  // output columns, so one builder serves both.
  view?:
    | "mz_console_cluster_utilization_overview"
    | "mz_console_cluster_utilization_overview_24h";
  // When true, expand clusterIds to past blue-green deployments in SQL. The
  // poll path pre-resolves lineage in JS instead (it also needs the reverse
  // map to relabel rows), so only the SUBSCRIBE paths set this.
  resolveLineage?: boolean;
}) {
  let query = queryBuilder
    .selectFrom(view)
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
    if (resolveLineage) {
      // Include the cluster's past blue-green deployments (continuous history
      // across a swap), plus the cluster itself (system clusters have no lineage).
      query = query.where((eb) =>
        eb.or([
          eb(
            "cluster_id",
            "in",
            eb
              .selectFrom("mz_cluster_deployment_lineage")
              .select("cluster_id")
              .where("current_deployment_cluster_id", "in", clusterIds),
          ),
          eb("cluster_id", "in", clusterIds),
        ]),
      );
    } else {
      query = query.where("cluster_id", "in", clusterIds);
    }
  }

  if (replicaId) {
    query = query.where("replica_id", "=", replicaId);
  }

  if (startDate) {
    query = query.where(
      "bucket_start",
      ">=",
      sql<Date>`${sql.lit(startDate)}::timestamptz`,
    );
  }

  return query;
}

/**
 * Point-lookup the un-binned 3h view (`_overview_3h`) by cluster. The Console
 * bins the returned samples client-side (`rebucketUtilizationSamples`).
 */
export function buildConsoleClusterUtilizationUnbinned3hQuery({
  clusterIds,
  replicaId,
  resolveLineage = false,
}: {
  clusterIds?: string[];
  replicaId?: string;
  // When true, expand clusterIds to past blue-green deployments in SQL. The
  // poll path pre-resolves lineage in JS instead (it also needs the reverse
  // map to relabel rows), so only the SUBSCRIBE paths set this.
  resolveLineage?: boolean;
}) {
  let query = queryBuilder
    .selectFrom("mz_console_cluster_utilization_overview_3h")
    .select([
      "replica_id as replicaId",
      "cluster_id as clusterId",
      "size",
      "name",
      "occurred_at as occurredAt",
      "cpu_percent as cpuPercent",
      "memory_percent as memoryPercent",
      "disk_percent as diskPercent",
      "heap_percent as heapPercent",
      "memory_and_disk_percent as memoryAndDiskPercent",
    ]);

  if (clusterIds !== undefined && clusterIds.length > 0) {
    if (resolveLineage) {
      // Include the cluster's past blue-green deployments (so history is
      // continuous across a swap), plus the cluster itself (system clusters
      // have no lineage row).
      query = query.where((eb) =>
        eb.or([
          eb(
            "cluster_id",
            "in",
            eb
              .selectFrom("mz_cluster_deployment_lineage")
              .select("cluster_id")
              .where("current_deployment_cluster_id", "in", clusterIds),
          ),
          eb("cluster_id", "in", clusterIds),
        ]),
      );
    } else {
      query = query.where("cluster_id", "in", clusterIds);
    }
  }
  if (replicaId) {
    query = query.where("replica_id", "=", replicaId);
  }

  return query;
}

/**
 * Live SUBSCRIBE to the un-binned 3h view by cluster. The streamed samples are
 * binned client-side (`rebucketUtilizationSamples`).
 */
export function buildConsoleClusterUtilizationUnbinned3hSubscribe<T>(
  clusterIds: string[],
  minDate: Date,
) {
  return buildSubscribeQuery<T>(
    buildConsoleClusterUtilizationUnbinned3hQuery({
      clusterIds,
      resolveLineage: true,
    }),
    { asOfAtLeast: minDate, upsertKey: ["replicaId", "occurredAt"] },
  );
}

/**
 * Live SUBSCRIBE to the server-binned 24h view by cluster. Its rows are already
 * binned, so unlike the 3h path they need no client-side rebinning.
 */
export function buildConsoleClusterUtilizationOverview24hSubscribe<T>(
  clusterIds: string[],
  minDate: Date,
) {
  return buildSubscribeQuery<T>(
    buildConsoleClusterUtilizationOverviewQuery({
      view: "mz_console_cluster_utilization_overview_24h",
      clusterIds,
      resolveLineage: true,
      startDate: minDate.toISOString(),
    }),
    { asOfAtLeast: minDate, upsertKey: ["replicaId", "bucketStart"] },
  );
}

/**
 * Offline events for a cluster's replicas since `startDate`, one row per
 * event. The un-binned 3h view carries no status columns, so the <=3h tier
 * fetches events with this query and merges them client-side
 * (`attachOfflineEvents`).
 */
export function buildReplicaOfflineEventsQuery({
  clusterIds,
  startDate,
  resolveLineage = false,
}: {
  clusterIds?: string[];
  startDate: string;
  // When true, expand clusterIds to past blue-green deployments in SQL, like
  // the utilization builders. The poll path passes pre-expanded ids instead.
  resolveLineage?: boolean;
}) {
  let query = queryBuilder
    .with("charted_replicas", (qb) => {
      const history = qb
        .selectFrom("mz_cluster_replica_history")
        .select(["replica_id", "cluster_id"]);
      // Union the current replicas since mz_cluster_replica_history doesn't
      // include system clusters.
      const current = qb
        .selectFrom("mz_cluster_replicas")
        .select(["id as replica_id", "cluster_id"]);
      return history.union(current);
    })
    .selectFrom("mz_cluster_replica_status_history as rsh")
    .innerJoin("charted_replicas as r", "r.replica_id", "rsh.replica_id")
    .select([
      "rsh.replica_id as replicaId",
      // Via timestamptz so the text carries an explicit offset. A naive
      // timestamp string would parse as local time in the browser.
      sql<string>`rsh.occurred_at::timestamptz::text`.as("occurredAt"),
      "rsh.status",
      "rsh.reason",
    ])
    // Processes should share the same state, so take the first process's.
    .where("rsh.process_id", "=", "0")
    .where("rsh.status", "=", "offline")
    .where(
      "rsh.occurred_at",
      ">=",
      sql<Date>`${sql.lit(startDate)}::timestamptz`,
    );

  if (clusterIds !== undefined && clusterIds.length > 0) {
    if (resolveLineage) {
      query = query.where((eb) =>
        eb.or([
          eb(
            "r.cluster_id",
            "in",
            eb
              .selectFrom("mz_cluster_deployment_lineage")
              .select("cluster_id")
              .where("current_deployment_cluster_id", "in", clusterIds),
          ),
          eb("r.cluster_id", "in", clusterIds),
        ]),
      );
    } else {
      query = query.where("r.cluster_id", "in", clusterIds);
    }
  }

  return query;
}

export async function fetchReplicaOfflineEvents({
  params,
  queryKey,
  requestOptions,
}: {
  params: Parameters<typeof buildReplicaOfflineEventsQuery>[0];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const res = await executeSqlV2({
    queries: buildReplicaOfflineEventsQuery(params).compile(),
    queryKey,
    requestOptions,
    sessionVariables: { transaction_isolation: "serializable" },
  });
  return res.rows;
}

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

  // Route to the cheapest indexed source for the requested window, falling back
  // to the ad-hoc whole-fleet recompute only when no view covers it (>14d).
  let rows: UtilizationBucketRow[];

  if (params.utilizationView === "unbinned3h") {
    // Un-binned 3h base: cheap indexed point-lookup, then bin client-side.
    // Offline events aren't in the un-binned view, so fetch them alongside.
    const [unbinnedRes, offlineEvents] = await Promise.all([
      executeSqlV2({
        queries: buildConsoleClusterUtilizationUnbinned3hQuery({
          clusterIds: clusterIdsFilter,
          replicaId: params.replicaId,
        }).compile(),
        queryKey,
        requestOptions,
        sessionVariables: { transaction_isolation: "serializable" },
      }),
      fetchReplicaOfflineEvents({
        params: {
          clusterIds: clusterIdsFilter,
          startDate: params.startDate,
        },
        queryKey: [...queryKey, "offlineEvents"],
        requestOptions,
      }),
    ]);
    rows = attachOfflineEvents(
      rebucketUtilizationSamples(
        unbinnedRes.rows,
        params.bucketSizeMs,
        new Date(params.startDate).getTime(),
      ),
      offlineEvents,
      params.bucketSizeMs,
    );
  } else {
    let utilizationQuery;
    if (params.utilizationView === "overview24h") {
      utilizationQuery = buildConsoleClusterUtilizationOverviewQuery({
        view: "mz_console_cluster_utilization_overview_24h",
        clusterIds: clusterIdsFilter,
        replicaId: params.replicaId,
        startDate: params.startDate,
      }).compile();
    } else if (params.shouldUseConsoleClusterUtilizationOverviewView) {
      utilizationQuery = buildConsoleClusterUtilizationOverviewQuery({
        clusterIds: clusterIdsFilter,
        replicaId: params.replicaId,
        startDate: params.startDate,
      }).compile();
    } else {
      utilizationQuery = buildReplicaUtilizationHistoryQuery({
        ...params,
        clusterIds: clusterIdsFilter,
      }).compile();
    }

    const utilizationRes = await executeSqlV2({
      queries: utilizationQuery,
      queryKey: queryKey,
      requestOptions,
      sessionVariables: {
        // We use serializable because we don't care about strict serializability and to get consistent performance
        transaction_isolation: "serializable",
      },
    });
    rows = utilizationRes.rows as UtilizationBucketRow[];
  }

  return bucketRowsToBucketsByReplicaId(
    rows,
    (clusterId) =>
      currentDeploymentByPastDeployment.get(clusterId)
        ?.currentDeploymentClusterId,
  );
}
