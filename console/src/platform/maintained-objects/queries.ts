// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";
import { sql } from "kysely";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { formatFullyQualifiedObjectName, queryBuilder } from "~/api/materialize";
import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";
import {
  calculateBucketSizeFromLookback,
  fetchLagHistory,
} from "~/api/materialize/freshness/lagHistory";
import { useSubscribe } from "~/api/materialize/useSubscribe";
import {
  ClusterUtilizationRow,
  fetchClusterUtilization,
  fetchMaintainedObjectsList,
} from "~/api/materialize/maintained-objects/maintainedObjectsList";
import {
  CriticalPathEdge,
  fetchCriticalPath,
  fetchDownstreamDependents,
  fetchUpstreamDependencies,
} from "~/api/materialize/maintained-objects/objectDependencies";
import {
  fetchObjectColumns,
  fetchObjectDetail,
  fetchObjectMemory,
} from "~/api/materialize/maintained-objects/objectDetail";
import {
  fetchObjectMemoryUnified,
} from "~/api/materialize/maintained-objects/objectMemoryUnified";
import { executeSqlV2 } from "~/api/materialize";
import { queryBuilder as rawQueryBuilder } from "~/api/materialize/db";
import { DataPoint, GraphLineSeries } from "~/components/FreshnessGraph/types";
import { notNullOrUndefined, sumPostgresIntervalMs } from "~/util";

const maintainedObjectsQueryKeys = {
  all: () => buildRegionQueryKey("maintainedObjects"),
  list: ({ lookbackMinutes }: { lookbackMinutes: number }) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("list", { lookbackMinutes }),
    ] as const,
  clusterUtilization: () =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("clusterUtilization"),
    ] as const,
  objectDetail: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("objectDetail", { objectId }),
    ] as const,
  objectMemory: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("objectMemory", { objectId }),
    ] as const,
  objectMemoryUnified: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("objectMemoryUnified", { objectId }),
    ] as const,
  allObjectSizes: () =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("allObjectSizes"),
    ] as const,
  objectColumns: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("objectColumns", { objectId }),
    ] as const,
  objectLag: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("objectLag", { objectId }),
    ] as const,
  upstreamDependencies: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("upstreamDependencies", { objectId }),
    ] as const,
  downstreamDependents: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("downstreamDependents", { objectId }),
    ] as const,
  criticalPath: (objectId: string) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("criticalPath", { objectId }),
    ] as const,
  freshnessHistory: (objectId: string, lookbackMs: number) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("freshnessHistory", { objectId, lookbackMs }),
    ] as const,
};

/**
 * Fetches all maintained objects with their latest lag values.
 * Polls every 60s to match lag data update cadence.
 */
export function useMaintainedObjectsList({
  lookbackMinutes,
}: {
  lookbackMinutes: number;
}) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.list({ lookbackMinutes }),
    queryFn: ({ queryKey, signal }) =>
      fetchMaintainedObjectsList({
        params: { lookbackMinutes },
        queryKey,
        requestOptions: { signal },
      }),
    refetchInterval: 60_000,
    staleTime: 30_000,
    select: (data) =>
      data.rows.map((row) => ({
        ...row,
        lagMs: row.lag ? sumPostgresIntervalMs(row.lag) : null,
      })),
  });
}

/**
 * Fetches current CPU and memory utilization per cluster.
 * Returns a Map keyed by clusterId for fast lookup.
 */
export function useClusterUtilization() {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.clusterUtilization(),
    queryFn: ({ queryKey, signal }) =>
      fetchClusterUtilization({
        queryKey,
        requestOptions: { signal },
      }),
    refetchInterval: 60_000,
    staleTime: 30_000,
    select: (data) => {
      const byClusterId = new Map<string, ClusterUtilizationRow>();
      for (const row of data.rows) {
        // If a cluster has multiple replicas, keep the first one.
        // Future enhancement: show per-replica or max utilization.
        if (!byClusterId.has(row.clusterId)) {
          byClusterId.set(row.clusterId, row);
        }
      }
      return byClusterId;
    },
  });
}

/**
 * Fetches detailed metadata for a single object.
 * Includes cluster replica info, hydration status, and lag.
 */
export function useObjectDetail(objectId: string | undefined) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.objectDetail(objectId ?? ""),
    queryFn: ({ queryKey, signal }) =>
      fetchObjectDetail({
        objectId: objectId!,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: !!objectId,
    staleTime: 30_000,
    select: (data) => {
      const row = data.rows[0];
      if (!row) return null;
      return {
        ...row,
        lagMs: row.lag ? sumPostgresIntervalMs(row.lag) : null,
      };
    },
  });
}

/**
 * Fetches per-object memory from mz_dataflow_arrangement_sizes.
 * Only enabled for compute objects (indexes, MVs) with a known cluster/replica.
 */
export function useObjectMemory({
  objectId,
  clusterName,
  replicaName,
  objectType,
}: {
  objectId: string | undefined;
  clusterName: string | undefined;
  replicaName: string | undefined;
  objectType: string | undefined;
}) {
  const isCompute =
    objectType === "index" || objectType === "materialized-view";

  return useQuery({
    queryKey: maintainedObjectsQueryKeys.objectMemory(objectId ?? ""),
    queryFn: ({ queryKey, signal }) =>
      fetchObjectMemory({
        objectId: objectId!,
        clusterName: clusterName!,
        replicaName: replicaName!,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: !!objectId && !!clusterName && !!replicaName && isCompute,
    staleTime: 30_000,
    select: (data) => data.rows[0] ?? null,
  });
}

/**
 * Fetches per-object arrangement sizes for ALL objects across all clusters.
 * Includes cost attribution (estimated credits/hour based on memory fraction).
 * Returns a Map keyed by object_id for fast lookup in the list view.
 */
export function useAllObjectSizes() {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.allObjectSizes(),
    queryFn: async ({ queryKey, signal }) => {
      const query = sql`
        SELECT
          oas.object_id AS "objectId",
          oas.replica_id AS "replicaId",
          oas.size::text AS "sizeBytes",
          cr.name AS "replicaName",
          crs.credits_per_hour::float8 AS "creditsPerHour",
          COALESCE(crm.heap_limit, crs.memory_bytes * crs.processes, 0)::text AS "replicaTotalMemoryBytes",
          CASE WHEN COALESCE(crm.heap_limit, crs.memory_bytes * crs.processes, 0) > 0
            THEN (oas.size::float8 / COALESCE(crm.heap_limit, crs.memory_bytes * crs.processes)::float8 * 100)
            ELSE NULL
          END AS "heapPercent",
          (oas.size::float8 / NULLIF(SUM(oas.size) OVER (PARTITION BY oas.replica_id), 0) * crs.credits_per_hour::float8) AS "estimatedCreditsPerHour"
        FROM mz_internal.mz_object_arrangement_sizes AS oas
        INNER JOIN mz_cluster_replicas AS cr ON cr.id = oas.replica_id
        INNER JOIN mz_cluster_replica_sizes AS crs ON crs.size = cr.size
        LEFT JOIN (
          SELECT replica_id, MAX(heap_limit) AS heap_limit
          FROM mz_internal.mz_cluster_replica_metrics
          GROUP BY replica_id
        ) AS crm ON crm.replica_id = cr.id
        WHERE oas.object_id LIKE 'u%'
        ORDER BY oas.size DESC
      `.compile(rawQueryBuilder);

      return executeSqlV2({
        queries: query,
        queryKey,
        requestOptions: { signal },
      });
    },
    refetchInterval: 30_000,
    staleTime: 15_000,
    select: (data) => {
      // Build a Map: objectId → { sizeBytes, replicaName, estimatedCreditsPerHour }
      // If an object has multiple replicas, keep the first (largest replica).
      const byObjectId = new Map<
        string,
        {
          sizeBytes: string;
          replicaId: string;
          replicaName: string;
          creditsPerHour: number;
          replicaTotalMemoryBytes: string;
          heapPercent: number | null;
          estimatedCreditsPerHour: number | null;
        }
      >();
      for (const row of data.rows) {
        const objectId = row.objectId as string;
        if (!byObjectId.has(objectId)) {
          byObjectId.set(objectId, {
            sizeBytes: row.sizeBytes as string,
            replicaId: row.replicaId as string,
            replicaName: row.replicaName as string,
            creditsPerHour: row.creditsPerHour as number,
            replicaTotalMemoryBytes: row.replicaTotalMemoryBytes as string,
            heapPercent: row.heapPercent as number | null,
            estimatedCreditsPerHour: row.estimatedCreditsPerHour as number | null,
          });
        }
      }
      return byObjectId;
    },
  });
}

/**
 * Fetches per-object memory from the unified mz_internal.mz_object_arrangement_sizes
 * collection. No session variables needed — queries mz_catalog_server directly.
 * Returns memory across all replicas for the object.
 */
export function useObjectMemoryUnified({
  objectId,
  objectType,
}: {
  objectId: string | undefined;
  objectType: string | undefined;
}) {
  const isCompute =
    objectType === "index" || objectType === "materialized-view";

  return useQuery({
    queryKey: maintainedObjectsQueryKeys.objectMemoryUnified(objectId ?? ""),
    queryFn: ({ queryKey, signal }) =>
      fetchObjectMemoryUnified({
        objectId: objectId!,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: !!objectId && isCompute,
    staleTime: 30_000,
    refetchInterval: 30_000,
    select: (data) => data.rows,
  });
}

/**
 * Fetches column definitions for a single object.
 */
export function useObjectColumns(objectId: string | undefined) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.objectColumns(objectId ?? ""),
    queryFn: ({ queryKey, signal }) =>
      fetchObjectColumns({
        objectId: objectId!,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: !!objectId,
    staleTime: 60_000,
    select: (data) => data.rows,
  });
}

/**
 * Fetches direct upstream dependencies with bottleneck detection.
 */
export function useUpstreamDependencies(objectId: string | undefined) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.upstreamDependencies(objectId ?? ""),
    queryFn: ({ queryKey, signal }) =>
      fetchUpstreamDependencies({
        objectId: objectId!,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: !!objectId,
    staleTime: 30_000,
    select: (data) => data.rows,
  });
}

/**
 * Fetches direct downstream dependents.
 */
export function useDownstreamDependents(objectId: string | undefined) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.downstreamDependents(objectId ?? ""),
    queryFn: ({ queryKey, signal }) =>
      fetchDownstreamDependents({
        objectId: objectId!,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: !!objectId,
    staleTime: 30_000,
    select: (data) => data.rows,
  });
}

/**
 * Fetches the full critical path back to root sources.
 * Uses Frank's WITH MUTUALLY RECURSIVE crit_path + crit_edge pattern.
 * Only enabled when `enabled` is true (user clicks "Show full path").
 */
export function useCriticalPath(
  objectId: string | undefined,
  enabled: boolean,
) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.criticalPath(objectId ?? ""),
    queryFn: ({ queryKey, signal }) =>
      fetchCriticalPath({
        objectId: objectId!,
        queryKey,
        requestOptions: { signal },
      }),
    enabled: !!objectId && enabled,
    staleTime: 30_000,
    select: (data) =>
      data.rows as unknown as CriticalPathEdge[],
  });
}

/**
 * Fetches historical freshness data for a single object.
 * Returns data formatted for the FreshnessGraph component.
 */
export function useObjectFreshnessHistory({
  objectId,
  lookbackMs,
}: {
  objectId: string | undefined;
  lookbackMs: number;
}) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.freshnessHistory(
      objectId ?? "",
      lookbackMs,
    ),
    queryFn: async ({ queryKey, signal }) => {
      const { rows } = await fetchLagHistory({
        params: {
          lookback: { type: "historical", lookbackMs },
          objectIds: [objectId!],
        },
        requestOptions: { signal },
        queryKey,
      });

      const bucketSizeMs = calculateBucketSizeFromLookback(lookbackMs);

      const historicalData: DataPoint[] = rows.map((row) => ({
        timestamp: row.bucketStart.getTime(),
        lag: {
          [row.objectId]: row.lag
            ? {
                queryable: true as const,
                totalMs: sumPostgresIntervalMs(row.lag),
                interval: row.lag,
                schemaName: row.schemaName,
                objectName: row.objectName,
              }
            : {
                queryable: false as const,
                schemaName: row.schemaName,
                objectName: row.objectName,
              },
        },
      }));

      const lastRow = rows.at(-1);
      const lines: GraphLineSeries[] = lastRow
        ? [
            {
              key: lastRow.objectId,
              label: formatFullyQualifiedObjectName({
                schemaName: lastRow.schemaName ?? "",
                name: lastRow.objectName ?? "",
              }),
              yAccessor: (d: DataPoint) => {
                const lagInfo = d.lag[lastRow.objectId];
                if (lagInfo?.queryable) return lagInfo.totalMs;
                return null;
              },
            },
          ]
        : [];

      return {
        historicalData,
        lines,
        bucketSizeMs,
        startTime: historicalData.at(0)?.timestamp ?? 0,
        endTime: historicalData.at(-1)?.timestamp ?? 0,
      };
    },
    enabled: !!objectId,
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
}

/**
 * Subscribes to the latest lag for a single object via SUBSCRIBE.
 * Uses mz_wallclock_global_lag which updates in real-time.
 */
export function useObjectLag(objectId: string | undefined) {
  const subscribe = React.useMemo(() => {
    if (!objectId) return undefined;
    const query = queryBuilder
      .selectFrom("mz_wallclock_global_lag as wl")
      .select(["wl.object_id", "wl.lag"])
      .where("wl.object_id", "=", objectId);
    return buildSubscribeQuery(query, { upsertKey: "object_id" });
  }, [objectId]);

  const { data } = useSubscribe({
    subscribe,
    upsertKey: (row) => row.data.object_id as string,
    select: (row) => ({
      object_id: row.data.object_id as string,
      lag: row.data.lag,
    }),
  });

  const result = React.useMemo(() => {
    if (!data || data.length === 0) return null;
    const row = data[0];
    if (!row?.lag) return null;
    return {
      lag: row.lag,
      lagMs: sumPostgresIntervalMs(row.lag),
    };
  }, [data]);

  return { data: result };
}
