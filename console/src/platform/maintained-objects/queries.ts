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
