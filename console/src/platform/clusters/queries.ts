// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  useMutation,
  useQuery,
  useQueryClient,
  useSuspenseQuery,
} from "@tanstack/react-query";
import { flatGroup, group } from "d3";
import { subMinutes } from "date-fns";
import { useCallback, useMemo } from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  formatFullyQualifiedObjectName,
  isSystemCluster,
} from "~/api/materialize";
import {
  alterCluster,
  AlterClusterNameParams,
  AlterClusterSettingsParams,
} from "~/api/materialize/cluster/alterCluster";
import {
  ArrangmentMemoryUsageParams,
  fetchArrangmentMemoryUsage,
} from "~/api/materialize/cluster/arrangementMemory";
import fetchAvailableClusterSizes from "~/api/materialize/cluster/availableClusterSizes";
import {
  ClusterListFilters,
  fetchClusters,
} from "~/api/materialize/cluster/clusterList";
import {
  fetchIndexesList,
  ListFilters,
} from "~/api/materialize/cluster/indexesList";
import {
  fetchLargestClusterReplica,
  LargestClusterReplicaParams,
} from "~/api/materialize/cluster/largestClusterReplica";
import { fetchLargestMaintainedQueries } from "~/api/materialize/cluster/largestMaintainedQueries";
import {
  fetchMaterializationLag,
  LagInfo,
  MaterializationLagParams,
} from "~/api/materialize/cluster/materializationLag";
import fetchMaxReplicasPerCluster from "~/api/materialize/cluster/maxReplicasPerCluster";
import {
  ClusterReplicasParams,
  fetchClusterReplicas,
} from "~/api/materialize/cluster/replicas";
import {
  ClusterReplicasWithUtilizationParams,
  fetchClusterReplicasWithUtilization,
} from "~/api/materialize/cluster/replicasWithUtilization";
import {
  fetchReplicaUtilizationHistory,
  ReplicaUtilizationHistoryParameters,
} from "~/api/materialize/cluster/replicaUtilizationHistory";
import { fetchLagHistory } from "~/api/materialize/freshness/lagHistory";
import { assertNoMoreThanOneRow } from "~/api/materialize/MoreThanOneRowError";
import { DataPoint, GraphLineSeries } from "~/components/FreshnessGraph/types";
import { DEFAULT_OPTIONS as TIME_PERIOD_OPTIONS } from "~/hooks/useTimePeriodSelect";
import { notNullOrUndefined, sumPostgresIntervalMs } from "~/util";
import { sortLagInfo } from "~/utils/freshness";

import { OfflineEvent } from "./ClusterOverview/types";
type ReplicaUtilizationHistoryFilters = {
  clusterIds: ReplicaUtilizationHistoryParameters["clusterIds"];
  replicaId: ReplicaUtilizationHistoryParameters["replicaId"];
  timePeriodMinutes: number;
  bucketSizeMs: ReplicaUtilizationHistoryParameters["bucketSizeMs"];
};

type ClusterFreshnessParams = {
  lookbackMs: number;
  clusterId: string;
};

export const clusterQueryKeys = {
  /**
   *
   * Currently we fetch all clusters in the environment and use it to display the list view
   * but also to get information about a specific cluster. It's useful to have the full list since
   * we do routing validation that requires all the clusters that's faster client-side vs. making a round-trip request
   * to the server. If the number of clusters grow and we have, we'll need to change
   * our caching strategy to be more denormalized.
   */
  all: () => buildRegionQueryKey("clusters"),
  list: (filters?: ClusterListFilters) =>
    [...clusterQueryKeys.all(), buildQueryKeyPart("list", filters)] as const,
  alter: () => [...clusterQueryKeys.all(), buildQueryKeyPart("alter")] as const,
  indexesList: (filters?: ListFilters) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("indexesList", filters),
    ] as const,
  largestClusterReplica: (params: LargestClusterReplicaParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("largestClusterReplica", params),
    ] as const,
  largestMaintainedQueries: (params: UseLargestMaintainedQueriesParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("largestMaintainedQueries", params),
    ] as const,
  availableClusterSizes: () =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("availableClusterSizes"),
    ] as const,
  maxReplicasPerCluster: () =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("maxReplicasPerCluster"),
    ] as const,
  replicas: (params: ClusterReplicasParams) =>
    [...clusterQueryKeys.all(), buildQueryKeyPart("replicas", params)] as const,
  replicasWithUtilization: (params: ClusterReplicasWithUtilizationParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicasWithUtilization", params),
    ] as const,
  arrangementMemory: (params: ArrangmentMemoryUsageParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("arrangementMemory", {
        ...params,
        replicaHeapLimit: params.replicaHeapLimit,
      }),
    ] as const,
  materializationLag: (params: MaterializationLagParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("materializationLag", params),
    ] as const,
  replicaUtilizationHistory: (params: ReplicaUtilizationHistoryFilters) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaUtilizationHistory", params),
    ] as const,
  clusterFreshness: (params: ClusterFreshnessParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("clusterFreshness", params),
    ] as const,
};

export function useClusters(filters?: ClusterListFilters) {
  const suspenseQueryResult = useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: clusterQueryKeys.list(filters),
    queryFn: ({ queryKey, signal }) => {
      const [, filtersKeyPart] = queryKey;
      return fetchClusters({
        queryKey,
        filters: filtersKeyPart,
        requestOptions: { signal },
      });
    },
    select: (data) => {
      return data.rows;
    },
  });

  const clusterMap = useMemo(() => {
    return new Map(
      suspenseQueryResult.data.map((cluster) => [cluster.id, cluster]),
    );
  }, [suspenseQueryResult.data]);

  const getClusterById = useCallback(
    (clusterId: string) => {
      return clusterMap.get(clusterId);
    },
    [clusterMap],
  );

  return {
    ...suspenseQueryResult,
    getClusterById,
  };
}

export type AlterClusterParams = AlterClusterSettingsParams &
  AlterClusterNameParams;

export function useAlterCluster() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationKey: clusterQueryKeys.alter(),
    mutationFn: (params: AlterClusterParams) => {
      return alterCluster({
        nameParams: params,
        settingsParams: params,
        queryKey: clusterQueryKeys.alter(),
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: clusterQueryKeys.all(),
      });
    },
  });
}

export function useIndexesList(filters: ListFilters) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: clusterQueryKeys.indexesList(filters),
    queryFn: ({ queryKey, signal }) => {
      const [, filtersKeyPart] = queryKey;
      return fetchIndexesList({
        queryKey,
        filters: filtersKeyPart,
        requestOptions: { signal },
      });
    },
  });
}

export function useLargestClusterReplica(params: LargestClusterReplicaParams) {
  return useSuspenseQuery({
    refetchInterval: 60_000,
    queryKey: clusterQueryKeys.largestClusterReplica(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, paramsFromKey] = queryKey;
      const result = await fetchLargestClusterReplica({
        queryKey,
        params: paramsFromKey,
        requestOptions: { signal },
      });
      assertNoMoreThanOneRow(result.rows.length, { skipQueryRetry: true });
      return result.rows.at(0) ?? null;
    },
  });
}

const STRIP_DATAFLOW_PREFIX = /^Dataflow: /;

export type UseLargestMaintainedQueriesParams = {
  clusterName: string;
  limit?: number;
  replicaName: string | undefined;
  replicaHeapLimit: number | undefined;
};
export function useLargestMaintainedQueries(
  params: UseLargestMaintainedQueriesParams,
) {
  return useSuspenseQuery({
    refetchInterval: 60_000,
    queryKey: clusterQueryKeys.largestMaintainedQueries(params),
    queryFn: ({ queryKey, signal }) => {
      const [, paramsFromKey] = queryKey;
      if (
        paramsFromKey.replicaHeapLimit === null ||
        paramsFromKey.replicaHeapLimit === undefined ||
        !paramsFromKey.replicaName
      )
        return null;

      return fetchLargestMaintainedQueries({
        queryKey,
        params: {
          ...paramsFromKey,
          replicaHeapLimit: paramsFromKey.replicaHeapLimit,
          replicaName: paramsFromKey.replicaName,
          limit: paramsFromKey.limit ?? 10,
        },
        requestOptions: { signal },
      });
    },
    select: (data) => {
      return data?.rows.map((row) => {
        // If you drop an index used by a materialization, the dataflow stays around, but
        // we can no longer look it up in mz_objects.
        const isOrphanedDataflow =
          !row.id || !row.name || !row.databaseName || !row.schemaName;

        let databaseName = row.databaseName,
          schemaName = row.schemaName,
          name = row.name;

        if (isOrphanedDataflow) {
          const fullyQualifiedName = row.dataflowName
            .replace(STRIP_DATAFLOW_PREFIX, "")
            .split(".");
          if (fullyQualifiedName.length === 3) {
            [databaseName, schemaName, name] = fullyQualifiedName;
          }
        }

        return {
          ...row,
          isOrphanedDataflow,
          databaseName: databaseName,
          schemaName: schemaName,
          name: name,
        };
      });
    },
  });
}

export function useAvailableClusterSizes() {
  return useSuspenseQuery({
    queryKey: clusterQueryKeys.availableClusterSizes(),
    queryFn: ({ queryKey, signal }) => {
      return fetchAvailableClusterSizes({
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useMaxReplicasPerCluster() {
  return useQuery({
    queryKey: clusterQueryKeys.maxReplicasPerCluster(),
    queryFn: ({ queryKey, signal }) => {
      return fetchMaxReplicasPerCluster({
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useReplicasBySize(params: ClusterReplicasParams) {
  return useQuery({
    queryKey: clusterQueryKeys.replicas({ ...params }),
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchClusterReplicas(queryKeyParams, queryKey, { signal });
    },
  });
}

export function useClusterReplicasWithUtilization(
  params: ClusterReplicasWithUtilizationParams,
) {
  const select = useCallback(
    ({
      rows,
    }: Awaited<ReturnType<typeof fetchClusterReplicasWithUtilization>>) => {
      return rows.map((row) => ({
        ...row,
        isOwner: Boolean(
          !isSystemCluster(params.clusterId) && !row.managed && row.isOwner,
        ),
      }));
    },
    [params.clusterId],
  );

  return useSuspenseQuery({
    queryKey: clusterQueryKeys.replicasWithUtilization(params),
    refetchInterval: 5000,
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchClusterReplicasWithUtilization(queryKeyParams, queryKey, {
        signal,
      });
    },
    select,
  });
}

/**
 * Returns a map of arrangment ID to memory usage as a percentage.
 *
 * Because this uses mz_compute_exports, it must run on the replica we want data from.
 */
export function useArrangmentsMemory(params: ArrangmentMemoryUsageParams) {
  return useQuery({
    refetchInterval: 5000,
    queryKey: clusterQueryKeys.arrangementMemory(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      const response = await fetchArrangmentMemoryUsage({
        params: queryKeyParams,
        queryKey,
        requestOptions: { signal },
      });
      if (!response) return null;

      return {
        ...response,
        memoryUsageById: new Map(
          response.rows?.map(({ id, size, memoryPercentage }) => [
            id,
            { size, memoryPercentage },
          ]),
        ),
      };
    },
  });
}

export type ArrangmentsMemoryUsageMap = NonNullable<
  ReturnType<typeof useArrangmentsMemory>["data"]
>["memoryUsageById"];

export type LagMap = Map<string, LagInfo>;

/**
 * Fetches a normalized table of an object, the lag between its direct parent, and
 * the lag between its source/table objects
 */
export function useMaterializationLag(params: MaterializationLagParams) {
  return useQuery({
    queryKey: clusterQueryKeys.materializationLag(params),
    queryFn: ({ queryKey, signal }) => {
      return fetchMaterializationLag(params, queryKey, { signal });
    },
    select: (lagData) => {
      const lagMap: LagMap = new Map();
      for (const r of lagData?.rows ?? []) {
        if (r.targetObjectId) {
          lagMap.set(r.targetObjectId, {
            hydrated: r.hydrated,
            lag: r.lag,
            isOutdated: r.isOutdated,
          });
        }
      }

      return {
        lagMap,
      };
    },
  });
}

type TimePeriodOptionValues =
  (typeof TIME_PERIOD_OPTIONS)[keyof typeof TIME_PERIOD_OPTIONS];

/**
 * Fetches a normalized table of an object, the lag between its direct parent, and
 * the lag between its source/table objects
 */
export function useReplicaUtilizationHistory(
  params: ReplicaUtilizationHistoryFilters,
  queryOptions?: { enabled?: boolean },
) {
  return useQuery({
    queryKey: clusterQueryKeys.replicaUtilizationHistory(params),
    refetchInterval: 20_000,
    enabled: queryOptions?.enabled,
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;

      const endDate = new Date();
      const startDate = subMinutes(endDate, queryKeyParams.timePeriodMinutes);

      const last14DaysOptionValue: TimePeriodOptionValues = "Last 14 days";

      const last14DaysTimePeriodMinutes = Number(
        Object.entries(TIME_PERIOD_OPTIONS).find(
          ([_, option]) => option === last14DaysOptionValue,
        )?.[0],
      );

      const data = await fetchReplicaUtilizationHistory({
        params: {
          ...queryKeyParams,
          startDate: startDate.toISOString(),
          shouldUseConsoleClusterUtilizationOverviewView:
            queryKeyParams.timePeriodMinutes === last14DaysTimePeriodMinutes,
        },
        queryKey,
        requestOptions: { signal },
      });

      const graphData = Object.entries(data.bucketsByReplicaId).map(
        ([replicaId, replicaData]) => {
          return {
            id: replicaId,
            data: replicaData.map(
              ({
                bucketEnd,
                bucketStart,
                maxHeap,
                maxMemory,
                maxCpu,
                maxDisk,
                maxMemoryAndDisk,
                size,
                offlineEvents,
                name,
              }) => ({
                id: replicaId,
                name,
                bucketEnd: bucketEnd.getTime(),
                bucketStart: bucketStart.getTime(),
                cpuPercent: maxCpu?.percent ? maxCpu.percent * 100 : null,
                diskPercent: maxDisk?.percent ? maxDisk.percent * 100 : null,
                memoryPercent: maxMemory?.percent
                  ? maxMemory.percent * 100
                  : null,
                // Memory utilization calculated in SQL: (memory_bytes + disk_bytes) / (available_memory + available_disk)
                maxMemoryAndDiskPercent: maxMemoryAndDisk?.percent
                  ? maxMemoryAndDisk.percent * 100
                  : null,
                heapPercent: maxHeap?.percent ? maxHeap.percent * 100 : null,
                size,
                offlineEvents:
                  offlineEvents?.map((event) => ({
                    id: event.replicaId,
                    offlineReason: event.reason,
                    status: event.status,
                    timestamp: new Date(event.occurredAt).getTime(),
                  })) ?? [],
              }),
            ),
          };
        },
      );

      const offlineEvents: Array<OfflineEvent> = [];

      for (const replicaId in graphData) {
        const replica = graphData[replicaId];

        for (const replicaDatum of replica?.data ?? []) {
          for (const {
            status,
            offlineReason,
            timestamp,
          } of replicaDatum.offlineEvents) {
            if (
              (status === "not-ready" || status === "offline") &&
              offlineReason !== "oom-killed"
            ) {
              offlineEvents.push({
                id: replicaDatum.id,
                offlineReason,
                status,
                timestamp,
              });
            }
          }
        }
      }

      /**
       * If the selected range is within the bounds of the data, we clamp it to the data's min and
       * max since we can have buckets outside the selected range so that each bucket has the same
       * size. However, if the selected range is larger than the bounds of the data, we wanted the
       * returned start date and end date to represent the selected range. For example, we might
       * select the last 30 days but if the data only goes back 7 days, we still want the range
       * in our graph to be the last 30 days.
       */
      const clampedStartDate = new Date(
        Math.min(new Date(startDate).getTime(), data.minBucketStartMs),
      );
      const clampedEndDate = new Date(
        Math.max(new Date(endDate).getTime(), data.maxBucketEndMs),
      );

      return {
        startDate: clampedStartDate,
        endDate: clampedEndDate,
        graphData,
        offlineEvents,
      };
    },
  });
}

export const LINE_MAX_COUNT = 10;

export function useClusterFreshness({
  lookbackMs,
  clusterId,
}: ClusterFreshnessParams) {
  return useSuspenseQuery({
    queryKey: clusterQueryKeys.clusterFreshness({
      lookbackMs,
      clusterId,
    }),
    queryFn: async ({ queryKey, signal }) => {
      const { rows } = await fetchLagHistory({
        params: {
          lookback: {
            type: "historical",
            lookbackMs,
          },
          clusterId,
          includeSystemObjects: isSystemCluster(clusterId),
        },
        requestOptions: { signal },
        queryKey,
      });

      const dataByObjectId = flatGroup(rows, (d) => d.objectId);

      const currentData = dataByObjectId
        .map(([_, rowsByObjectId]) => {
          // We can assume the last row is the most current because the data is sorted
          // by bucket start time.
          const lastRow = rowsByObjectId.at(-1);
          if (!lastRow) {
            return null;
          }
          return lastRow;
        })
        .filter(notNullOrUndefined)
        .sort(sortLagInfo)
        .slice(0, LINE_MAX_COUNT);

      const dataByBucketStart = group(rows, (d) => d.bucketStart.getTime());

      const historicalData = [...dataByBucketStart.entries()].map(
        ([timestamp, rowsByBucketStart]) => {
          const dataPoint: DataPoint = {
            timestamp,
            lag: {},
          };

          rowsByBucketStart.forEach((row) => {
            dataPoint.lag[row.objectId] =
              row.lag !== null
                ? {
                    queryable: true,
                    totalMs: sumPostgresIntervalMs(row.lag),
                    interval: row.lag,
                    schemaName: row.schemaName,
                    objectName: row.objectName,
                  }
                : {
                    queryable: false,
                    schemaName: row.schemaName,
                    objectName: row.objectName,
                  };
          });

          return dataPoint;
        },
      );

      const lines = historicalData.reduce((acc, curr) => {
        const objects = Object.entries(curr.lag);

        objects.forEach(([objectId, lagInfo]) => {
          acc.set(objectId, {
            key: objectId,
            label: formatFullyQualifiedObjectName({
              schemaName: lagInfo.schemaName ?? "",
              name: lagInfo.objectName ?? "",
            }),
            yAccessor: (d: DataPoint) => {
              const dataPointLag = d.lag[objectId];
              if (dataPointLag) {
                // We want it to show up at the bottom.
                return dataPointLag.queryable ? dataPointLag.totalMs : 0;
              }

              return null;
            },
          });
        });

        return acc;
      }, new Map<string, GraphLineSeries>());

      return {
        historicalData,
        currentData,
        lines: Array.from(lines.values()),
        startTime: historicalData.at(0)?.timestamp ?? 0,
        endTime: historicalData.at(-1)?.timestamp ?? 0,
      };
    },
  });
}

export type CurrentClusterFreshnessData = Awaited<
  ReturnType<typeof useClusterFreshness>["data"]
>["currentData"][0];
