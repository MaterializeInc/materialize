// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSuspenseQuery } from "@tanstack/react-query";
import { flatGroup, group } from "d3";
import { subDays } from "date-fns";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { fetchReplicaUtilizationHistory } from "~/api/materialize/cluster/replicaUtilizationHistory";
import {
  ClusterReplicaDetails,
  fetchClusterDetails,
} from "~/api/materialize/environment-overview/clusterDetails";
import { fetchLagHistory } from "~/api/materialize/freshness/lagHistory";
import { DataPoint } from "~/components/FreshnessGraph/types";
import { notNullOrUndefined, sumPostgresIntervalMs } from "~/util";
import { sortLagInfo } from "~/utils/freshness";

import {
  BucketMap,
  BucketWithUtilizationData,
  calculateClusterCategory,
  calculateClusterStatus,
  calculateMemDiskUtilizationStatus,
  calculatePeakMemDiskUtilization,
  initializeBuckets,
  MemDiskUtilizationStatus,
} from "./utils";

const environmentOverviewQueryKeys = {
  all: () => buildRegionQueryKey("environmentOverview"),
  clusterMemDiskUtilization: () =>
    [
      ...environmentOverviewQueryKeys.all(),
      buildQueryKeyPart("clusterMemDiskUtilization"),
    ] as const,
  clusterFreshnessHistorical: ({ lookbackMs }: { lookbackMs: number }) =>
    [
      ...environmentOverviewQueryKeys.all(),
      buildQueryKeyPart("clusterFreshnessHistorical", { lookbackMs }),
    ] as const,
};

const UTILIZATION_LOOKBACK_DAYS = 14;
const EIGHT_HOURS_IN_MS = 8 * 60 * 60 * 1000;

export function useClusterMemDiskUtilization() {
  return useSuspenseQuery({
    queryKey: environmentOverviewQueryKeys.clusterMemDiskUtilization(),
    queryFn: async ({ queryKey, signal }) => {
      const endDate = new Date();

      const startDate = subDays(endDate, UTILIZATION_LOOKBACK_DAYS);

      const [replicaDetailsRes, replicaUtilizationHistoryRes] =
        await Promise.all([
          fetchClusterDetails({
            queryKey,
            requestOptions: { signal },
          }),
          fetchReplicaUtilizationHistory({
            params: {
              bucketSizeMs: EIGHT_HOURS_IN_MS,
              startDate: startDate.toISOString(),
              shouldUseConsoleClusterUtilizationOverviewView: true,
            },
            queryKey,
            requestOptions: { signal },
          }),
        ]);

      const clusterDetailsById = replicaDetailsRes.rows.reduce((accum, row) => {
        accum.set(row.clusterId, row);
        return accum;
      }, new Map<string, ClusterReplicaDetails>());

      const clusterBucketsMap = new Map<
        string,
        Map<number, BucketWithUtilizationData>
      >();

      // For each bucket per replica, we aggregate the peak memory and disk utilization
      // by each replica's cluster
      for (const replicaId in replicaUtilizationHistoryRes.bucketsByReplicaId) {
        const buckets =
          replicaUtilizationHistoryRes.bucketsByReplicaId[replicaId];

        if (buckets.length === 0) {
          continue;
        }

        for (const bucket of buckets) {
          const clusterDetails = clusterDetailsById.get(
            bucket.currentDeploymentClusterId,
          );
          // Skip buckets whose clusters don't belong in the current list.
          if (!clusterDetails) {
            continue;
          }

          let extantClusterBuckets = clusterBucketsMap.get(
            bucket.currentDeploymentClusterId,
          );

          // If a cluster doesn't have buckets yet, initialize it with an empty map
          if (!extantClusterBuckets) {
            extantClusterBuckets = new Map();
            clusterBucketsMap.set(
              bucket.currentDeploymentClusterId,
              extantClusterBuckets,
            );
          }

          const bucketStartTs = bucket.bucketStart.getTime();

          const clusterCategory = calculateClusterCategory({
            numSources: Number(clusterDetails.numSources),
            numSinks: Number(clusterDetails.numSinks),
            numIndexes: Number(clusterDetails.numIndexes),
            numMaterializedViews: Number(clusterDetails.numMaterializedViews),
          });

          const peakUtilization = calculatePeakMemDiskUtilization({
            category: clusterCategory,
            bucket,
          });

          const extantBucket = extantClusterBuckets.get(bucketStartTs);

          // For a cluster with many replicas, if the current replica has higher utilization
          // than the current replica in clusterBucketsMap, we override it with the current replica.
          if (
            !extantBucket ||
            (extantBucket.peakMemDiskUtilizationPercent ?? 0) <
              (peakUtilization.peakMemDiskUtilizationPercent ?? 0)
          ) {
            const oomEvents =
              bucket.offlineEvents?.filter(
                ({ reason }) => reason === "oom-killed",
              ) ?? [];

            extantClusterBuckets.set(bucketStartTs, {
              status:
                oomEvents.length > 0
                  ? "underProvisioned"
                  : calculateMemDiskUtilizationStatus({
                      peakMemDiskUtilizationPercent:
                        peakUtilization.peakMemDiskUtilizationPercent,
                      thresholdPercentages:
                        peakUtilization.thresholdPercentages,
                    }),
              oomEvents,
              peakMemDiskUtilizationPercent:
                peakUtilization.peakMemDiskUtilizationPercent,
              memoryPercent: peakUtilization.memoryPercent,
              diskPercent: peakUtilization.diskPercent,
              heapPercent: peakUtilization.heapPercent,
              occurredAt: peakUtilization.occurredAt,
              thresholdPercents: peakUtilization.thresholdPercentages,
              bucketStart: bucket.bucketStart,
              bucketEnd: bucket.bucketEnd,
              replicaSize: bucket.size,
            });
          }
        }
      }

      const res = new Map<
        string,
        {
          clusterId: string;
          clusterName: string;
          replicas: {
            id: string;
            name: string;
            size: string | null;
            disk: boolean | null;
          }[];
          status: MemDiskUtilizationStatus;
          buckets: BucketMap;
        }
      >();

      // We need to combine the clusters from the bucket map and the cluster details map
      // because if a cluster has a replication factor of 0, it won't have any buckets
      const clusterIds = new Set([
        ...clusterBucketsMap.keys(),
        ...clusterDetailsById.keys(),
      ]);

      for (const clusterId of clusterIds) {
        const clusterDetails = clusterDetailsById.get(clusterId);
        const buckets = clusterBucketsMap.get(clusterId);

        if (!clusterDetails) {
          continue;
        }

        const bucketsWithGapsFilled: BucketMap = initializeBuckets({
          startMs: startDate.getTime(),
          endMs: endDate.getTime(),
          minBucketStartMs: replicaUtilizationHistoryRes.minBucketStartMs,
          maxBucketEndMs: replicaUtilizationHistoryRes.maxBucketEndMs,
          bucketSizeMs: EIGHT_HOURS_IN_MS,
        });

        if (buckets) {
          for (const bucket of buckets.values()) {
            const bucketStartTs = bucket.bucketStart.getTime();
            // If there exists a bucket for that timestamp, we update it
            if (bucketsWithGapsFilled.has(bucketStartTs)) {
              bucketsWithGapsFilled.set(bucketStartTs, bucket);
            }
          }
        }

        res.set(clusterId, {
          clusterId,
          clusterName: clusterDetails.clusterName,
          replicas: clusterDetails.replicas,
          status: calculateClusterStatus(bucketsWithGapsFilled),
          buckets: bucketsWithGapsFilled,
        });
      }

      return res;
    },
  });
}

export function useClusterFreshnessHistorical({
  lookbackMs,
}: {
  lookbackMs: number;
}) {
  return useSuspenseQuery({
    queryKey: environmentOverviewQueryKeys.clusterFreshnessHistorical({
      lookbackMs,
    }),
    queryFn: async ({ queryKey, signal }) => {
      const { rows } = await fetchLagHistory({
        params: {
          groupByCluster: true,
          lookback: {
            type: "historical",
            lookbackMs,
          },
        },
        requestOptions: { signal },
        queryKey,
      });

      const dataByClusterId = flatGroup(rows, (d) => d.clusterId);

      const currentDataByClusterId = dataByClusterId
        .map(([_, rowsByClusterId]) => {
          // We can assume the last row is the most current because the data is sorted
          // by bucket start time.
          const lastRow = rowsByClusterId.at(-1);
          if (!lastRow) {
            return null;
          }
          return lastRow;
        })
        .filter(notNullOrUndefined)
        .sort(sortLagInfo);

      const lines = currentDataByClusterId.map((row) => {
        return {
          key: row.clusterId,
          label: row.clusterName,
          yAccessor: (d: DataPoint) => {
            const dataPointLag = d.lag[row.clusterId];
            if (dataPointLag) {
              return dataPointLag.queryable ? dataPointLag.totalMs : 0;
            }
            return null;
          },
        };
      });

      const dataByBucketStart = group(rows, (d) => d.bucketStart.getTime());

      const historicalData = [...dataByBucketStart.entries()].map(
        ([timestamp, rowsByBucketStart]) => {
          const dataPoint: DataPoint = {
            timestamp,
            lag: {},
          };

          rowsByBucketStart.forEach((row) => {
            dataPoint.lag[row.clusterId] = row.lag
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

      return {
        historicalData,
        currentData: currentDataByClusterId,
        lines,
        startTime: historicalData.at(0)?.timestamp ?? 0,
        endTime: historicalData.at(-1)?.timestamp ?? 0,
      };
    },
  });
}

export type CurrentClusterFreshnessData = ReturnType<
  typeof useClusterFreshnessHistorical
>["data"]["currentData"][0];
