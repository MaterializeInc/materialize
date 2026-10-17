// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import { group } from "d3";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { hasUtilizationReading } from "~/api/materialize/cluster/replicaUtilizationBinning";
import { ReplicaData } from "~/platform/clusters/ClusterOverview/types";
import { MIN_BUCKET_SIZE_MS } from "~/platform/clusters/constants";
import {
  OVERVIEW_MAX_MINUTES,
  REPLICA_MEMORY_BREAKDOWN_MIN_VERSION,
  useObjectCreationTimes,
  usePolledReplicaUtilizationHistory,
  useReplicaHeapLimits,
  useReplicaHydrationEpisodes,
  useReplicaStatusHistory,
  useUnhydratedObjectCounts,
  useUtilizationTier,
} from "~/platform/clusters/queries";
import { useClusterObjectsLive } from "~/store/allObjectsCollection";
import { useEnvironmentGate } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import {
  assignReplicaColors,
  groupReplicasByName,
  mergeTimelines,
  missingUsageCause,
  replicasRestartedSince,
  ReplicaTimeline,
  sortReplicasForDisplay,
  transformDdlEvents,
  transformReplicaTimeline,
} from "./resourceUsageModel";
import { useLiveClusterUtilization } from "./useLiveClusterUtilization";

const DDL_OBJECT_TYPES = ["index", "materialized-view"];
const EMPTY_SERIES: ReplicaData[] = [];

export interface ColoredReplica {
  id: string;
  name: string;
  size?: string;
  isCurrent: boolean;
  color: string;
}

/** Charted replicas, then current replicas with no data yet, current first. */
const summarizeReplicas = (cluster: Cluster, graphData: ReplicaData[]) => {
  const currentIds = new Set(cluster.replicas.map((replica) => replica.id));
  const charted = graphData.map(({ id, data }) => {
    const latest = data.at(-1);
    return {
      id,
      name: latest?.name ?? id,
      clusterId: latest?.clusterId ?? cluster.id,
      size: latest?.size ?? undefined,
      isCurrent: currentIds.has(id),
    };
  });
  const chartedIds = new Set(charted.map((replica) => replica.id));
  const uncharted = cluster.replicas
    .filter((replica) => !chartedIds.has(replica.id))
    .map(({ id, name, size }) => ({
      id,
      name,
      clusterId: cluster.id,
      size: size ?? undefined,
      isCurrent: true,
    }));
  return sortReplicasForDisplay([...charted, ...uncharted]);
};

/**
 * Loads and derives what the resource usage card draws for a cluster. Covers
 * every charted replica, and leaves which ones are visible to the caller.
 */
export function useResourceUsageData({
  cluster,
  timePeriodMinutes,
}: {
  cluster: Cluster;
  timePeriodMinutes: number;
}) {
  const { colors } = useTheme<MaterializeTheme>();
  const bucketSizeMs = Math.max(timePeriodMinutes * 1000, MIN_BUCKET_SIZE_MS);
  const tier = useUtilizationTier(timePeriodMinutes);
  const hasMemoryBreakdown =
    useEnvironmentGate(REPLICA_MEMORY_BREAKDOWN_MIN_VERSION) === true;
  const live = useLiveClusterUtilization({
    clusterId: cluster.id,
    timePeriodMinutes,
    bucketSizeMs,
    tier: tier === "poll" ? undefined : tier,
    includeMemoryBreakdown: hasMemoryBreakdown,
  });
  const polled = usePolledReplicaUtilizationHistory(
    {
      bucketSizeMs: Math.max(timePeriodMinutes * 1000, MIN_BUCKET_SIZE_MS),
      timePeriodMinutes,
      clusterIds: [cluster.id],
      replicaId: undefined,
      includeMemoryBreakdown: true,
    },
    { enabled: tier === "poll" },
  );
  const history =
    tier === "poll"
      ? {
          data: polled.data,
          isLoading: polled.isLoading,
          isError: polled.isError,
          isRefreshing: false,
        }
      : live;
  const graphData = history.data?.graphData;

  // Replicas that share a name, such as a scheduled cluster's refreshes, share
  // a color, a toggle and a status row, keyed by the id standing for the name.
  const { replicasById, replicaGroups, groupIdById, colorMap } =
    React.useMemo(() => {
      const replicas = summarizeReplicas(cluster, graphData ?? []);
      const lastReportedMs = new Map(
        (graphData ?? []).map(({ id, data }) => [
          id,
          data.at(-1)?.bucketEnd ?? 0,
        ]),
      );
      const groupIds = groupReplicasByName(replicas, lastReportedMs);
      const groups = replicas.filter(({ id }) => groupIds.get(id) === id);
      const groupColors = assignReplicaColors(groups, colors.lineGraph);
      const colorOf = (id: string) =>
        groupColors.get(groupIds.get(id) ?? id) ?? colors.lineGraph[0];
      const withColor = (replica: (typeof replicas)[number]) => ({
        ...replica,
        color: colorOf(replica.id),
      });
      return {
        replicasById: new Map(
          replicas.map((replica): [string, ColoredReplica] => [
            replica.id,
            withColor(replica),
          ]),
        ),
        // Display order: current replicas first, then by name.
        replicaGroups: groups.map(withColor),
        groupIdById: groupIds,
        colorMap: new Map(replicas.map(({ id }) => [id, colorOf(id)])),
      };
    }, [cluster, graphData, colors.lineGraph]);
  // Sorted by id for stable query keys, unlike display order.
  const replicaIds = React.useMemo(
    () => [...replicasById.keys()].sort(),
    [replicasById],
  );
  const chartData = graphData ?? EMPTY_SERIES;

  const currentReplicaIds = cluster.replicas
    .map((replica) => replica.id)
    .sort();

  const statusHistory = useReplicaStatusHistory(replicaIds);
  // Only a replica that came back online in the window has a hydration the
  // timeline can place, so the episodes query skips the rest.
  const restartedReplicaIds = history.data
    ? replicasRestartedSince(
        statusHistory.data ?? [],
        new Set(currentReplicaIds),
        history.data.startDate.getTime(),
      )
    : [];
  const hydrationEpisodes = useReplicaHydrationEpisodes({
    replicaIds: restartedReplicaIds,
    timePeriodMinutes,
  });
  const unhydratedObjectCounts = useUnhydratedObjectCounts(currentReplicaIds);

  const { data: clusterObjects, isError: isObjectsError } =
    useClusterObjectsLive(cluster.id, DDL_OBJECT_TYPES);
  const creationTimes = useObjectCreationTimes({
    objectIds: clusterObjects.map((object) => object.id).sort(),
    timePeriodMinutes,
  });
  const ddlEvents = React.useMemo(
    () => transformDdlEvents(clusterObjects, creationTimes.data ?? []),
    [clusterObjects, creationTimes.data],
  );

  const heapLimits = useReplicaHeapLimits(currentReplicaIds);
  // Only running replicas report a heap limit, and replicas of one size share
  // it, so a dropped replica's limit comes from a running one of its size.
  const heapLimitBytesBySize = new Map(
    cluster.replicas.flatMap(({ id, size }) => {
      const bytes = heapLimits.data?.get(id);
      return size && typeof bytes === "number" ? [[size, bytes] as const] : [];
    }),
  );
  // A sample without a heap limit has `heap_percent` as a share of the memory
  // allocation instead, and no RAM limit share. Only the views' rows on 26.44
  // and later carry that share, so other windows keep the heap labels.
  const rowsCarryHeapLimit =
    hasMemoryBreakdown && timePeriodMinutes <= OVERVIEW_MAX_MINUTES;
  const hasHeapLimit =
    !rowsCarryHeapLimit ||
    chartData.some(({ data }) =>
      data.some((point) => point.ramLimitPercent !== null),
    );

  const timelines = React.useMemo(() => {
    const timelinesById = new Map<string, ReplicaTimeline>();
    if (!history.data) return timelinesById;
    const { startDate, endDate } = history.data;
    const dataById = new Map(
      history.data.graphData.map(({ id, data }) => [id, data]),
    );
    // Every replica gets a row, including one with no samples in the window,
    // whose status history may still say it was offline.
    for (const id of replicasById.keys()) {
      const data = dataById.get(id) ?? [];
      const last = data.at(-1);
      // A down replica still reports, with empty readings, so only readings
      // show it running.
      const measured = data.filter(hasUtilizationReading);
      const firstMeasured = measured.at(0);
      const lastMeasured = measured.at(-1);
      timelinesById.set(
        id,
        transformReplicaTimeline({
          transitions: (statusHistory.data ?? []).filter(
            (transition) => transition.replicaId === id,
          ),
          hydrationEpisodes: (hydrationEpisodes.data ?? []).filter(
            (episode) => episode.replicaId === id,
          ),
          windowStartMs: startDate.getTime(),
          // A dropped replica stops reporting, so end its row with its data.
          windowEndMs: replicasById.get(id)?.isCurrent
            ? endDate.getTime()
            : (last?.bucketEnd ?? endDate.getTime()),
          sampleRange:
            firstMeasured && lastMeasured
              ? {
                  startMs: firstMeasured.bucketStart,
                  endMs: lastMeasured.bucketEnd,
                }
              : undefined,
        }),
      );
    }
    return new Map(
      [...group([...timelinesById], ([id]) => groupIdById.get(id) ?? id)].map(
        ([groupId, entries]) => [
          groupId,
          mergeTimelines(entries.map(([, timeline]) => timeline)),
        ],
      ),
    );
  }, [
    history.data,
    statusHistory.data,
    hydrationEpisodes.data,
    replicasById,
    groupIdById,
  ]);
  const missingUsage = missingUsageCause({
    timelines: [...timelines.values()],
    transitions: statusHistory.data ?? [],
    isStatusError: statusHistory.isError,
  });

  return {
    history,
    chartData,
    replicasById,
    replicaGroups,
    groupIdById,
    colorMap,
    timelines,
    ddlEvents,
    heapLimitBytesBySize,
    hasHeapLimit,
    unhydratedObjectCounts: unhydratedObjectCounts.data,
    missingUsage,
    isDdlError: isObjectsError || creationTimes.isError,
    isStatusLoading: statusHistory.isLoading,
    isStatusError: statusHistory.isError,
    isHydrationError:
      hydrationEpisodes.isError || unhydratedObjectCounts.isError,
  };
}
