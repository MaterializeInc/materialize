// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { ReplicaData } from "~/platform/clusters/ClusterOverview/types";
import { MIN_BUCKET_SIZE_MS } from "~/platform/clusters/constants";
import {
  useHydratingReplicaIds,
  useObjectCreationTimes,
  useReplicaHeapLimits,
  useReplicaHydrationEpisodes,
  useReplicaStatusHistory,
  useReplicaUtilizationHistory,
} from "~/platform/clusters/queries";
import { useClusterObjectsLive } from "~/store/allObjectsCollection";
import { MaterializeTheme } from "~/theme";

import {
  assignReplicaColors,
  replicasRestartedSince,
  ReplicaTimeline,
  sortReplicasForDisplay,
  transformDdlEvents,
  transformReplicaTimeline,
} from "./resourceUsageModel";

const DDL_OBJECT_TYPES = ["index", "materialized-view"];

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
  // The status timeline reads offline stretches from status history.
  const history = useReplicaUtilizationHistory(
    {
      bucketSizeMs: Math.max(timePeriodMinutes * 1000, MIN_BUCKET_SIZE_MS),
      timePeriodMinutes,
      clusterIds: [cluster.id],
      replicaId: undefined,
      includeMemoryBreakdown: true,
    },
    { includeOfflineEvents: false },
  );
  const graphData = history.data?.graphData;

  // Insertion order is display order: current replicas first, then by name.
  const { replicasById, colorMap } = React.useMemo(() => {
    const replicas = summarizeReplicas(cluster, graphData ?? []);
    const colorsById = assignReplicaColors(replicas, colors.lineGraph);
    return {
      colorMap: colorsById,
      replicasById: new Map(
        replicas.map((replica): [string, ColoredReplica] => [
          replica.id,
          {
            ...replica,
            color: colorsById.get(replica.id) ?? colors.lineGraph[0],
          },
        ]),
      ),
    };
  }, [cluster, graphData, colors.lineGraph]);
  const replicaIds = [...replicasById.keys()].sort();
  const currentReplicaIds = cluster.replicas
    .map((replica) => replica.id)
    .sort();

  const statusHistory = useReplicaStatusHistory(replicaIds);
  // Only a replica that came back online in the window can be mid-hydration
  // in a way the timeline shows, so the hydration queries skip the rest.
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
  const hydratingReplicaIds = useHydratingReplicaIds(restartedReplicaIds);

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
      return size && bytes !== undefined ? [[size, bytes] as const] : [];
    }),
  );

  const timelines = React.useMemo(() => {
    const timelinesById = new Map<string, ReplicaTimeline>();
    if (!history.data) return timelinesById;
    const { startDate, endDate } = history.data;
    for (const { id, data } of history.data.graphData) {
      const first = data.at(0);
      const last = data.at(-1);
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
          isHydratingNow: hydratingReplicaIds.data?.has(id),
          sampleRange:
            first && last
              ? { startMs: first.bucketStart, endMs: last.bucketEnd }
              : undefined,
        }),
      );
    }
    return timelinesById;
  }, [
    history.data,
    statusHistory.data,
    hydrationEpisodes.data,
    hydratingReplicaIds.data,
    replicasById,
  ]);

  return {
    history,
    replicasById,
    replicaIds,
    colorMap,
    timelines,
    ddlEvents,
    heapLimitBytesBySize,
    isDdlError: isObjectsError || creationTimes.isError,
    isStatusLoading: statusHistory.isLoading,
    isStatusError: statusHistory.isError,
    isHydrationError: hydrationEpisodes.isError || hydratingReplicaIds.isError,
  };
}
