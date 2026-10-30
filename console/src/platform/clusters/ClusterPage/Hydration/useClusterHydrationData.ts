// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import {
  REPLICA_HYDRATION_HISTORY_MIN_VERSION,
  replicaStatusesUpdatedAt,
  useClusterHydrationEpisodes,
  useClusterReplicaHistory,
  useEpisodeObjectHydrations,
  useObjectCreationTimes,
  useReplicaCpuPeak,
  useReplicaHeapLimits,
  useReplicaSizeAllocations,
  useReplicaStatusHistory,
} from "~/platform/clusters/queries";
import { useClusterObjectsLive } from "~/store/allObjectsCollection";
import { useEnvironmentGate } from "~/store/environments";

import {
  findUnfinishedHydrations,
  heapLimitBytes,
  Hydration,
  mergeHydrationReplicas,
  replicaBootTimes,
  transformHydrations,
  transformObjectHydrationSpans,
} from "./hydrationModel";
import { useLiveHydrationProgress } from "./useLiveHydration";

const COMPUTE_OBJECT_TYPES = ["index", "materialized-view"];

/** An object created on the cluster in the window, named from the catalog. */
export interface CreatedObject {
  id: string | null;
  name: string | undefined;
  occurredAt: Date;
}

/**
 * Loads and derives what the hydration tab shows for a cluster over the
 * window: its replicas, dropped ones included, their finished and unfinished
 * hydrations, and each current replica's live progress.
 */
export function useClusterHydrationData({
  cluster,
  timePeriodMinutes,
}: {
  cluster: Cluster;
  timePeriodMinutes: number;
}) {
  const hasHistory = useEnvironmentGate(REPLICA_HYDRATION_HISTORY_MIN_VERSION);
  const currentReplicaIds = cluster.replicas.map(({ id }) => id);
  const replicaHistory = useClusterReplicaHistory({
    clusterId: cluster.id,
    timePeriodMinutes,
    currentReplicaIds,
  });
  // Rounded down to the minute, so memos keyed on it recompute once a minute
  // as the polled history rerenders the tab, not on every render.
  const startMs =
    Math.floor(Date.now() / millisecondsInMinute) * millisecondsInMinute -
    timePeriodMinutes * millisecondsInMinute;
  const replicas = React.useMemo(
    () =>
      mergeHydrationReplicas(
        cluster.replicas,
        replicaHistory.data ?? [],
        startMs,
      ),
    [cluster.replicas, replicaHistory.data, startMs],
  );
  const statusHistory = useReplicaStatusHistory(
    replicas.map(({ id }) => id),
    replicaStatusesUpdatedAt(cluster.replicas),
  );
  const episodes = useClusterHydrationEpisodes({
    clusterId: cluster.id,
    timePeriodMinutes,
  });
  // A hydration is full when it covers every object the cluster had, so
  // classifying one needs the objects and when they were created.
  const {
    data: objects,
    snapshotComplete: hasObjects,
    isError: isObjectsError,
  } = useClusterObjectsLive(cluster.id, COMPUTE_OBJECT_TYPES);
  const creationTimes = useObjectCreationTimes({
    objectIds: objects.map(({ id }) => id).sort(),
    timePeriodMinutes,
  });
  // Undefined until the catalog and the creation times load. A cluster with no
  // objects has nothing to look up.
  const createdObjects = React.useMemo((): CreatedObject[] | undefined => {
    if (!hasObjects) return undefined;
    if (objects.length === 0) return [];
    const namesById = new Map(objects.map(({ id, name }) => [id, name]));
    return creationTimes.data?.map(({ id, occurredAt }) => ({
      id,
      name: id ? (namesById.get(id) ?? id) : undefined,
      occurredAt,
    }));
  }, [hasObjects, objects, creationTimes.data]);
  const progress = useLiveHydrationProgress({
    clusterId: cluster.id,
    replicaIds: currentReplicaIds,
  });
  const heapLimits = useReplicaHeapLimits(currentReplicaIds);
  const allocations = useReplicaSizeAllocations([
    ...new Set(
      replicas.flatMap(({ size }) => (size === undefined ? [] : [size])),
    ),
  ]);

  const derived = React.useMemo(() => {
    const transitions = statusHistory.data?.transitions ?? [];
    const bootTimes = replicaBootTimes(transitions, replicas);
    // Objects created after a time weren't on the cluster then. Ones created
    // before the window have no row, so they count throughout.
    // NOTE: dropped objects aren't counted, so after a drop a hydration of
    // only some objects can read as full.
    const objectCountAt = createdObjects
      ? (ms: number) =>
          objects.length -
          createdObjects.filter(({ occurredAt }) => occurredAt.getTime() > ms)
            .length
      : undefined;
    const allHydrations = transformHydrations(
      episodes.data ?? [],
      bootTimes,
      objectCountAt,
    );
    // Episodes with no objects only mark that a boot finished.
    const hydrations = allHydrations.filter(
      ({ objectCount }) => objectCount > 0,
    );
    const unfinished = findUnfinishedHydrations({
      transitions,
      oomKills: statusHistory.data?.oomKills ?? [],
      hydrations: allHydrations,
      replicas,
      bootTimes,
      objectCreatedAtMs: (createdObjects ?? []).map(({ occurredAt }) =>
        occurredAt.getTime(),
      ),
      startMs,
    });
    const heapLimitBytesById = new Map(
      replicas.flatMap((replica) => {
        const limit = heapLimitBytes(
          replica,
          heapLimits.data,
          allocations.data,
        );
        return limit === undefined ? [] : [[replica.id, limit]];
      }),
    );
    return { bootTimes, hydrations, unfinished, heapLimitBytesById, startMs };
  }, [
    statusHistory.data,
    objects,
    createdObjects,
    episodes.data,
    replicas,
    startMs,
    heapLimits.data,
    allocations.data,
  ]);

  return {
    /** Whether the environment records hydration history, null while unknown. */
    hasHistory,
    replicas,
    ...derived,
    progressByReplicaId: progress.progressByReplicaId,
    isProgressLoading: !progress.snapshotComplete,
    isLoading:
      replicaHistory.isLoading ||
      statusHistory.isLoading ||
      (hasHistory === true &&
        (episodes.isLoading ||
          (!createdObjects && !creationTimes.isError && !isObjectsError))),
    createdObjects,
    isError:
      replicaHistory.isError ||
      statusHistory.isError ||
      episodes.isError ||
      progress.isError,
    isCreatedObjectsError: creationTimes.isError || isObjectsError,
  };
}

/**
 * The objects of one finished hydration, in the order they started and named
 * from the catalog, and the replica's highest sampled CPU during it. Each loads
 * and fails on its own.
 */
export function useHydrationDetail({
  clusterId,
  hydration,
}: {
  clusterId: string;
  hydration: Hydration | undefined;
}) {
  const objectHydrations = useEpisodeObjectHydrations(hydration);
  const cpuPeak = useReplicaCpuPeak(
    hydration && {
      replicaId: hydration.replicaId,
      startDate: hydration.startedAt,
      endDate: hydration.finishedAt,
    },
  );
  const {
    data: objects,
    snapshotComplete: hasObjectNames,
    isError: isObjectNamesError,
  } = useClusterObjectsLive(clusterId, COMPUTE_OBJECT_TYPES);
  const spans = React.useMemo(() => {
    if (!hydration || !objectHydrations.data) return undefined;
    const namesById = new Map(objects.map(({ id, name }) => [id, name]));
    return transformObjectHydrationSpans(
      objectHydrations.data,
      hydration.startedAt,
    ).map((span) => ({
      ...span,
      name:
        (span.objectId ? namesById.get(span.objectId) : undefined) ??
        span.globalId,
    }));
  }, [hydration, objectHydrations.data, objects]);
  return {
    spans,
    peakCpuNanoCores: cpuPeak.data,
    // Without the catalog every object would be named by its id, so the spans
    // wait for it unless it failed.
    isSpansLoading:
      objectHydrations.isLoading || (!hasObjectNames && !isObjectNamesError),
    isSpansError: objectHydrations.isError,
    isCpuLoading: cpuPeak.isLoading,
    isCpuError: cpuPeak.isError,
  };
}
