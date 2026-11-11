// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { greatest } from "d3";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import {
  REPLICA_HYDRATION_HISTORY_MIN_VERSION,
  replicaStatusesUpdatedAt,
  useClusterHydrationEpisodes,
  useClusterReplicaHistory,
  useDroppedObjectLifetimes,
  useEpisodeObjectHydrations,
  useObjectCreationTimes,
  useReplicaHeapLimits,
  useReplicaObjectHydrations,
  useReplicaPeakUsage,
  useReplicaSizeAllocations,
  useReplicaStatusHistory,
} from "~/platform/clusters/queries";
import { useClusterObjectsLive } from "~/store/allObjectsCollection";
import { useEnvironmentGate } from "~/store/environments";

import {
  findRestarts,
  findUnfinishedHydrations,
  heapLimitBytes,
  Hydration,
  latestFinishedEpisode,
  mergeHydrationReplicas,
  ObjectHydrationSpan,
  replicaBootTimes,
  restartsReadingAsFinished,
  transformHydrations,
  transformObjectHydrationSpans,
} from "./hydrationModel";
import { isUnrecordedEpisode, UnrecordedHydration } from "./hydrationView";
import { useLiveHydrationProgress } from "./useLiveHydration";

const COMPUTE_OBJECT_TYPES = ["index", "materialized-view"];

/** When an object on the cluster was created, named from the catalog. */
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
  // The current replicas' status loads at once. Dropped replicas are only
  // known after the replica history, and their status no longer changes, so
  // a query of their own keeps them from refetching the current replicas'.
  const currentStatus = useReplicaStatusHistory(
    currentReplicaIds,
    replicaStatusesUpdatedAt(cluster.replicas),
    // A resize or burst changes the current replicas, and the history's
    // restarts would otherwise drop out until the new set loads.
    { keepOverlapping: true },
  );
  const droppedStatus = useReplicaStatusHistory(
    replicas.filter(({ isCurrent }) => !isCurrent).map(({ id }) => id),
    undefined,
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
  // Not bound to the window: a replica hydrating an object created before it
  // is dated from that creation.
  const creationTimes = useObjectCreationTimes({
    objectIds: objects.map(({ id }) => id).sort(),
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
    const transitions = [
      ...(currentStatus.data?.transitions ?? []),
      ...(droppedStatus.data?.transitions ?? []),
    ];
    const bootTimes = replicaBootTimes(transitions, replicas);
    // Objects created after a time weren't on the cluster then. Ones with no
    // creation row count throughout.
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
    const restarts = findRestarts({
      transitions,
      oomKills: [
        ...(currentStatus.data?.oomKills ?? []),
        ...(droppedStatus.data?.oomKills ?? []),
      ],
      replicas,
      bootTimes,
    });
    const objectLifetimes = (createdObjects ?? []).map(({ occurredAt }) => ({
      createdAtMs: occurredAt.getTime(),
    }));
    // Without the history nothing finished is recorded, so every restart
    // would read as unfinished.
    const unfinishedByCurrentObjects =
      hasHistory === true
        ? findUnfinishedHydrations({
            restarts,
            hydrations: allHydrations,
            replicas,
            bootTimes,
            objectLifetimes,
            startMs,
          })
        : [];
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
    const allocationById = new Map(
      replicas.flatMap(({ id, size }) => {
        const allocation = size ? allocations.data?.get(size) : undefined;
        return allocation ? [[id, allocation]] : [];
      }),
    );
    return {
      bootTimes,
      allHydrations,
      hydrations,
      restarts,
      objectLifetimes,
      unfinishedByCurrentObjects,
      heapLimitBytesById,
      allocationById,
      startMs,
    };
  }, [
    hasHistory,
    currentStatus.data,
    droppedStatus.data,
    objects,
    createdObjects,
    episodes.data,
    replicas,
    startMs,
    heapLimits.data,
    allocations.data,
  ]);

  // A restart that reads as finished can have cut short the hydration of an
  // object dropped since, which the current objects don't show. Reading those
  // is a scan, so it waits for such a restart. A failed read changes nothing.
  const readingAsFinished =
    hasHistory === true && createdObjects
      ? restartsReadingAsFinished(
          derived.restarts,
          derived.unfinishedByCurrentObjects,
          startMs,
        )
      : [];
  const droppedObjects = useDroppedObjectLifetimes({
    clusterId: cluster.id,
    timePeriodMinutes,
    refetchKey:
      readingAsFinished.length > 0
        ? `${readingAsFinished.length}|${readingAsFinished.at(-1)?.occurredAt.getTime()}`
        : "",
  });
  const unfinished = React.useMemo(
    () =>
      hasHistory === true &&
      droppedObjects.data &&
      droppedObjects.data.length > 0
        ? findUnfinishedHydrations({
            restarts: derived.restarts,
            hydrations: derived.allHydrations,
            replicas,
            bootTimes: derived.bootTimes,
            objectLifetimes: [
              ...derived.objectLifetimes,
              ...droppedObjects.data.map(({ createdAt, droppedAt }) => ({
                createdAtMs: createdAt.getTime(),
                droppedAtMs: droppedAt.getTime(),
              })),
            ],
            startMs,
          })
        : derived.unfinishedByCurrentObjects,
    [hasHistory, derived, droppedObjects.data, replicas, startMs],
  );

  return {
    /** Whether the environment records hydration history, null while unknown. */
    hasHistory,
    replicas,
    bootTimes: derived.bootTimes,
    hydrations: derived.hydrations,
    unfinished,
    restarts: derived.restarts,
    heapLimitBytesById: derived.heapLimitBytesById,
    allocationById: derived.allocationById,
    startMs,
    progressByReplicaId: progress.progressByReplicaId,
    /** Live progress and the current replicas' status, which say where each stands. */
    isProgressLoading: !progress.snapshotComplete || currentStatus.isLoading,
    /**
     * The hydration history, whose restarts come from the current replicas'
     * status. Dropped replicas' restarts fill in later.
     */
    isHistoryLoading:
      hasHistory === null ||
      replicaHistory.isLoading ||
      currentStatus.isLoading ||
      (hasHistory === true &&
        (episodes.isLoading ||
          (!createdObjects && !creationTimes.isError && !isObjectsError))),
    /**
     * When the cluster's objects were created, undefined until known for the
     * objects it has now.
     */
    createdObjects: creationTimes.isPlaceholderData
      ? undefined
      : createdObjects,
    /** The cluster's indexes and materialized views, undefined until known. */
    objectCount: hasObjects ? objects.length : undefined,
    isProgressError: progress.isError || currentStatus.isError,
    isHistoryError:
      replicaHistory.isError ||
      currentStatus.isError ||
      droppedStatus.isError ||
      episodes.isError,
  };
}

/** Names each span from the catalog, or by its dataflow's id once dropped. */
const nameSpans = (
  spans: ObjectHydrationSpan[],
  objects: Array<{ id: string; name: string }>,
) => {
  const namesById = new Map(objects.map(({ id, name }) => [id, name]));
  return spans.map((span) => ({
    ...span,
    name:
      (span.objectId ? namesById.get(span.objectId) : undefined) ??
      span.globalId,
  }));
};

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
  const peakUsage = useReplicaPeakUsage(
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
  const spans = React.useMemo(
    () =>
      hydration && objectHydrations.data
        ? nameSpans(
            transformObjectHydrationSpans(
              objectHydrations.data,
              hydration.startedAt,
            ),
            objects,
          )
        : undefined,
    [hydration, objectHydrations.data, objects],
  );
  return {
    spans,
    peakCpuNanoCores: peakUsage.data?.peakCpuNanoCores,
    // Without the catalog every object would be named by its id, so the spans
    // wait for it unless it failed.
    isSpansLoading:
      objectHydrations.isLoading || (!hasObjectNames && !isObjectNamesError),
    isSpansError: objectHydrations.isError,
    isCpuLoading: peakUsage.isLoading,
    isCpuError: peakUsage.isError,
  };
}

/**
 * The last hydration a current replica finished that Materialize hasn't
 * recorded yet, read from the replicas that may have one, classified like a
 * recorded one, with its objects named from the catalog. Undefined while the
 * replicas are read, and when none confirms one or none can be read.
 */
export function useLastUnrecordedHydration({
  clusterId,
  clusterName,
  candidates,
  hydrations,
  bootTimes,
  clusterObjectCount,
}: {
  clusterId: string;
  clusterName: string;
  candidates: UnrecordedHydration[];
  /** The recorded hydrations, which a replica's episode may already be. */
  hydrations: Hydration[];
  bootTimes: Map<string, number[]>;
  /** The cluster's indexes and materialized views, undefined until known. */
  clusterObjectCount: number | undefined;
}) {
  const reads = useReplicaObjectHydrations(
    candidates.map(({ replica, since }) => ({
      clusterName,
      replicaId: replica.id,
      replicaName: replica.name,
      since,
    })),
  );
  const { data: objects } = useClusterObjectsLive(
    clusterId,
    COMPUTE_OBJECT_TYPES,
  );
  const confirmed = candidates.flatMap(({ replica, since }, index) => {
    const rows = reads[index];
    const episode = rows && latestFinishedEpisode(rows);
    const replicaHydrations = hydrations.filter(
      ({ replicaId }) => replicaId === replica.id,
    );
    return episode &&
      isUnrecordedEpisode({ episode, since, hydrations: replicaHydrations })
      ? [{ replica, episode }]
      : [];
  });
  const latest = greatest(confirmed, ({ episode }) =>
    episode.finishedAt.getTime(),
  );
  if (!latest) return undefined;
  const { replica, episode } = latest;
  const [hydration] = transformHydrations(
    [
      {
        replicaId: replica.id,
        startedAt: episode.startedAt,
        finishedAt: episode.finishedAt,
        objectCount: episode.objects.length,
        peakHeapBytes: null,
        peakMemoryBytes: null,
        peakDiskBytes: null,
      },
    ],
    bootTimes,
    clusterObjectCount === undefined ? undefined : () => clusterObjectCount,
  );
  return {
    replica,
    hydration,
    spans: nameSpans(
      transformObjectHydrationSpans(episode.objects, episode.startedAt),
      objects,
    ),
  };
}
