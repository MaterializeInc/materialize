// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { eq, useLiveQuery } from "@tanstack/react-db";
import { useAtomValue } from "jotai";
import React from "react";

import {
  buildLiveObjectHydrationsSubscribe,
  buildReplicaHydrationStatesSubscribe,
  LiveObjectHydration,
  ReplicaObjectHydrationState,
  replicaObjectHydrationStateKey,
} from "~/api/materialize/cluster/hydrationHistory";
import { createScopedSubscribeCollections } from "~/api/materialize/subscribeCollection";
import { useScopedSubscribeCollection } from "~/api/materialize/useScopedSubscribeCollection";
import { OBJECT_HYDRATION_TIMES_MIN_VERSION } from "~/platform/clusters/queries";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { useEnvironmentGate } from "~/store/environments";

/** Each cluster's per-object hydration states, across its replicas. */
export const hydrationStatesByCluster =
  createScopedSubscribeCollections<ReplicaObjectHydrationState>({
    id: "replica-hydration-states",
    getKey: replicaObjectHydrationStateKey,
  });

const LIVE_OBJECT_HYDRATIONS_SUBSCRIBE = buildLiveObjectHydrationsSubscribe();

/** Each replica's live per-object hydration. */
export const liveObjectHydrationsByReplica =
  createScopedSubscribeCollections<LiveObjectHydration>({
    id: "live-object-hydrations",
    getKey: (row) => row.globalId,
  });

export interface ReplicaHydrationProgress {
  objectCount: number;
  hydratedCount: number;
  /** Catalog ids of the objects not hydrated yet. */
  unhydratedObjectIds: string[];
}

/**
 * How many compute objects each of the cluster's current replicas maintains
 * and has hydrated, streamed while mounted.
 */
export function useLiveHydrationProgress({
  clusterId,
  replicaIds,
}: {
  clusterId: string;
  replicaIds: string[];
}) {
  // Key on content, not array identity. Ids contain no commas.
  const replicaIdsKey = [...replicaIds].sort().join(",");
  const subscribe = React.useMemo(
    () =>
      replicaIdsKey
        ? buildReplicaHydrationStatesSubscribe({
            replicaIds: replicaIdsKey.split(","),
          })
        : undefined,
    [replicaIdsKey],
  );
  const { collection, status, isScopeError } = useScopedSubscribeCollection(
    hydrationStatesByCluster,
    clusterId,
    subscribe,
  );
  const { data: rows } = useLiveQuery(
    (q) => (collection ? q.from({ state: collection.collection }) : undefined),
    [collection],
  );
  const progressByReplicaId = React.useMemo(() => {
    const progress = new Map<string, ReplicaHydrationProgress>(
      replicaIdsKey
        .split(",")
        .filter(Boolean)
        .map((id) => [
          id,
          { objectCount: 0, hydratedCount: 0, unhydratedObjectIds: [] },
        ]),
    );
    for (const { replicaId, objectId, isHydrated } of rows ?? []) {
      // The collection can briefly hold a replica no longer in the cluster.
      const counts = progress.get(replicaId);
      if (!counts) continue;
      counts.objectCount += 1;
      if (isHydrated) counts.hydratedCount += 1;
      else counts.unhydratedObjectIds.push(objectId);
    }
    return progress;
  }, [rows, replicaIdsKey]);
  return {
    progressByReplicaId,
    // With no replicas there's nothing to subscribe to, so nothing to wait for.
    snapshotComplete: !replicaIdsKey || status.snapshotComplete,
    isError: Boolean(status.error) || isScopeError,
  };
}

export type LiveObjectState = "queued" | "working" | "hydrated";

export interface LiveObject {
  globalId: string;
  /** The catalog name, or the dataflow's id for a dropped object. */
  name: string;
  objectType: string | null;
  state: LiveObjectState;
  installedAt: Date;
  startedAt: Date | null;
  hydratedAt: Date | null;
}

/**
 * Each compute object's hydration on one replica, named from
 * `allObjectsCollection` and streamed while mounted. The subscribe runs on
 * the replica itself, so mount this only while it's worth that cost, such as
 * while the replica is hydrating. Idle before the environment records the
 * per-worker hydration times.
 */
export function useLiveObjectHydrations({
  clusterName,
  replicaId,
  replicaName,
}: {
  clusterName: string;
  replicaId: string;
  replicaName: string;
}) {
  const hasHydrationTimes =
    useEnvironmentGate(OBJECT_HYDRATION_TIMES_MIN_VERSION) === true;
  const { collection, status, isScopeError } = useScopedSubscribeCollection(
    liveObjectHydrationsByReplica,
    replicaId,
    hasHydrationTimes ? LIVE_OBJECT_HYDRATIONS_SUBSCRIBE : undefined,
    { clusterName, clusterReplicaName: replicaName },
  );
  const { data: rows } = useLiveQuery(
    (q) =>
      collection
        ? q
            .from({ hydration: collection.collection })
            .leftJoin(
              { object: allObjectsCollection.collection },
              ({ hydration, object }) => eq(hydration.objectId, object.id),
            )
            .orderBy(({ hydration }) => hydration.installedAt)
            .orderBy(({ hydration }) => hydration.globalId)
        : undefined,
    [collection],
  );
  const objectsStatus = useAtomValue(allObjectsCollection.statusAtom);
  const objects = (rows ?? []).map(({ hydration, object }): LiveObject => ({
    globalId: hydration.globalId,
    name: object?.name ?? hydration.globalId,
    objectType: object?.objectType ?? null,
    state: hydration.hydratedAt
      ? "hydrated"
      : hydration.startedAt
        ? "working"
        : "queued",
    installedAt: hydration.installedAt,
    startedAt: hydration.startedAt,
    hydratedAt: hydration.hydratedAt,
  }));
  return {
    objects,
    // Names come from the catalog, so without it every object would read as
    // a dropped object's dataflow.
    snapshotComplete: status.snapshotComplete && objectsStatus.snapshotComplete,
    isError:
      Boolean(status.error) || Boolean(objectsStatus.error) || isScopeError,
  };
}
