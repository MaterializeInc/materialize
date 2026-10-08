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
  buildReplicaObjectSizesSubscribe,
  ReplicaObjectSize,
} from "~/api/materialize/cluster/largestMaintainedQueries";
import {
  createSubscribeCollection,
  SubscribeCollection,
} from "~/api/materialize/subscribeCollection";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { useGlobalSubscribeCollection } from "~/api/materialize/useSubscribe";
import { allObjectsCollection } from "~/store/allObjectsCollection";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

import { TOP_OBJECT_COUNT } from "./memoryByObjectModel";

const sizeKey = (row: ReplicaObjectSize) => row.objectId;

// A stand-in while the tenant scope resolves, so the session hook always has a
// target. Its subscribe is undefined then, so nothing is ever written.
const placeholderSizes = createSubscribeCollection<ReplicaObjectSize>({
  id: "replica-object-sizes|placeholder",
  getKey: sizeKey,
});

// Keyed by tenant scope and replica for the same reasons as the utilization
// collections in `useLiveClusterUtilization.ts`.
const sizesByScopedReplica = new Map<
  string,
  SubscribeCollection<ReplicaObjectSize>
>();

function sizesFor(scope: string, replicaId: string) {
  const key = `${scope}|${replicaId}`;
  let sizes = sizesByScopedReplica.get(key);
  if (!sizes) {
    sizes = createSubscribeCollection<ReplicaObjectSize>({
      id: `replica-object-sizes|${key}`,
      getKey: sizeKey,
    });
    sizesByScopedReplica.set(key, sizes);
  }
  return sizes;
}

/**
 * The largest objects in `sizes`, named from `allObjectsCollection`, in the
 * shape `useLargestMaintainedQueries` returns. An object missing from the
 * catalog is an orphaned dataflow, named by its id.
 */
export function useLargestObjects(
  sizes: SubscribeCollection<ReplicaObjectSize> | undefined,
  heapLimit: number,
) {
  const { data: rows } = useLiveQuery(
    (q) =>
      sizes
        ? q
            .from({ size: sizes.collection })
            .leftJoin(
              { object: allObjectsCollection.collection },
              ({ size, object }) => eq(size.objectId, object.id),
            )
            .orderBy(({ size }) => size.size, {
              direction: "desc",
              nulls: "last",
            })
            // Sizes are 10 MiB-quantized, so ties are common.
            .orderBy(({ size }) => size.objectId)
            .limit(TOP_OBJECT_COUNT)
        : undefined,
    [sizes],
  );
  const status = useAtomValue((sizes ?? placeholderSizes).statusAtom);
  const objects = React.useMemo(
    () =>
      (rows ?? []).map(({ size, object }) => ({
        id: size.objectId as string | null,
        name: object?.name ?? size.objectId,
        size: size.size,
        memoryPercentage:
          size.size === null ? null : (size.size / heapLimit) * 100,
        type: (object?.objectType ?? null) as "materialized-view" | "index",
        schemaName: object?.schemaName ?? null,
        databaseName: object?.databaseName ?? null,
        dataflowId: null as string | null,
        dataflowName: null as string | null,
        isOrphanedDataflow: !object,
      })),
    [rows, heapLimit],
  );
  return {
    objects,
    snapshotComplete: status.snapshotComplete,
    isError: Boolean(status.error),
  };
}

/**
 * The largest objects on a replica, streamed into a per-replica collection.
 * The rows stay in memory for the app session, so returning to the cluster
 * shows them straight away.
 */
export function useLiveLargestObjects({
  replicaId,
  heapLimit,
}: {
  replicaId: string | undefined;
  heapLimit: number;
}) {
  const scope = useAtomValue(syncEngineCacheScopeLoadableAtom);
  const sizes =
    scope.state === "hasData" && scope.data && replicaId
      ? sizesFor(scope.data, replicaId)
      : undefined;
  const options = React.useMemo(
    () => ({
      target: sizes ?? placeholderSizes,
      subscribe:
        sizes && replicaId
          ? buildReplicaObjectSizesSubscribe(replicaId)
          : undefined,
      select: (row: SubscribeRow<ReplicaObjectSize>) => row.data,
      upsertKey: (row: SubscribeRow<ReplicaObjectSize>) => row.data.objectId,
    }),
    [sizes, replicaId],
  );
  useGlobalSubscribeCollection(options);
  return useLargestObjects(sizes, heapLimit);
}
