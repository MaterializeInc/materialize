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
  createScopedSubscribeCollections,
  idleCollectionStatusAtom,
  SubscribeCollection,
} from "~/api/materialize/subscribeCollection";
import { useScopedSubscribeCollection } from "~/api/materialize/useScopedSubscribeCollection";
import { allObjectsCollection } from "~/store/allObjectsCollection";

import { LargestObject, TOP_OBJECT_COUNT } from "./memoryByObjectModel";

/** Each replica's streamed object sizes. */
export const sizesByReplica =
  createScopedSubscribeCollections<ReplicaObjectSize>({
    id: "replica-object-sizes",
    getKey: (row) => row.objectId,
  });

/**
 * The largest objects in `sizes`, named from `allObjectsCollection`. An object
 * missing from the catalog is an orphaned dataflow, named by its id.
 */
export function useLargestObjects(
  sizes: SubscribeCollection<ReplicaObjectSize> | undefined,
  heapLimit: number | null,
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
  const status = useAtomValue(sizes?.statusAtom ?? idleCollectionStatusAtom);
  const objectsStatus = useAtomValue(allObjectsCollection.statusAtom);
  const objects = (rows ?? []).map(({ size, object }): LargestObject => ({
    id: size.objectId,
    name: object?.name ?? size.objectId,
    size: size.size,
    // A replica that reports no heap limit has no share to show.
    memoryPercentage:
      size.size === null || !heapLimit ? null : (size.size / heapLimit) * 100,
    type: object?.objectType ?? null,
    schemaName: object?.schemaName ?? null,
    databaseName: object?.databaseName ?? null,
    dataflowId: null,
    dataflowName: null,
    isOrphanedDataflow: !object,
  }));
  return {
    objects,
    // Names come from the catalog, so without it every object would read as
    // an orphaned dataflow.
    snapshotComplete: status.snapshotComplete && objectsStatus.snapshotComplete,
    isRefreshing: status.isRefreshing,
    isError: Boolean(status.error) || Boolean(objectsStatus.error),
  };
}

/** The largest objects on a replica, streamed into a per-replica collection. */
export function useLiveLargestObjects({
  replicaId,
  heapLimit,
}: {
  replicaId: string;
  heapLimit: number | null;
}) {
  const subscribe = React.useMemo(
    () => buildReplicaObjectSizesSubscribe(replicaId),
    [replicaId],
  );
  const { collection, isScopeError } = useScopedSubscribeCollection(
    sizesByReplica,
    replicaId,
    subscribe,
  );
  const largest = useLargestObjects(collection, heapLimit);
  return { ...largest, isError: largest.isError || isScopeError };
}
