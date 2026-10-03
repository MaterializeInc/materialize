// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { and, eq, inArray, useLiveQuery } from "@tanstack/react-db";
import { useAtomValue, useStore } from "jotai";
import React from "react";

import { DatabaseObject } from "~/api/materialize/objects";
import { createSubscribeCollection } from "~/api/materialize/subscribeCollection";
import { createAtomFedCollectionSession } from "~/api/materialize/subscribeSession";
import { allObjects } from "~/store/allObjects";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

/**
 * TanStack DB-backed view of the `allObjects` jotai atom, so consumers can run
 * reactive live queries over the objects set. Fed from the app-wide `allObjects`
 * SUBSCRIBE rather than its own, so it adds no catalog_server load.
 */
export const allObjectsCollection = createSubscribeCollection<DatabaseObject>({
  id: "all-objects",
  getKey: (object) => object.id,
  persistName: "all-objects",
});

/**
 * Bridges the app-wide `allObjects` subscribe (held open for the session by
 * AppInitializer) into `allObjectsCollection`, reusing that one SUBSCRIBE
 * instead of opening a second identical stream. No keep-alive subscriber is
 * needed: `useAllObjectsLive`'s live query keeps the collection active while the
 * tree is mounted, and sync re-seeds from the retained row set if it restarts.
 */
export function useSubscribeToAllObjectsCollection() {
  const store = useStore();
  React.useEffect(() => {
    const session = createAtomFedCollectionSession({
      store,
      sourceAtom: allObjects,
      target: allObjectsCollection,
      scopeAtom: syncEngineCacheScopeLoadableAtom,
    });
    return session.destroy;
  }, [store]);
}

/**
 * Drop-in replacement for `useAllObjects`, returning the same
 * `{ data, snapshotComplete, isError }` shape sourced from the collection.
 */
export function useAllObjectsLive() {
  const { data } = useLiveQuery((q) =>
    q.from({ objects: allObjectsCollection.collection }),
  );
  const status = useAtomValue(allObjectsCollection.statusAtom);
  return React.useMemo(
    () => ({
      data: data ?? [],
      snapshotComplete: status.snapshotComplete,
      error: status.error,
      isError: Boolean(status.error),
    }),
    [data, status],
  );
}

const NO_OBJECTS: DatabaseObject[] = [];

/**
 * A cluster's objects of the given types, as a live query over
 * `allObjectsCollection`. The result is maintained incrementally, so it keeps
 * its identity through DDL elsewhere in the environment, where filtering
 * `useAllObjects` gets a new array on every change. `objectTypes` must keep its
 * identity across renders, or the query is rebuilt each render.
 */
export function useClusterObjectsLive(
  clusterId: string,
  objectTypes: readonly string[],
) {
  const { data } = useLiveQuery(
    (q) =>
      q
        .from({ object: allObjectsCollection.collection })
        .where(({ object }) =>
          and(
            eq(object.clusterId, clusterId),
            inArray(object.objectType, [...objectTypes]),
          ),
        ),
    [clusterId, objectTypes],
  );
  const status = useAtomValue(allObjectsCollection.statusAtom);
  return {
    data: data ?? NO_OBJECTS,
    snapshotComplete: status.snapshotComplete,
    isError: Boolean(status.error),
  };
}
