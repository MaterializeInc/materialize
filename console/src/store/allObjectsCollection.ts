// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLiveQuery } from "@tanstack/react-db";
import { useAtomValue } from "jotai";
import React from "react";

import {
  buildAllObjectsQuery,
  DatabaseObject,
} from "~/api/materialize/objects";
import { createSubscribeCollection } from "~/api/materialize/subscribeCollection";
import {
  buildSubscribeQuery,
  useGlobalSubscribeCollection,
} from "~/api/materialize/useSubscribe";

/**
 * TanStack DB-backed alternative to the `allObjects` jotai atom. Same upstream
 * SUBSCRIBE, but the upsert-reduced rows land in a collection so consumers can
 * run reactive live queries over them.
 */
export const allObjectsCollection = createSubscribeCollection<DatabaseObject>({
  id: "all-objects",
  getKey: (object) => object.id,
  persistKey: "mz-console:sync-engine:all-objects",
});

export function useSubscribeToAllObjectsCollection() {
  const subscribe = React.useMemo(() => {
    return buildSubscribeQuery(buildAllObjectsQuery(), { upsertKey: "id" });
  }, []);

  return useGlobalSubscribeCollection<DatabaseObject, DatabaseObject>({
    target: allObjectsCollection,
    subscribe,
    select: (row) => row.data,
    upsertKey: (row) => row.data.id,
  });
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
