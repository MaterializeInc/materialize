// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomValue } from "jotai";
import { RawBuilder } from "kysely";
import React from "react";

import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

import {
  idleCollectionStatusAtom,
  ScopedSubscribeCollections,
} from "./subscribeCollection";
import { SubscribeRow } from "./SubscribeManager";
import { useGlobalSubscribeCollection } from "./useSubscribe";

// NOTE: kept out of useSubscribe.ts. Importing ~/store/syncEngineCache there
// closes an import cycle, observed to break UsageRoutes.test.tsx.
/**
 * Runs `subscribe` into the collection for `key` under the current tenant
 * scope, for as long as the calling component is mounted. While the scope
 * resolves, `collection` is undefined and no subscribe runs.
 *
 * `subscribe` must be referentially stable, since a new builder restarts the
 * subscribe session. Its upsert key must identify a row as
 * `collections.getKey` does. `session` pins the session to a cluster, or to
 * one of its replicas, for subscribes that read introspection.
 */
export function useScopedSubscribeCollection<T extends object>(
  collections: ScopedSubscribeCollections<T>,
  key: string,
  subscribe: RawBuilder<T> | undefined,
  session?: { clusterName: string; clusterReplicaName?: string },
) {
  const scope = useAtomValue(syncEngineCacheScopeLoadableAtom);
  const collection =
    scope.state === "hasData" && scope.data
      ? collections.get(scope.data, key)
      : undefined;
  const clusterName = session?.clusterName;
  const clusterReplicaName = session?.clusterReplicaName;
  const options = React.useMemo(
    () => ({
      target: collection,
      subscribe,
      clusterName,
      clusterReplicaName,
      select: (row: SubscribeRow<T>) => row.data,
      upsertKey: (row: SubscribeRow<T>) => collections.getKey(row.data),
    }),
    [collections, collection, subscribe, clusterName, clusterReplicaName],
  );
  useGlobalSubscribeCollection(options);
  const status = useAtomValue(
    collection?.statusAtom ?? idleCollectionStatusAtom,
  );
  return { collection, status, isScopeError: scope.state === "hasError" };
}
