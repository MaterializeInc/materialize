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
import { useMemo } from "react";

import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";
import {
  AllNamespaceItem,
  buildAllNamespacesQuery,
} from "~/api/materialize/schemaList";
import { createSubscribeCollection } from "~/api/materialize/subscribeCollection";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { useGlobalSubscribeCollection } from "~/api/materialize/useSubscribe";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

/** Subset of AllNamespaceItem stored per row, matching the existing atom's select. */
export type NamespaceItem = Pick<
  AllNamespaceItem,
  "schemaId" | "schemaName" | "databaseId" | "databaseName"
>;

const namespaceKey = (item: Pick<NamespaceItem, "databaseId" | "schemaId">) =>
  JSON.stringify({ databaseId: item.databaseId, schemaId: item.schemaId });

/** Collection of database/schema namespace items backing the object explorer tree. */
export const allNamespacesCollection = createSubscribeCollection<NamespaceItem>(
  {
    id: "all-namespaces",
    getKey: namespaceKey,
    persistName: "all-namespaces",
  },
);

const ALL_NAMESPACES_SUBSCRIBE_OPTIONS = {
  target: allNamespacesCollection,
  scopeAtom: syncEngineCacheScopeLoadableAtom,
  subscribe: buildSubscribeQuery(buildAllNamespacesQuery(), {
    upsertKey: ["schemaId", "databaseId"],
  }),
  select: (row: SubscribeRow<AllNamespaceItem>): NamespaceItem => ({
    schemaId: row.data.schemaId,
    schemaName: row.data.schemaName,
    databaseId: row.data.databaseId,
    databaseName: row.data.databaseName,
  }),
  upsertKey: (row: SubscribeRow<AllNamespaceItem>) => namespaceKey(row.data),
};

export function useSubscribeToAllNamespacesCollection() {
  useGlobalSubscribeCollection(ALL_NAMESPACES_SUBSCRIBE_OPTIONS);
}

/** Returns the namespace items for the object explorer tree, from the collection. */
export function useAllNamespacesLive() {
  const { data } = useLiveQuery((q) =>
    q.from({ namespaces: allNamespacesCollection.collection }),
  );
  const status = useAtomValue(allNamespacesCollection.statusAtom);
  return useMemo(
    () => ({
      data: data ?? [],
      snapshotComplete: status.snapshotComplete,
      error: status.error,
      isError: Boolean(status.error),
    }),
    [data, status],
  );
}
