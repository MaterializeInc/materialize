// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtomValue } from "jotai";
import { useMemo } from "react";

import { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";
import {
  AllNamespaceItem,
  buildAllNamespacesQuery,
} from "~/api/materialize/schemaList";
import {
  SubscribeRow,
  SubscribeState,
} from "~/api/materialize/SubscribeManager";
import { useGlobalUpsertSubscribe } from "~/api/materialize/useSubscribe";

export const allNamespaceItems = atom<SubscribeState<AllNamespaceItem>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

export function useSubscribeToAllNamespaces() {
  const subscribe = useMemo(() => {
    return buildSubscribeQuery(buildAllNamespacesQuery(), {
      upsertKey: ["schemaId", "databaseId"],
    });
  }, []);

  return useGlobalUpsertSubscribe({
    atom: allNamespaceItems,
    subscribe,
    select: (row: SubscribeRow<AllNamespaceItem>) => ({
      schemaId: row.data.schemaId,
      schemaName: row.data.schemaName,
      databaseId: row.data.databaseId,
      databaseName: row.data.databaseName,
    }),
    upsertKey: (row: SubscribeRow<AllNamespaceItem>) =>
      JSON.stringify({
        databaseId: row.data.databaseId,
        schemaId: row.data.schemaId,
      }),
  });
}

export function useAllNamespaces() {
  const result = useAtomValue(allNamespaceItems);
  return useMemo(() => {
    return {
      ...result,
      data: result.data,
      isError: Boolean(result.error),
      snapshotComplete: result.snapshotComplete,
    };
  }, [result]);
}
