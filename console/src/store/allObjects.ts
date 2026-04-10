// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomValue } from "jotai";
import React from "react";

import {
  buildAllObjectsQuery,
  DatabaseObject,
} from "~/api/materialize/objects";
import { createCatalogStore } from "~/store/createCatalogStore";

const store = createCatalogStore<DatabaseObject>({
  query: buildAllObjectsQuery,
  upsertKey: "id",
});

export const allObjects = store.atom;
export const useSubscribeToAllObjects = store.useSubscribe;

export function useAllObjects() {
  const result = useAtomValue(allObjects);
  return React.useMemo(
    () => ({
      ...result,
      isError: Boolean(result.error),
    }),
    [result],
  );
}
