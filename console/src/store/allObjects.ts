// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtomValue } from "jotai";
import React from "react";

import {
  buildAllObjectsQuery,
  DatabaseObject,
} from "~/api/materialize/objects";
import {
  SubscribeRow,
  SubscribeState,
} from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useGlobalUpsertSubscribe,
} from "~/api/materialize/useSubscribe";

export const allObjects = atom<SubscribeState<DatabaseObject>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

const ALL_OBJECTS_SUBSCRIBE_OPTIONS = {
  atom: allObjects,
  subscribe: buildSubscribeQuery(buildAllObjectsQuery(), { upsertKey: "id" }),
  select: (row: SubscribeRow<DatabaseObject>) => row.data,
  upsertKey: (row: SubscribeRow<DatabaseObject>) => row.data.id,
};

export function useSubscribeToAllObjects() {
  useGlobalUpsertSubscribe(ALL_OBJECTS_SUBSCRIBE_OPTIONS);
}

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
