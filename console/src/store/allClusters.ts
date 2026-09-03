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
  buildClustersQuery,
  Cluster,
} from "~/api/materialize/cluster/clusterList";
import {
  SubscribeRow,
  SubscribeState,
} from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useGlobalUpsertSubscribe,
} from "~/api/materialize/useSubscribe";

export const allClusters = atom<SubscribeState<Cluster>>({
  data: [],
  error: undefined,
  snapshotComplete: false,
});

const ALL_CLUSTERS_SUBSCRIBE_OPTIONS = {
  atom: allClusters,
  subscribe: buildSubscribeQuery(
    buildClustersQuery({ queryOwnership: false, includeSystemObjects: true }),
    { upsertKey: "id" },
  ),
  select: (row: SubscribeRow<Cluster>) => row.data,
  upsertKey: (row: SubscribeRow<Cluster>) => row.data.id,
};

export function useSubscribeToAllClusters() {
  useGlobalUpsertSubscribe(ALL_CLUSTERS_SUBSCRIBE_OPTIONS);
}

export function useAllClusters() {
  const result = useAtomValue(allClusters);
  return React.useMemo(
    () => ({
      ...result,
      isError: Boolean(result.error),
      getClusterById: (clusterId: string) =>
        result.data.find((c) => c.id === clusterId),
    }),
    [result],
  );
}
