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
  buildClustersQuery,
  Cluster,
} from "~/api/materialize/cluster/clusterList";
import { createCatalogStore } from "~/store/createCatalogStore";

const store = createCatalogStore<Cluster>({
  query: () =>
    buildClustersQuery({ queryOwnership: false, includeSystemObjects: true }),
  upsertKey: "id",
});

export const allClusters = store.atom;
export const useSubscribeToAllClusters = store.useSubscribe;

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
