// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSuspenseQuery } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  ClusterReplicaMetricsParameters,
  fetchClusterReplicaMetrics,
} from "~/api/materialize/cluster/clusterReplicaMetrics";

export const queryKeys = {
  clusterMetrics: (params: ClusterReplicaMetricsParameters) =>
    [
      buildRegionQueryKey("cluster-metrics"),
      buildQueryKeyPart("metrics", params),
    ] as const,
};

export const clusterMetricsQueryKey = (
  params: ClusterReplicaMetricsParameters,
) =>
  [
    ...queryKeys.clusterMetrics(params),
    buildQueryKeyPart("cluster-metrics", params),
  ] as const;

export function useClusterReplicaMetrics(
  params: ClusterReplicaMetricsParameters,
) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: clusterMetricsQueryKey({ ...params }),
    queryFn: async ({ queryKey, signal }) => {
      const [, , queryKeyParameters] = queryKey;

      return fetchClusterReplicaMetrics({
        queryKey,
        parameters: queryKeyParameters,
        requestOptions: { signal },
      });
    },
  });
}
