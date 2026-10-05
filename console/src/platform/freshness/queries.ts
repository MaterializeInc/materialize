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
import { fetchHydrationCounts } from "~/api/materialize/freshness/hydrationCounts";

export interface HydrationCounts {
  hydratedReplicas: number;
  totalReplicas: number;
}

export const freshnessQueryKeys = {
  // Region-scoped, because object IDs repeat across regions: `u1` exists in
  // every one. A bare key would let a region switch serve the previous
  // region's counts for a cluster whose IDs happen to match.
  all: () => buildRegionQueryKey("freshness"),
  hydrationCounts: (objectIds: string[]) =>
    [
      ...freshnessQueryKeys.all(),
      // Sorted so the key tracks set membership rather than the order the
      // objects happened to arrive in.
      buildQueryKeyPart("hydrationCounts", {
        objectIds: [...objectIds].sort().join(","),
      }),
    ] as const,
};

/**
 * Replica hydration for the objects on screen, keyed by object ID.
 *
 * Its own query rather than a column on the lag history, because that builder
 * is shared with pages that never show hydration.
 *
 * The caller passes the objects it means to show, not the ones the lag query
 * returned. Reading them from that result would make this request wait for it,
 * since a suspending query stops the component before this line is reached.
 */
export function useFreshnessHydration(objectIds: string[]) {
  return useSuspenseQuery({
    queryKey: freshnessQueryKeys.hydrationCounts(objectIds),
    queryFn: async ({ queryKey, signal }) => {
      if (objectIds.length === 0) return new Map<string, HydrationCounts>();

      const { rows } = await fetchHydrationCounts({
        objectIds,
        queryKey,
        requestOptions: { signal },
      });

      return new Map<string, HydrationCounts>(
        rows.map((row) => [
          row.objectId,
          {
            hydratedReplicas: Number(row.hydratedReplicas),
            totalReplicas: Number(row.totalReplicas),
          },
        ]),
      );
    },
    // Matched to the lag query's cadence: refreshing the pills on a different
    // clock to the numbers beside them would show the two disagreeing.
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
}
