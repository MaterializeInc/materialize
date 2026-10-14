// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { fetchHydrationCounts } from "~/api/materialize/freshness/hydrationCounts";
import {
  fetchLatestLag,
  LATEST_READING_INTERVAL_MS,
} from "~/api/materialize/freshness/lagHistory";
import { sumPostgresIntervalMs } from "~/util";

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
  // No lookback: the newest reading is the newest reading whatever range is
  // being graphed, so changing the range must not refetch it.
  latestLag: (objectIds: string[]) =>
    [
      ...freshnessQueryKeys.all(),
      buildQueryKeyPart("latestLag", {
        objectIds: [...objectIds].sort().join(","),
      }),
    ] as const,
};

/**
 * Each object's newest lag in milliseconds, keyed by object ID.
 *
 * Null where the newest reading reports the object as unreadable, which is not
 * the same as the object being absent from the map: that means no reading at
 * all.
 */
export function useLatestLag(objectIds: string[]) {
  return useQuery({
    queryKey: freshnessQueryKeys.latestLag(objectIds),
    queryFn: async ({ queryKey, signal }) => {
      if (objectIds.length === 0) return new Map<string, number | null>();

      const { rows } = await fetchLatestLag({
        objectIds,
        queryKey,
        requestOptions: { signal },
      });

      return new Map<string, number | null>(
        rows.map((row) => [
          row.objectId,
          row.lag === null ? null : sumPostgresIntervalMs(row.lag),
        ]),
      );
    },
    // A reading lands once a minute, so anything shorter re-asks a question
    // whose answer cannot have changed.
    staleTime: LATEST_READING_INTERVAL_MS,
    refetchInterval: LATEST_READING_INTERVAL_MS,
  });
}

/**
 * Replica hydration for the objects on screen, keyed by object ID.
 *
 * Its own query rather than a column on the lag history, because that builder
 * is shared with pages that never show hydration.
 *
 * The caller passes the objects it means to show, not the ones the lag query
 * returned. Reading them from that result would make this request wait for it.
 */
export function useFreshnessHydration(objectIds: string[]) {
  return useQuery({
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
