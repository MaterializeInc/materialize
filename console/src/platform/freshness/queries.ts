// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSuspenseQuery } from "@tanstack/react-query";

import { buildQueryKeyPart } from "~/api/buildQueryKeySchema";
import { fetchHydrationCounts } from "~/api/materialize/freshness/hydrationCounts";

export interface HydrationCounts {
  hydratedReplicas: number;
  totalReplicas: number;
}

export const freshnessQueryKeys = {
  all: () => ["freshness"] as const,
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
  });
}
