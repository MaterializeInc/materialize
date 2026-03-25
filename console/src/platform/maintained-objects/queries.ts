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
import {
  ClusterUtilizationRow,
  fetchClusterUtilization,
  fetchMaintainedObjectsList,
} from "~/api/materialize/maintained-objects/maintainedObjectsList";
import { sumPostgresIntervalMs } from "~/util";

const maintainedObjectsQueryKeys = {
  all: () => buildRegionQueryKey("maintainedObjects"),
  list: ({ lookbackMinutes }: { lookbackMinutes: number }) =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("list", { lookbackMinutes }),
    ] as const,
  clusterUtilization: () =>
    [
      ...maintainedObjectsQueryKeys.all(),
      buildQueryKeyPart("clusterUtilization"),
    ] as const,
};

/**
 * Fetches all maintained objects with their latest lag values.
 * Polls every 60s to match lag data update cadence.
 */
export function useMaintainedObjectsList({
  lookbackMinutes,
}: {
  lookbackMinutes: number;
}) {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.list({ lookbackMinutes }),
    queryFn: ({ queryKey, signal }) =>
      fetchMaintainedObjectsList({
        params: { lookbackMinutes },
        queryKey,
        requestOptions: { signal },
      }),
    refetchInterval: 60_000,
    staleTime: 30_000,
    select: (data) =>
      data.rows.map((row) => ({
        ...row,
        lagMs: row.lag ? sumPostgresIntervalMs(row.lag) : null,
      })),
  });
}

/**
 * Fetches current CPU and memory utilization per cluster.
 * Returns a Map keyed by clusterId for fast lookup.
 */
export function useClusterUtilization() {
  return useQuery({
    queryKey: maintainedObjectsQueryKeys.clusterUtilization(),
    queryFn: ({ queryKey, signal }) =>
      fetchClusterUtilization({
        queryKey,
        requestOptions: { signal },
      }),
    refetchInterval: 60_000,
    staleTime: 30_000,
    select: (data) => {
      const byClusterId = new Map<string, ClusterUtilizationRow>();
      for (const row of data.rows) {
        // If a cluster has multiple replicas, keep the first one.
        // Future enhancement: show per-replica or max utilization.
        if (!byClusterId.has(row.clusterId)) {
          byClusterId.set(row.clusterId, row);
        }
      }
      return byClusterId;
    },
  });
}
