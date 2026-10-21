// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  keepPreviousData,
  QueryFunctionContext,
  useMutation,
  useQuery,
  useQueryClient,
  useSuspenseQuery,
} from "@tanstack/react-query";
import { flatGroup, group, sort } from "d3";
import { subMinutes } from "date-fns";
import { millisecondsInMinute } from "date-fns/constants";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  formatFullyQualifiedObjectName,
  isSystemCluster,
  isSystemId,
} from "~/api/materialize";
import {
  alterCluster,
  AlterClusterNameParams,
  AlterClusterSettingsParams,
} from "~/api/materialize/cluster/alterCluster";
import {
  ArrangmentMemoryUsageParams,
  fetchArrangmentMemoryUsage,
} from "~/api/materialize/cluster/arrangementMemory";
import fetchAvailableClusterSizes from "~/api/materialize/cluster/availableClusterSizes";
import {
  expandClusterLineage,
  fetchClusterDeploymentLineage,
} from "~/api/materialize/cluster/clusterDeploymentLineage";
import {
  Cluster,
  ClusterListFilters,
  fetchClusters,
} from "~/api/materialize/cluster/clusterList";
import {
  fetchIndexesList,
  ListFilters,
} from "~/api/materialize/cluster/indexesList";
import {
  fetchLargestClusterReplica,
  LargestClusterReplicaParams,
} from "~/api/materialize/cluster/largestClusterReplica";
import {
  fetchLargestMaintainedObjectSizes,
  fetchLargestMaintainedQueries,
  fetchMaintainedObjectNames,
} from "~/api/materialize/cluster/largestMaintainedQueries";
import {
  fetchMaterializationLag,
  LagInfo,
  MaterializationLagParams,
} from "~/api/materialize/cluster/materializationLag";
import fetchMaxReplicasPerCluster from "~/api/materialize/cluster/maxReplicasPerCluster";
import { fetchObjectCreationTimes } from "~/api/materialize/cluster/objectCreationTimes";
import { fetchReplicaHeapLimits } from "~/api/materialize/cluster/replicaHeapLimits";
import { fetchReplicaHydration } from "~/api/materialize/cluster/replicaHydration";
import {
  fetchReplicaSizeAllocations,
  SizeAllocation,
} from "~/api/materialize/cluster/replicaProcessMetrics";
import {
  ClusterReplicasParams,
  fetchClusterReplicas,
} from "~/api/materialize/cluster/replicas";
import {
  fetchObjectHydrationTimes,
  fetchReplicaHydrationEpisodes,
  fetchReplicaStatusHistory,
  fetchUnhydratedComputeObjects,
} from "~/api/materialize/cluster/replicaStatusTimeline";
import {
  ClusterReplicasWithUtilizationParams,
  fetchClusterReplicasWithUtilization,
} from "~/api/materialize/cluster/replicasWithUtilization";
import {
  fetchReplicaUtilization,
  ReplicaUtilization,
} from "~/api/materialize/cluster/replicaUtilization";
import {
  attachOfflineEvents,
  BinnedSubscribeRow,
  bucketRowsToBucketsByReplicaId,
  parseBinnedSubscribeRow,
  rebucketUtilizationSamples,
  toReplicaUtilizationGraphData,
  UtilizationSample,
} from "~/api/materialize/cluster/replicaUtilizationBinning";
import {
  buildConsoleClusterUtilizationOverview24hSubscribe,
  buildConsoleClusterUtilizationUnbinned3hSubscribe,
  fetchReplicaOfflineEvents,
  fetchReplicaUtilizationHistory,
  ReplicaUtilizationHistoryParameters,
} from "~/api/materialize/cluster/replicaUtilizationHistory";
import {
  calculateBucketSizeFromLookback,
  fetchObjectLagHistory,
  LATEST_READING_INTERVAL_MS,
} from "~/api/materialize/freshness/lagHistory";
import { assertNoMoreThanOneRow } from "~/api/materialize/MoreThanOneRowError";
import { DatabaseObject } from "~/api/materialize/objects";
import { fetchOwners } from "~/api/materialize/owners";
import { useSubscribe } from "~/api/materialize/useSubscribe";
import { DataPoint, GraphLineSeries } from "~/components/FreshnessGraph/types";
import { roleQueryKeys } from "~/platform/roles/queries";
import { useAllObjects } from "~/store/allObjects";
import { useEnvironmentGate } from "~/store/environments";
import { notNullOrUndefined, sumPostgresIntervalMs } from "~/util";
import { sortLagInfo } from "~/utils/freshness";

type ReplicaUtilizationHistoryFilters = {
  clusterIds: ReplicaUtilizationHistoryParameters["clusterIds"];
  replicaId: ReplicaUtilizationHistoryParameters["replicaId"];
  timePeriodMinutes: number;
  bucketSizeMs: ReplicaUtilizationHistoryParameters["bucketSizeMs"];
  includeMemoryBreakdown?: ReplicaUtilizationHistoryParameters["includeMemoryBreakdown"];
};

type ClusterFreshnessParams = {
  lookbackMs: number;
  /** The objects to report on, already filtered by the caller. */
  objects: FreshnessObject[];
};

export const clusterQueryKeys = {
  /**
   *
   * Currently we fetch all clusters in the environment and use it to display the list view
   * but also to get information about a specific cluster. It's useful to have the full list since
   * we do routing validation that requires all the clusters that's faster client-side vs. making a round-trip request
   * to the server. If the number of clusters grow and we have, we'll need to change
   * our caching strategy to be more denormalized.
   */
  all: () => buildRegionQueryKey("clusters"),
  list: (filters?: ClusterListFilters) =>
    [...clusterQueryKeys.all(), buildQueryKeyPart("list", filters)] as const,
  alter: () => [...clusterQueryKeys.all(), buildQueryKeyPart("alter")] as const,
  indexesList: (filters?: ListFilters) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("indexesList", filters),
    ] as const,
  largestClusterReplica: (params: LargestClusterReplicaParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("largestClusterReplica", params),
    ] as const,
  largestMaintainedQueries: (
    params: UseLargestMaintainedQueriesParams & { unifiedSizes: boolean },
  ) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("largestMaintainedQueries", params),
    ] as const,
  availableClusterSizes: () =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("availableClusterSizes"),
    ] as const,
  maxReplicasPerCluster: () =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("maxReplicasPerCluster"),
    ] as const,
  replicas: (params: ClusterReplicasParams) =>
    [...clusterQueryKeys.all(), buildQueryKeyPart("replicas", params)] as const,
  replicasWithUtilization: (params: ClusterReplicasWithUtilizationParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicasWithUtilization", params),
    ] as const,
  arrangementMemory: (params: ArrangmentMemoryUsageParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("arrangementMemory", {
        ...params,
        replicaHeapLimit: params.replicaHeapLimit,
      }),
    ] as const,
  materializationLag: (params: MaterializationLagParams) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("materializationLag", params),
    ] as const,
  replicaUtilizationHistory: (params: ReplicaUtilizationHistoryFilters) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaUtilizationHistory", params),
    ] as const,
  deploymentLineage: (params: { clusterIdsKey: string }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("deploymentLineage", params),
    ] as const,
  replicaOfflineEvents: (params: {
    replicaIdsKey: string;
    timePeriodMinutes: number;
  }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaOfflineEvents", params),
    ] as const,
  // Sorted so these keys track set membership, not the order the objects
  // arrived in.
  clusterFreshnessSeries: (params: {
    lookbackMs: number;
    objectIds: string[];
  }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("clusterFreshnessSeries", {
        lookbackMs: params.lookbackMs,
        objectIds: [...params.objectIds].sort().join(","),
      }),
    ] as const,
  replicaUtilization: () =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaUtilization"),
    ] as const,
  replicaHydration: () =>
    [...clusterQueryKeys.all(), buildQueryKeyPart("replicaHydration")] as const,
  replicaStatusHistory: (params: {
    replicaIds: string[];
    statusesUpdatedAt: number | undefined;
  }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaStatusHistory", params),
    ] as const,
  replicaHydrationEpisodes: (params: {
    replicaIds: string[];
    timePeriodMinutes: number;
  }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaHydrationEpisodes", params),
    ] as const,
  unhydratedComputeObjects: (params: { replicaIds: string[] }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("unhydratedComputeObjects", params),
    ] as const,
  objectHydrationTimes: (params: {
    replicaIds: string[];
    objects: Array<{ id: string; createdAtMs: number }>;
  }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("objectHydrationTimes", params),
    ] as const,
  replicaHeapLimits: (params: { replicaIds: string[] }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaHeapLimits", params),
    ] as const,
  replicaSizeAllocations: (params: { sizes: string[] }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("replicaSizeAllocations", params),
    ] as const,
  objectCreationTimes: (params: {
    objectIds: string[];
    timePeriodMinutes: number;
  }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("objectCreationTimes", params),
    ] as const,
  maintainedObjectNames: (objectIds: string[]) =>
    [
      ...clusterQueryKeys.all(),
      // Sort so the key tracks set membership, not the size ranking.
      buildQueryKeyPart("maintainedObjectNames", {
        objectIds: [...objectIds].sort().join(","),
      }),
    ] as const,
};

export type ReplicaUtilizationMap = Map<string, ReplicaUtilization>;

const toUtilizationMap = ({
  rows,
}: Awaited<
  ReturnType<typeof fetchReplicaUtilization>
>): ReplicaUtilizationMap => new Map(rows.map((row) => [row.replicaId, row]));

/**
 * Last-hour peak utilization per replica, keyed by replica id.
 *
 * Polled rather than subscribed. Replica metrics only change on the
 * controller's scrape, so a subscribe would hold a dataflow open on
 * `mz_catalog_server` to deliver one update a minute, for every open tab and
 * every page, for as long as the session lasts.
 */
export function useReplicaUtilization() {
  return useQuery({
    refetchInterval: 30_000,
    queryKey: clusterQueryKeys.replicaUtilization(),
    queryFn: ({ queryKey, signal }) =>
      fetchReplicaUtilization({ queryKey, requestOptions: { signal } }),
    select: toUtilizationMap,
  });
}

/** Hydrated and total counted objects for one replica. */
export interface ReplicaHydrationCounts {
  hydratedObjects: number;
  totalObjects: number;
}

export type ReplicaHydrationMap = Map<string, ReplicaHydrationCounts>;

const toHydrationMap = ({
  rows,
}: Awaited<ReturnType<typeof fetchReplicaHydration>>): ReplicaHydrationMap => {
  const map: ReplicaHydrationMap = new Map();
  for (const row of rows) {
    // The query already drops rows naming no replica. Narrowing here as well
    // keeps `replica_id` nullable all the way through, so the guarantee is one
    // the compiler checks rather than one a cast asserts.
    if (row.replicaId === null) continue;

    map.set(row.replicaId, {
      // Aggregates arrive as bigints, which throw when mixed with the numbers
      // the column sorts and divides by.
      hydratedObjects: Number(row.hydratedObjects),
      totalObjects: Number(row.totalObjects),
    });
  }
  return map;
};

/**
 * Hydration counts per replica, keyed by replica id. A replica with no counted
 * objects is absent from the map rather than present with zeroes.
 *
 * Polled on the same cadence as utilization rather than riding along with
 * `useClusters`, whose five-second interval would scan the
 * `mz_hydration_statuses` arrangement far more often than hydration changes.
 */
export function useReplicaHydration() {
  return useQuery({
    refetchInterval: 30_000,
    queryKey: clusterQueryKeys.replicaHydration(),
    queryFn: ({ queryKey, signal }) =>
      fetchReplicaHydration({ queryKey, requestOptions: { signal } }),
    select: toHydrationMap,
  });
}

export function useClusters(filters?: ClusterListFilters) {
  const { data, refetch } = useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: clusterQueryKeys.list(filters),
    queryFn: ({ queryKey, signal }) => {
      const [, filtersKeyPart] = queryKey;
      return fetchClusters({
        queryKey,
        filters: filtersKeyPart,
        requestOptions: { signal },
      });
    },
    select: (result) => {
      return result.rows;
    },
  });

  const clusterMap = useMemo(() => {
    return new Map(data.map((cluster) => [cluster.id, cluster]));
  }, [data]);

  const getClusterById = useCallback(
    (clusterId: string) => {
      return clusterMap.get(clusterId);
    },
    [clusterMap],
  );

  return {
    data,
    refetch,
    getClusterById,
  };
}

// Declared at module scope so react-query's select memoization holds. An inline
// select is a new function every render, which makes react-query re-run it and
// hand back a fresh Map, which in turn breaks `isOwner`'s referential stability.
const selectOwnersById = (result: Awaited<ReturnType<typeof fetchOwners>>) =>
  new Map(result.rows.map((row) => [row.id, row.isOwner]));

/**
 * Returns `isOwner`, a predicate on an object's owner id, for deriving
 * ownership on rows from the allClusters subscribe.
 *
 * An unknown owner id and an in-flight query both resolve to false, so
 * owner-only controls stay hidden until ownership is known rather than
 * appearing and then disappearing.
 *
 * `isOwner` keeps a stable reference while the underlying data is unchanged, so
 * callers can safely put it in a dependency array.
 */
export function useOwners() {
  const { data: ownersById, isPending } = useQuery({
    // Role mutations invalidate this key, so this long interval is only a
    // backstop for external role changes while the page stays open.
    refetchInterval: 300_000,
    queryKey: roleQueryKeys.owners(),
    queryFn: ({ queryKey, signal }) => {
      return fetchOwners({ queryKey, requestOptions: { signal } });
    },
    select: selectOwnersById,
  });

  const isOwner = useCallback(
    (ownerId: string) => !isPending && (ownersById?.get(ownerId) ?? false),
    [isPending, ownersById],
  );

  return { isOwner };
}

export type AlterClusterParams = AlterClusterSettingsParams &
  AlterClusterNameParams;

export function useAlterCluster() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationKey: clusterQueryKeys.alter(),
    mutationFn: (params: AlterClusterParams) => {
      return alterCluster({
        nameParams: params,
        settingsParams: params,
        queryKey: clusterQueryKeys.alter(),
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: clusterQueryKeys.all(),
      });
    },
  });
}

export function useIndexesList(filters: ListFilters) {
  return useSuspenseQuery({
    refetchInterval: 5000,
    queryKey: clusterQueryKeys.indexesList(filters),
    queryFn: ({ queryKey, signal }) => {
      const [, filtersKeyPart] = queryKey;
      return fetchIndexesList({
        queryKey,
        filters: filtersKeyPart,
        requestOptions: { signal },
      });
    },
  });
}

export function useLargestClusterReplica(params: LargestClusterReplicaParams) {
  return useSuspenseQuery({
    refetchInterval: 60_000,
    queryKey: clusterQueryKeys.largestClusterReplica(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, paramsFromKey] = queryKey;
      const result = await fetchLargestClusterReplica({
        queryKey,
        params: paramsFromKey,
        requestOptions: { signal },
      });
      assertNoMoreThanOneRow(result.rows.length, { skipQueryRetry: true });
      return result.rows.at(0) ?? null;
    },
  });
}

const STRIP_DATAFLOW_PREFIX = /^Dataflow: /;

export type UseLargestMaintainedQueriesParams = {
  clusterId: string;
  clusterName: string;
  limit?: number;
  replicaName: string | undefined;
  replicaHeapLimit: number | undefined;
};
export function useLargestMaintainedQueries(
  params: UseLargestMaintainedQueriesParams,
) {
  // mz_object_arrangement_sizes reports sizes reliably from v26.35, where
  // they no longer go stale across replica restarts.
  const unifiedSizes = useEnvironmentGate("26.35.0-dev") ?? false;
  const queryClient = useQueryClient();
  // queryClient is a stable singleton, not query input.
  // eslint-disable-next-line @tanstack/query/exhaustive-deps
  return useSuspenseQuery({
    refetchInterval: 60_000,
    queryKey: clusterQueryKeys.largestMaintainedQueries({
      ...params,
      unifiedSizes,
    }),
    queryFn: async ({ queryKey, signal }) => {
      const [, paramsFromKey] = queryKey;
      if (
        paramsFromKey.replicaHeapLimit === null ||
        paramsFromKey.replicaHeapLimit === undefined ||
        !paramsFromKey.replicaName
      )
        return null;

      if (paramsFromKey.unifiedSizes) {
        const sizes = await fetchLargestMaintainedObjectSizes({
          queryKey,
          params: {
            clusterId: paramsFromKey.clusterId,
            replicaName: paramsFromKey.replicaName,
            replicaHeapLimit: paramsFromKey.replicaHeapLimit,
            limit: paramsFromKey.limit ?? 10,
          },
          requestOptions: { signal },
        });
        const objectIds = sizes.rows.map((row) => row.object_id);
        // Names are stable, so serve them from the query cache keyed by the
        // id set: the lookup only refires when the top-N membership changes.
        const namesById = objectIds.length
          ? await queryClient
              .fetchQuery({
                queryKey: clusterQueryKeys.maintainedObjectNames(objectIds),
                staleTime: 5 * 60_000,
                queryFn: ({ signal: namesSignal }) =>
                  fetchMaintainedObjectNames({
                    objectIds,
                    queryKey: clusterQueryKeys.maintainedObjectNames(objectIds),
                    requestOptions: { signal: namesSignal },
                  }),
              })
              .then((names) => new Map(names.rows.map((row) => [row.id, row])))
          : undefined;
        return {
          ...sizes,
          rows: sizes.rows.map((row) => {
            const named = namesById?.get(row.object_id);
            return {
              id: row.object_id as string | null,
              name: named?.name ?? null,
              size: row.size,
              memoryPercentage: row.memoryPercentage,
              type: (named?.type ?? null) as "materialized-view" | "index",
              schemaName: named?.schemaName ?? null,
              databaseName: named?.databaseName ?? null,
              dataflowId: null as string | null,
              dataflowName: null as string | null,
            };
          }),
        };
      }
      return fetchLargestMaintainedQueries({
        queryKey,
        params: {
          ...paramsFromKey,
          replicaHeapLimit: paramsFromKey.replicaHeapLimit,
          replicaName: paramsFromKey.replicaName,
          limit: paramsFromKey.limit ?? 10,
        },
        requestOptions: { signal },
      });
    },
    select: (data) => {
      return data?.rows.map((row) => {
        // If you drop an index used by a materialization, the dataflow stays around, but
        // we can no longer look it up in mz_objects.
        const isOrphanedDataflow =
          !row.id || !row.name || !row.databaseName || !row.schemaName;

        let databaseName = row.databaseName,
          schemaName = row.schemaName,
          name = row.name;

        if (isOrphanedDataflow) {
          const fullyQualifiedName = (row.dataflowName ?? "")
            .replace(STRIP_DATAFLOW_PREFIX, "")
            .split(".");
          if (fullyQualifiedName.length === 3) {
            [databaseName, schemaName, name] = fullyQualifiedName;
          } else if (!name) {
            // Unified rows carry no dataflow name, fall back to the object id.
            name = row.id;
          }
        }

        return {
          ...row,
          isOrphanedDataflow,
          databaseName: databaseName,
          schemaName: schemaName,
          name: name,
        };
      });
    },
  });
}

/** Query options for the replica sizes the environment offers. */
export const availableClusterSizesQuery = () => ({
  queryKey: clusterQueryKeys.availableClusterSizes(),
  queryFn: ({ queryKey, signal }: QueryFunctionContext) => {
    return fetchAvailableClusterSizes({
      queryKey,
      requestOptions: { signal },
    });
  },
});

export function useMaxReplicasPerCluster() {
  return useQuery({
    queryKey: clusterQueryKeys.maxReplicasPerCluster(),
    queryFn: ({ queryKey, signal }) => {
      return fetchMaxReplicasPerCluster({
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export function useReplicasBySize(params: ClusterReplicasParams) {
  return useQuery({
    queryKey: clusterQueryKeys.replicas({ ...params }),
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchClusterReplicas(queryKeyParams, queryKey, { signal });
    },
  });
}

export function useClusterReplicasWithUtilization(
  params: ClusterReplicasWithUtilizationParams,
) {
  const select = useCallback(
    ({
      rows,
    }: Awaited<ReturnType<typeof fetchClusterReplicasWithUtilization>>) => {
      return rows.map((row) => ({
        ...row,
        isOwner: Boolean(
          !isSystemCluster(params.clusterId) && !row.managed && row.isOwner,
        ),
      }));
    },
    [params.clusterId],
  );

  return useSuspenseQuery({
    queryKey: clusterQueryKeys.replicasWithUtilization(params),
    refetchInterval: 5000,
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchClusterReplicasWithUtilization(queryKeyParams, queryKey, {
        signal,
      });
    },
    select,
  });
}

/**
 * Returns a map of arrangment ID to memory usage as a percentage.
 *
 * Because this uses mz_compute_exports, it must run on the replica we want data from.
 */
export function useArrangmentsMemory(params: ArrangmentMemoryUsageParams) {
  return useQuery({
    refetchInterval: 5000,
    queryKey: clusterQueryKeys.arrangementMemory(params),
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      const response = await fetchArrangmentMemoryUsage({
        params: queryKeyParams,
        queryKey,
        requestOptions: { signal },
      });
      if (!response) return null;

      return {
        ...response,
        memoryUsageById: new Map(
          response.rows?.map(({ id, size, memoryPercentage }) => [
            id,
            { size, memoryPercentage },
          ]),
        ),
      };
    },
  });
}

export type ArrangmentsMemoryUsageMap = NonNullable<
  ReturnType<typeof useArrangmentsMemory>["data"]
>["memoryUsageById"];

export type LagMap = Map<string, LagInfo>;

/**
 * Fetches a normalized table of an object, the lag between its direct parent, and
 * the lag between its source/table objects
 */
export function useMaterializationLag(params: MaterializationLagParams) {
  return useQuery({
    queryKey: clusterQueryKeys.materializationLag(params),
    queryFn: ({ queryKey, signal }) => {
      return fetchMaterializationLag(params, queryKey, { signal });
    },
    select: (lagData) => {
      const lagMap: LagMap = new Map();
      for (const r of lagData?.rows ?? []) {
        if (r.targetObjectId) {
          lagMap.set(r.targetObjectId, {
            hydrated: r.hydrated,
            lag: r.lag,
            isOutdated: r.isOutdated,
          });
        }
      }

      return {
        lagMap,
      };
    },
  });
}

// Window tiers, bounded by the maintained view that serves each. Up to 24h is a
// live SUBSCRIBE (push). Beyond that we poll. Bounds are the views' retentions.
const SUBSCRIBE_UNBINNED_MAX_MINUTES = 180; // 3h: live un-binned base, client-binned
const SUBSCRIBE_BINNED_MAX_MINUTES = 1440; // 24h: live 5-min binned view
const OVERVIEW_MAX_MINUTES = 20160; // 14d: polled overview view; beyond, ad-hoc

// A blue-green cutover changes a cluster's lineage. Refetching on this interval
// bounds how long that takes to reach an open chart.
const DEPLOYMENT_LINEAGE_REFRESH_MS = 5 * 60_000;

/**
 * Expands `clusterIds` to every cluster in their blue-green lineage with one
 * peek, for the utilization subscribes to filter on as literal ids. A lineage
 * subquery inside the SUBSCRIBE instead plans as a full scan of the view's
 * index joined with `mz_cluster_deployment_lineage`, a dataflow kept alive per
 * open chart. Literal ids plan as a lookup on the view's `cluster_id` index.
 */
export function useClusterLineageIds(
  clusterIds: string[] | undefined,
  enabled: boolean,
) {
  // Key on content, not array identity (the caller passes a fresh array each
  // render). Ids contain no commas, so the comma join/split round-trips them.
  const clusterIdsKey = (clusterIds ?? []).join(",");
  return useQuery({
    queryKey: clusterQueryKeys.deploymentLineage({ clusterIdsKey }),
    enabled: enabled && clusterIdsKey.length > 0,
    staleTime: DEPLOYMENT_LINEAGE_REFRESH_MS,
    refetchInterval: DEPLOYMENT_LINEAGE_REFRESH_MS,
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      const ids = queryKeyParams.clusterIdsKey.split(",");
      const { pastDeploymentsByCurrentDeployment } =
        await fetchClusterDeploymentLineage({
          params: { clusterIds: ids },
          queryKey,
          requestOptions: { signal },
        });
      return expandClusterLineage(ids, pastDeploymentsByCurrentDeployment);
    },
  });
}

/**
 * Polls offline events for the replicas present in `samples`, the only
 * replicas `attachOfflineEvents` can attach events to. Idles while `samples`
 * is empty.
 */
export function useReplicaOfflineEvents(
  samples: { replicaId: string }[],
  timePeriodMinutes: number,
  enabled: boolean,
) {
  // Sorted so the key tracks set membership, not sample order.
  const replicaIdsKey = useMemo(
    () => [...new Set(samples.map((s) => s.replicaId))].sort().join(","),
    [samples],
  );
  const { data } = useQuery({
    queryKey: clusterQueryKeys.replicaOfflineEvents({
      replicaIdsKey,
      timePeriodMinutes,
    }),
    refetchInterval: 20_000,
    enabled: enabled && replicaIdsKey.length > 0,
    // A changed replica set is a new key with no cached events. Hold the
    // previous ones until it loads so the markers don't blink out.
    placeholderData: keepPreviousData,
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      const startDate = subMinutes(
        new Date(),
        queryKeyParams.timePeriodMinutes,
      ).toISOString();
      return fetchReplicaOfflineEvents({
        params: {
          replicaIds: queryKeyParams.replicaIdsKey.split(","),
          startDate,
        },
        queryKey,
        requestOptions: { signal },
      });
    },
  });
  return data;
}

// TODO: remove these gates once all environments are past them.
const UTILIZATION_INDEXED_VIEWS_MIN_VERSION = "26.32.0";
const REPLICA_MEMORY_BREAKDOWN_MIN_VERSION = "26.44.0";
const REPLICA_HYDRATION_HISTORY_MIN_VERSION = "26.43.0";

/**
 * Which source serves a utilization window: a live SUBSCRIBE to the un-binned
 * 3h view, a live SUBSCRIBE to the binned 24h view, or a poll.
 */
export function useUtilizationTier(timePeriodMinutes: number) {
  // The un-binned 3h and 24h indexed views (and their SUBSCRIBEs) only exist on
  // mz >= 26.32. On older environments (e.g. mid-rollout) these paths are gated
  // off and everything falls back to the poll. The 14d `overview` view predates
  // this, so its poll path is not gated.
  const hasIndexedViews =
    useEnvironmentGate(UTILIZATION_INDEXED_VIEWS_MIN_VERSION) === true;
  if (!hasIndexedViews || timePeriodMinutes > SUBSCRIBE_BINNED_MAX_MINUTES) {
    return "poll" as const;
  }
  return timePeriodMinutes <= SUBSCRIBE_UNBINNED_MAX_MINUTES
    ? ("unbinned3h" as const)
    : ("binned24h" as const);
}

/**
 * SUBSCRIBE variant for the live (≤3h) window: streams the un-binned 3h base
 * (lineage resolved by `useClusterLineageIds`), bins client-side, shapes like
 * the poll path. Subscribes by cluster (not replica) so the socket survives the
 * replica dropdown.
 *
 * NOTE: when `enabled` is false the subscribe is undefined, so the socket opens
 * but sends no query: an idle connection, not catalog-server load.
 */
function useReplicaUtilizationHistorySubscribe(
  params: ReplicaUtilizationHistoryFilters,
  enabled: boolean,
  includeOfflineEvents: boolean,
) {
  const { replicaId, timePeriodMinutes, bucketSizeMs, includeMemoryBreakdown } =
    params;
  const lineage = useClusterLineageIds(params.clusterIds, enabled);
  const lineageIdsKey = lineage.data?.join(",") ?? "";

  const subscribe = useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    if (!enabled || clusterIds.length === 0) {
      return undefined;
    }
    // The frontier only needs to reach back to the window start; the view itself
    // retains the last 3h.
    const minDate = subMinutes(new Date(), timePeriodMinutes);
    return buildConsoleClusterUtilizationUnbinned3hSubscribe<UtilizationSample>(
      clusterIds,
      minDate,
      includeMemoryBreakdown,
    );
  }, [enabled, lineageIdsKey, timePeriodMinutes, includeMemoryBreakdown]);

  const { data, isError, snapshotComplete, resubscribing } = useSubscribe({
    subscribe,
    upsertKey: (row) => `${row.data.replicaId} ${row.data.occurredAt}`,
    select: (row) => ({
      ...row.data,
      occurredAt: new Date(row.data.occurredAt),
    }),
  });

  // Offline events aren't in the un-binned view, so poll them separately and
  // merge into the client-binned buckets. Without this the <=3h windows would
  // hide replica crashes and OOMs that every other tier surfaces.
  const offlineEvents = useReplicaOfflineEvents(
    data,
    timePeriodMinutes,
    enabled && includeOfflineEvents,
  );

  const result = useMemo(() => {
    const endDate = new Date();
    const startDate = subMinutes(endDate, timePeriodMinutes);

    const samples = replicaId
      ? data.filter((sample) => sample.replicaId === replicaId)
      : data;
    const rows = attachOfflineEvents(
      rebucketUtilizationSamples(samples, bucketSizeMs, startDate.getTime()),
      offlineEvents ?? [],
      bucketSizeMs,
    );
    return toReplicaUtilizationGraphData(
      bucketRowsToBucketsByReplicaId(rows),
      startDate,
      endDate,
    );
  }, [data, replicaId, timePeriodMinutes, bucketSizeMs, offlineEvents]);

  return {
    data: result,
    isLoading: enabled && !snapshotComplete,
    isRefreshing: enabled && resubscribing,
    isError: isError || lineage.isLoadingError,
  };
}

/**
 * SUBSCRIBE variant for the 3h-24h window: streams the server-binned 24h view
 * (lineage resolved by `useClusterLineageIds`). The rows are already binned, so
 * they feed `bucketRowsToBucketsByReplicaId` directly with no client-side
 * rebinning. ENVELOPE UPSERT yields an unordered keyed set, so we sort by
 * bucket start.
 */
function useReplicaUtilizationHistoryBinnedSubscribe(
  params: ReplicaUtilizationHistoryFilters,
  enabled: boolean,
) {
  const { replicaId, timePeriodMinutes, includeMemoryBreakdown } = params;
  const lineage = useClusterLineageIds(params.clusterIds, enabled);
  const lineageIdsKey = lineage.data?.join(",") ?? "";

  const subscribe = useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    if (!enabled || clusterIds.length === 0) {
      return undefined;
    }
    const minDate = subMinutes(new Date(), timePeriodMinutes);
    return buildConsoleClusterUtilizationOverview24hSubscribe<BinnedSubscribeRow>(
      clusterIds,
      minDate,
      includeMemoryBreakdown,
    );
  }, [enabled, lineageIdsKey, timePeriodMinutes, includeMemoryBreakdown]);

  const { data, isError, snapshotComplete, resubscribing } = useSubscribe({
    subscribe,
    upsertKey: (row) => `${row.data.replicaId} ${row.data.bucketStart}`,
    select: (row) => parseBinnedSubscribeRow(row.data),
  });

  const result = useMemo(() => {
    const endDate = new Date();
    const startDate = subMinutes(endDate, timePeriodMinutes);

    // Clip to the window like the SQL does. Held rows from a previous wider
    // window would otherwise stretch the chart domain past the selected range.
    const rows = data.filter(
      (row) =>
        row.bucketStart.getTime() >= startDate.getTime() &&
        (!replicaId || row.replicaId === replicaId),
    );
    // ENVELOPE UPSERT yields an unordered keyed set; the chart needs time order.
    rows.sort((a, b) => a.bucketStart.getTime() - b.bucketStart.getTime());

    return toReplicaUtilizationGraphData(
      bucketRowsToBucketsByReplicaId(rows),
      startDate,
      endDate,
    );
  }, [data, replicaId, timePeriodMinutes]);

  return {
    data: result,
    isLoading: enabled && !snapshotComplete,
    isRefreshing: enabled && resubscribing,
    isError: isError || lineage.isLoadingError,
  };
}

export function useReplicaUtilizationHistory(
  params: ReplicaUtilizationHistoryFilters,
  queryOptions?: {
    enabled?: boolean;
    // `false` skips the <=3h windows' offline-events poll, for callers that
    // read replica status from elsewhere. Their buckets carry no events then.
    includeOfflineEvents?: boolean;
  },
) {
  const enabled = queryOptions?.enabled ?? true;
  const tier = useUtilizationTier(params.timePeriodMinutes);
  const hasMemoryBreakdown =
    useEnvironmentGate(REPLICA_MEMORY_BREAKDOWN_MIN_VERSION) === true;
  const filters = {
    ...params,
    // `false` would change the query key for callers that don't pass this.
    includeMemoryBreakdown:
      (params.includeMemoryBreakdown && hasMemoryBreakdown) || undefined,
  };

  const useUnbinnedSubscribe = tier === "unbinned3h";
  const useBinnedSubscribe = tier === "binned24h";

  const unbinnedResult = useReplicaUtilizationHistorySubscribe(
    filters,
    enabled && useUnbinnedSubscribe,
    queryOptions?.includeOfflineEvents ?? true,
  );
  const binnedResult = useReplicaUtilizationHistoryBinnedSubscribe(
    filters,
    enabled && useBinnedSubscribe,
  );

  const queryResult = useQuery({
    queryKey: clusterQueryKeys.replicaUtilizationHistory(filters),
    refetchInterval: 20_000,
    // Poll whatever a subscribe isn't serving: >24h on new mz, and every window
    // on old mz (where the subscribes are gated off).
    enabled: enabled && !useUnbinnedSubscribe && !useBinnedSubscribe,
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;

      const endDate = new Date();
      const startDate = subMinutes(endDate, queryKeyParams.timePeriodMinutes);

      // The 14d `overview` view serves 24h..14d. Beyond 14d, and on old mz the
      // <=24h windows a subscribe would cover, use the ad-hoc whole-fleet query.
      const data = await fetchReplicaUtilizationHistory({
        params: {
          ...queryKeyParams,
          startDate: startDate.toISOString(),
          shouldUseConsoleClusterUtilizationOverviewView:
            queryKeyParams.timePeriodMinutes > SUBSCRIBE_BINNED_MAX_MINUTES &&
            queryKeyParams.timePeriodMinutes <= OVERVIEW_MAX_MINUTES,
        },
        queryKey,
        requestOptions: { signal },
      });

      return toReplicaUtilizationGraphData(data, startDate, endDate);
    },
  });

  const active = useUnbinnedSubscribe
    ? unbinnedResult
    : useBinnedSubscribe
      ? binnedResult
      : queryResult;
  return {
    data: active.data,
    isLoading: active.isLoading,
    isError: active.isError,
    isRefreshing: useUnbinnedSubscribe
      ? unbinnedResult.isRefreshing
      : useBinnedSubscribe
        ? binnedResult.isRefreshing
        : false,
  };
}

/**
 * When any of the replicas' processes last changed status, in ms. It covers
 * system clusters, unlike a cluster's `latestStatusUpdate`, which comes from
 * `mz_cluster_replica_history` and so is always null for them.
 */
export function replicaStatusesUpdatedAt(replicas: Cluster["replicas"]) {
  const times = replicas.flatMap(({ statuses }) =>
    statuses.map(({ updated_at }) => new Date(updated_at).getTime()),
  );
  return times.length > 0 ? Math.max(...times) : undefined;
}

/**
 * Status transitions and OOM kills of the given replicas, fetched again only
 * when `statusesUpdatedAt` changes. Pass `replicaStatusesUpdatedAt` of the
 * cluster's replicas. Idle while `replicaIds` is empty.
 */
export function useReplicaStatusHistory(
  replicaIds: string[],
  statusesUpdatedAt: number | undefined,
) {
  return useQuery({
    queryKey: clusterQueryKeys.replicaStatusHistory({
      replicaIds,
      statusesUpdatedAt,
    }),
    enabled: replicaIds.length > 0,
    staleTime: Infinity,
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchReplicaStatusHistory({
        params: { replicaIds: queryKeyParams.replicaIds },
        queryKey,
        requestOptions: { signal },
      });
    },
    // A new transition changes the key. The same replicas' history stays up
    // meanwhile, but never other replicas'. After `queryFn`, so the data type
    // is inferred from it.
    placeholderData: (previous, previousQuery) =>
      previousQuery?.queryKey[1].replicaIds.join(",") === replicaIds.join(",")
        ? previous
        : undefined,
  });
}

/**
 * Hydration episodes of the given replicas over the window. Idle while
 * `replicaIds` is empty or the environment predates the history.
 */
export function useReplicaHydrationEpisodes(params: {
  replicaIds: string[];
  timePeriodMinutes: number;
}) {
  const hasHydrationHistory =
    useEnvironmentGate(REPLICA_HYDRATION_HISTORY_MIN_VERSION) === true;
  return useQuery({
    queryKey: clusterQueryKeys.replicaHydrationEpisodes(params),
    enabled: hasHydrationHistory && params.replicaIds.length > 0,
    staleTime: 60_000,
    refetchInterval: 60_000,
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchReplicaHydrationEpisodes({
        params: {
          replicaIds: queryKeyParams.replicaIds,
          startDate: subMinutes(
            new Date(),
            queryKeyParams.timePeriodMinutes,
          ).toISOString(),
        },
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

// Module-level so react-query reuses the result while the rows are unchanged.
// An inline function reruns on every render, and a Set or Map skips
// structural sharing, so each render would get a new identity.
const toUnhydratedCountByReplicaId = (
  rows: Array<{ replicaId: string; objectId: string }>,
) => {
  const objectIds = new Map<string, Set<string>>();
  for (const { replicaId, objectId } of rows) {
    const ids = objectIds.get(replicaId) ?? new Set<string>();
    ids.add(objectId);
    objectIds.set(replicaId, ids);
  }
  return new Map(
    [...objectIds].map(([replicaId, ids]) => [replicaId, ids.size]),
  );
};

const toHeapLimitBytesByReplicaId = (
  rows: Awaited<ReturnType<typeof fetchReplicaHeapLimits>>,
) => {
  const limits = new Map<string, number>();
  for (const { replicaId, heapLimit } of rows) {
    if (heapLimit === null) continue;
    limits.set(
      replicaId,
      Math.max(limits.get(replicaId) ?? 0, Number(heapLimit)),
    );
  }
  return limits;
};

/**
 * How many objects each of the given replicas has yet to hydrate, for replicas
 * that are hydrating. Idle while `replicaIds` is empty.
 */
export function useUnhydratedObjectCounts(replicaIds: string[]) {
  return useQuery({
    queryKey: clusterQueryKeys.unhydratedComputeObjects({ replicaIds }),
    enabled: replicaIds.length > 0,
    staleTime: 30_000,
    refetchInterval: 30_000,
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchUnhydratedComputeObjects({
        params: queryKeyParams,
        queryKey,
        requestOptions: { signal },
      });
    },
    select: toUnhydratedCountByReplicaId,
  });
}

type ObjectHydrationRow = {
  replicaId: string;
  objectId: string;
  createdAtMs: number;
  hydrationMs: number | null;
};

const toObjectHydrationsByReplicaId = (rows: ObjectHydrationRow[]) => {
  const hydrationsByReplicaId = new Map<
    string,
    Array<Omit<ObjectHydrationRow, "replicaId">>
  >();
  for (const { replicaId, ...hydration } of rows) {
    const hydrations = hydrationsByReplicaId.get(replicaId) ?? [];
    hydrations.push(hydration);
    hydrationsByReplicaId.set(replicaId, hydrations);
  }
  return hydrationsByReplicaId;
};

/**
 * Each replica's hydration of the given objects, keyed by replica id, for
 * `paintObjectHydration`. Pass the objects created inside the window with
 * their creation times. Leaves out objects created before environmentd last
 * started. Idle while either list is empty.
 */
export function useObjectHydrationStretches({
  replicaIds,
  objects,
}: {
  replicaIds: string[];
  objects: Array<{ id: string; createdAt: Date }>;
}) {
  return useQuery({
    // Sorted so the key tracks set membership. A creation time never changes,
    // so keying on it costs no fetches and hands it to `queryFn`.
    queryKey: clusterQueryKeys.objectHydrationTimes({
      replicaIds: [...replicaIds].sort(),
      objects: sort(
        objects.map(({ id, createdAt }) => ({
          id,
          createdAtMs: createdAt.getTime(),
        })),
        ({ id }) => id,
      ),
    }),
    enabled: replicaIds.length > 0 && objects.length > 0,
    staleTime: 30_000,
    refetchInterval: 30_000,
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      const { hydrations, environmentStartedAt } =
        await fetchObjectHydrationTimes({
          params: {
            replicaIds: queryKeyParams.replicaIds,
            objectIds: queryKeyParams.objects.map(({ id }) => id),
          },
          queryKey,
          requestOptions: { signal },
        });
      const createdAtMsById = new Map(
        queryKeyParams.objects.map(({ id, createdAtMs }) => [id, createdAtMs]),
      );
      return hydrations.flatMap(
        ({ replicaId, objectId, hydrationMs }): ObjectHydrationRow[] => {
          const createdAtMs = createdAtMsById.get(objectId);
          // An upgrade, or a restart under the process orchestrator, starts
          // every replica on new processes that rehydrate every object, and
          // status history records no restart. So an object created before
          // then reports the rehydration, which the timeline can't place.
          return createdAtMs === undefined ||
            createdAtMs < environmentStartedAt.getTime()
            ? []
            : [{ replicaId, objectId, createdAtMs, hydrationMs }];
        },
      );
    },
    select: toObjectHydrationsByReplicaId,
  });
}

const toAllocationsBySize = (rows: Array<SizeAllocation & { size: string }>) =>
  new Map(rows.map(({ size, ...allocation }) => [size, allocation]));

/**
 * Each size's process count and per-process allocations. Sizes don't change,
 * so this is fetched once. Idle while `sizes` is empty.
 */
export function useReplicaSizeAllocations(sizes: string[]) {
  return useQuery({
    queryKey: clusterQueryKeys.replicaSizeAllocations({ sizes }),
    enabled: sizes.length > 0,
    staleTime: Infinity,
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchReplicaSizeAllocations({
        params: queryKeyParams,
        queryKey,
        requestOptions: { signal },
      });
    },
    select: toAllocationsBySize,
  });
}

const windowStartFor = (windowMs: number | undefined) =>
  windowMs === undefined
    ? undefined
    : { windowMs, startMs: Date.now() - windowMs };

/**
 * The start of a live subscribe that must cover the last `windowMinutes`. It
 * begins one window back and moves forward once per window, so the subscribe
 * restarts that often and holds at most two windows of rows. Undefined while
 * `windowMinutes` is.
 */
export function useRollingWindowStartMs(windowMinutes: number | undefined) {
  const windowMs =
    windowMinutes === undefined
      ? undefined
      : windowMinutes * millisecondsInMinute;
  const [start, setStart] = useState(() => windowStartFor(windowMs));
  useEffect(() => {
    if (windowMs === undefined) {
      // Cleared, or a return to this window would keep the old start below.
      setStart(undefined);
      return;
    }
    // Keeps a start already taken for this window, such as the initializer's.
    // Taking another restarts the subscribe built on it.
    setStart((current) =>
      current?.windowMs === windowMs ? current : windowStartFor(windowMs),
    );
    let timeout: ReturnType<typeof setTimeout>;
    const roll = () => {
      setStart(windowStartFor(windowMs));
      timeout = setTimeout(roll, windowMs);
    };
    timeout = setTimeout(roll, windowMs);
    return () => clearTimeout(timeout);
  }, [windowMs]);
  // A start kept from another window covers the wrong span until the effect
  // replaces it.
  return start?.windowMs === windowMs ? start?.startMs : undefined;
}

/**
 * Each replica's per-process heap limit in bytes, for replicas reporting one.
 * Idle while `replicaIds` is empty.
 */
export function useReplicaHeapLimits(replicaIds: string[]) {
  return useQuery({
    queryKey: clusterQueryKeys.replicaHeapLimits({ replicaIds }),
    enabled: replicaIds.length > 0,
    // A replica's heap limit is fixed by its size, so polling stops once each
    // replica has a row with one, or with memory but none (an orchestrator
    // without heap limits, or a failed usage fetch on Kubernetes). A failed
    // metrics fetch leaves both null, which keeps polling.
    staleTime: Infinity,
    refetchInterval: (query) => {
      const settled = new Set(
        query.state.data
          ?.filter((row) => row.heapLimit !== null || row.memoryBytes !== null)
          .map((row) => row.replicaId) ?? [],
      );
      return replicaIds.every((id) => settled.has(id)) ? false : 60_000;
    },
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchReplicaHeapLimits({
        params: queryKeyParams,
        queryKey,
        requestOptions: { signal },
      });
    },
    select: toHeapLimitBytesByReplicaId,
  });
}

/** Creation times of the given objects over the window. Idle while `objectIds` is empty. */
export function useObjectCreationTimes(params: {
  objectIds: string[];
  timePeriodMinutes: number;
}) {
  return useQuery({
    queryKey: clusterQueryKeys.objectCreationTimes(params),
    enabled: params.objectIds.length > 0,
    // A creation time never changes, and a new object changes the key.
    staleTime: Infinity,
    queryFn: ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      return fetchObjectCreationTimes({
        params: {
          objectIds: queryKeyParams.objectIds,
          startDate: subMinutes(
            new Date(),
            queryKeyParams.timePeriodMinutes,
          ).toISOString(),
        },
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

export const LINE_MAX_COUNT = 10;

/** Whether an object is one of a cluster's, as the freshness views count them. */
export function isOnCluster(object: DatabaseObject, clusterId: string) {
  return (
    object.clusterId === clusterId &&
    (isSystemCluster(clusterId) || !isSystemId(object.id))
  );
}

/** The objects on a cluster that the freshness views report on. */
export function useFreshnessObjects(clusterId: string): FreshnessObject[] {
  return useFreshnessObjectsWhere((object) => isOnCluster(object, clusterId));
}

/**
 * The objects matching `include` that the freshness views report on, from the
 * `useAllObjects` subscribe rather than a query.
 *
 * Subsources and progress collections are excluded to match the rest of the
 * Console, which hides them: each source carries a progress collection and
 * often several subsources, so including them would multiply the line count
 * without adding anything a reader recognises.
 */
export function useFreshnessObjectsWhere(
  include: (object: DatabaseObject) => boolean,
): FreshnessObject[] {
  const { data: allObjects } = useAllObjects();

  // Keyed on content rather than on `allObjects`'s identity. That subscribe
  // emits a new array for any change anywhere in the environment, so keying on
  // it would hand this scope a new `objects` array because something outside
  // it changed, rebuilding the whole stats and rows chain behind it.
  //
  // Only the fields read downstream are in the key. A rename has to invalidate
  // it; a column nothing reads must not.
  const objects = allObjects
    .filter(
      (object) =>
        include(object) &&
        object.sourceType !== "subsource" &&
        object.sourceType !== "progress",
    )
    .map((object): FreshnessObject => ({
      objectId: object.id,
      objectName: object.name,
      schemaId: object.schemaId,
      schemaName: object.schemaName,
      databaseName: object.databaseName,
      clusterId: object.clusterId,
      clusterName: object.clusterName,
      objectType: object.objectType,
    }));
  // JSON keeps a null name apart from an empty one, and no name can break it.
  const objectsKey = JSON.stringify(objects);

  // Rebuilt from the key, so the dependency is the whole truth: nothing else
  // is read in here.
  return useMemo(
    () => JSON.parse(objectsKey) as FreshnessObject[],
    [objectsKey],
  );
}

/** An object's identity, invariant across the window. */
export interface FreshnessObject {
  objectId: string;
  objectName: string | null;
  schemaId: string | null;
  schemaName: string | null;
  databaseName: string | null;
  /** Null for an object that runs on no cluster, such as a table. */
  clusterId: string | null;
  clusterName: string | null;
  objectType: string;
}

type LagReading = Awaited<
  ReturnType<typeof fetchObjectLagHistory>
>["rows"][number];
/**
 * Shapes lag readings for the freshness graph, naming each object from the
 * caller's list.
 *
 * Separate from the query so names are not cached with the readings: the query
 * key tracks which objects were asked for, not what they are called, so a
 * rename would otherwise survive in the cache until the set changed.
 *
 * @param readings One binned point per object per bin, each the worst reading
 * in its span.
 */
export function buildFreshnessData(
  readings: LagReading[],
  objects: FreshnessObject[],
) {
  // Everything below reads row order as time order: the graph's start and end,
  // and each object's last row as its newest. Sorted here so that holds
  // whatever order the query returns.
  const rows = [...readings].sort(
    (a, b) => a.bucketStart.getTime() - b.bucketStart.getTime(),
  );
  const objectsById = new Map(
    objects.map((object) => [object.objectId, object]),
  );

  const currentData = flatGroup(rows, (d) => d.objectId)
    .map(([, rowsByObjectId]) => rowsByObjectId.at(-1))
    .filter(notNullOrUndefined)
    .sort(sortLagInfo)
    .slice(0, LINE_MAX_COUNT)
    .map((row) => {
      const object = objectsById.get(row.objectId);
      return {
        ...row,
        objectName: object?.objectName ?? null,
        schemaName: object?.schemaName ?? null,
        databaseName: object?.databaseName ?? null,
      };
    });

  const dataByBucketStart = group(rows, (d) => d.bucketStart.getTime());

  const historicalData = [...dataByBucketStart.entries()].map(
    ([timestamp, rowsByBucketStart]) => {
      const dataPoint: DataPoint = { timestamp, lag: {} };

      rowsByBucketStart.forEach((row) => {
        const object = objectsById.get(row.objectId);
        const names = {
          schemaName: object?.schemaName ?? null,
          objectName: object?.objectName ?? null,
        };
        dataPoint.lag[row.objectId] =
          row.lag !== null
            ? {
                queryable: true,
                totalMs: sumPostgresIntervalMs(row.lag),
                interval: row.lag,
                ...names,
              }
            : { queryable: false, ...names };
      });

      return dataPoint;
    },
  );

  // TODO: Cap this the way `currentData` is capped by LINE_MAX_COUNT. One
  // line per object is unbounded, and dragging a threshold over these lines
  // re-renders every path on each pointer move.
  const lines = new Map<string, GraphLineSeries>();
  historicalData.forEach((point) => {
    Object.entries(point.lag).forEach(([objectId, lagInfo]) => {
      lines.set(objectId, {
        key: objectId,
        label: formatFullyQualifiedObjectName({
          schemaName: lagInfo.schemaName ?? "",
          name: lagInfo.objectName ?? "",
        }),
        yAccessor: (d: DataPoint) => {
          const dataPointLag = d.lag[objectId];
          if (dataPointLag) {
            // `null` breaks the line rather than drawing a point. A reading
            // that could not be taken has no height, and drawing it at zero
            // put the worst state at the bottom of the plot, which reads as
            // the healthiest line on the chart.
            return dataPointLag.queryable ? dataPointLag.totalMs : null;
          }
          return null;
        },
      });
    });
  });

  return {
    historicalData,
    currentData,
    objectsById,
    lines: Array.from(lines.values()),
    startTime: historicalData.at(0)?.timestamp ?? 0,
    endTime: historicalData.at(-1)?.timestamp ?? 0,
  };
}

/**
 * Lag readings for a known set of objects, shaped for the freshness graph.
 *
 * The caller supplies the objects rather than naming a cluster, because the
 * `useAllObjects` subscribe already holds every name, schema, database and
 * type. Resolving those in SQL cost three joins and three full scans per
 * request; here they are a map lookup.
 */
export function useClusterFreshness({
  lookbackMs,
  objects,
}: ClusterFreshnessParams) {
  const objectIds = objects.map((object) => object.objectId);

  // While the page sits open, the series refetches once per bin, which at a
  // 24 hour range is 24 minutes, so it re-reads the whole window at most once
  // per new point. A load or a return to the tab refetches anything older than
  // a reading: people open this page when they suspect a problem, and a cached
  // series up to a bin old would hide it.
  // NOTE: the trailing bin is partial, so between fetches it can miss a lag
  // that started since. See `computeStats`.
  const binSizeMs = calculateBucketSizeFromLookback(lookbackMs);

  // NOTE: `useQuery`, not `useSuspenseQuery`. A suspending query stops the
  // component before the next hook runs, so each request would wait for the
  // one above it, including a caller's own queries after this hook.
  const series = useQuery({
    queryKey: clusterQueryKeys.clusterFreshnessSeries({
      lookbackMs,
      objectIds,
    }),
    queryFn: async ({ queryKey, signal }): Promise<LagReading[]> => {
      if (objectIds.length === 0) return [];

      const { rows } = await fetchObjectLagHistory({
        objectIds,
        lookbackMs,
        requestOptions: { signal },
        queryKey,
      });
      return rows;
    },
    staleTime: LATEST_READING_INTERVAL_MS,
    refetchInterval: binSizeMs,
  });

  const data = useMemo(
    () => (series.data ? buildFreshnessData(series.data, objects) : undefined),
    [series.data, objects],
  );

  // Only these fields are returned. Spreading a query object would make every
  // consumer observe all of its state.
  return { data, isError: series.isError };
}

export type FreshnessData = ReturnType<typeof buildFreshnessData>;

export type CurrentClusterFreshnessData = FreshnessData["currentData"][0];
