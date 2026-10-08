// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  useMutation,
  useQuery,
  useQueryClient,
  useSuspenseQuery,
} from "@tanstack/react-query";
import { flatGroup, group } from "d3";
import { subMinutes } from "date-fns";
import { useCallback, useMemo } from "react";

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
import { fetchReplicaHydration } from "~/api/materialize/cluster/replicaHydration";
import {
  ClusterReplicasParams,
  fetchClusterReplicas,
} from "~/api/materialize/cluster/replicas";
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
  fetchLatestLag,
  fetchObjectLagHistory,
  LATEST_READING_INTERVAL_MS,
} from "~/api/materialize/freshness/lagHistory";
import { assertNoMoreThanOneRow } from "~/api/materialize/MoreThanOneRowError";
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
  replicaOfflineEvents: (params: {
    clusterIdsKey: string;
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
  // No lookback: the newest reading is the newest reading whatever range is
  // being graphed, so changing the range must not refetch it.
  clusterFreshnessLatest: (params: { objectIds: string[] }) =>
    [
      ...clusterQueryKeys.all(),
      buildQueryKeyPart("clusterFreshnessLatest", {
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

export function useAvailableClusterSizes() {
  return useSuspenseQuery({
    queryKey: clusterQueryKeys.availableClusterSizes(),
    queryFn: ({ queryKey, signal }) => {
      return fetchAvailableClusterSizes({
        queryKey,
        requestOptions: { signal },
      });
    },
  });
}

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

/**
 * SUBSCRIBE variant for the live (≤3h) window: streams the un-binned 3h base
 * (lineage resolved in SQL), bins client-side, shapes like the poll path.
 * Subscribes by cluster (not replica) so the socket survives the replica dropdown.
 *
 * NOTE: when `enabled` is false the subscribe is undefined, so the socket opens
 * but sends no query: an idle connection, not catalog-server load.
 */
function useReplicaUtilizationHistorySubscribe(
  params: ReplicaUtilizationHistoryFilters,
  enabled: boolean,
) {
  const { replicaId, timePeriodMinutes, bucketSizeMs } = params;
  // Key on content, not array identity (the caller passes a fresh array each
  // render), so the socket survives re-renders. Ids contain no commas, so the
  // comma join/split round-trips them.
  const clusterIdsKey = (params.clusterIds ?? []).join(",");

  const subscribe = useMemo(() => {
    const clusterIds = clusterIdsKey ? clusterIdsKey.split(",") : [];
    if (!enabled || clusterIds.length === 0) {
      return undefined;
    }
    // The frontier only needs to reach back to the window start; the view itself
    // retains the last 3h.
    const minDate = subMinutes(new Date(), timePeriodMinutes);
    return buildConsoleClusterUtilizationUnbinned3hSubscribe<UtilizationSample>(
      clusterIds,
      minDate,
    );
  }, [enabled, clusterIdsKey, timePeriodMinutes]);

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
  const { data: offlineEvents } = useQuery({
    queryKey: clusterQueryKeys.replicaOfflineEvents({
      clusterIdsKey,
      timePeriodMinutes,
    }),
    refetchInterval: 20_000,
    enabled: enabled && clusterIdsKey.length > 0,
    queryFn: async ({ queryKey, signal }) => {
      const [, queryKeyParams] = queryKey;
      const clusterIds = queryKeyParams.clusterIdsKey
        ? queryKeyParams.clusterIdsKey.split(",")
        : [];
      const startDate = subMinutes(
        new Date(),
        queryKeyParams.timePeriodMinutes,
      ).toISOString();
      return fetchReplicaOfflineEvents({
        params: { clusterIds, startDate, resolveLineage: true },
        queryKey,
        requestOptions: { signal },
      });
    },
  });

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
    isError,
  };
}

/**
 * SUBSCRIBE variant for the 3h-24h window: streams the server-binned 24h view
 * (lineage resolved in SQL). The rows are already binned, so they feed
 * `bucketRowsToBucketsByReplicaId` directly with no client-side rebinning.
 * ENVELOPE UPSERT yields an unordered keyed set, so we sort by bucket start.
 */
function useReplicaUtilizationHistoryBinnedSubscribe(
  params: ReplicaUtilizationHistoryFilters,
  enabled: boolean,
) {
  const { replicaId, timePeriodMinutes } = params;
  const clusterIdsKey = (params.clusterIds ?? []).join(",");

  const subscribe = useMemo(() => {
    const clusterIds = clusterIdsKey ? clusterIdsKey.split(",") : [];
    if (!enabled || clusterIds.length === 0) {
      return undefined;
    }
    const minDate = subMinutes(new Date(), timePeriodMinutes);
    return buildConsoleClusterUtilizationOverview24hSubscribe<BinnedSubscribeRow>(
      clusterIds,
      minDate,
    );
  }, [enabled, clusterIdsKey, timePeriodMinutes]);

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
    isError,
  };
}

export function useReplicaUtilizationHistory(
  params: ReplicaUtilizationHistoryFilters,
  queryOptions?: { enabled?: boolean },
) {
  const enabled = queryOptions?.enabled ?? true;
  const minutes = params.timePeriodMinutes;
  // The un-binned 3h and 24h indexed views (and their SUBSCRIBEs) only exist on
  // mz >= 26.32. On older environments (e.g. mid-rollout) these paths are gated
  // off and everything falls back to the poll. The 14d `overview` view predates
  // this, so its poll path is not gated.
  // TODO: remove the gate once all environments are >= 26.32.
  const hasIndexedViews = useEnvironmentGate("26.32.0") === true;

  const useUnbinnedSubscribe =
    hasIndexedViews && minutes <= SUBSCRIBE_UNBINNED_MAX_MINUTES;
  const useBinnedSubscribe =
    hasIndexedViews &&
    minutes > SUBSCRIBE_UNBINNED_MAX_MINUTES &&
    minutes <= SUBSCRIBE_BINNED_MAX_MINUTES;

  const unbinnedResult = useReplicaUtilizationHistorySubscribe(
    params,
    enabled && useUnbinnedSubscribe,
  );
  const binnedResult = useReplicaUtilizationHistoryBinnedSubscribe(
    params,
    enabled && useBinnedSubscribe,
  );

  const queryResult = useQuery({
    queryKey: clusterQueryKeys.replicaUtilizationHistory(params),
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

export const LINE_MAX_COUNT = 10;

// Separators for the key below. Control characters, which an identifier
// cannot contain, and a marker because a name can be null and null has to stay
// distinguishable from the empty string.
const FIELD_SEP = "\u0000";
const RECORD_SEP = "\u0001";
const NULL_MARKER = "\u0002";

const encode = (value: string | null) => value ?? NULL_MARKER;
const decode = (value: string) => (value === NULL_MARKER ? null : value);

/**
 * The objects on a cluster that the freshness views report on, from the
 * `useAllObjects` subscribe rather than a query.
 *
 * Subsources and progress collections are excluded to match the rest of the
 * Console, which hides them: each source carries a progress collection and
 * often several subsources, so including them would multiply the line count
 * without adding anything a reader recognises.
 */
export function useFreshnessObjects(clusterId: string): FreshnessObject[] {
  const { data: allObjects } = useAllObjects();

  // Keyed on content rather than on `allObjects`'s identity. That subscribe
  // emits a new array for any change anywhere in the environment, so keying on
  // it would hand this cluster a new `objects` array because some other
  // cluster changed, rebuilding the whole stats and rows chain behind it.
  //
  // Only the fields read downstream are in the key. A rename has to invalidate
  // it; a column nothing reads must not.
  const objectsKey = allObjects
    .filter(
      (object) =>
        object.clusterId === clusterId &&
        (isSystemCluster(clusterId) || !isSystemId(object.id)) &&
        object.sourceType !== "subsource" &&
        object.sourceType !== "progress",
    )
    .map((object) =>
      [
        object.id,
        encode(object.name),
        encode(object.schemaName),
        encode(object.databaseName),
        object.objectType,
      ].join(FIELD_SEP),
    )
    .join(RECORD_SEP);

  // Rebuilt from the key, so the dependency is the whole truth: nothing else
  // is read in here.
  return useMemo(() => {
    if (objectsKey === "") return [];

    return objectsKey.split(RECORD_SEP).map((record) => {
      const [objectId, objectName, schemaName, databaseName, objectType] =
        record.split(FIELD_SEP);
      return {
        objectId,
        objectName: decode(objectName),
        schemaName: decode(schemaName),
        databaseName: decode(databaseName),
        objectType,
      };
    });
  }, [objectsKey]);
}

/** An object's identity, invariant across the window. */
export interface FreshnessObject {
  objectId: string;
  objectName: string | null;
  schemaName: string | null;
  databaseName: string | null;
  objectType: string;
}

type LagReading = Awaited<
  ReturnType<typeof fetchObjectLagHistory>
>["rows"][number];
type LatestReading = Awaited<ReturnType<typeof fetchLatestLag>>["rows"][number];

export interface LagQueryRows {
  /** One binned point per object per bin, each the worst reading in its span. */
  readings: LagReading[];
  /** One row per object, its most recent reading. */
  latest: LatestReading[];
}

/**
 * Shapes lag readings for the freshness graph, naming each object from the
 * caller's list.
 *
 * Separate from the query so names are not cached with the readings: the query
 * key tracks which objects were asked for, not what they are called, so a
 * rename would otherwise survive in the cache until the set changed.
 */
export function buildFreshnessData(
  { readings: rows, latest }: LagQueryRows,
  objects: FreshnessObject[],
) {
  const objectsById = new Map(
    objects.map((object) => [object.objectId, object]),
  );

  // Null where the newest reading reports the object as unreadable, which is
  // not the same as the object having no reading at all.
  const latestByObjectId = new Map<string, number | null>(
    latest.map((row) => [
      row.objectId,
      row.lag === null ? null : sumPostgresIntervalMs(row.lag),
    ]),
  );

  const currentData = flatGroup(rows, (d) => d.objectId)
    // The last row per object is the most current: the query sorts by bucket.
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
    latestByObjectId,
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

  // The binned series and the latest readings go in separate requests because
  // they go stale at different rates. A bin cannot change faster than its own
  // width, which at a 24 hour range is 24 minutes, so refetching the series on
  // the one minute clock would re-read the whole window to change at most one
  // point.
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
    staleTime: binSizeMs,
    refetchInterval: binSizeMs,
  });

  const latest = useQuery({
    queryKey: clusterQueryKeys.clusterFreshnessLatest({ objectIds }),
    queryFn: async ({ queryKey, signal }): Promise<LatestReading[]> => {
      if (objectIds.length === 0) return [];

      const { rows } = await fetchLatestLag({
        objectIds,
        requestOptions: { signal },
        queryKey,
      });
      return rows;
    },
    // A reading lands once a minute, so anything shorter re-asks a question
    // whose answer cannot have changed.
    staleTime: LATEST_READING_INTERVAL_MS,
    refetchInterval: LATEST_READING_INTERVAL_MS,
  });

  // Undefined until both have answered. Building from one alone would show a
  // graph whose "Now" column disagrees with it.
  const data = useMemo(
    () =>
      series.data && latest.data
        ? buildFreshnessData(
            { readings: series.data, latest: latest.data },
            objects,
          )
        : undefined,
    [series.data, latest.data, objects],
  );

  // Only these fields are returned. Spreading a query object would make every
  // consumer observe all of its state.
  return { data, isError: series.isError || latest.isError };
}

export type FreshnessData = ReturnType<typeof buildFreshnessData>;

export type CurrentClusterFreshnessData = FreshnessData["currentData"][0];
