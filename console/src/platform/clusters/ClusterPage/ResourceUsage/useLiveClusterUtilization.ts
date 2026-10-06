// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLiveQuery } from "@tanstack/react-db";
import { subMinutes } from "date-fns";
import { useAtomValue } from "jotai";
import React from "react";

import {
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
} from "~/api/materialize/cluster/replicaUtilizationHistory";
import {
  createSubscribeCollection,
  SubscribeCollection,
} from "~/api/materialize/subscribeCollection";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { useGlobalSubscribeCollection } from "~/api/materialize/useSubscribe";
import { useClusterLineageIds } from "~/platform/clusters/queries";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

const UNBINNED_RETENTION_MINUTES = 3 * 60;
const BINNED_RETENTION_MINUTES = 24 * 60;

/** A 3h-view sample as streamed, with its timestamp still a string. */
export type StoredSample = Omit<UtilizationSample, "occurredAt"> & {
  occurredAt: string;
};

/**
 * Shapes a live collection's rows for a chart window ending at `endDate`. The
 * 3h tier holds raw samples, binned here. The 24h tier holds the whole day's
 * 5-minute buckets, clipped here to the window.
 */
export function transformLiveUtilization({
  rows,
  endDate,
  timePeriodMinutes,
  bucketSizeMs,
}: {
  rows:
    | { tier: "unbinned3h"; samples: StoredSample[] }
    | { tier: "binned24h"; buckets: BinnedSubscribeRow[] };
  endDate: Date;
  timePeriodMinutes: number;
  bucketSizeMs: number;
}) {
  const startDate = subMinutes(endDate, timePeriodMinutes);
  const bucketRows =
    rows.tier === "unbinned3h"
      ? rebucketUtilizationSamples(
          rows.samples.map((sample) => ({
            ...sample,
            occurredAt: new Date(sample.occurredAt),
          })),
          bucketSizeMs,
          startDate.getTime(),
        )
      : rows.buckets
          .map(parseBinnedSubscribeRow)
          .filter((row) => row.bucketStart >= startDate)
          // ENVELOPE UPSERT yields an unordered keyed set; the chart needs
          // time order.
          .sort((a, b) => a.bucketStart.getTime() - b.bucketStart.getTime());
  return toReplicaUtilizationGraphData(
    bucketRowsToBucketsByReplicaId(bucketRows),
    startDate,
    endDate,
  );
}

const sampleKey = (sample: StoredSample) =>
  `${sample.replicaId}|${sample.occurredAt}`;
const bucketKey = (row: BinnedSubscribeRow) =>
  `${row.replicaId}|${row.bucketStart}`;

interface ClusterUtilizationCollections {
  samples: SubscribeCollection<StoredSample>;
  buckets: SubscribeCollection<BinnedSubscribeRow>;
}

// Stand-ins while the tenant scope resolves, so the session hooks always have
// a target. Their subscribes are undefined then, so nothing is ever written.
const placeholderSamples = createSubscribeCollection<StoredSample>({
  id: "cluster-utilization-3h|placeholder",
  getKey: sampleKey,
});
const placeholderBuckets = createSubscribeCollection<BinnedSubscribeRow>({
  id: "cluster-utilization-24h|placeholder",
  getKey: bucketKey,
});

// Keyed by tenant scope as well as cluster: cluster ids repeat across
// environments, so a collection must never serve another environment's rows.
// Rows stay in memory for the app session, which is what makes returning to a
// cluster instant. They are not persisted, since a cache entry per viewed
// cluster would have no bound in localStorage.
const collectionsByScopedCluster = new Map<
  string,
  ClusterUtilizationCollections
>();

function collectionsFor(scope: string, clusterId: string) {
  const key = `${scope}|${clusterId}`;
  let collections = collectionsByScopedCluster.get(key);
  if (!collections) {
    collections = {
      samples: createSubscribeCollection<StoredSample>({
        id: `cluster-utilization-3h|${key}`,
        getKey: sampleKey,
      }),
      buckets: createSubscribeCollection<BinnedSubscribeRow>({
        id: `cluster-utilization-24h|${key}`,
        getKey: bucketKey,
      }),
    };
    collectionsByScopedCluster.set(key, collections);
  }
  return collections;
}

/**
 * Live utilization for a cluster over windows of 24h or less, streamed from the
 * indexed 3h or 24h view into a per-cluster TanStack DB collection. Each
 * subscribe covers its view's whole retention, so switching between windows
 * that one view serves reuses the rows instead of resubscribing. Returns the
 * same shape as `useReplicaUtilizationHistory`.
 */
export function useLiveClusterUtilization({
  clusterId,
  timePeriodMinutes,
  bucketSizeMs,
  tier,
  includeMemoryBreakdown,
}: {
  clusterId: string;
  timePeriodMinutes: number;
  bucketSizeMs: number;
  tier: "unbinned3h" | "binned24h" | undefined;
  includeMemoryBreakdown: boolean;
}) {
  const scope = useAtomValue(syncEngineCacheScopeLoadableAtom);
  const collections =
    scope.state === "hasData" && scope.data
      ? collectionsFor(scope.data, clusterId)
      : undefined;
  const lineage = useClusterLineageIds([clusterId], tier !== undefined);
  const lineageIdsKey = lineage.data?.join(",") ?? "";

  const samplesOptions = React.useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    return {
      target: collections?.samples ?? placeholderSamples,
      subscribe:
        collections && tier === "unbinned3h" && clusterIds.length > 0
          ? buildConsoleClusterUtilizationUnbinned3hSubscribe<StoredSample>(
              clusterIds,
              subMinutes(new Date(), UNBINNED_RETENTION_MINUTES),
              includeMemoryBreakdown,
            )
          : undefined,
      select: (row: SubscribeRow<StoredSample>) => row.data,
      upsertKey: (row: SubscribeRow<StoredSample>) => sampleKey(row.data),
    };
  }, [collections, tier, lineageIdsKey, includeMemoryBreakdown]);
  useGlobalSubscribeCollection(samplesOptions);

  const bucketsOptions = React.useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    return {
      target: collections?.buckets ?? placeholderBuckets,
      subscribe:
        collections && tier === "binned24h" && clusterIds.length > 0
          ? buildConsoleClusterUtilizationOverview24hSubscribe<BinnedSubscribeRow>(
              clusterIds,
              subMinutes(new Date(), BINNED_RETENTION_MINUTES),
              includeMemoryBreakdown,
            )
          : undefined,
      select: (row: SubscribeRow<BinnedSubscribeRow>) => row.data,
      upsertKey: (row: SubscribeRow<BinnedSubscribeRow>) => bucketKey(row.data),
    };
  }, [collections, tier, lineageIdsKey, includeMemoryBreakdown]);
  useGlobalSubscribeCollection(bucketsOptions);

  const active =
    tier === "unbinned3h"
      ? collections?.samples
      : tier === "binned24h"
        ? collections?.buckets
        : undefined;
  const { data: rows } = useLiveQuery(
    (q) => (active ? q.from({ row: active.collection }) : undefined),
    [active],
  );
  const status = useAtomValue((active ?? placeholderSamples).statusAtom);

  const data = React.useMemo(() => {
    if (!active || !status.snapshotComplete || !rows) return undefined;
    return transformLiveUtilization({
      rows:
        tier === "unbinned3h"
          ? { tier, samples: rows as StoredSample[] }
          : { tier: "binned24h", buckets: rows as BinnedSubscribeRow[] },
      endDate: new Date(),
      timePeriodMinutes,
      bucketSizeMs,
    });
  }, [
    active,
    status.snapshotComplete,
    rows,
    tier,
    timePeriodMinutes,
    bucketSizeMs,
  ]);

  return {
    data,
    isLoading: active !== undefined && !status.snapshotComplete,
    isError: Boolean(status.error) || lineage.isLoadingError,
    isRefreshing: false,
  };
}
