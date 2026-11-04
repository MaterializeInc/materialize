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
import React from "react";

import {
  BinnedSubscribeRow,
  bucketRowsToBucketsByReplicaId,
  clipBinnedRowsToWindow,
  parseBinnedSubscribeRow,
  rebucketUtilizationSamples,
  toReplicaUtilizationGraphData,
  UtilizationSample,
} from "~/api/materialize/cluster/replicaUtilizationBinning";
import {
  buildConsoleClusterUtilizationOverview24hSubscribe,
  buildConsoleClusterUtilizationUnbinned3hSubscribe,
} from "~/api/materialize/cluster/replicaUtilizationHistory";
import { createScopedSubscribeCollections } from "~/api/materialize/subscribeCollection";
import { useScopedSubscribeCollection } from "~/api/materialize/useScopedSubscribeCollection";
import {
  LIVE_TIER_RETENTION_MINUTES,
  useClusterLineageIds,
} from "~/platform/clusters/queries";

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
      : clipBinnedRowsToWindow(
          rows.buckets.map(parseBinnedSubscribeRow),
          startDate,
        );
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

const samplesByCluster = createScopedSubscribeCollections<StoredSample>({
  id: "cluster-utilization-3h",
  getKey: sampleKey,
});
const bucketsByCluster = createScopedSubscribeCollections<BinnedSubscribeRow>({
  id: "cluster-utilization-24h",
  getKey: bucketKey,
});

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
  const lineage = useClusterLineageIds([clusterId], tier !== undefined);
  const lineageIdsKey = lineage.data?.join(",") ?? "";

  const samplesSubscribe = React.useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    return tier === "unbinned3h" && clusterIds.length > 0
      ? buildConsoleClusterUtilizationUnbinned3hSubscribe<StoredSample>(
          clusterIds,
          subMinutes(new Date(), LIVE_TIER_RETENTION_MINUTES.unbinned3h),
          includeMemoryBreakdown,
        )
      : undefined;
  }, [tier, lineageIdsKey, includeMemoryBreakdown]);
  const samples = useScopedSubscribeCollection(
    samplesByCluster,
    clusterId,
    samplesSubscribe,
  );

  const bucketsSubscribe = React.useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    return tier === "binned24h" && clusterIds.length > 0
      ? buildConsoleClusterUtilizationOverview24hSubscribe<BinnedSubscribeRow>(
          clusterIds,
          subMinutes(new Date(), LIVE_TIER_RETENTION_MINUTES.binned24h),
          includeMemoryBreakdown,
        )
      : undefined;
  }, [tier, lineageIdsKey, includeMemoryBreakdown]);
  const buckets = useScopedSubscribeCollection(
    bucketsByCluster,
    clusterId,
    bucketsSubscribe,
  );

  const active =
    tier === "unbinned3h"
      ? samples
      : tier === "binned24h"
        ? buckets
        : undefined;
  const collection = active?.collection;
  const { data: rows } = useLiveQuery(
    (q) => (collection ? q.from({ row: collection.collection }) : undefined),
    [collection],
  );
  const snapshotComplete = active?.status.snapshotComplete ?? false;
  const isRefreshing = active?.status.isRefreshing ?? false;

  const data = React.useMemo(() => {
    if (!collection || !snapshotComplete || !rows) return undefined;
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
    collection,
    snapshotComplete,
    rows,
    tier,
    timePeriodMinutes,
    bucketSizeMs,
  ]);

  // Held rows can have aged out of the window entirely. Until the new
  // snapshot lands that would read as a window with no usage.
  const isAwaitingRows = isRefreshing && data?.graphData.length === 0;
  return {
    data: isAwaitingRows ? undefined : data,
    isLoading:
      collection !== undefined && (!snapshotComplete || isAwaitingRows),
    isError:
      Boolean(active?.status.error) ||
      lineage.isLoadingError ||
      samples.isScopeError,
    isRefreshing,
  };
}
