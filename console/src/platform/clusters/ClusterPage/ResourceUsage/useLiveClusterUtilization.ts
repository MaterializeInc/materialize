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
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { useGlobalSubscribeCollection } from "~/api/materialize/useSubscribe";
import {
  LIVE_TIER_RETENTION_MINUTES,
  useClusterLineageIds,
} from "~/platform/clusters/queries";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

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
  const scope = useAtomValue(syncEngineCacheScopeLoadableAtom);
  const scopeId = scope.state === "hasData" ? scope.data : undefined;
  const samples = scopeId
    ? samplesByCluster.get(scopeId, clusterId)
    : undefined;
  const buckets = scopeId
    ? bucketsByCluster.get(scopeId, clusterId)
    : undefined;
  const lineage = useClusterLineageIds([clusterId], tier !== undefined);
  const lineageIdsKey = lineage.data?.join(",") ?? "";

  const samplesOptions = React.useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    return {
      target: samples ?? samplesByCluster.placeholder,
      subscribe:
        samples && tier === "unbinned3h" && clusterIds.length > 0
          ? buildConsoleClusterUtilizationUnbinned3hSubscribe<StoredSample>(
              clusterIds,
              subMinutes(new Date(), LIVE_TIER_RETENTION_MINUTES.unbinned3h),
              includeMemoryBreakdown,
            )
          : undefined,
      select: (row: SubscribeRow<StoredSample>) => row.data,
      upsertKey: (row: SubscribeRow<StoredSample>) => sampleKey(row.data),
    };
  }, [samples, tier, lineageIdsKey, includeMemoryBreakdown]);
  useGlobalSubscribeCollection(samplesOptions);

  const bucketsOptions = React.useMemo(() => {
    const clusterIds = lineageIdsKey ? lineageIdsKey.split(",") : [];
    return {
      target: buckets ?? bucketsByCluster.placeholder,
      subscribe:
        buckets && tier === "binned24h" && clusterIds.length > 0
          ? buildConsoleClusterUtilizationOverview24hSubscribe<BinnedSubscribeRow>(
              clusterIds,
              subMinutes(new Date(), LIVE_TIER_RETENTION_MINUTES.binned24h),
              includeMemoryBreakdown,
            )
          : undefined,
      select: (row: SubscribeRow<BinnedSubscribeRow>) => row.data,
      upsertKey: (row: SubscribeRow<BinnedSubscribeRow>) => bucketKey(row.data),
    };
  }, [buckets, tier, lineageIdsKey, includeMemoryBreakdown]);
  useGlobalSubscribeCollection(bucketsOptions);

  const active =
    tier === "unbinned3h"
      ? samples
      : tier === "binned24h"
        ? buckets
        : undefined;
  const { data: rows } = useLiveQuery(
    (q) => (active ? q.from({ row: active.collection }) : undefined),
    [active],
  );
  const status = useAtomValue(
    (active ?? samplesByCluster.placeholder).statusAtom,
  );

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

  // Held rows can have aged out of the window entirely. Until the new
  // snapshot lands that would read as a window with no usage.
  const isAwaitingRows = status.isRefreshing && data?.graphData.length === 0;
  return {
    data: isAwaitingRows ? undefined : data,
    isLoading:
      active !== undefined && (!status.snapshotComplete || isAwaitingRows),
    isError:
      Boolean(status.error) ||
      lineage.isLoadingError ||
      scope.state === "hasError",
    isRefreshing: status.isRefreshing,
  };
}
