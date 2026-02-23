// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Bucket,
  OfflineEvent,
} from "~/api/materialize/cluster/replicaUtilizationHistory";

// Console-specific categories of a cluster. Compute implies the cluster has compute objects only, storage likewise, and hybrid means both.
export type ClusterCategory = "compute" | "storage" | "hybrid" | "empty";

export type MemDiskUtilizationStatus =
  | "optimal"
  | "suboptimal"
  | "underProvisioned"
  | "empty";

export type ThresholdPercentages = {
  // We call a replica optimal if < thresholdPercentages.optimal
  optimal: number;
  // We call a replica suboptimal if < thresholdPercentages.suboptimal and underProvisioned if above
  suboptimal: number;
};

export type BucketWithUtilizationData = {
  status: MemDiskUtilizationStatus;
  oomEvents: OfflineEvent[];

  peakMemDiskUtilizationPercent: number | null;
  memoryPercent: number | null;
  diskPercent: number | null;
  heapPercent: number | null;
  occurredAt: Date;
  thresholdPercents: ThresholdPercentages;

  bucketStart: Date;
  bucketEnd: Date;
  replicaSize: string | null;
};
export type EmptyBucket = {
  status: "empty";
  bucketStart: Date;
  bucketEnd: Date;
};

export type BucketMap = Map<number, BucketWithUtilizationData | EmptyBucket>;

/**
 * Compute and hybrid replicas use a percentage that's derived from (memory + disk) / (total memory). What
 * this represents is since we usually only use disk when running out of memory, anything over 100% means we're
 * spilling to disk. We thus derive the thresholds from the following percentages:
 *
 * 90%: We're close to spilling to disk.
 * 180%: We're almost using as much disk as we are memory.
 */
const COMPUTE_AND_HYBRID_THRESHOLD_PERCENTAGES = {
  /**
   * We divide each percentage by to normalize each percent from 0 to 100%. We can do this because
   * for cc sizes, there's 2x as much disk as CPU available. Thus the maximum percentage you can
   * get is 300%.
   */

  optimal: 0.9 / 3,
  suboptimal: 1.8 / 3,
};

/**
 * Generic thresholds for replicas that don't have compute objects.
 */
const DEFAULT_THRESHOLD_PERCENTAGES = {
  optimal: 0.7,
  suboptimal: 0.85,
};

export function calculateClusterCategory({
  numSources,
  numSinks,
  numIndexes,
  numMaterializedViews,
}: {
  numSources: number | null;
  numSinks: number | null;
  numIndexes: number | null;
  numMaterializedViews: number | null;
}) {
  const hasStorageObjects = (numSources ?? 0) > 0 || (numSinks ?? 0) > 0;
  const hasComputeObjects =
    (numIndexes ?? 0) > 0 || (numMaterializedViews ?? 0) > 0;
  const hasComputeAndStorageObjects = hasStorageObjects && hasComputeObjects;

  return hasComputeAndStorageObjects
    ? "hybrid"
    : hasStorageObjects
      ? "storage"
      : hasComputeObjects
        ? "compute"
        : "empty";
}

export function calculateMemDiskUtilizationStatus({
  thresholdPercentages,
  peakMemDiskUtilizationPercent,
}: {
  thresholdPercentages: ThresholdPercentages;
  peakMemDiskUtilizationPercent: number | null;
}): MemDiskUtilizationStatus {
  if (peakMemDiskUtilizationPercent === null) {
    return "empty";
  }

  return peakMemDiskUtilizationPercent < thresholdPercentages.optimal
    ? "optimal"
    : peakMemDiskUtilizationPercent < thresholdPercentages.suboptimal
      ? "suboptimal"
      : "underProvisioned";
}

/**
 *
 * Given the category of a replica and a bucket, finds the peak storage utilization percentage
 * and thresholds that determine its utilization status. If null, it implies there's no
 * utilization data in the bucket to calculate the peak storage utilization.
 */
export function calculatePeakMemDiskUtilization(params: {
  category: ClusterCategory;
  bucket: Bucket;
}) {
  const { category } = params;

  let peakMemDiskUtilizationPercent: number | null = null,
    memoryPercent: number | null = null,
    diskPercent: number | null = null,
    heapPercent: number | null = null,
    occurredAt: Date | null = null;

  let thresholdPercentages: ThresholdPercentages =
    DEFAULT_THRESHOLD_PERCENTAGES;

  switch (category) {
    case "hybrid":
    case "compute": {
      const peakUtilization = params.bucket.maxMemoryAndDisk;
      occurredAt = peakUtilization.occurredAt;
      memoryPercent = peakUtilization.memoryPercent;
      diskPercent = peakUtilization.diskPercent;
      peakMemDiskUtilizationPercent = peakUtilization.percent;
      heapPercent = params.bucket.maxHeap.percent;
      thresholdPercentages = COMPUTE_AND_HYBRID_THRESHOLD_PERCENTAGES;
      break;
    }
    default: {
      if (
        (params.bucket.maxMemory.percent ?? 0) >
        (params.bucket.maxDisk.percent ?? 0)
      ) {
        peakMemDiskUtilizationPercent = params.bucket.maxMemory.percent;
        memoryPercent = params.bucket.maxMemory.percent;
        diskPercent = params.bucket.maxDisk.percent;
        occurredAt = params.bucket.maxMemory.occurredAt;
      } else {
        peakMemDiskUtilizationPercent = params.bucket.maxDisk.percent;
        memoryPercent = params.bucket.maxMemory.percent;
        diskPercent = params.bucket.maxDisk.percent;
        occurredAt = params.bucket.maxDisk.occurredAt;
      }
      heapPercent = params.bucket.maxHeap.percent;
    }
  }

  return {
    peakMemDiskUtilizationPercent,
    occurredAt,
    thresholdPercentages,
    memoryPercent,
    diskPercent,
    heapPercent,
  };
}

/**
 *
 *
 * Fills in the gaps with "empty" buckets between startMs and endMs
 *
 * @param params
 *
 * startMs: The start date in milliseconds
 * endMs: The end date in milliseconds
 * minBucketStartMs: The minimum start date of any bucket in milliseconds
 * maxBucketEndMs: The maximum end date of any bucket in milliseconds
 * bucketSizeMs: The size of each bucket in milliseconds
 *
 */
export function initializeBuckets({
  startMs,
  endMs,
  minBucketStartMs,
  maxBucketEndMs,
  bucketSizeMs,
}: {
  startMs: number;
  endMs: number;
  minBucketStartMs: number;
  maxBucketEndMs: number;
  bucketSizeMs: number;
}) {
  const bucketTimestamps: BucketMap = new Map();
  // Generate all possible buckets between the bucket with the minimum start time and the
  // bucket with the maximum end time
  for (let ts = minBucketStartMs; ts < maxBucketEndMs; ts += bucketSizeMs) {
    bucketTimestamps.set(ts, {
      bucketStart: new Date(ts),
      bucketEnd: new Date(ts + bucketSizeMs),
      status: "empty",
    });
  }

  // Fill in gaps between the bucket with the minimum start time and the requested start time
  for (let ts = minBucketStartMs; ts >= startMs; ts -= bucketSizeMs) {
    bucketTimestamps.set(ts, {
      bucketStart: new Date(ts),
      bucketEnd: new Date(ts + bucketSizeMs),
      status: "empty",
    });
  }

  // Fill in gaps between the bucket with the maximum start time and the requested end time
  for (let ts = maxBucketEndMs; ts <= endMs; ts += bucketSizeMs) {
    bucketTimestamps.set(ts, {
      bucketStart: new Date(ts),
      bucketEnd: new Date(ts + bucketSizeMs),
      status: "empty",
    });
  }

  return bucketTimestamps;
}

export function calculateClusterStatus(buckets: BucketMap) {
  const clusterStatusExists = new Set<MemDiskUtilizationStatus>();

  for (const bucket of buckets.values()) {
    clusterStatusExists.add(bucket.status);
  }

  // Status ranked from lowest to highest.
  const statusRanking = [
    "empty",
    "optimal",
    "suboptimal",
    "underProvisioned",
  ] as const;

  const clusterStatus = statusRanking.reduce((accum, status) => {
    if (clusterStatusExists.has(status)) {
      return status;
    }
    return accum;
  }, statusRanking[0]);

  return clusterStatus;
}
