// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SubscribeRow } from "~/api/materialize/SubscribeManager";

/**
 * Data does not always arrive exactly on the interval, we account for this with a
 * percentage margin of error.
 */
export const DATA_INTERVAL_VARIANCE_MARGIN = 0.1;

/**
 * Builds a new array with any gaps in data filled based on previous values.
 *
 * Specifically this function handles the following cases.
 *
 * 1. The statistics have not changed during the time period, so we only get the snapshot.
 * In this case, we create points every 60s with the snapshot data. We don't have the
 * exact timestamps of the data collection, so those are synthetic in this case.
 *
 * 2. There is a gap in beginning of the data. In this case, we look forward to the first
 * update in the stream and work backwards so that the timestamps are properly aligned to
 * the collection interval.
 *
 * 3. Finally, if there are gaps in the middle or end of the data, we use the most recent
 * update timestamp to create new point for each collection interval until we find more
 * data points or reach the most recent progress message.
 */
export function normalizeStatisticsData<T>(
  rawData: SubscribeRow<T>[],
  dataInterval: number,
) {
  const latestTimestamp = rawData.at(-1)?.mzTimestamp;

  const dataPoints = rawData.filter((row) => !row.mzProgressed);
  const snapshot = dataPoints[0];
  if (!snapshot) return [];

  // The first point is the snapshot. We can't rely on the timestamp to align with data
  // collection, since our AS OF is based on arbitrary wall clock time. However, we need
  // it so we know if a connector just hasn't produced any data data during the time
  // peroid, or if the connector is brand new. In the former case, we will create synthetic
  // points with 0 rates to avoid weird artifacts in the graph.
  const syntheticData: SubscribeRow<T>[] = [];

  if (!latestTimestamp) {
    // If we don't have any progress messages, the connector is stuck or the data is still
    // loading.
    return syntheticData;
  }
  if (dataPoints.length < 2) {
    // If we only get the snapshot and no other points, there is no data to show, so we
    // will synthesize all zeros to fill the graph.
    const newInitialPoints = numberOfPointsToCreate(
      snapshot.mzTimestamp,
      latestTimestamp,
      dataInterval,
    );
    // i = 1 so we skip the snapshot
    for (let i = 1; i <= newInitialPoints; i++) {
      const timestamp = snapshot.mzTimestamp + i * dataInterval;
      syntheticData.push({
        data: {
          ...snapshot.data,
        },
        mzProgressed: false,
        mzState: "upsert",
        mzTimestamp: timestamp,
      });
    }
    return syntheticData;
  }

  const firstAlignedPoint = dataPoints[1];
  const newInitialPoints = numberOfPointsToCreate(
    snapshot.mzTimestamp,
    dataPoints[1].mzTimestamp,
    dataInterval,
  );
  for (let i = newInitialPoints; i > 0; i--) {
    // We want to align the synthetic points to the other data points.
    const timestamp = firstAlignedPoint.mzTimestamp - i * dataInterval;
    syntheticData.push({
      data: {
        ...snapshot.data,
      },
      mzProgressed: false,
      mzState: "upsert",
      mzTimestamp: timestamp,
    });
  }

  let index = 1; // skip the snapshot
  while (dataPoints[index]?.mzTimestamp <= latestTimestamp) {
    const current = dataPoints[index];
    const next = dataPoints[index + 1];
    const endingTimestamp = next ? next.mzTimestamp : latestTimestamp;
    const diff = endingTimestamp - current.mzTimestamp;
    syntheticData.push(current);

    if (diff > dataInterval * (1 + DATA_INTERVAL_VARIANCE_MARGIN)) {
      const numberToCreate = numberOfPointsToCreate(
        current.mzTimestamp,
        endingTimestamp,
        dataInterval,
      );
      for (let i = 1; i <= numberToCreate; i++) {
        const timestamp = current.mzTimestamp + i * dataInterval;
        syntheticData.push({
          data: {
            ...current.data,
          },
          mzProgressed: false,
          mzState: "upsert",
          mzTimestamp: timestamp,
        });
      }
    }
    index++;
  }
  return syntheticData;
}

/**
 * How many points to create for between a pair of timestamps, inclusive of the end time.
 */
function numberOfPointsToCreate(
  startingTimestamp: number,
  endingTimestamp: number,
  dataInterval: number,
) {
  // This can be < dataInterval if we have updates for the beginning of the time period.
  // It can be arbitrarily large if there is a gap at the beginning of the time period.
  // We also add a margin of error because the diff can be a few milliseconds off.
  const snapshotDiff =
    endingTimestamp -
    startingTimestamp +
    dataInterval * DATA_INTERVAL_VARIANCE_MARGIN;
  return Math.floor(snapshotDiff / dataInterval) - 1;
}

export type BucketedData<T> = {
  timestamp: number;
  dataPoints: Array<SubscribeRow<T>>;
};

/**
 * Buckets data based on an array of end times. Assumes all data points are valid, since
 * normalizeStatisticsData filters out the snapshot.
 */
export function bucketPoints<T>(
  syntheticData: SubscribeRow<T>[],
  bucketEndTimes: number[],
  bucketSizeMs: number,
) {
  const buckets: Array<BucketedData<T>> = [];
  let dataIndex = 0;
  for (const currentBucketEnd of bucketEndTimes) {
    const currentBucketStart = currentBucketEnd - bucketSizeMs;
    const dataPoints: Array<SubscribeRow<T>> = [];
    buckets.push({ timestamp: currentBucketEnd, dataPoints });
    let currentPoint = syntheticData[dataIndex];

    if (!currentPoint) continue;
    if (currentPoint.mzTimestamp > currentBucketEnd) continue;

    while (currentPoint && currentPoint.mzTimestamp <= currentBucketEnd) {
      // Don't include data that is outside the bucket. This should only happen at the
      // beginning of the graph, because we add padding to the start of the subscribe to
      // ensure we have non-snapshot data points for the first bucket.
      if (currentPoint.mzTimestamp >= currentBucketStart) {
        dataPoints.push(currentPoint);
      }
      dataIndex++;
      currentPoint = syntheticData[dataIndex];
    }
  }

  return buckets;
}

export function aggregateBucketData<StatDataPoint, StatGraphDataPoint>(
  buckets: BucketedData<StatDataPoint>[],
  aggregator: (
    start: SubscribeRow<StatDataPoint> | undefined,
    dataPoints: SubscribeRow<StatDataPoint>[],
  ) => Omit<StatGraphDataPoint, "timestamp">,
): StatGraphDataPoint[] {
  const result: StatGraphDataPoint[] = [];
  for (let i = 0; i < buckets.length; i++) {
    const { timestamp, dataPoints } = buckets[i];
    let start = undefined;
    if (dataPoints.length > 1) {
      start = dataPoints.at(0);
    } else {
      let lookback = i - 1;
      start = buckets[lookback]?.dataPoints.at(-1);
      while (start === undefined && i > 0) {
        const lookbackBucket = buckets[lookback];
        if (!lookbackBucket) break;

        // If we don't have two points, look back at previous buckets.
        // This always happens on the 1 hour graph, because the bucket size is only 2
        // minutes, so buckets often only contain a single point.
        start = lookbackBucket.dataPoints.at(-1);
        lookback--;
      }
    }
    result.push({
      timestamp,
      ...aggregator(start, dataPoints),
    } as StatGraphDataPoint);
  }
  return result;
}
