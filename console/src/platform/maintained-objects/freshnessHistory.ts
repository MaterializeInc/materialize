// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  formatFullyQualifiedObjectName,
  IPostgresInterval,
} from "~/api/materialize";
import { calculateBucketSizeFromLookback } from "~/api/materialize/freshness/lagHistory";
import { DataPoint, GraphLineSeries } from "~/components/FreshnessGraph/types";
import { sumPostgresIntervalMs } from "~/util";

export interface LagHistoryRow {
  bucketStart: Date;
  objectId: string;
  lag: IPostgresInterval | null;
  schemaName: string | null;
  objectName: string | null;
}

export interface ObjectFreshnessHistory {
  historicalData: DataPoint[];
  lines: GraphLineSeries[];
  bucketSizeMs: number;
  startTime: number;
  endTime: number;
}

export const buildObjectFreshnessHistory = (
  rows: LagHistoryRow[],
  lookbackMs: number,
): ObjectFreshnessHistory => {
  const bucketSizeMs = calculateBucketSizeFromLookback(lookbackMs);

  const historicalData: DataPoint[] = rows.map((row) => ({
    timestamp: row.bucketStart.getTime(),
    lag: {
      [row.objectId]: row.lag
        ? {
            queryable: true as const,
            totalMs: sumPostgresIntervalMs(row.lag),
            interval: row.lag,
            schemaName: row.schemaName,
            objectName: row.objectName,
          }
        : {
            queryable: false as const,
            schemaName: row.schemaName,
            objectName: row.objectName,
          },
    },
  }));

  const lastRow = rows.at(-1);
  const lines: GraphLineSeries[] = lastRow
    ? [
        {
          key: lastRow.objectId,
          label: formatFullyQualifiedObjectName({
            schemaName: lastRow.schemaName ?? "",
            name: lastRow.objectName ?? "",
          }),
          yAccessor: (d: DataPoint) => {
            const lagInfo = d.lag[lastRow.objectId];
            return lagInfo?.queryable ? lagInfo.totalMs : null;
          },
        },
      ]
    : [];

  return {
    historicalData,
    lines,
    bucketSizeMs,
    startTime: historicalData.at(0)?.timestamp ?? 0,
    endTime: historicalData.at(-1)?.timestamp ?? 0,
  };
};

export interface PMaxLag {
  value: IPostgresInterval;
  ms: number;
}

export const pMaxFromHistory = (
  historicalData: DataPoint[],
  objectId: string,
): PMaxLag | null => {
  let maxMs = -1;
  let maxInterval: IPostgresInterval | null = null;
  for (const point of historicalData) {
    const lagInfo = point.lag[objectId];
    if (lagInfo?.queryable && lagInfo.totalMs > maxMs) {
      maxMs = lagInfo.totalMs;
      maxInterval = lagInfo.interval;
    }
  }
  return maxInterval ? { value: maxInterval, ms: maxMs } : null;
};
