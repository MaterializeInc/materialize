// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SourceStatisticsDataPoint } from "~/api/materialize/source/sourceStatistics";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { notNullOrUndefined } from "~/util";

import { SourceStatisticsRow } from "../queries";

export function sourceReplicationMessagesAreRows(sourceType: string) {
  switch (sourceType) {
    case "postgres":
    case "mysql":
      return true;
    default:
      return false;
  }
}

export function calculateRate(
  current: SourceStatisticsRow | undefined,
  previous: SourceStatisticsRow | undefined,
  key: keyof Omit<
    SourceStatisticsRow["data"],
    "id" | "offsetKnown" | "offsetCommitted" | "rehydrationLatency"
  >,
) {
  if (current === undefined || previous === undefined) return null;

  const timeDiff = current.mzTimestamp - previous.mzTimestamp;
  const currentDatum = Number(current.data[key]);
  const previousDataum = Number(previous.data[key]);
  if (currentDatum === null || previousDataum === null) return null;

  const result = ((currentDatum - previousDataum) / timeDiff) * 1000;
  if (Number.isNaN(result)) return 0;
  if (result < 0) return 0;
  return result;
}

export function bucketAggregator(
  start: SubscribeRow<SourceStatisticsDataPoint> | undefined,
  dataPoints: SubscribeRow<SourceStatisticsDataPoint>[],
) {
  const end = dataPoints.at(-1);
  const offsetDeltaValues = dataPoints
    .map((p) => p.data.offsetDelta)
    .filter(notNullOrUndefined);
  return {
    messagesReceivedPerSecond: calculateRate(end, start, "messagesReceived"),
    bytesReceivedPerSecond: calculateRate(end, start, "bytesReceived"),
    updatesStagedPerSecond: calculateRate(end, start, "updatesStaged"),
    updatesCommittedPerSecond: calculateRate(end, start, "updatesCommitted"),
    offsetDelta:
      offsetDeltaValues.length > 0 ? Math.max(...offsetDeltaValues) : null,
  };
}
