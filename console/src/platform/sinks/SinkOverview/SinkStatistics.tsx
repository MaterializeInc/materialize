// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme, VStack } from "@chakra-ui/react";
import React from "react";

import { Sink } from "~/api/materialize/sink/sinkList";
import {
  COLLECTION_INTERVAL_MS,
  SinkStatisticsDataPoint,
} from "~/api/materialize/sink/sinkStatistics";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { ConnectorStatisticsGraph } from "~/platform/connectors/ConnectorStatisticsGraph";
import {
  aggregateBucketData,
  bucketPoints,
  normalizeStatisticsData,
} from "~/platform/connectors/graph";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";

import { useSinkStatistics } from "../queries";
import { calculateRate } from "./utils";

export interface SinkStatisticsProps {
  sink: Sink;
  timePeriodMinutes: number;
}

export type SinkStatisticsGraphDataPoint = {
  messagesStagedPerSecond: number | null;
  messagesCommittedPerSecond: number | null;
  bytesStagedPerSecond: number | null;
  bytesCommittedPerSecond: number | null;
  timestamp: number;
};

const SinkStatisticsGraph =
  ConnectorStatisticsGraph<SinkStatisticsGraphDataPoint>;

function bucketAggregator(
  start: SubscribeRow<SinkStatisticsDataPoint> | undefined,
  dataPoints: SubscribeRow<SinkStatisticsDataPoint>[],
) {
  const end = dataPoints.at(-1);
  return {
    messagesStagedPerSecond: calculateRate(end, start, "messagesStaged"),
    messagesCommittedPerSecond: calculateRate(end, start, "messagesCommitted"),
    bytesStagedPerSecond: calculateRate(end, start, "bytesStaged"),
    bytesCommittedPerSecond: calculateRate(end, start, "bytesCommitted"),
  };
}

export const SinkStatisticsInner = (props: SinkStatisticsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const {
    data: rawData,
    currentStartTime,
    currentEndTime,
    paddedStartTime,
  } = useSinkStatistics({
    sinkId: props.sink.id,
    timePeriodMinutes: props.timePeriodMinutes,
  });

  // We bucket the data to make the graph easier to read
  const bucketSizeMs = React.useMemo(
    // Never use a bucket size smaller than the collection interval
    () => Math.max(props.timePeriodMinutes * 1000, COLLECTION_INTERVAL_MS),
    [props.timePeriodMinutes],
  );

  // An array of timestamps that represents the buckets we actually display on the graph
  const buckets = React.useMemo(() => {
    const startTimestamp = paddedStartTime.getTime();
    const result = [];
    let currentBucket = startTimestamp + bucketSizeMs;
    while (currentBucket <= currentEndTime.getTime()) {
      result.push(currentBucket);
      currentBucket += bucketSizeMs;
    }
    return result;
  }, [paddedStartTime, bucketSizeMs, currentEndTime]);

  const graphData = React.useMemo(() => {
    const snapshot = rawData[0];
    if (!snapshot) return [];

    const syntheticData = normalizeStatisticsData(
      rawData,
      COLLECTION_INTERVAL_MS,
    );
    const bucketedData = bucketPoints(syntheticData, buckets, bucketSizeMs);
    const aggregated = aggregateBucketData<
      SinkStatisticsDataPoint,
      SinkStatisticsGraphDataPoint
    >(bucketedData, bucketAggregator);
    const startTimestamp = currentStartTime.getTime();
    // Filter out any data before the graph start time
    return aggregated.filter((d) => d.timestamp > startTimestamp);
  }, [bucketSizeMs, buckets, currentStartTime, rawData]);

  const bytesProducedLines = React.useMemo(
    () => [
      {
        key: "bytesStagedPerSecond" as const,
        label: "Staged",
        color: colors.foreground.tertiary,
      },
      {
        key: "bytesCommittedPerSecond" as const,
        label: "Committed",
        color: colors.accent.purple,
      },
    ],
    [colors.accent.purple, colors.foreground.tertiary],
  );
  const throughputLines = React.useMemo(
    () => [
      {
        key: "messagesStagedPerSecond" as const,
        label: "Staged",
        color: colors.foreground.tertiary,
      },
      {
        key: "messagesCommittedPerSecond" as const,
        label: "Committed",
        color: colors.accent.purple,
      },
    ],
    [colors.accent.purple, colors.foreground.tertiary],
  );

  return (
    <VStack alignItems="flex-start" width="100%" spacing="64px" mb="64px">
      <SinkStatisticsGraph
        title="Messages produced"
        lines={throughputLines}
        data={graphData}
        bucketSizeMs={bucketSizeMs}
        startTime={currentStartTime}
        endTime={currentEndTime}
        currentValueKey="messagesCommittedPerSecond"
        unitLabel={(value) => pluralize(value ?? 0, "msg/s", "msgs/s")}
      />
      <SinkStatisticsGraph
        title="Bytes produced"
        lines={bytesProducedLines}
        yAxisUnitType="bytes"
        data={graphData}
        bucketSizeMs={bucketSizeMs}
        startTime={currentStartTime}
        endTime={currentEndTime}
        currentValueKey="bytesCommittedPerSecond"
        yAxisUnitLabel={(label, _) => label}
        unitLabel={(_, yAxisUnit) => `${yAxisUnit}/s`}
      />
    </VStack>
  );
};

export const SinkStatistics = (props: SinkStatisticsProps) => {
  return (
    <AppErrorBoundary message="An error occurred loading sink progress.">
      <SinkStatisticsInner {...props} />
    </AppErrorBoundary>
  );
};
