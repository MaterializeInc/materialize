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

import { Source } from "~/api/materialize/source/sourceList";
import {
  COLLECTION_INTERVAL_MS,
  SourceStatisticsDataPoint,
} from "~/api/materialize/source/sourceStatistics";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { ConnectorStatisticsGraph } from "~/platform/connectors/ConnectorStatisticsGraph";
import {
  aggregateBucketData,
  bucketPoints,
  normalizeStatisticsData,
} from "~/platform/connectors/graph";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";

import { useSourceStatistics } from "../queries";
import { bucketAggregator, sourceReplicationMessagesAreRows } from "./utils";

export interface SourceStatisticsProps {
  source: Source;
  timePeriodMinutes: number;
}

export type SourceStatisticsGraphDataPoint = {
  messagesReceivedPerSecond: number | null;
  bytesReceivedPerSecond: number | null;
  updatesStagedPerSecond: number | null;
  updatesCommittedPerSecond: number | null;
  offsetDelta: number | null;
  timestamp: number;
};

const SourceStatisticsGraph =
  ConnectorStatisticsGraph<SourceStatisticsGraphDataPoint>;

export const SourceStatisticsInner = (props: SourceStatisticsProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const {
    data: rawData,
    currentStartTime,
    currentEndTime,
    paddedStartTime,
  } = useSourceStatistics({
    sourceId: props.source.id,
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
      SourceStatisticsDataPoint,
      SourceStatisticsGraphDataPoint
    >(bucketedData, bucketAggregator);
    const startTimestamp = currentStartTime.getTime();
    // Filter out any data before the graph start time
    return aggregated.filter((d) => d.timestamp > startTimestamp);
  }, [bucketSizeMs, buckets, currentStartTime, rawData]);

  const offsetDeltaData = React.useMemo(
    () => graphData.filter((d) => d.offsetDelta !== null),
    [graphData],
  );

  const messagesReceivedLines = React.useMemo(
    () => [
      {
        key: "messagesReceivedPerSecond" as const,
        label: sourceReplicationMessagesAreRows(props.source.type)
          ? "Rows"
          : "Messages",
        color: colors.accent.purple,
      },
    ],
    [colors.accent.purple, props.source.type],
  );
  const bytesReceivedLines = React.useMemo(
    () => [
      {
        key: "bytesReceivedPerSecond" as const,
        label: "Bytes received",
        color: colors.accent.purple,
      },
    ],
    [colors.accent.purple],
  );
  const updatesCommittedLines = React.useMemo(
    () => [
      {
        key: "updatesStagedPerSecond" as const,
        label: "Staged",
        color: colors.foreground.tertiary,
      },
      // we want updatesCommitted last, so that it appears on top when the lines overlap
      {
        key: "updatesCommittedPerSecond" as const,
        label: "Committed",
        color: colors.accent.purple,
      },
    ],
    [colors.accent.purple, colors.foreground.tertiary],
  );
  const ingestionLagLines = React.useMemo(
    () => [
      {
        key: "offsetDelta" as const,
        label: "Upstream lag",
        color: colors.accent.purple,
      },
    ],
    [colors.accent.purple],
  );

  return (
    <VStack alignItems="flex-start" width="100%" spacing="64px" mb="64px">
      {props.source.type !== "webhook" && (
        <SourceStatisticsGraph
          title="Ingestion lag"
          lines={ingestionLagLines}
          data={offsetDeltaData}
          bucketSizeMs={bucketSizeMs}
          startTime={currentStartTime}
          endTime={currentEndTime}
          currentValueKey="offsetDelta"
          unitLabel={(value, yAxisUnit) => {
            switch (props.source.type) {
              case "postgres":
                return `${yAxisUnit} WAL`;
              case "kafka":
                return `${pluralize(value ?? 0, "msg", "msgs")}`;
              case "mysql":
                return `${pluralize(value ?? 0, "transaction", "transactions")}`;
              default:
                return `${pluralize(value ?? 0, "update", "updates")}`;
            }
          }}
          yAxisUnitType={props.source.type === "postgres" ? "bytes" : undefined}
        />
      )}
      <SourceStatisticsGraph
        title={
          sourceReplicationMessagesAreRows(props.source.type)
            ? "Rows received"
            : "Messages received"
        }
        lines={messagesReceivedLines}
        data={graphData}
        bucketSizeMs={bucketSizeMs}
        startTime={currentStartTime}
        endTime={currentEndTime}
        currentValueKey="messagesReceivedPerSecond"
        unitLabel={(value) =>
          sourceReplicationMessagesAreRows(props.source.type)
            ? pluralize(value ?? 0, "row/s", "rows/s")
            : pluralize(value ?? 0, "msg/s", "msgs/s")
        }
      />
      <SourceStatisticsGraph
        title="Bytes received"
        lines={bytesReceivedLines}
        yAxisUnitType="bytes"
        data={graphData}
        bucketSizeMs={bucketSizeMs}
        startTime={currentStartTime}
        endTime={currentEndTime}
        currentValueKey="bytesReceivedPerSecond"
        unitLabel={(_, yAxisUnit) => `${yAxisUnit}/s`}
      />
      <SourceStatisticsGraph
        title="Ingestion rate"
        lines={updatesCommittedLines}
        data={graphData}
        bucketSizeMs={bucketSizeMs}
        startTime={currentStartTime}
        endTime={currentEndTime}
        currentValueKey="updatesCommittedPerSecond"
        unitLabel={(value) =>
          sourceReplicationMessagesAreRows(props.source.type)
            ? pluralize(value ?? 0, "row/s", "rows/s")
            : pluralize(value ?? 0, "msg/s", "msgs/s")
        }
      />
    </VStack>
  );
};

export const SourceStatistics = (props: SourceStatisticsProps) => {
  return (
    <AppErrorBoundary message="An error occurred loading source progress.">
      <SourceStatisticsInner {...props} />
    </AppErrorBoundary>
  );
};
