// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Center, Spinner, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { FreshnessGraph } from "~/components/FreshnessGraph/FreshnessGraph";
import { MaterializeTheme } from "~/theme";

import { useObjectFreshnessHistory } from "./queries";

export interface ObjectFreshnessChartProps {
  objectId: string;
  timePeriodMinutes: number;
  onTimestampSelect?: (timestamp: Date | null) => void;
  onTimestampLock?: (timestamp: Date | null) => void;
  selectedTimestamp?: Date | null;
  isLocked?: boolean;
}

export const ObjectFreshnessChart = ({
  objectId,
  timePeriodMinutes,
  onTimestampSelect,
  onTimestampLock,
  selectedTimestamp,
  isLocked,
}: ObjectFreshnessChartProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const lookbackMs = timePeriodMinutes * 60 * 1000;

  const { data, isLoading } = useObjectFreshnessHistory({
    objectId,
    lookbackMs,
  });

  if (isLoading || !data) {
    return (
      <Center height="180px" width="100%">
        <Spinner size="sm" />
      </Center>
    );
  }
  if (data.historicalData.length === 0) {
    return (
      <Center height="180px" width="100%">
        <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
          No freshness data available for this time period.
        </Text>
      </Center>
    );
  }

  const isInteractive = Boolean(onTimestampSelect || onTimestampLock);

  return (
    <FreshnessGraph
      bucketSizeMs={data.bucketSizeMs}
      xAccessor={(d) => d.timestamp}
      lines={data.lines}
      data={data.historicalData}
      startTime={data.startTime}
      endTime={data.endTime}
      variant={isInteractive ? "interactive" : "static"}
      onPointMove={
        onTimestampSelect
          ? (ts) => onTimestampSelect(ts !== null ? new Date(ts) : null)
          : undefined
      }
      onPointClick={
        onTimestampLock
          ? (ts) => onTimestampLock(ts !== null ? new Date(ts) : null)
          : undefined
      }
      selectedTimestamp={selectedTimestamp?.getTime() ?? null}
      isLocked={isLocked}
    />
  );
};

export default ObjectFreshnessChart;
