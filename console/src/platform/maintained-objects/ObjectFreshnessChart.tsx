// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Card,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { FreshnessGraph } from "~/components/FreshnessGraph/FreshnessGraph";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MaterializeTheme } from "~/theme";

import { useObjectFreshnessHistory } from "./queries";

const FRESHNESS_CHART_OPTIONS: Record<string, string> = {
  "60": "Last hour",
  "180": "Last 3 hours",
  "360": "Last 6 hours",
  "1440": "Last 24 hours",
};

export interface ObjectFreshnessChartProps {
  objectId: string;
}

export const ObjectFreshnessChart = ({
  objectId,
}: ObjectFreshnessChartProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "maintained-objects-freshness-chart-period",
    defaultValue: "60",
    timePeriodOptions: FRESHNESS_CHART_OPTIONS,
  });

  const lookbackMs = timePeriodMinutes * 60 * 1000;

  const { data, isLoading } = useObjectFreshnessHistory({
    objectId,
    lookbackMs,
  });

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      border="1px"
      borderColor={colors.border.primary}
    >
      <VStack align="start" spacing={3} width="100%">
        <HStack width="100%" justify="space-between">
          <Text textStyle="heading-sm">Freshness over time</Text>
          <TimePeriodSelect
            timePeriodMinutes={timePeriodMinutes}
            setTimePeriodMinutes={setTimePeriodMinutes}
            options={FRESHNESS_CHART_OPTIONS}
          />
        </HStack>

        {isLoading || !data ? (
          <Box height="180px" width="100%" display="flex" alignItems="center" justifyContent="center">
            <Spinner size="sm" />
          </Box>
        ) : data.historicalData.length === 0 ? (
          <Box height="180px" width="100%" display="flex" alignItems="center" justifyContent="center">
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              No freshness data available for this time period.
            </Text>
          </Box>
        ) : (
          <FreshnessGraph
            bucketSizeMs={data.bucketSizeMs}
            xAccessor={(d) => d.timestamp}
            lines={data.lines}
            data={data.historicalData}
            startTime={data.startTime}
            endTime={data.endTime}
          />
        )}
      </VStack>
    </Card>
  );
};

export default ObjectFreshnessChart;
