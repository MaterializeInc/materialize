// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme } from "@chakra-ui/react";
import { TooltipInPortalProps } from "@visx/tooltip/lib/hooks/useTooltipInPortal";
import React from "react";

import { GraphTooltip, TooltipColorSwatch } from "~/components/graphComponents";
import { MaterializeTheme } from "~/theme";
import { formatDate } from "~/utils/dateFormat";
import { ByteUnit } from "~/utils/format";

export type GraphDataPoint = { [s: string]: number | null; timestamp: number };

export type ConnectorStatisticsTooltipData<GDP extends GraphDataPoint> = {
  nearestDatum: GDP;
  points: Array<{
    key: Extract<keyof GDP, string>;
    label?: string;
    color: string;
    x: number;
    y: number;
  }>;
  nearestPointX: number;
};

export interface ConnectorStatisticsTooltipProps<GDP extends GraphDataPoint> {
  bucketSizeMs: number;
  component: React.FC<TooltipInPortalProps>;
  formatValue: (value: number | null) => string;
  tooltipData: ConnectorStatisticsTooltipData<GDP>;
  left: number;
  top: number;
  unitLabel: (value: number | null, yAxisUnit: ByteUnit | null) => string;
  yAxisUnit: ByteUnit | null;
}

export const ConnectorStatisticsTooltip = <GDP extends GraphDataPoint>({
  tooltipData,
  ...props
}: ConnectorStatisticsTooltipProps<GDP>) => {
  const { colors } = useTheme<MaterializeTheme>();
  const endTime = tooltipData.nearestDatum.timestamp;
  const startTime = endTime - props.bucketSizeMs;

  return (
    <GraphTooltip {...props}>
      <Box
        width="100%"
        borderBottomWidth="1px"
        borderColor={colors.border.secondary}
        px="4"
        py="2"
        textStyle="text-ui-med"
      >
        {tooltipData.points.map(({ key, label, color }) => {
          const value = tooltipData.nearestDatum[key];
          const formattedValue = props.formatValue(value);
          // this could be NaN, but that's ok, since we don't actually display it
          const displayedNumber = Number.parseFloat(formattedValue);
          // Round trip the value through our formatter to correctly pluralize rounded
          // values.
          const unitLabel = props.unitLabel(displayedNumber, props.yAxisUnit);
          return (
            <HStack
              alignItems="flex-start"
              key={key}
              justifyContent="space-between"
              gap={4}
            >
              <HStack alignItems="center" width="100%" gap={2}>
                <TooltipColorSwatch color={color} />
                <Text>{label}</Text>
              </HStack>
              <Text whiteSpace="nowrap">
                {formattedValue}{" "}
                <Text as="span" color={colors.foreground.secondary}>
                  {unitLabel}
                </Text>
              </Text>
            </HStack>
          );
        })}
      </Box>
      <Box py="2" px="4">
        <Text textStyle="text-small">
          {formatDate(startTime, "MMM. dd")}
          {" · "}
          {formatDate(startTime, "HH:mm")}
          {" –  "}
          {formatDate(endTime, "MMM. dd")}
          {" · "}
          {formatDate(endTime, "HH:mm z")}
        </Text>
      </Box>
    </GraphTooltip>
  );
};
