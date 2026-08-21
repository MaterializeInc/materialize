// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import {
  calculateMemDiskUtilizationStatus,
  DEFAULT_THRESHOLD_PERCENTAGES,
  utilizationStatusToColor,
} from "~/platform/environment-overview/utils";
import { MaterializeTheme } from "~/theme";
import { formatPercentage } from "~/utils/format";

export interface PercentBarProps {
  /**
   * Utilization as a fraction of the allocation, where 1 is fully used.
   * Callers reading a relation that reports percentages must divide by 100.
   */
  fraction: number | null | undefined;
}

const PercentBar = ({ fraction }: PercentBarProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (fraction === null || fraction === undefined) {
    return (
      <Text as="span" color={colors.foreground.secondary}>
        —
      </Text>
    );
  }

  // Shares thresholds and palette with the utilization cards so one reading is
  // coloured the same wherever it appears. Thresholds see the unrounded value,
  // so a reading just above a boundary is coloured for where it sits rather
  // than for its rounded display value.
  const barColor = utilizationStatusToColor(
    calculateMemDiskUtilizationStatus({
      thresholdPercentages: DEFAULT_THRESHOLD_PERCENTAGES,
      peakMemDiskUtilizationPercent: fraction,
    }),
    colors,
  );

  return (
    <HStack spacing="2" minWidth="80px">
      <Text as="span" whiteSpace="nowrap" minWidth="32px">
        {formatPercentage(fraction, 1)}
      </Text>
      <Box
        width="48px"
        height="8px"
        borderRadius="full"
        bg={colors.background.secondary}
        overflow="hidden"
        flexShrink={0}
      >
        <Box
          height="100%"
          width={`${Math.min(1, fraction) * 100}%`}
          borderRadius="full"
          bg={barColor}
          transition="width 0.3s ease"
        />
      </Box>
    </HStack>
  );
};
export default PercentBar;
