// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { formatDurationForAxis } from "~/utils/format";

import { FreshnessBreakdown } from "./freshnessBreakdown";

export const FreshnessBreakdownStrip = ({
  breakdown,
}: {
  breakdown: FreshnessBreakdown;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { selfDelayMs, maxInputMs, isSelfBottleneck, dominantUpstreamNames } =
    breakdown;
  const upstreamLabel =
    dominantUpstreamNames.length > 0
      ? `${formatDurationForAxis(maxInputMs)} from ${dominantUpstreamNames.join(", ")}`
      : `${formatDurationForAxis(maxInputMs)} upstream`;

  return (
    <HStack
      width="100%"
      px={3}
      py={2}
      borderRadius="md"
      bg={colors.background.secondary}
      spacing={1}
      flexWrap="wrap"
    >
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        Output lag breakdown:
      </Text>
      <Text
        textStyle="text-ui-med"
        color={isSelfBottleneck ? colors.foreground.primary : colors.accent.red}
      >
        {upstreamLabel}
      </Text>
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        +
      </Text>
      <Text
        textStyle="text-ui-med"
        color={isSelfBottleneck ? colors.accent.red : colors.foreground.primary}
      >
        {formatDurationForAxis(selfDelayMs)} self-delay
      </Text>
    </HStack>
  );
};
