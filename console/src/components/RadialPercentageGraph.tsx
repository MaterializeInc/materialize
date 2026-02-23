// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, useTheme, VStack } from "@chakra-ui/react";
import { Group } from "@visx/group";
import { Arc } from "@visx/shape";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { toRadians } from "~/utils/geometry";

const ARC_START = -115;
const ARC_END = 115;
const ARC_RANGE = Math.abs(ARC_START) + Math.abs(ARC_END);

export interface RadialPercentageGraphProps {
  percentage: number;
  primaryColor?: string;
}

const SIZE = 80;

export const RadialPercentageGraph = ({
  percentage,
  children,
  primaryColor,
}: React.PropsWithChildren<RadialPercentageGraphProps>) => {
  const { colors } = useTheme<MaterializeTheme>();

  const radius = SIZE / 2;
  const centerY = SIZE / 2;
  const centerX = SIZE / 2;
  const barThickness = 10;
  const endAngle = ARC_RANGE * (percentage / 100) + ARC_START;

  return (
    <VStack gap="0" position="relative">
      <svg width={SIZE} height={SIZE}>
        <Group top={centerY} left={centerX}>
          <Arc
            startAngle={toRadians(ARC_START)}
            endAngle={toRadians(ARC_END)}
            outerRadius={radius}
            innerRadius={radius - barThickness}
            fill={colors.background.tertiary}
          />
          <Arc
            startAngle={toRadians(ARC_START)}
            endAngle={toRadians(endAngle)}
            outerRadius={radius}
            innerRadius={radius - barThickness}
            fill={primaryColor ?? colors.accent.purple}
          />
        </Group>
      </svg>
      <Text
        color={colors.foreground.secondary}
        textStyle="text-small"
        position="absolute"
        top="28px"
      >
        {percentage.toFixed(0)}%
      </Text>
      <VStack gap="1" mt="-5">
        {children}
      </VStack>
    </VStack>
  );
};
