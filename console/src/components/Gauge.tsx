// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { chakra, useTheme } from "@chakra-ui/react";
import { Group } from "@visx/group";
import { Arc, Circle, Polygon } from "@visx/shape";
import React, { ComponentProps } from "react";

import { MaterializeTheme } from "~/theme";
import { toDegrees, toRadians } from "~/utils/geometry";

const START_ANGLE = toRadians(205);
const END_ANGLE = toRadians(-25);
const ARC_RANGE = Math.abs(START_ANGLE) + Math.abs(END_ANGLE);

/**
 *
 * @param angle - Angle in the cartesian plane in radians
 * @returns - An angle mapped to the plane for Visx's Arc component
 */
function toVisxArcAngle(angle: number) {
  // We need to rotate each angle clockwise 90 degrees and flip them since Visx's Arc component rotation starts
  // from the positive y axis and continues clockwise
  return toRadians(180) - (angle + toRadians(90));
}

const GaugeTriangle = ({
  top,
  left,
  height,
  percentage,
  triangleColor,
}: {
  top?: number;
  left?: number;
  triangleColor?: string;
  height: number;
  percentage: number;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  const gaugeTriangleRadius = 6;

  // We create a symmetrical isosceles triangle, fix the middle-bottom point to the center of the gauge, and rotate it
  const trianglePoints = [
    [-gaugeTriangleRadius, 0],
    [gaugeTriangleRadius, 0],
    // We must invert the y coordinates since SVG grid coordinates ascend from top-left to bottom-right.
    [0, -height],
  ] as [number, number][];

  return (
    <Group top={top} left={left}>
      <Polygon
        points={trianglePoints}
        stroke={colors.border.primary}
        fill={triangleColor}
        transform={`rotate(${toDegrees(toVisxArcAngle(START_ANGLE - percentage * ARC_RANGE))}, 0, 0)`}
      />
      <Circle
        r={gaugeTriangleRadius}
        strokeWidth="3"
        stroke={colors.border.primary}
        fill={colors.foreground.tertiary}
      />
    </Group>
  );
};

export interface GaugeProps {
  percentage: number;
  size?: number;
  paddingThickness?: number;
  gaugeThickness?: number;

  tickMarks?: {
    startPercentage: number;
    endPercentage: number;
    color: string;
  }[];

  containerProps?: ComponentProps<typeof chakra.svg>;
}

export const Gauge = ({
  percentage,
  tickMarks,
  size = 80,
  gaugeThickness = 12,
  paddingThickness = 2,
  containerProps,
}: GaugeProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const radius = size / 2;
  const centerY = size / 2;
  const centerX = size / 2;

  const gaugeInnerRadius = radius - gaugeThickness;

  const gaugeTriangleHeight = gaugeInnerRadius - paddingThickness * 4;

  const tickMarkArcs = (tickMarks ?? []).map(
    ({ startPercentage, endPercentage, color }, i) => (
      <Arc
        key={i}
        startAngle={toVisxArcAngle(START_ANGLE - ARC_RANGE * startPercentage)}
        endAngle={toVisxArcAngle(START_ANGLE - ARC_RANGE * endPercentage)}
        outerRadius={radius}
        innerRadius={gaugeInnerRadius}
        fill={color}
        stroke={colors.border.primary}
      />
    ),
  );

  const gaugeTriangleColor =
    tickMarks?.find(
      ({ startPercentage, endPercentage }) =>
        percentage >= startPercentage && percentage <= endPercentage,
    )?.color ?? colors.accent.purple;

  return (
    <chakra.svg width={`${size}px`} height={`${size}px`} {...containerProps}>
      <GaugeTriangle
        top={centerY}
        left={centerX}
        height={gaugeTriangleHeight}
        percentage={percentage}
        triangleColor={gaugeTriangleColor}
      />
      <Group top={centerY} left={centerX}>
        {tickMarkArcs}
      </Group>
    </chakra.svg>
  );
};
