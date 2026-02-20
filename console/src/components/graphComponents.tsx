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
  StackProps,
  Text,
  useColorMode,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { Line } from "@visx/shape";
import { TooltipInPortalProps } from "@visx/tooltip/lib/hooks/useTooltipInPortal";
import React from "react";

import { MODAL_TOOLTIP_Z_INDEX } from "~/layouts/zIndex";
import { MaterializeTheme } from "~/theme";
import { Point } from "~/types/geometry";
import { DATE_FORMAT, TIME_FORMAT } from "~/utils/dateFormat";
import { formatDate, formatDateInUtc } from "~/utils/dateFormat";

export interface TooltipProps {
  top: number;
  left: number;
  component: React.FC<TooltipInPortalProps>;
  children: React.ReactNode;
}

export const GraphTooltip = ({
  top,
  left,
  component: TooltipInPortal,
  children,
}: TooltipProps) => {
  const { colors, radii, shadows } = useTheme<MaterializeTheme>();
  const mode = useColorMode();

  return (
    <TooltipInPortal
      data-testid="chart-tooltip"
      top={top}
      left={left}
      style={{
        backgroundColor:
          mode.colorMode === "dark"
            ? colors.background.secondary
            : colors.background.primary,
        borderRadius: radii.lg,
        borderWidth: "1px",
        boxShadow: shadows.level3,
        minWidth: 60,
        overflow: "hidden",
        position: "absolute",
        zIndex: MODAL_TOOLTIP_Z_INDEX,
      }}
      /**
       * TooltipInPortal uses https://github.com/airbnb/visx/blob/master/packages/visx-bounds/src/enhancers/withBoundingRects.tsx
       * to figure out bounds detection on mount. However if it doesn't remount and recalibrate its dimensions,
       * you'll get a case where bounds detection won't work properly. Changing the key per render forces a remount.
       */
      key={Math.random()}
    >
      {children}
    </TooltipInPortal>
  );
};

export interface GraphTooltipCursorProps {
  points: Array<{
    key: string;
    color: string;
    x: number;
    y: number;
  }>;
}

export const GraphTooltipCursor = (props: GraphTooltipCursorProps) => {
  return (
    <g>
      {props.points.map(({ x, y, key, color }) => (
        <circle
          key={key}
          cx={x}
          cy={y}
          r={4}
          fill={color}
          stroke="white"
          strokeWidth={1}
          pointerEvents="none"
        />
      ))}
    </g>
  );
};

export interface GraphLineCursorProps {
  graphBottom: number;
  graphTop: number;
  point: Point;
}

/**
 * A vertical line that follows the cursor.
 */
export const GraphLineCursor = (props: GraphLineCursorProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Line
      from={{ x: props.point.x, y: props.graphTop }}
      to={{ x: props.point.x, y: props.graphBottom }}
      stroke={colors.foreground.tertiary}
      strokeWidth={1}
      pointerEvents="none"
    />
  );
};

/**
 * A transparent rectangle that covers the graph and emits pointer events.
 */
export const GraphEventOverlay = (props: {
  width: number;
  height: number;
  onPointerMove: (event: React.PointerEvent<SVGRectElement>) => void;
  onPointerLeave: (event: React.PointerEvent<SVGRectElement>) => void;
  x: number;
  y: number;
}) => {
  return <rect fill="transparent" {...props} />;
};

/**
 * For the tooltip of a graph where data is divided in buckets,
 * shows the current bucket range in UTC and local time.
 */
export const GraphTooltipBucketRange = ({
  containerProps,
  bucketStart,
  bucketEnd,
}: {
  bucketStart: Date;
  bucketEnd: Date;
  containerProps?: StackProps;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <VStack
      spacing={0}
      alignItems="flex-start"
      paddingX="4"
      paddingY="2"
      width="100%"
      {...containerProps}
    >
      <Text color={colors.foreground.secondary} textStyle="text-ui-reg">
        <Text color={colors.foreground.primary} as="span" mr="4">
          {formatDate(bucketStart, DATE_FORMAT)}
        </Text>
        {formatDate(bucketStart, TIME_FORMAT)}-
        {formatDate(bucketEnd, TIME_FORMAT + " z")}
      </Text>
      <Text color={colors.foreground.secondary} textStyle="text-ui-reg">
        <Text color={colors.foreground.primary} as="span" mr="4">
          {formatDateInUtc(bucketStart, DATE_FORMAT)}
        </Text>
        {formatDateInUtc(bucketStart, TIME_FORMAT)}
        {`-${formatDateInUtc(bucketEnd, TIME_FORMAT)} UTC`}
      </Text>
    </VStack>
  );
};

export const TooltipColorSwatch = ({ color }: { color: string }) => {
  return <Box width="2" height="2" background={color} borderRadius="sm" />;
};

export const AlertCircle = (props: React.SVGProps<SVGCircleElement>) => {
  return (
    <circle r={5} fill="white" stroke="#000082" strokeWidth={1.5} {...props} />
  );
};
