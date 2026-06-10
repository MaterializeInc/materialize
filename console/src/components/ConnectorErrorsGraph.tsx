// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, useTheme, VStack } from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { localPoint } from "@visx/event";
import { GridRows } from "@visx/grid";
import { Group } from "@visx/group";
import ParentSize from "@visx/responsive/lib/components/ParentSize";
import { scaleLinear, scaleTime } from "@visx/scale";
import { Bar } from "@visx/shape";
import { useTooltip } from "@visx/tooltip";
import useTooltipInPortal, {
  TooltipInPortalProps,
} from "@visx/tooltip/lib/hooks/useTooltipInPortal";
import {
  differenceInDays,
  differenceInHours,
  differenceInMinutes,
} from "date-fns";
import React from "react";

import { MaterializeTheme } from "~/theme";
import {
  DATE_FORMAT_SHORT,
  formatDate,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { findNearestDatum } from "~/utils/graph";

import { GraphEventOverlay, GraphTooltip } from "./graphComponents";

export interface ConnectorStatus {
  count: number;
  timestamp: number;
}
export interface ConnectorErrorGraphProps {
  bucketSizeMs: number;
  connectorStatuses: ConnectorStatus[];
  endTime: Date;
  startTime: Date;
  timePeriodMinutes: number;
}

export const HEIGHT_PX = 300;
const Y_TICK_COUNT = 4;
const margin = { top: 0, right: 0, bottom: 36, left: 64 };

const ConnectorErrorGraphInner = ({
  bucketSizeMs,
  connectorStatuses,
  timePeriodMinutes,
  startTime,
  endTime,
  height,
  width,
}: ConnectorErrorGraphProps & { width: number; height: number }) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();

  // Show an X axis tick on every other possible bar, even if there are no errors
  const startTimeMs = startTime.getTime();
  const duration = endTime.getTime() - startTimeMs;

  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;

  const { tooltipData, tooltipLeft, tooltipTop, showTooltip, hideTooltip } =
    useTooltip<ConnectorErrorGraphTooltipData>();
  const {
    containerRef: tooltipContainerRef,
    containerBounds,
    TooltipInPortal,
  } = useTooltipInPortal({
    detectBounds: true,
  });

  const xScale = React.useMemo(
    () =>
      scaleTime({
        domain: [startTime.getTime(), endTime.getTime()],
        range: [margin.left, width],
      }),
    [endTime, startTime, width],
  );

  const { yMin, yMax } = React.useMemo(() => {
    return {
      yMin: 0,
      yMax: Math.max(...connectorStatuses.map((s) => s.count)),
    };
  }, [connectorStatuses]);

  const yScale = React.useMemo(() => {
    return scaleLinear({
      domain: [yMin, yMax || 10],
      range: [innerHeight, margin.bottom],
    });
  }, [innerHeight, yMax, yMin]);

  const xTicks = React.useMemo(() => {
    const tickSlots = Array.from({
      length: Math.round(duration / bucketSizeMs / 2),
    }) as undefined[];
    return tickSlots.map((_, i) => i * bucketSizeMs * 2 + startTimeMs);
  }, [bucketSizeMs, duration, startTimeMs]);

  const yTicks = React.useMemo(() => {
    const tickSlots = Array.from({
      length: Y_TICK_COUNT,
    }) as undefined[];
    const ticks = tickSlots.map((_, i) =>
      Math.round((i + 1) * (yMax / Y_TICK_COUNT)),
    );
    // If the domain is very small or empty, we can end up with duplicate ticks
    return [...new Set(ticks)];
  }, [yMax]);

  return (
    <Box position="relative">
      <svg width={width} height={height} ref={tooltipContainerRef}>
        <GridRows
          left={margin.left}
          pointerEvents="none"
          scale={yScale}
          stroke={colors.border.primary}
          strokeDasharray="4"
          tickValues={yTicks}
          width={innerWidth}
        />
        <AxisLeft<AxisScale<number>>
          hideAxisLine
          hideTicks
          left={margin.left}
          scale={yScale}
          stroke={colors.border.secondary}
          tickLabelProps={() => ({
            fill: colors.foreground.secondary,
            fontFamily: fonts.mono,
            fontSize: "12",
            textAnchor: "end",
            dx: "-0.25em",
            dy: "0.5em",
          })}
          tickValues={yTicks}
          tickFormat={(value) => value.toFixed()}
        />
        <AxisBottom<AxisScale<number>>
          hideTicks
          scale={xScale}
          stroke={colors.border.secondary}
          strokeWidth={2}
          tickLabelProps={() => ({
            fill: colors.foreground.secondary,
            fontFamily: fonts.mono,
            fontSize: "12",
            textAnchor: "middle",
          })}
          tickFormat={(value) => {
            if (timePeriodMinutes < 6 * 60) {
              return `${differenceInMinutes(
                endTime,
                new Date(value),
              ).toString()}m`;
            }
            if (timePeriodMinutes < 30 * 24 * 60) {
              return `${differenceInHours(endTime, new Date(value)).toString()}h`;
            }
            return `${differenceInDays(endTime, new Date(value)).toString()}d`;
          }}
          tickValues={xTicks}
          top={innerHeight}
        />
        <Group>
          {connectorStatuses.map((status) => {
            const barHeight = innerHeight - yScale(status.count);
            const barX = xScale(status.timestamp);
            const barY = innerHeight - barHeight;

            return (
              <Bar
                key={status.timestamp}
                x={barX}
                y={barY}
                width={4}
                height={barHeight}
                fill={colors.red[500]}
              />
            );
          })}
        </Group>
        <GraphEventOverlay
          x={margin.left}
          y={margin.top}
          width={innerWidth}
          height={innerHeight}
          onPointerMove={(event) => {
            if (!("clientX" in event) || !("clientY" in event)) return;
            const containerX = event.clientX - containerBounds.left;
            const containerY = event.clientY - containerBounds.top;
            const svgPoint = localPoint(event);
            if (!svgPoint) return;

            const nearestResult = findNearestDatum({
              scale: xScale,
              accessor: (value: ConnectorStatus) => value.timestamp,
              scaledValue: svgPoint.x,
              data: connectorStatuses,
              // If the closest point is more than the width of a bucket away,
              // don't show it in the tooltip.
              maxDistance: bucketSizeMs,
            });
            if (!nearestResult) {
              hideTooltip();
              return null;
            }
            showTooltip({
              tooltipTop: containerY,
              tooltipLeft: containerX,
              tooltipData: {
                bucketStart: new Date(nearestResult.datum.timestamp),
                bucketEnd: new Date(
                  nearestResult.datum.timestamp + bucketSizeMs,
                ),
                count: nearestResult.datum.count,
              },
            });
          }}
          onPointerLeave={() => {
            hideTooltip();
          }}
        />
      </svg>
      {tooltipTop && tooltipLeft && tooltipData && (
        <ConnectorErrorGraphTooltip
          component={TooltipInPortal}
          top={tooltipTop}
          left={tooltipLeft}
          tooltipData={tooltipData}
        />
      )}
    </Box>
  );
};

export interface ConnectorErrorGraphTooltipData {
  bucketStart: Date;
  bucketEnd: Date;
  count: number;
}

export interface ConnectorErrorGraphTooltipProps {
  tooltipData: ConnectorErrorGraphTooltipData;
  left: number;
  top: number;
  component: React.FC<TooltipInPortalProps>;
}

const ConnectorErrorGraphTooltip = ({
  tooltipData,
  ...props
}: ConnectorErrorGraphTooltipProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const startLabel = formatDate(
    tooltipData.bucketStart,
    `${DATE_FORMAT_SHORT} ${TIME_FORMAT_NO_SECONDS} z`,
  );
  const endLabel = formatDate(
    tooltipData.bucketEnd,
    `${DATE_FORMAT_SHORT} ${TIME_FORMAT_NO_SECONDS} z`,
  );

  return (
    <GraphTooltip {...props} data-testid="chart-tooltip">
      <VStack align="start" fontSize="sm" py="4" px="2">
        <Text>{`${tooltipData.count} errors`}</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {`${startLabel} - ${endLabel}`}
        </Text>
      </VStack>
    </GraphTooltip>
  );
};

export const ConnectorErrorGraph = (props: ConnectorErrorGraphProps) => {
  return (
    <ParentSize
      className="graph-container"
      debounceTime={10}
      style={{ width: "100%", minWidth: 0 }}
    >
      {(parentSizeState) => {
        return (
          <ConnectorErrorGraphInner
            {...parentSizeState}
            height={HEIGHT_PX}
            {...props}
          />
        );
      }}
    </ParentSize>
  );
};
