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
  chakra,
  Flex,
  HStack,
  StackProps,
  Text,
  useTheme,
} from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { GridRows } from "@visx/grid";
import ParentSize, {
  ParentSizeProvidedProps,
} from "@visx/responsive/lib/components/ParentSize";
import { scaleLinear, scaleTime } from "@visx/scale";
import { Line, LinePath } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import React, { PointerEvent, useMemo } from "react";

import {
  AlertCircle,
  GraphEventOverlay,
  GraphLineCursor,
  GraphTooltipCursor,
} from "~/components/graphComponents";
import { useHandleEmittedEvent } from "~/hooks/useHandleEmittedEvent";
import {
  useCursorState,
  usePointerEventEmitters,
} from "~/hooks/usePointerEventEmitters";
import { MaterializeTheme } from "~/theme";
import { notNullOrUndefined } from "~/util";
import {
  buildXYGraphLayoutProps,
  calculateTooltipPosition,
  findNearestDatum,
  relativeTimeTickFormat,
} from "~/utils/graph";

import { DataPoint, GraphDataKey, OfflineEvent, ReplicaData } from "./types";
import {
  UtilizationTooltip,
  UtilizationTooltipData,
} from "./UtilizationTooltip";

// Constants for graph element height so we can show the correct height loading container
const GRAPH_HEIGHT_PX = 300;
const LABEL_HEIGHT_PX = 18;
const LABEL_MARGIN_PX = 8;
export const TOTAL_GRAPH_HEIGHT_PX =
  GRAPH_HEIGHT_PX + LABEL_HEIGHT_PX + LABEL_MARGIN_PX;

// number of ticks we show on the graph
const tickCount = 8;

const margin = { top: 10, right: 0, bottom: 36, left: 40 };
export const graphHeightPx = 300;

interface UtilizationGraphProps {
  bucketSizeMs: number;
  /** Array of lines to show on the graph */
  data: ReplicaData[];
  dataKey: GraphDataKey;
  endTime: Date;
  offlineEvents: Array<OfflineEvent>;
  replicaColorMap: Map<string, { label: string; color: string }>;
  startTime: Date;
  title: string;
}

export const ClusterUtilizationGraphInner = ({
  bucketSizeMs,
  data,
  dataKey,
  endTime,
  height,
  offlineEvents,
  replicaColorMap,
  startTime,
  title,
  width,
}: UtilizationGraphProps & ParentSizeProvidedProps) => {
  const [statusVisible, setStatusVisible] = React.useState(true);
  const [metricVisible, setMetricVisible] = React.useState(true);
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const { tooltipLeft, tooltipTop, showTooltip, hideTooltip } =
    useTooltip<UtilizationTooltipData>();
  const { containerRef: tooltipContainerRef, TooltipInPortal } =
    useTooltipInPortal({
      detectBounds: true,
      scroll: true,
    });
  const { cursor, handleCursorMove, hideCursor } = useCursorState();
  const { onPointerMove, onPointerOut } = usePointerEventEmitters({
    source: "ClusterUtilizationGraph",
  });

  const {
    svgProps,
    gridRowsProps,
    axisLeftProps,
    axisBottomProps,
    graphEventOverlayProps,
    graphLineCursorProps,
    xScaleRange,
    yScaleRange,
  } = React.useMemo(
    () => buildXYGraphLayoutProps({ width, height, margin }),
    [width, height],
  );

  const xScale = React.useMemo(
    () =>
      scaleTime({
        domain: [startTime.getTime(), endTime.getTime()],
        range: xScaleRange,
      }),
    [endTime, startTime, xScaleRange],
  );
  const yScale = React.useMemo(
    () =>
      scaleLinear({
        domain: [0, 100],
        range: yScaleRange,
      }),
    [yScaleRange],
  );

  const handleTooltip = React.useCallback(
    (event: PointerEvent) => {
      showTooltip({
        ...calculateTooltipPosition({ event }),
      });
    },
    [showTooltip],
  );
  useHandleEmittedEvent(
    "pointermove",
    ({ event }) => {
      if ("clientX" in event && "clientY" in event) {
        handleCursorMove(event);
      }
    },
    ["ClusterUtilizationGraph"],
  );
  useHandleEmittedEvent("pointerout", hideCursor, ["ClusterUtilizationGraph"]);

  const tooltipData = useMemo(() => {
    if (!cursor) {
      return null;
    }

    // find all data points that are closest to the cursor
    const nearestPoints = data
      .map(({ data: replicaData }) => {
        const nearestResult = findNearestDatum({
          scale: xScale,
          accessor: (value: DataPoint) => value.bucketStart,
          scaledValue: cursor.x,
          data: replicaData,
          // If the closest point is more than the width of a bucket away,
          // don't show it in the tooltip.
          maxDistance: bucketSizeMs,
        });
        return nearestResult;
      })
      .filter(notNullOrUndefined);

    const maxXValue = xScale(
      Math.max(...nearestPoints.map((p) => p.datum.bucketStart)),
    );

    return {
      data: nearestPoints.map((p) => p.datum),
      nearestPointX: maxXValue,
      points: nearestPoints.map((point) => ({
        color:
          replicaColorMap.get(point.datum.id)?.color ?? colors.accent.purple,
        key: point.datum.id,
        // Snap the point to the start of the time bucket
        x: xScale(point.datum.bucketStart),
        // Show the point on the largest y value
        y: yScale(point.datum[dataKey] ?? 0),
      })),
    };
  }, [
    bucketSizeMs,
    colors.accent.purple,
    cursor,
    data,
    dataKey,
    replicaColorMap,
    xScale,
    yScale,
  ]);

  const yTicks = React.useMemo(() => [0, 25, 50, 75, 100], []);

  const xTicks = React.useMemo(() => {
    const startTimeMs = startTime.getTime();
    const duration = endTime.getTime() - startTimeMs;
    const tickSlots = Array.from({
      length: tickCount,
    }) as undefined[];
    return tickSlots.map((_, i) => i * (duration / tickCount) + startTimeMs);
  }, [endTime, startTime]);
  const legendData = React.useMemo(
    () => Array.from(replicaColorMap.entries()),
    [replicaColorMap],
  );
  const replicaColors = React.useMemo(
    () => Array.from(replicaColorMap.values()).map(({ color }) => color),
    [replicaColorMap],
  );
  const statusLineColor = colors.foreground.tertiary;

  return (
    <Box position="relative">
      <HStack justifyContent="space-between" mb={2}>
        <Text fontSize="sm" lineHeight="16px" fontWeight={500}>
          {title}
        </Text>
        <HStack spacing={4} as="ul">
          <LegendItem
            icon={<ColoredDotRow colors={replicaColors} />}
            text="max()"
            onClick={() => setMetricVisible((val) => !val)}
          />
          <LegendItem
            icon={<ColoredDot color={statusLineColor} />}
            text="replica restarts"
            onClick={() => setStatusVisible((val) => !val)}
          />
        </HStack>
      </HStack>
      <svg {...svgProps} ref={tooltipContainerRef}>
        <GridRows
          {...gridRowsProps}
          scale={yScale}
          stroke={colors.border.primary}
          strokeDasharray="4"
          tickValues={yTicks}
          pointerEvents="none"
        />
        <AxisLeft<AxisScale<number>>
          {...axisLeftProps}
          hideAxisLine
          hideTicks
          scale={yScale}
          stroke={colors.border.secondary}
          tickFormat={(value) => `${value.toFixed()}%`}
          tickLabelProps={() => ({
            fill: colors.foreground.secondary,
            fontFamily: fonts.mono,
            fontSize: "12",
            textAnchor: "end",
            dx: "-4px",
            dy: "5px",
          })}
          tickValues={yTicks}
        />
        <AxisBottom<AxisScale<number>>
          {...axisBottomProps}
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
          tickFormat={(value) =>
            relativeTimeTickFormat(value, startTime, endTime)
          }
          tickValues={xTicks}
        />
        {data.map((replicaData) => {
          if (replicaData.data.length < 1) return null;
          return (
            metricVisible && (
              <LinePath
                key={replicaData.id}
                defined={(datapoint) => datapoint[dataKey] !== null}
                stroke={replicaColorMap.get(replicaData.id)?.color}
                strokeWidth={1}
                data={replicaData.data}
                x={(d) => xScale(d.bucketStart)}
                y={(d) => yScale(d[dataKey] ?? 0)}
              />
            )
          );
        })}
        {data.map((replicaData) =>
          replicaData.data.map((event) => {
            const offlineEventsWithReasons = event.offlineEvents.filter(
              ({ offlineReason }) => !!offlineReason,
            );
            if (offlineEventsWithReasons.length === 0) return null;
            return (
              <AlertCircle
                key={event.id + event.bucketStart}
                cx={xScale(event.bucketStart)}
                cy={yScale(event[dataKey] ?? 0)}
              />
            );
          }),
        )}
        {offlineEvents.map(
          (event) =>
            statusVisible && (
              <Line
                key={event.id + event.timestamp}
                from={{
                  x: xScale(event.timestamp),
                  y: graphLineCursorProps.graphTop,
                }}
                to={{
                  x: xScale(event.timestamp),
                  y: graphLineCursorProps.graphBottom,
                }}
                stroke={statusLineColor}
                strokeWidth={1}
                pointerEvents="none"
              />
            ),
        )}
        {cursor && <GraphLineCursor {...graphLineCursorProps} point={cursor} />}

        {tooltipData?.points && tooltipData.points.length > 0 && (
          <GraphTooltipCursor points={tooltipData.points} />
        )}
        <GraphEventOverlay
          {...graphEventOverlayProps}
          onPointerMove={(event) => {
            handleTooltip(event);
            onPointerMove?.(event);
          }}
          onPointerLeave={(event) => {
            hideTooltip();
            onPointerOut?.(event);
          }}
        />
      </svg>
      <Flex justifyContent="space-between" alignItems="start" gap="4">
        <HStack spacing={4} as="ul" ml={8} overflow="auto">
          {legendData.map(([replicaId, { label, color }]) => (
            <HStack as="li" alignItems="center" key={replicaId}>
              <LegendItem
                as="div"
                text={label}
                icon={<ReplicaLineIcon color={color} />}
              />
            </HStack>
          ))}
        </HStack>
      </Flex>
      {tooltipLeft && tooltipTop && tooltipData && (
        <UtilizationTooltip
          component={TooltipInPortal}
          dataKey={dataKey}
          replicaColorMap={replicaColorMap}
          tooltipData={tooltipData}
          left={tooltipLeft}
          top={tooltipTop}
        />
      )}
    </Box>
  );
};

export const UtilizationGraph = (props: UtilizationGraphProps) => {
  return (
    <ParentSize
      className="graph-container"
      debounceTime={10}
      style={{ width: "100%", minWidth: 0 }}
    >
      {(parentSizeState) => {
        return (
          <ClusterUtilizationGraphInner
            {...parentSizeState}
            height={graphHeightPx}
            {...props}
          />
        );
      }}
    </ParentSize>
  );
};

const ColoredDot = (props: { color?: string }) => {
  return (
    <chakra.div
      backgroundColor={props.color}
      height="8px"
      width="8px"
      borderRadius="8px"
    />
  );
};

const ColoredDotRow = (props: { colors: Array<string> }) => {
  return (
    <HStack spacing={1} maxWidth="72px" flexWrap="wrap" rowGap="2">
      {props.colors.map((color, i) => (
        <ColoredDot key={color + i} color={color} />
      ))}
    </HStack>
  );
};

const ReplicaLineIcon = (props: { color: string }) => {
  return (
    <Flex
      position="relative"
      width="3"
      height="2"
      justifyContent="center"
      alignItems="center"
    >
      <chakra.div
        backgroundColor={props.color}
        height="2"
        width="2"
        borderRadius="8px"
        position="absolute"
      />
      <chakra.div
        position="absolute"
        backgroundColor={props.color}
        width="100%"
        height="2px"
      />
    </Flex>
  );
};

const LegendItem = ({
  text,
  icon,
  ...props
}: React.PropsWithChildren<
  {
    text: string;
    icon?: React.ReactNode;
  } & StackProps
>) => {
  return (
    <HStack as="button" {...props}>
      <Box minWidth="2">{icon}</Box>
      <Text fontSize="xs" whiteSpace="nowrap">
        {text}
      </Text>
    </HStack>
  );
};
