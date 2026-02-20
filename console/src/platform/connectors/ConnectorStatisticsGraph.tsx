// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale, TickRendererProps } from "@visx/axis";
import { localPoint } from "@visx/event";
import { GridRows } from "@visx/grid";
import ParentSize from "@visx/responsive/lib/components/ParentSize";
import { scaleLinear, scaleTime } from "@visx/scale";
import { LinePath } from "@visx/shape";
import { Text as VisxText } from "@visx/text";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import React, { PointerEvent } from "react";

import {
  GraphEventOverlay,
  GraphLineCursor,
  GraphTooltipCursor,
} from "~/components/graphComponents";
import { useCursorState } from "~/hooks/usePointerEventEmitters";
import { MaterializeTheme } from "~/theme";
import { lineHeightFromFontSize } from "~/theme/utils";
import { isNumber, maybeParseFloat } from "~/util";
import { DATE_FORMAT, formatDate } from "~/utils/dateFormat";
import { ByteUnit, convertBytes } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  calculateTooltipPosition,
  findNearestDatum,
} from "~/utils/graph";

import {
  ConnectorStatisticsTooltip,
  ConnectorStatisticsTooltipData,
  GraphDataPoint,
} from "./ConnectorStatisticsTooltip";
import {
  calculateAxisTicks,
  formatValue,
  largestPossibleByteUnit,
  prettyFormatAxisValue,
} from "./utils";

const MARGIN = { top: 10, right: 10, bottom: 40, left: 60 };

export interface ConnectorStatisticsGraphLine<GDP extends GraphDataPoint> {
  key: Extract<keyof GDP, string>;
  label?: string;
  color: string;
}

export interface ConnectorStatisticsGraphProps<GDP extends GraphDataPoint> {
  bucketSizeMs: number;
  endTime: Date;
  startTime: Date;
  currentValueKey: Extract<keyof GDP, string>;
  data: GDP[];
  lines: ConnectorStatisticsGraphLine<GDP>[];
  title: string;
  unitLabel: (value: number | null, yAxisUnit: ByteUnit | null) => string;
  yAxisUnitType?: "bytes" | "count";
  yAxisUnitLabel?: (
    label: string | undefined,
    yAxisUnit: ByteUnit | null,
  ) => string | undefined;
}

const yTickCount = 4;

const XTick = ({ x, y, formattedValue, ...props }: TickRendererProps) => {
  if (!formattedValue) return null;
  const lines = formattedValue.split("\n");
  const baseDy = isNumber(props.dy)
    ? props.dy
    : (maybeParseFloat(props.dy) ?? 0);
  const fontSize = isNumber(props.fontSize)
    ? props.fontSize
    : (maybeParseFloat(props.fontSize) ?? 12);
  const lineHeight = maybeParseFloat(lineHeightFromFontSize(fontSize)) ?? 16;
  return (
    <>
      {lines.map((line, ix) => (
        <VisxText
          key={ix}
          x={x}
          y={y}
          {...props}
          fontSize={fontSize}
          dy={baseDy + ix * lineHeight}
        >
          {line}
        </VisxText>
      ))}
    </>
  );
};

export const ConnectorStatisticsGraph = <GDP extends GraphDataPoint>(
  props: ConnectorStatisticsGraphProps<GDP>,
) => {
  const yAxisInBytes = props.yAxisUnitType === "bytes";

  const {
    min: minY,
    max: maxY,
    ticks: yTicks,
    unit: yAxisUnit,
  } = React.useMemo(() => {
    let min = 0;
    let max = 0;
    // find the min and max across all data for this graph
    for (const datum of props.data) {
      for (const { key } of props.lines) {
        min = Math.min(min, Number(datum[key]));
        max = Math.max(max, Number(datum[key]));
      }
    }
    const { alignedMin, alignedMax, ticks } = calculateAxisTicks({
      max,
      min,
      tickCount: yTickCount,
    });
    return {
      min: alignedMin,
      max: alignedMax,
      unit: yAxisInBytes ? largestPossibleByteUnit(max) : null,
      ticks,
    };
  }, [props.data, props.lines, yAxisInBytes]);

  const xTickCount = 3;
  const xTicks = React.useMemo(() => {
    const startTimeMs = props.startTime.getTime();
    const duration = props.endTime.getTime() - startTimeMs;
    const tickGap = duration / xTickCount;
    const tickSlots = Array.from({
      length: xTickCount,
    }) as undefined[];
    return tickSlots.map(
      // Offset the ticks by half the gap so they are evenly distributed
      (_, i) => new Date(i * tickGap + startTimeMs + tickGap / 2),
    );
  }, [props.endTime, props.startTime]);

  return (
    <VStack alignItems="flex-start" width="100%" gap="2">
      <HStack justifyContent="space-between" width="100%">
        <VStack alignItems="flex-start" gap="1">
          <Text textStyle="heading-xs">{props.title}</Text>
          <HStack gap="4">
            {props.lines.map(({ key, label, color }) => (
              <HStack key={key} gap="1">
                <Box
                  background={color}
                  borderRadius="2px"
                  height="2"
                  width="2"
                />
                <Text>
                  {props.yAxisUnitType === "bytes" && yAxisUnit
                    ? `${props.yAxisUnitLabel?.(label, yAxisUnit) ?? "Data"} (${yAxisUnit})`
                    : label}
                </Text>
              </HStack>
            ))}
          </HStack>
        </VStack>
        <Box>
          <CurrentValue
            value={props.data.at(-1)?.[props.currentValueKey] ?? 0}
            yAxisUnit={yAxisUnit}
            unitLabel={props.unitLabel}
          />
        </Box>
      </HStack>
      <Box height="180px" width="100%">
        <ParentSize debounceTime={10}>
          {(parent) => (
            <ConnectorStatisticsGraphInner
              {...props}
              maxY={maxY}
              minY={minY}
              xTicks={xTicks}
              yTicks={yTicks}
              yAxisUnit={yAxisUnit}
              height={parent.height}
              width={parent.width}
            />
          )}
        </ParentSize>
      </Box>
    </VStack>
  );
};

const ConnectorStatisticsGraphInner = <GDP extends GraphDataPoint>(
  props: ConnectorStatisticsGraphProps<GDP> & {
    maxY: number;
    minY: number;
    xTicks: Date[];
    yTicks: number[];
    yAxisUnit: null | ByteUnit;
    height: number;
    width: number;
  },
) => {
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
    () =>
      buildXYGraphLayoutProps({
        width: props.width,
        height: props.height,
        margin: MARGIN,
      }),
    [props.width, props.height],
  );

  const { colors, fonts } = useTheme<MaterializeTheme>();
  const { xTicks, yTicks, minY, maxY } = props;
  const {
    tooltipOpen,
    tooltipLeft,
    tooltipTop,
    tooltipData,
    hideTooltip,
    showTooltip,
  } = useTooltip<ConnectorStatisticsTooltipData<GDP>>();
  const { containerRef: tooltipContainerRef, TooltipInPortal } =
    useTooltipInPortal({
      scroll: true,
      detectBounds: true,
    });

  const xScale = React.useMemo(
    () =>
      scaleTime({
        // Cartesian system values (ascending left to right)
        domain: [props.startTime, props.endTime],
        // SVG coordinate values (ascending left to right)
        range: xScaleRange,
      }),
    [props.endTime, props.startTime, xScaleRange],
  );

  const yScale = React.useMemo(
    () =>
      scaleLinear({
        // Cartesian system values (ascending left to right)
        // when min and max are both 0, the line is vertically centered in the graph, so
        // we default to an arbitrary value to prevent that.
        domain: [
          Math.min(yTicks[0], minY),
          Math.max(yTicks.at(-1) ?? 0, maxY) || 100,
        ],
        // SVG coordinate values (ascending top to bottom)
        range: yScaleRange,
      }),
    [yTicks, minY, maxY, yScaleRange],
  );

  const { cursor, handleCursorMove, hideCursor } = useCursorState();

  const handleTooltip = React.useCallback(
    (event: PointerEvent) => {
      const svgPoint = localPoint(event);
      if (!svgPoint) return;

      const nearestResult = findNearestDatum({
        scale: xScale,
        accessor: (value: GDP) => value.timestamp,
        scaledValue: svgPoint.x,
        data: props.data,
        maxDistance: props.bucketSizeMs,
      });
      const allValuesAreNull = props.lines
        .map((l) => nearestResult?.datum[l.key])
        .every((v) => v === null);
      if (allValuesAreNull || !nearestResult) {
        // If all values are null or
        // If the closest point is more than the width of a bucket away,
        // don't show it in the tooltip.
        hideTooltip();
        return null;
      }
      showTooltip({
        ...calculateTooltipPosition({ event }),
        tooltipData: {
          // Snap the point to the start of the time bucket
          nearestPointX: xScale(nearestResult.datum.timestamp),
          points: props.lines.map((l) => ({
            ...l,
            // key: l.key,
            // Snap the point to the start of the time bucket
            x: xScale(nearestResult.datum.timestamp),
            // Show the point on the largest y value
            y: yScale(nearestResult.datum[l.key] ?? 0),
          })),
          nearestDatum: nearestResult.datum,
        },
      });
    },
    [
      hideTooltip,
      props.bucketSizeMs,
      props.data,
      props.lines,
      showTooltip,
      xScale,
      yScale,
    ],
  );

  return (
    <VStack alignItems="flex-start" gap="2" position="relative">
      {/* Setting width=100% here is important for ParentSize to work correctly inside a
          flex container. */}
      <svg ref={tooltipContainerRef} height={svgProps.height} width="100%">
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
          tickFormat={(value: number) => {
            if (props.yAxisUnitType === "bytes" && props.yAxisUnit) {
              const converted = convertBytes(value, props.yAxisUnit);
              const maxYConverted = convertBytes(maxY, props.yAxisUnit);
              return prettyFormatAxisValue(converted, maxYConverted);
            }
            return prettyFormatAxisValue(value, maxY, 5);
          }}
          scale={yScale}
          stroke={colors.border.primary}
          tickValues={yTicks}
          tickLabelProps={() => ({
            dy: "4px",
            fill: colors.foreground.primary,
            fontFamily: fonts.mono,
            fontSize: 12,
            textAnchor: "end",
          })}
        />
        <AxisBottom<AxisScale<number>>
          {...axisBottomProps}
          scale={xScale}
          stroke={colors.border.primary}
          strokeWidth={2}
          tickLabelProps={() => ({
            dy: 4,
            fill: colors.foreground.primary,
            fontFamily: fonts.mono,
            fontSize: 12,
            textAnchor: "middle",
          })}
          tickComponent={XTick}
          tickFormat={(value) => {
            // To apply a custom tick component, we must format our structured data in a string
            return `${formatDate(value, "HH:mm z")}\n${formatDate(value, DATE_FORMAT)}`;
          }}
          tickStroke={colors.border.primary}
          tickValues={xTicks}
        />
        {props.lines.map(({ key, color }, i) => (
          <LinePath
            key={i}
            data={props.data}
            stroke={color}
            strokeWidth={2}
            // The data could have gaps, though in practice they don't tend to show up
            defined={(value) => value[key] !== null}
            x={(d) => xScale(d.timestamp)}
            y={(d) => yScale(d[key] ?? 0)}
          />
        ))}
        <GraphEventOverlay
          {...graphEventOverlayProps}
          onPointerMove={(event) => {
            handleCursorMove(event);
            handleTooltip(event);
          }}
          onPointerLeave={() => {
            hideCursor();
            hideTooltip();
          }}
        />
        {cursor && <GraphLineCursor point={cursor} {...graphLineCursorProps} />}
        {tooltipOpen && tooltipData && (
          <GraphTooltipCursor points={tooltipData.points} />
        )}
        {}
      </svg>
      {tooltipTop && tooltipLeft && tooltipOpen && tooltipData && (
        <ConnectorStatisticsTooltip<GDP>
          bucketSizeMs={props.bucketSizeMs}
          component={TooltipInPortal}
          top={tooltipTop}
          left={tooltipLeft}
          tooltipData={tooltipData}
          formatValue={(value) => formatValue(value, props.yAxisUnit)}
          unitLabel={props.unitLabel}
          yAxisUnit={props.yAxisUnit}
        />
      )}
    </VStack>
  );
};

const CurrentValue = ({
  unitLabel,
  value,
  yAxisUnit,
}: {
  value: number;
  unitLabel: (value: number | null, yAxisUnit: ByteUnit | null) => string;
  yAxisUnit: ByteUnit | null;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  if (value === null) return null;

  const formattedValue = formatValue(value, yAxisUnit);
  const displayedNumber = Number.parseFloat(formattedValue);
  // Round trip the value through our formatter to correctly pluralize rounded
  // values.
  const label = unitLabel(displayedNumber, yAxisUnit);

  return (
    <Text textStyle="heading-sm">
      {formattedValue}{" "}
      <Text as="span" color={colors.foreground.secondary}>
        {label}
      </Text>
    </Text>
  );
};
