// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { localPoint } from "@visx/event";
import { GridRows } from "@visx/grid";
import ParentSize from "@visx/responsive/lib/components/ParentSize";
import { scaleLinear, scaleTime } from "@visx/scale";
import { LinePath } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import { max } from "d3";
import React, { PointerEvent } from "react";

import { NULL_LAG_TEXT } from "~/api/materialize/freshness/lagHistory";
import {
  AlertCircle,
  GraphEventOverlay,
  GraphLineCursor,
  GraphTooltip,
  GraphTooltipBucketRange,
  GraphTooltipCursor,
  TooltipColorSwatch,
} from "~/components/graphComponents";
import { useChartLegend } from "~/hooks/useChartLegend";
import { useCursorState } from "~/hooks/usePointerEventEmitters";
import { OverflowMenuIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { notNullOrUndefined, pluralize } from "~/util";
import { formatDurationForAxis, formatInterval } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  calculateTooltipPosition,
  findNearestDatum,
  niceTicks,
  relativeTimeTickFormat,
} from "~/utils/graph";

import { DataPoint, GraphLineSeries } from "./types";

const MARGIN = { top: 20, right: 8, bottom: 36, left: 44 };

const MAX_LAG_MS = 86_400_000; // 24 hours
const X_TICK_COUNT = 4;
const Y_TICK_COUNT = 4;

export type FreshnessGraphProps = {
  bucketSizeMs: number;
  endTime: number;
  startTime: number;
  data: DataPoint[];
  lines: GraphLineSeries[];
  xAccessor: (d: DataPoint) => number;
  renderTooltipLabel?: (props: {
    dataPoint: DataPoint;
    color: string;
    lineData: GraphLineSeries;
  }) => React.ReactNode;
  maxTooltipLines?: number;
};

export const FreshnessGraph = (props: FreshnessGraphProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const legendItems = React.useMemo(
    () => props.lines.map((line) => line.key),
    [props.lines],
  );
  const { visibleLegendItems, toggleLegendItem } = useChartLegend({
    allLegendItems: legendItems,
  });

  const linesWithDisplayState = React.useMemo(
    () =>
      props.lines.map((line, idx) => ({
        ...line,
        color: colors.lineGraph[idx % colors.lineGraph.length],
        isVisible: visibleLegendItems.has(line.key),
      })),
    [props.lines, colors.lineGraph, visibleLegendItems],
  );

  return (
    <VStack alignItems="flex-start" width="100%" spacing="0">
      <HStack width="100%" overflowX="auto">
        <HStack gap="4" paddingBottom="2">
          {[...linesWithDisplayState]
            .sort((a, b) => a.label.localeCompare(b.label))
            .map(({ key, label, color, isVisible }) => (
              <HStack
                key={key}
                gap="1"
                onClick={(e) => toggleLegendItem(key, e)}
                cursor="pointer"
                opacity={isVisible ? 1 : 0.5}
              >
                <TooltipColorSwatch color={color} />
                <Text textStyle="text-small" color={colors.foreground.primary}>
                  {label}
                </Text>
              </HStack>
            ))}
        </HStack>
      </HStack>

      <Box height="180px" width="100%">
        <ParentSize debounceTime={10}>
          {(parent) => (
            <FreshnessGraphInner
              {...props}
              height={parent.height}
              width={parent.width}
              linesWithDisplayState={linesWithDisplayState}
            />
          )}
        </ParentSize>
      </Box>
    </VStack>
  );
};

type LineWithDisplayState = GraphLineSeries & {
  color: string;
  isVisible: boolean;
};

const FreshnessGraphInner = (
  props: FreshnessGraphProps & {
    height: number;
    width: number;
    linesWithDisplayState: LineWithDisplayState[];
  },
) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();

  const { data, xAccessor } = props;

  const xTicks = React.useMemo(
    () => [...niceTicks(props.startTime, props.endTime, X_TICK_COUNT)],
    [props.endTime, props.startTime],
  );

  const visibleLines = React.useMemo(
    () => props.linesWithDisplayState.filter(({ isVisible }) => isVisible),
    [props.linesWithDisplayState],
  );

  const yTicks = React.useMemo(() => {
    const visibleLinesSet = new Set(visibleLines.map((line) => line.key));

    const greatestLag = max(data, (d) => {
      return max(Object.entries(d.lag ?? {}), ([key, lag]) => {
        const lagValue = lag.queryable ? lag.totalMs : 0;

        return visibleLinesSet.has(key) ? lagValue : 0;
      });
    });

    return niceTicks(0, Math.min(MAX_LAG_MS, greatestLag ?? 0), Y_TICK_COUNT);
  }, [data, visibleLines]);

  const {
    xScaleRange,
    yScaleRange,
    svgProps,
    gridRowsProps,
    axisLeftProps,
    axisBottomProps,
    graphEventOverlayProps,
    graphLineCursorProps,
  } = React.useMemo(
    () =>
      buildXYGraphLayoutProps({
        width: props.width,
        height: props.height,
        margin: MARGIN,
      }),
    [props.height, props.width],
  );

  const {
    tooltipData,
    tooltipLeft,
    tooltipTop,
    tooltipOpen,
    showTooltip,
    hideTooltip,
  } = useTooltip<{
    nearestDatum: DataPoint;
  }>();

  const { containerRef: tooltipContainerRef, TooltipInPortal } =
    useTooltipInPortal({
      detectBounds: true,
      scroll: true,
    });

  const xScale = React.useMemo(
    () =>
      scaleTime({
        // Cartesian system values (ascending left to right)
        domain: [props.startTime, props.endTime],
        range: xScaleRange,
      }),
    [props.endTime, props.startTime, xScaleRange],
  );

  const yScale = React.useMemo(
    () =>
      scaleLinear({
        domain: [yTicks[0], yTicks[yTicks.length - 1]],
        // SVG coordinate values (ascending top to bottom)
        range: yScaleRange,
        clamp: true,
      }),
    [yTicks, yScaleRange],
  );

  const { cursor, handleCursorMove, hideCursor } = useCursorState();

  const handleTooltip = React.useCallback(
    (event: PointerEvent) => {
      const svgPoint = localPoint(event);
      if (!svgPoint) return;

      const nearestResult = findNearestDatum({
        scale: xScale,
        accessor: xAccessor,
        scaledValue: svgPoint.x,
        data,
        // If the closest point is more than the width of a bucket away,
        // don't show it in the tooltip.
        maxDistance: props.bucketSizeMs,
      });

      if (!nearestResult) {
        hideTooltip();
        return null;
      }
      showTooltip({
        ...calculateTooltipPosition({ event }),
        tooltipData: {
          nearestDatum: nearestResult.datum,
        },
      });
    },
    [hideTooltip, props.bucketSizeMs, data, xAccessor, showTooltip, xScale],
  );

  let tooltipLines = tooltipData?.nearestDatum
    ? [...visibleLines].sort((a, b) => {
        return (
          (b.yAccessor(tooltipData.nearestDatum) ?? 0) -
          (a.yAccessor(tooltipData.nearestDatum) ?? 0)
        );
      })
    : [];

  if (props.maxTooltipLines) {
    tooltipLines = tooltipLines.slice(0, props.maxTooltipLines);
  }

  const remainingTooltipLines = visibleLines.length - tooltipLines.length;

  const nonReadableEvents = React.useMemo(() => {
    return data
      .filter((d) => {
        return Object.entries(d.lag ?? {}).some(([key, lag]) => {
          // Filter on lines that are visible via the legend filter.
          const isVisible = visibleLines.some((line) => line.key === key);
          return !lag.queryable && isVisible;
        });
      })
      .map((d) => {
        return {
          cx: xScale(xAccessor(d)),
          cy: yScale(0),
        };
      });
  }, [data, visibleLines, xAccessor, xScale, yScale]);

  return (
    <>
      <svg ref={tooltipContainerRef} {...svgProps}>
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
          scale={yScale}
          hideAxisLine
          hideTicks
          tickFormat={(value) => {
            return formatDurationForAxis(value);
          }}
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
          tickFormat={(value) => {
            return relativeTimeTickFormat(
              value,
              new Date(props.startTime),
              new Date(props.endTime),
            );
          }}
          tickStroke={colors.border.primary}
          tickValues={xTicks}
        />

        {visibleLines.map(({ key, yAccessor, color }) => (
          <LinePath
            key={key}
            data={props.data}
            stroke={color}
            strokeWidth={2}
            defined={(d) => {
              return yAccessor(d) !== null;
            }}
            x={(d) => xScale(xAccessor(d))}
            y={(d) => yScale(yAccessor(d) ?? 0)}
          />
        ))}
        {nonReadableEvents.map(({ cx, cy }, idx) => (
          <AlertCircle key={idx} cx={cx} cy={cy} />
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
          <GraphTooltipCursor
            points={visibleLines
              .map(({ key, yAccessor, color }) => {
                const yValue = yAccessor(tooltipData.nearestDatum);

                if (yValue === null) {
                  return null;
                }
                return {
                  key,
                  color,
                  x: xScale(xAccessor(tooltipData.nearestDatum)),
                  y: yScale(yValue),
                };
              })
              .filter(notNullOrUndefined)}
          />
        )}
      </svg>
      {tooltipTop && tooltipLeft && tooltipOpen && tooltipData && (
        <GraphTooltip
          component={TooltipInPortal}
          top={tooltipTop}
          left={tooltipLeft}
        >
          <Box
            width="100%"
            borderBottomWidth="1px"
            borderColor={colors.border.secondary}
            px="4"
            py="2"
            textStyle="text-ui-med"
          >
            {tooltipLines.map((lineData) => {
              const { key, label } = lineData;
              const { nearestDatum } = tooltipData;

              const lagInfo = nearestDatum.lag[key];

              if (lagInfo === undefined) {
                return null;
              }

              let intervalText = null;
              if (lagInfo.queryable) {
                intervalText = formatInterval(lagInfo.interval);
              } else {
                intervalText = NULL_LAG_TEXT;
              }

              return (
                <HStack
                  alignItems="flex-start"
                  key={key}
                  justifyContent="space-between"
                  gap={4}
                >
                  {props.renderTooltipLabel ? (
                    props.renderTooltipLabel({
                      dataPoint: nearestDatum,
                      color: lineData.color,
                      lineData,
                    })
                  ) : (
                    <VStack alignItems="flex-start" width="100%" spacing="0">
                      <HStack gap={2}>
                        <TooltipColorSwatch color={lineData.color} />
                        <Text>{label}</Text>
                      </HStack>
                    </VStack>
                  )}
                  <Text whiteSpace="nowrap">{intervalText}</Text>
                </HStack>
              );
            })}
            {remainingTooltipLines > 0 && (
              <>
                <Text color={colors.foreground.secondary}>
                  <OverflowMenuIcon />
                  {remainingTooltipLines} other{" "}
                  {pluralize(remainingTooltipLines, "object", "objects")}
                </Text>
              </>
            )}
          </Box>

          <GraphTooltipBucketRange
            bucketStart={new Date(xAccessor(tooltipData.nearestDatum))}
            bucketEnd={
              new Date(xAccessor(tooltipData.nearestDatum) + props.bucketSizeMs)
            }
          />
        </GraphTooltip>
      )}
    </>
  );
};
