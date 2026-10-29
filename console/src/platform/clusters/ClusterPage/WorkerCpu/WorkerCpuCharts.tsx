// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Text, useTheme, VStack } from "@chakra-ui/react";
import { curveStepAfter } from "@visx/curve";
import { scaleLinear, scaleTime } from "@visx/scale";
import { Area, Line, LinePath } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import { GraphLineCursor, GraphTooltip } from "~/components/graphComponents";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  TIME_FORMAT,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps, findNearestDatum } from "~/utils/graph";

import {
  ChartHeader,
  HoverOverlay,
  Layout,
  PercentAxis,
  PercentScale,
  TIME_TICK_COUNT,
  TimeAxis,
  TimeScale,
} from "../ResourceUsage/ResourceUsageCharts";
import {
  CHART_MARGIN,
  PERCENT_TICKS,
} from "../ResourceUsage/resourceUsageStyles";
import { TooltipRow } from "./WorkerBars";
import {
  CpuPoint,
  SMOOTHING_WINDOW_MS,
  WorkerBusyPoint,
  WorkerReplica,
  workersPerCore,
} from "./workerCpuModel";
import { gapFill } from "./workerCpuStyles";

const PLOT_HEIGHT_PX = 132;
const SVG_HEIGHT_PX = CHART_MARGIN.top + PLOT_HEIGHT_PX + CHART_MARGIN.bottom;

export interface WorkerCpuChartsProps {
  width: number;
  startMs: number;
  endMs: number;
  openedAtMs: number;
  replicaCpu: CpuPoint[];
  series: WorkerBusyPoint[];
  replica: WorkerReplica;
  /** The span the report above measured, shaded. */
  measured: { startMs: number; endMs: number } | undefined;
}

// A point averages the readings over the window before it, so it says nothing
// about a time further away.
const WORKER_HOVER_MAX_DISTANCE_MS = SMOOTHING_WINDOW_MS;

/** The point nearest `atMs`, of points sorted by time. */
const nearest = <T extends { atMs: number }>(
  xScale: TimeScale,
  points: T[],
  atMs: number,
  maxDistanceMs?: number,
) =>
  findNearestDatum({
    scale: xScale,
    accessor: (point: T) => point.atMs,
    scaledValue: xScale(new Date(atMs)),
    data: points,
    maxDistance: maxDistanceMs,
  })?.datum;

/** The replica CPU point for the minute that holds `atMs`, which starts at it. */
const replicaCpuAt = (points: CpuPoint[], atMs: number) =>
  points.find(
    (point) => point.atMs <= atMs && atMs < point.atMs + millisecondsInMinute,
  );

const BeforeOpen = ({
  layout,
  x,
  label,
}: {
  layout: Layout;
  x: number;
  label?: string;
}) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const [left] = layout.xScaleRange;
  const { graphTop, graphBottom } = layout.graphLineCursorProps;
  const width = Math.max(0, x - left);
  return (
    <g pointerEvents="none">
      <rect
        x={left}
        y={graphTop}
        width={width}
        height={graphBottom - graphTop}
        fill={colors.background.secondary}
      />
      {label && width > 240 && (
        <text
          x={left + width / 2}
          y={graphBottom - 10}
          textAnchor="middle"
          fill={colors.foreground.secondary}
          fontFamily={fonts.body}
          fontSize={12}
        >
          {label}
        </text>
      )}
      <Line
        from={{ x, y: graphTop }}
        to={{ x, y: graphBottom }}
        stroke={colors.foreground.secondary}
        strokeDasharray="3 3"
      />
    </g>
  );
};

// Clock times, since the window runs ahead of now while readings fill it and
// times relative to its end would read wrong.
// Ticks under a minute apart need seconds, or neighbors share a label.
const clockTick = (startMs: number, endMs: number) => {
  const format =
    (endMs - startMs) / TIME_TICK_COUNT < millisecondsInMinute
      ? TIME_FORMAT
      : TIME_FORMAT_NO_SECONDS;
  return (ms: number) => formatDate(new Date(ms), format);
};

type ChartPoint = WorkerBusyPoint & {
  /** The workers' time, as a percent of the replica's allocation. */
  dataflowPercent: number;
  replicaPercent: number | undefined;
};

interface MarksProps {
  layout: Layout;
  xScale: TimeScale;
  yScale: PercentScale;
  openedX: number;
  /** Runs of points between pauses, so no line bridges a pause. */
  segments: ChartPoint[][];
  clipId: string;
  /** The replica's color in the resource usage charts. */
  color: string;
  measured: WorkerCpuChartsProps["measured"];
}

// The marks are memoized so pointer moves, which only change the cursor and
// tooltip, skip redrawing them.
const CpuMarks = React.memo(
  ({
    layout,
    xScale,
    yScale,
    openedX,
    segments,
    clipId,
    color,
    measured,
    replicaCpu,
  }: MarksProps & { replicaCpu: CpuPoint[] }) => {
    const { colors } = useTheme<MaterializeTheme>();
    const outsideFill = gapFill(colors.foreground.secondary);
    const { graphTop, graphBottom } = layout.graphLineCursorProps;
    const [plotLeft, plotRight] = layout.xScaleRange;
    const x = (point: { atMs: number }) => xScale(new Date(point.atMs));
    const measuredLeft = measured && xScale(new Date(measured.startMs));
    const measuredRight = measured && xScale(new Date(measured.endMs));
    const lastCpu = replicaCpu.at(-1);
    const replicaSteps = lastCpu
      ? [
          ...replicaCpu,
          { ...lastCpu, atMs: lastCpu.atMs + millisecondsInMinute },
        ]
      : replicaCpu;
    return (
      <>
        <BeforeOpen
          layout={layout}
          x={openedX}
          label="The workers' time starts when you start measuring"
        />
        {measuredLeft !== undefined && measuredRight !== undefined && (
          <rect
            data-testid="measured-span"
            x={Math.max(plotLeft, measuredLeft)}
            y={graphTop}
            width={Math.max(
              0,
              Math.min(plotRight, measuredRight) -
                Math.max(plotLeft, measuredLeft),
            )}
            height={graphBottom - graphTop}
            fill={colors.background.accent}
            pointerEvents="none"
          />
        )}
        <PercentAxis layout={layout} yScale={yScale} ticks={PERCENT_TICKS} />
        {/* Readings and metrics outside the window would otherwise draw over
            the axis labels. Both charts clip to this. */}
        <defs>
          <clipPath id={clipId}>
            <rect
              x={plotLeft}
              y={graphTop}
              width={Math.max(0, plotRight - plotLeft)}
              height={Math.max(0, graphBottom - graphTop)}
            />
          </clipPath>
        </defs>
        <g clipPath={`url(#${clipId})`}>
          {segments.map((segment) => (
            <Area
              key={segment[0].atMs}
              data={segment}
              x={x}
              y0={(point) => yScale(point.dataflowPercent)}
              y1={(point) =>
                yScale(
                  Math.max(point.dataflowPercent, point.replicaPercent ?? 0),
                )
              }
              fill={outsideFill}
              pointerEvents="none"
            />
          ))}
          {/* Each sample covers the minute after it, as `replicaCpuAt` reads
              it, so the line steps and holds the last one to its minute's
              end. Joined points cut under the fill between samples. */}
          <LinePath
            data={replicaSteps}
            x={x}
            y={(point) => yScale(point.percent)}
            curve={curveStepAfter}
            stroke={color}
            strokeWidth={2}
            pointerEvents="none"
          />
          {segments.map((segment) => (
            <LinePath
              key={segment[0].atMs}
              data={segment}
              x={x}
              y={(point) => yScale(point.dataflowPercent)}
              stroke={colors.foreground.primary}
              strokeWidth={2}
              pointerEvents="none"
            />
          ))}
        </g>
      </>
    );
  },
);

/**
 * The replica's CPU against the time its workers spent running work, both as
 * shares of its allocation, with the measured span shaded.
 */
export const WorkerCpuCharts = ({
  width,
  startMs,
  endMs,
  openedAtMs,
  replicaCpu,
  series,
  replica,
  measured,
}: WorkerCpuChartsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { tooltipData, tooltipLeft, tooltipTop, showTooltip, hideTooltip } =
    useTooltip<number>();
  const { containerRef, containerBounds, TooltipInPortal } = useTooltipInPortal(
    { detectBounds: true, scroll: true },
  );
  const layout = React.useMemo(
    () =>
      buildXYGraphLayoutProps({
        width,
        height: SVG_HEIGHT_PX,
        margin: CHART_MARGIN,
      }),
    [width],
  );
  const xScale = React.useMemo(
    () =>
      scaleTime<number>({
        domain: [new Date(startMs), new Date(endMs)],
        range: layout.xScaleRange,
      }),
    [startMs, endMs, layout],
  );
  const yScale = React.useMemo(
    () =>
      scaleLinear<number>({
        domain: [0, 100],
        range: layout.yScaleRange,
        clamp: true,
      }),
    [layout],
  );
  const perCore = workersPerCore(replica.allocation);
  const segments = React.useMemo(() => {
    const result: ChartPoint[][] = [];
    for (const point of series) {
      if (point.afterGap || result.length === 0) result.push([]);
      result[result.length - 1].push({
        ...point,
        dataflowPercent: point.averageBusyFraction * perCore * 100,
        replicaPercent: replicaCpuAt(replicaCpu, point.atMs)?.percent,
      });
    }
    return result;
  }, [series, replicaCpu, perCore]);
  const openedX = xScale(new Date(openedAtMs));
  const { graphTop, graphBottom } = layout.graphLineCursorProps;
  const clipId = `worker-cpu-clip-${React.useId().replace(/[^a-zA-Z0-9]/g, "")}`;
  const marksProps = {
    layout,
    xScale,
    yScale,
    openedX,
    segments,
    clipId,
    color: replica.color,
    measured,
  };

  const hoverPoint =
    tooltipData === undefined
      ? undefined
      : nearest(
          xScale,
          segments.flat(),
          tooltipData,
          WORKER_HOVER_MAX_DISTANCE_MS,
        );
  const hoverReplica =
    tooltipData === undefined
      ? undefined
      : replicaCpuAt(replicaCpu, tooltipData)?.percent;
  const hover = (
    <>
      {tooltipData !== undefined && (
        <GraphLineCursor
          point={{ x: xScale(new Date(tooltipData)), y: 0 }}
          graphTop={graphTop}
          graphBottom={graphBottom}
        />
      )}
      <HoverOverlay
        layout={layout}
        xScale={xScale}
        onPointerMove={(timeMs, event) =>
          showTooltip({
            tooltipData: timeMs,
            tooltipLeft: event.clientX - containerBounds.left,
            tooltipTop: event.clientY - containerBounds.top,
          })
        }
        onPointerLeave={hideTooltip}
      />
    </>
  );

  return (
    <Box ref={containerRef} position="relative" width="100%">
      <VStack spacing={2} alignItems="stretch">
        <ChartHeader
          title="CPU"
          description={`% of ${replica.name}'s allocation${measured ? ". The shaded span is the one measured above" : ""}`}
          legend={[
            {
              label: "Replica CPU",
              color: replica.color,
              variant: "line",
            },
            {
              label: "Workers' time",
              color: colors.foreground.primary,
              variant: "line",
            },
            {
              label: "Overhead",
              color: gapFill(colors.foreground.secondary),
              variant: "square",
            },
          ]}
        />
        <VStack spacing={0} alignItems="stretch">
          <svg
            {...layout.svgProps}
            role="img"
            aria-label={`${replica.name}'s CPU and its workers' time over the last minutes${measured ? ", with the measured span shaded" : ""}`}
          >
            <CpuMarks {...marksProps} replicaCpu={replicaCpu} />
            {hover}
          </svg>
          <TimeAxis
            width={width}
            xScale={xScale}
            tickFormat={clockTick(startMs, endMs)}
          />
        </VStack>
      </VStack>
      {tooltipData !== undefined &&
        tooltipLeft !== undefined &&
        tooltipTop !== undefined && (
          <GraphTooltip
            component={TooltipInPortal}
            top={tooltipTop}
            left={tooltipLeft}
          >
            <VStack
              spacing={1.5}
              alignItems="stretch"
              padding={3}
              width="268px"
            >
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                {formatDate(
                  new Date(hoverPoint?.atMs ?? tooltipData),
                  TIME_FORMAT,
                )}
              </Text>
              {hoverReplica !== undefined && (
                <TooltipRow
                  label="Replica CPU"
                  value={`${formatPercentage(hoverReplica / 100, 0)} of allocation`}
                />
              )}
              {hoverPoint ? (
                <>
                  <TooltipRow
                    label="Workers' time"
                    value={`${formatPercentage(hoverPoint.dataflowPercent / 100, 0)} of allocation`}
                  />
                  {hoverReplica !== undefined && (
                    <TooltipRow
                      label="Overhead"
                      value={`${formatPercentage(Math.max(0, hoverReplica - hoverPoint.dataflowPercent) / 100, 0)} of allocation`}
                    />
                  )}
                </>
              ) : (
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  No worker reading at this time.
                </Text>
              )}
            </VStack>
          </GraphTooltip>
        )}
    </Box>
  );
};
