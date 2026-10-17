// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { scaleLinear, scaleTime } from "@visx/scale";
import { Area, Line, LinePath } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import React from "react";

import { GraphLineCursor, GraphTooltip } from "~/components/graphComponents";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  TIME_FORMAT,
  TIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import {
  ChartHeader,
  HoverOverlay,
  Layout,
  PercentAxis,
  PercentScale,
  TimeAxis,
  TimeScale,
} from "../ResourceUsage/ResourceUsageCharts";
import {
  CHART_MARGIN,
  PERCENT_TICKS,
} from "../ResourceUsage/resourceUsageStyles";
import {
  CpuPoint,
  SMOOTHING_WINDOW_MS,
  WorkerBusyPoint,
  workerLabel,
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
}

const nearest = <T extends { atMs: number }>(points: T[], atMs: number) => {
  let best: T | undefined;
  for (const point of points) {
    if (!best || Math.abs(point.atMs - atMs) < Math.abs(best.atMs - atMs)) {
      best = point;
    }
  }
  return best;
};

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
        stroke={colors.accent.purple}
        strokeDasharray="3 3"
      />
    </g>
  );
};

// Clock times, since the window runs ahead of now while readings fill it and
// times relative to its end would read wrong.
const clockTick = (ms: number) =>
  formatDate(new Date(ms), TIME_FORMAT_NO_SECONDS);

type ChartPoint = WorkerBusyPoint & {
  /** Time running dataflows, as a percent of the replica's allocation. */
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
    replicaCpu,
  }: MarksProps & { replicaCpu: CpuPoint[] }) => {
    const { colors } = useTheme<MaterializeTheme>();
    const outsideFill = gapFill(colors.foreground.secondary);
    const { graphTop, graphBottom } = layout.graphLineCursorProps;
    const [plotLeft, plotRight] = layout.xScaleRange;
    const x = (point: { atMs: number }) => xScale(new Date(point.atMs));
    return (
      <>
        <BeforeOpen
          layout={layout}
          x={openedX}
          label="Per-worker CPU starts when you open this"
        />
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
          <LinePath
            data={replicaCpu}
            x={x}
            y={(point) => yScale(point.percent)}
            stroke={colors.accent.purple}
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

const WorkerMarks = React.memo(
  ({ layout, xScale, yScale, openedX, segments, clipId }: MarksProps) => {
    const { colors } = useTheme<MaterializeTheme>();
    const skewFill = gapFill(colors.accent.orange);
    const x = (point: { atMs: number }) => xScale(new Date(point.atMs));
    return (
      <>
        <BeforeOpen layout={layout} x={openedX} />
        <PercentAxis layout={layout} yScale={yScale} ticks={PERCENT_TICKS} />
        <g clipPath={`url(#${clipId})`}>
          {segments.map((segment) => (
            <React.Fragment key={segment[0].atMs}>
              <Area
                data={segment}
                x={x}
                y0={(point) => yScale(point.averageBusyFraction * 100)}
                y1={(point) => yScale(point.busiest.busyFraction * 100)}
                fill={skewFill}
                pointerEvents="none"
              />
              <LinePath
                data={segment}
                x={x}
                y={(point) => yScale(point.averageBusyFraction * 100)}
                stroke={colors.foreground.secondary}
                strokeWidth={2}
                strokeDasharray="4 3"
                pointerEvents="none"
              />
              <LinePath
                data={segment}
                x={x}
                y={(point) => yScale(point.busiest.busyFraction * 100)}
                stroke={colors.accent.orange}
                strokeWidth={2}
                pointerEvents="none"
              />
            </React.Fragment>
          ))}
        </g>
      </>
    );
  },
);

/**
 * Two charts on one time axis, each in one unit: the replica's CPU against
 * the time its workers spent running dataflows, as shares of its allocation,
 * and the busiest worker against the average one, as shares of one core.
 */
export const WorkerCpuCharts = ({
  width,
  startMs,
  endMs,
  openedAtMs,
  replicaCpu,
  series,
  replica,
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
  const perCore = workersPerCore(
    replica.workersPerProcess,
    replica.cpuNanoCoresPerProcess,
  );
  const segments = React.useMemo(() => {
    const result: ChartPoint[][] = [];
    for (const point of series) {
      if (point.afterGap || result.length === 0) result.push([]);
      result[result.length - 1].push({
        ...point,
        dataflowPercent: point.averageBusyFraction * perCore * 100,
        replicaPercent: nearest(replicaCpu, point.atMs)?.percent,
      });
    }
    return result;
  }, [series, replicaCpu, perCore]);
  const openedX = xScale(new Date(openedAtMs));
  const { graphTop, graphBottom } = layout.graphLineCursorProps;
  const clipId = `worker-cpu-clip-${React.useId().replace(/[^a-zA-Z0-9]/g, "")}`;
  const marksProps = { layout, xScale, yScale, openedX, segments, clipId };

  const hoverPoint =
    tooltipData === undefined
      ? undefined
      : nearest(segments.flat(), tooltipData);
  const hoverReplica =
    tooltipData === undefined
      ? undefined
      : nearest(replicaCpu, tooltipData)?.percent;
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
      <VStack spacing={5} alignItems="stretch">
        <VStack spacing={2} alignItems="stretch">
          <ChartHeader
            title="CPU"
            description={`% of ${replica.name}'s allocation`}
            legend={[
              {
                label: "Replica CPU",
                color: colors.accent.purple,
                variant: "line",
              },
              {
                label: "Running dataflows",
                color: colors.foreground.primary,
                variant: "line",
              },
              {
                label: "Outside dataflows",
                color: gapFill(colors.foreground.secondary),
                variant: "square",
              },
            ]}
          />
          <svg {...layout.svgProps} role="img" aria-label="Replica CPU">
            <CpuMarks {...marksProps} replicaCpu={replicaCpu} />
            {hover}
          </svg>
        </VStack>
        <VStack spacing={2} alignItems="stretch">
          <ChartHeader
            title="Workers"
            description={`% of one core spent running dataflows, over the last ${SMOOTHING_WINDOW_MS / 1000}s at each point`}
            legend={[
              {
                label: "Busiest worker",
                color: colors.accent.orange,
                variant: "line",
              },
              {
                label: "Average worker",
                color: colors.foreground.secondary,
                variant: "dashed",
              },
              {
                label: "Skew",
                color: gapFill(colors.accent.orange),
                variant: "square",
              },
            ]}
          />
          <VStack spacing={0} alignItems="stretch">
            <svg {...layout.svgProps} role="img" aria-label="Worker CPU">
              <WorkerMarks {...marksProps} />
              {hover}
            </svg>
            <TimeAxis width={width} xScale={xScale} tickFormat={clockTick} />
          </VStack>
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
                    label="Running dataflows"
                    value={`${formatPercentage(hoverPoint.dataflowPercent / 100, 0)} of allocation`}
                  />
                  <TooltipRow
                    label={`Busiest · ${workerLabel(hoverPoint.busiest.workerId, replica)}`}
                    value={`${formatPercentage(hoverPoint.busiest.busyFraction, 0)} of a core`}
                    isStrong
                  />
                  <TooltipRow
                    label="Average worker"
                    value={`${formatPercentage(hoverPoint.averageBusyFraction, 0)} of a core`}
                  />
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

const TooltipRow = ({
  label,
  value,
  isStrong,
}: {
  label: string;
  value: string;
  isStrong?: boolean;
}) => (
  <HStack justifyContent="space-between" spacing={3}>
    <Text textStyle="text-small">{label}</Text>
    <Text
      textStyle={isStrong ? "text-ui-med" : "text-small"}
      whiteSpace="nowrap"
    >
      {value}
    </Text>
  </HStack>
);
