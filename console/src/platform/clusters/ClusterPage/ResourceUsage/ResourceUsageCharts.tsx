// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { AxisBottom, AxisLeft, AxisScale } from "@visx/axis";
import { GridRows } from "@visx/grid";
import { scaleLinear, scaleTime } from "@visx/scale";
import { Line, LinePath } from "@visx/shape";
import React from "react";

import {
  GraphEventOverlay,
  GraphLineCursor,
  GraphTooltipCursor,
  TooltipColorSwatch,
} from "~/components/graphComponents";
import { ReplicaData } from "~/platform/clusters/ClusterOverview/types";
import { MaterializeTheme } from "~/theme";
import { kebabToScreamingSpaceCase, notNullOrUndefined } from "~/util";
import { formatBytesShort } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  eventToTimestamp,
  relativeTimeTickFormat,
} from "~/utils/graph";

import {
  canSwap,
  datumAt,
  DdlEvent,
  memoryBar,
  ramLimitPercent,
  ReplicaTimeline,
} from "./resourceUsageModel";
import {
  CHART_MARGIN,
  PERCENT_TICKS,
  useStatusStyles,
} from "./resourceUsageStyles";

export type TimeScale = ReturnType<typeof scaleTime<number>>;

type PercentScale = ReturnType<typeof scaleLinear<number>>;

type Layout = ReturnType<typeof buildXYGraphLayoutProps>;

const PLOT_HEIGHT_PX = 168;
const PLOT_SVG_HEIGHT_PX =
  CHART_MARGIN.top + PLOT_HEIGHT_PX + CHART_MARGIN.bottom;
const STATUS_ROW_HEIGHT_PX = 14;
const STATUS_ROW_GAP_PX = 10;
const STATUS_MARGIN = { ...CHART_MARGIN, top: 0, bottom: 0 };
const DDL_LABEL_MIN_SPACING_PX = 200;
const TIME_TICK_COUNT = 8;

interface LegendSwatchProps {
  /** Several colors split the swatch, one stripe per replica it stands for. */
  color: string | string[];
  variant: "square" | "line" | "dashed" | "diamond";
}

const LegendSwatch = ({ color, variant }: LegendSwatchProps) => {
  const palette = typeof color === "string" ? [color] : color;
  if (variant === "square") {
    if (palette.length === 1) return <TooltipColorSwatch color={palette[0]} />;
    return (
      <Flex width="2" height="2" borderRadius="sm" overflow="hidden">
        {palette.map((stripe, stripeIndex) => (
          <Box key={stripeIndex} flex="1" background={stripe} />
        ))}
      </Flex>
    );
  }
  if (variant === "diamond") {
    return (
      <Box boxSize="2" background={palette[0]} transform="rotate(45deg)" />
    );
  }
  const segmentWidth = 16 / palette.length;
  return (
    <svg width={16} height={8} aria-hidden>
      {palette.map((stroke, segmentIndex) => (
        <line
          key={segmentIndex}
          x1={segmentIndex * segmentWidth}
          x2={(segmentIndex + 1) * segmentWidth}
          y1={4}
          y2={4}
          stroke={stroke}
          strokeWidth={2}
          strokeDasharray={variant === "dashed" ? "4 3" : undefined}
        />
      ))}
    </svg>
  );
};

/** The charted replicas' colors, for a swatch that stands for all of them. */
const replicaColors = (
  series: ReplicaData[],
  colorMap: Map<string, string>,
  fallback: string,
) => {
  const palette = series.flatMap(({ id }) => {
    const color = colorMap.get(id);
    return color ? [color] : [];
  });
  return palette.length > 0 ? palette : fallback;
};

interface ChartHeaderProps {
  title: string;
  description?: string;
  legend?: Array<{ label: string } & LegendSwatchProps>;
}

const ChartHeader = ({ title, description, legend }: ChartHeaderProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Flex
      justifyContent="space-between"
      alignItems="baseline"
      flexWrap="wrap"
      columnGap={6}
      rowGap={1}
      width="100%"
    >
      <HStack spacing={2} alignItems="baseline">
        <Text textStyle="text-ui-med">{title}</Text>
        {description && (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {description}
          </Text>
        )}
      </HStack>
      {legend && (
        <Flex
          as="ul"
          flexWrap="wrap"
          columnGap={4}
          rowGap={1}
          listStyleType="none"
        >
          {legend.map(({ label, color, variant }) => (
            <HStack as="li" key={label} spacing={1.5}>
              <LegendSwatch color={color} variant={variant} />
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
                whiteSpace="nowrap"
              >
                {label}
              </Text>
            </HStack>
          ))}
        </Flex>
      )}
    </Flex>
  );
};

const PercentAxis = ({
  layout,
  yScale,
  ticks,
}: {
  layout: Layout;
  yScale: PercentScale;
  ticks: number[];
}) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  return (
    <>
      <GridRows
        {...layout.gridRowsProps}
        scale={yScale}
        tickValues={ticks}
        stroke={colors.border.primary}
        strokeDasharray="2 4"
        pointerEvents="none"
      />
      <AxisLeft<AxisScale<number>>
        {...layout.axisLeftProps}
        scale={yScale}
        tickValues={ticks}
        hideAxisLine
        hideTicks
        tickFormat={(value) => `${value.valueOf()}%`}
        tickLabelProps={() => ({
          fill: colors.foreground.secondary,
          fontFamily: fonts.mono,
          fontSize: 12,
          textAnchor: "end",
          dx: -4,
          dy: 4,
        })}
      />
    </>
  );
};

const ThresholdLine = ({
  layout,
  y,
  color,
}: {
  layout: Layout;
  y: number;
  color: string;
}) => {
  const [left, right] = layout.xScaleRange;
  return (
    <Line
      from={{ x: left, y }}
      to={{ x: right, y }}
      stroke={color}
      strokeWidth={1.5}
      strokeDasharray="4 3"
      pointerEvents="none"
    />
  );
};

const ddlLabel = (event: DdlEvent) =>
  `CREATE ${kebabToScreamingSpaceCase(event.objectType)} ${event.name}`;

// Labels only markers with room for their text, so a burst of DDL doesn't
// stack unreadable labels. Every marker keeps its line.
const placeDdlMarkers = (
  events: DdlEvent[],
  xScale: TimeScale,
  showLabels: boolean,
) => {
  const [rangeStart, rangeEnd] = xScale.range();
  const placed = [];
  let lastLabelX = Number.NEGATIVE_INFINITY;
  for (const event of events) {
    const x = xScale(event.occurredAtMs);
    if (x < rangeStart || x > rangeEnd) continue;
    const isLabeled = showLabels && x - lastLabelX >= DDL_LABEL_MIN_SPACING_PX;
    if (isLabeled) lastLabelX = x;
    placed.push({
      event,
      x,
      isLabeled,
      isFlipped: rangeEnd - x < DDL_LABEL_MIN_SPACING_PX,
    });
  }
  return placed;
};

// Memoized like the marks, so pointer moves don't redraw the markers.
const DdlMarkers = React.memo(
  ({
    events,
    xScale,
    layout,
    showLabels,
  }: {
    events: DdlEvent[];
    xScale: TimeScale;
    layout: Layout;
    showLabels: boolean;
  }) => {
    const { colors, fonts } = useTheme<MaterializeTheme>();
    const { graphTop, graphBottom } = layout.graphLineCursorProps;
    return (
      <g pointerEvents="none">
        {placeDdlMarkers(events, xScale, showLabels).map(
          ({ event, x, isLabeled, isFlipped }) => (
            <g key={event.id}>
              <Line
                from={{ x, y: graphTop }}
                to={{ x, y: graphBottom }}
                stroke={colors.foreground.tertiary}
                strokeWidth={1}
                strokeDasharray="2 3"
              />
              {isLabeled && (
                <text
                  x={isFlipped ? x - 4 : x + 4}
                  y={graphTop - 6}
                  textAnchor={isFlipped ? "end" : "start"}
                  fill={colors.foreground.secondary}
                  fontFamily={fonts.mono}
                  fontSize={11}
                >
                  {ddlLabel(event)}
                </text>
              )}
            </g>
          ),
        )}
      </g>
    );
  },
);

interface PlotProps {
  width: number;
  xScale: TimeScale;
  series: ReplicaData[];
  colorMap: Map<string, string>;
  ddlEvents: DdlEvent[];
  hoverTimeMs?: number;
  onPointerMove: (timeMs: number, event: React.PointerEvent) => void;
  onPointerLeave: () => void;
}

const HoverOverlay = ({
  layout,
  xScale,
  onPointerMove,
  onPointerLeave,
}: { layout: Layout; xScale: TimeScale } & Pick<
  PlotProps,
  "onPointerMove" | "onPointerLeave"
>) => {
  const [startTime, endTime] = xScale.domain().map((date) => date.getTime());
  return (
    <GraphEventOverlay
      {...layout.graphEventOverlayProps}
      onPointerMove={(event) => {
        const timeMs = eventToTimestamp({ event, xScale, startTime, endTime });
        if (timeMs !== null) onPointerMove(timeMs, event);
      }}
      onPointerLeave={onPointerLeave}
    />
  );
};

type LineMetric = "cpuPercent" | "diskPercent";

// Memoized so pointer moves, which only change the cursor, skip redrawing the
// lines.
const LineMarks = React.memo(
  ({
    layout,
    xScale,
    yScale,
    series,
    colorMap,
    metric,
  }: {
    layout: Layout;
    xScale: TimeScale;
    yScale: PercentScale;
    series: ReplicaData[];
    colorMap: Map<string, string>;
    metric: LineMetric;
  }) => (
    <>
      <PercentAxis layout={layout} yScale={yScale} ticks={PERCENT_TICKS} />
      {series.map((replica) => (
        <LinePath
          key={replica.id}
          data={replica.data}
          defined={(point) => point[metric] !== null}
          x={(point) => xScale(point.bucketStart)}
          y={(point) => yScale(point[metric] ?? 0)}
          stroke={colorMap.get(replica.id)}
          strokeWidth={2}
          pointerEvents="none"
        />
      ))}
    </>
  ),
);

const LineChart = ({
  width,
  xScale,
  series,
  colorMap,
  ddlEvents,
  hoverTimeMs,
  onPointerMove,
  onPointerLeave,
  metric,
  title,
  description,
  ariaLabel,
  showDdlLabels,
}: PlotProps & {
  metric: LineMetric;
  title: string;
  description: string;
  ariaLabel: string;
  showDdlLabels: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const layout = React.useMemo(
    () =>
      buildXYGraphLayoutProps({
        width,
        height: PLOT_SVG_HEIGHT_PX,
        margin: CHART_MARGIN,
      }),
    [width],
  );
  const yScale = React.useMemo(
    () =>
      // Clamped: CPU bursts past its allocation draw as saturated, and the
      // tooltip keeps the real reading.
      scaleLinear<number>({
        domain: [0, 100],
        range: layout.yScaleRange,
        clamp: true,
      }),
    [layout],
  );

  return (
    <VStack spacing={2} alignItems="stretch" width="100%">
      <ChartHeader title={title} description={description} />
      <svg {...layout.svgProps} role="img" aria-label={ariaLabel}>
        <DdlMarkers
          events={ddlEvents}
          xScale={xScale}
          layout={layout}
          showLabels={showDdlLabels}
        />
        <LineMarks
          layout={layout}
          xScale={xScale}
          yScale={yScale}
          series={series}
          colorMap={colorMap}
          metric={metric}
        />
        {hoverTimeMs !== undefined && (
          <>
            <GraphLineCursor
              {...layout.graphLineCursorProps}
              point={{ x: xScale(hoverTimeMs), y: 0 }}
            />
            <GraphTooltipCursor
              points={series.flatMap((replica) => {
                const point = datumAt(replica.data, hoverTimeMs);
                const value = point?.[metric];
                if (!point || !notNullOrUndefined(value)) return [];
                return [
                  {
                    key: replica.id,
                    color: colorMap.get(replica.id) ?? colors.lineGraph[0],
                    x: xScale(point.bucketStart),
                    y: yScale(value),
                  },
                ];
              })}
            />
          </>
        )}
        <HoverOverlay
          layout={layout}
          xScale={xScale}
          onPointerMove={onPointerMove}
          onPointerLeave={onPointerLeave}
        />
      </svg>
    </VStack>
  );
};

export type CpuChartProps = PlotProps;

export const CpuChart = (props: CpuChartProps) => (
  <LineChart
    {...props}
    metric="cpuPercent"
    title="CPU"
    description="% of replica CPU allocation"
    ariaLabel="CPU usage"
    showDdlLabels
  />
);

export type DiskChartProps = PlotProps;

export const DiskChart = (props: DiskChartProps) => (
  <LineChart
    {...props}
    metric="diskPercent"
    title="Disk"
    description="% of replica disk allocation"
    ariaLabel="Disk usage"
    showDdlLabels={false}
  />
);

const MemoryMarks = React.memo(
  ({
    layout,
    xScale,
    yScale,
    series,
    colorMap,
    ramLimit,
  }: {
    layout: Layout;
    xScale: TimeScale;
    yScale: PercentScale;
    series: ReplicaData[];
    colorMap: Map<string, string>;
    ramLimit: number | undefined;
  }) => {
    const { colors } = useTheme<MaterializeTheme>();
    const { graphBottom } = layout.graphLineCursorProps;
    return (
      <>
        <PercentAxis layout={layout} yScale={yScale} ticks={PERCENT_TICKS} />
        <g pointerEvents="none">
          {series.map((replica, seriesIndex) =>
            replica.data.map((point) => {
              const bar = memoryBar(point);
              if (!bar) return null;
              const bucketLeft = xScale(point.bucketStart);
              const slotWidth =
                (xScale(point.bucketEnd) - bucketLeft) / series.length;
              // 2px gap between neighboring bars.
              const barWidth = Math.max(1, slotWidth - 2);
              const x = bucketLeft + seriesIndex * slotWidth + 1;
              const radius = Math.min(2, barWidth / 2);
              const ramTop = yScale(bar.heap - bar.swap);
              return (
                <g key={`${replica.id}-${point.bucketStart}`}>
                  <rect
                    x={x}
                    y={ramTop}
                    width={barWidth}
                    height={Math.max(0, graphBottom - ramTop)}
                    rx={radius}
                    fill={colorMap.get(replica.id)}
                  />
                  {bar.swap > 0 && (
                    <rect
                      x={x}
                      y={yScale(bar.heap)}
                      width={barWidth}
                      // 1px short of the RAM bar so a surface gap separates them.
                      height={Math.max(0, ramTop - yScale(bar.heap) - 1)}
                      rx={radius}
                      fill={colors.accent.orange}
                    />
                  )}
                </g>
              );
            }),
          )}
        </g>
        {ramLimit !== undefined && (
          <ThresholdLine
            layout={layout}
            y={yScale(ramLimit)}
            color={colors.accent.orange}
          />
        )}
        <ThresholdLine
          layout={layout}
          y={yScale(100)}
          color={colors.accent.red}
        />
      </>
    );
  },
);

/** A limit's legend label, with its size in bytes when the heap limit is known. */
const limitLabel = (
  name: string,
  heapLimitBytes: number | undefined,
  percentOfHeapLimit: number,
) => {
  if (heapLimitBytes === undefined) return name;
  const bytes = BigInt(Math.round((heapLimitBytes * percentOfHeapLimit) / 100));
  return `${name} · ${formatBytesShort(bytes)}`;
};

export type MemoryChartProps = PlotProps & {
  heapLimitBytes: number | undefined;
};

export const MemoryChart = ({
  width,
  xScale,
  series,
  colorMap,
  ddlEvents,
  hoverTimeMs,
  onPointerMove,
  onPointerLeave,
  heapLimitBytes,
}: MemoryChartProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const layout = React.useMemo(
    () =>
      buildXYGraphLayoutProps({
        width,
        height: PLOT_SVG_HEIGHT_PX,
        margin: CHART_MARGIN,
      }),
    [width],
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
  const { hasSplit, hasSwap, ramLimit } = React.useMemo(() => {
    const seriesData = series.map((replica) => replica.data);
    return {
      // Without the split, a bar is all of heap, swap included.
      hasSplit: seriesData.some((data) =>
        data.some((point) => point.swapPercent !== null),
      ),
      hasSwap: seriesData.some((data) => data.some(canSwap)),
      ramLimit: ramLimitPercent(seriesData),
    };
  }, [series]);

  const legend: ChartHeaderProps["legend"] = [
    {
      label: hasSplit ? "memory" : "heap",
      color: replicaColors(series, colorMap, colors.foreground.tertiary),
      variant: "square",
    },
  ];
  if (hasSwap) {
    legend.push({
      label: "swap",
      color: colors.accent.orange,
      variant: "square",
    });
  }
  if (ramLimit !== undefined) {
    legend.push({
      label: limitLabel("RAM limit", heapLimitBytes, ramLimit),
      color: colors.accent.orange,
      variant: "dashed",
    });
  }
  legend.push({
    label: limitLabel("heap limit", heapLimitBytes, 100),
    color: colors.accent.red,
    variant: "dashed",
  });

  return (
    <VStack spacing={2} alignItems="stretch" width="100%">
      <ChartHeader
        title="Memory"
        description="% of heap limit (RAM + swap)"
        legend={legend}
      />
      <svg {...layout.svgProps} role="img" aria-label="Memory usage">
        <DdlMarkers
          events={ddlEvents}
          xScale={xScale}
          layout={layout}
          showLabels={false}
        />
        <MemoryMarks
          layout={layout}
          xScale={xScale}
          yScale={yScale}
          series={series}
          colorMap={colorMap}
          ramLimit={ramLimit}
        />
        {hoverTimeMs !== undefined && (
          <GraphLineCursor
            {...layout.graphLineCursorProps}
            point={{ x: xScale(hoverTimeMs), y: 0 }}
          />
        )}
        <HoverOverlay
          layout={layout}
          xScale={xScale}
          onPointerMove={onPointerMove}
          onPointerLeave={onPointerLeave}
        />
      </svg>
    </VStack>
  );
};

export interface TimeAxisProps {
  width: number;
  xScale: TimeScale;
}

export const TimeAxis = ({ width, xScale }: TimeAxisProps) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const [startDate, endDate] = xScale.domain();
  // Evenly spaced and labeled by time before now, matching `UtilizationGraph`.
  const tickValues = Array.from(
    { length: TIME_TICK_COUNT },
    (_, tickIndex) =>
      startDate.getTime() +
      (tickIndex * (endDate.getTime() - startDate.getTime())) / TIME_TICK_COUNT,
  );
  return (
    // Visible overflow keeps the edge labels, centered on the plot's ends, from
    // being clipped. The card's padding leaves room for them.
    <svg width={width} height={24} overflow="visible" aria-hidden>
      <AxisBottom<AxisScale<number>>
        top={0}
        scale={xScale}
        tickValues={tickValues}
        hideTicks
        stroke={colors.border.secondary}
        strokeWidth={2}
        tickFormat={(value) =>
          relativeTimeTickFormat(value.valueOf(), startDate, endDate)
        }
        tickLabelProps={() => ({
          fill: colors.foreground.secondary,
          fontFamily: fonts.mono,
          fontSize: 12,
          textAnchor: "middle",
          dy: 4,
        })}
      />
    </svg>
  );
};

export interface StatusTimelineRow {
  id: string;
  name: string;
  timeline: ReplicaTimeline;
}

const StatusRows = React.memo(
  ({
    layout,
    xScale,
    rows,
  }: {
    layout: Layout;
    xScale: TimeScale;
    rows: StatusTimelineRow[];
  }) => {
    const { colors, fonts } = useTheme<MaterializeTheme>();
    const statusStyles = useStatusStyles();
    const [rangeStart, rangeEnd] = layout.xScaleRange;
    const rowPitch = STATUS_ROW_HEIGHT_PX + STATUS_ROW_GAP_PX;
    return (
      <>
        {rows.map((row, rowIndex) => {
          const top = rowIndex * rowPitch + STATUS_ROW_GAP_PX / 2;
          const center = top + STATUS_ROW_HEIGHT_PX / 2;
          return (
            <g key={row.id} pointerEvents="none">
              <text
                x={rangeStart - 8}
                y={center + 4}
                textAnchor="end"
                fill={colors.foreground.secondary}
                fontFamily={fonts.mono}
                fontSize={12}
              >
                {row.name}
              </text>
              <rect
                x={rangeStart}
                y={top}
                width={Math.max(0, rangeEnd - rangeStart)}
                height={STATUS_ROW_HEIGHT_PX}
                rx={3}
                fill={colors.background.secondary}
              />
              {row.timeline.segments.map((segment) => {
                const style = statusStyles[segment.state];
                const x = xScale(segment.startMs);
                return (
                  <rect
                    key={segment.startMs}
                    x={x}
                    y={top}
                    width={Math.max(1, xScale(segment.endMs) - x)}
                    height={STATUS_ROW_HEIGHT_PX}
                    rx={3}
                    fill={style.color}
                    fillOpacity={style.opacity}
                  />
                );
              })}
              {row.timeline.oomAtMs.map((atMs) => (
                <rect
                  key={atMs}
                  x={xScale(atMs) - 5}
                  y={center - 5}
                  width={10}
                  height={10}
                  transform={`rotate(45 ${xScale(atMs)} ${center})`}
                  fill={colors.accent.red}
                  stroke={colors.background.primary}
                  strokeWidth={2}
                />
              ))}
            </g>
          );
        })}
      </>
    );
  },
);

export interface ReplicaStatusTimelineProps {
  width: number;
  xScale: TimeScale;
  rows: StatusTimelineRow[];
  hoverTimeMs?: number;
  onPointerMove: PlotProps["onPointerMove"];
  onPointerLeave: PlotProps["onPointerLeave"];
}

export const ReplicaStatusTimeline = ({
  width,
  xScale,
  rows,
  hoverTimeMs,
  onPointerMove,
  onPointerLeave,
}: ReplicaStatusTimelineProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const statusStyles = useStatusStyles();
  const height =
    Math.max(1, rows.length) * (STATUS_ROW_HEIGHT_PX + STATUS_ROW_GAP_PX);
  const layout = React.useMemo(
    () => buildXYGraphLayoutProps({ width, height, margin: STATUS_MARGIN }),
    [width, height],
  );

  return (
    <VStack spacing={2} alignItems="stretch" width="100%">
      <ChartHeader
        title="Replica Status"
        legend={[
          ...Object.values(statusStyles).map(({ label, color }) => ({
            label,
            color,
            variant: "square" as const,
          })),
          {
            label: "out of memory",
            color: colors.accent.red,
            variant: "diamond",
          },
        ]}
      />
      <svg
        {...layout.svgProps}
        role="img"
        aria-label="Replica status over time"
      >
        <StatusRows layout={layout} xScale={xScale} rows={rows} />
        {hoverTimeMs !== undefined && (
          <GraphLineCursor
            {...layout.graphLineCursorProps}
            point={{ x: xScale(hoverTimeMs), y: 0 }}
          />
        )}
        <HoverOverlay
          layout={layout}
          xScale={xScale}
          onPointerMove={onPointerMove}
          onPointerLeave={onPointerLeave}
        />
      </svg>
    </VStack>
  );
};
