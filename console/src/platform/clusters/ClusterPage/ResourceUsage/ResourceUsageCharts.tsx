// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { GridRows } from "@visx/grid";
import { scaleLinear, scaleTime } from "@visx/scale";
import { Line, LinePath } from "@visx/shape";
import { max } from "d3";
import React from "react";

import {
  GraphEventOverlay,
  GraphLineCursor,
  GraphTooltipCursor,
  TooltipColorSwatch,
} from "~/components/graphComponents";
import {
  DataPoint,
  ReplicaData,
} from "~/platform/clusters/ClusterOverview/types";
import { MaterializeTheme } from "~/theme";
import { kebabToSentenceCase, notNullOrUndefined, pluralize } from "~/util";
import {
  DATE_FORMAT,
  formatDate,
  FRIENDLY_DATETIME_FORMAT,
  TIME_FORMAT,
} from "~/utils/dateFormat";
import { formatBytesShort } from "~/utils/format";
import {
  buildXYGraphLayoutProps,
  eventToTimestamp,
  relativeTimeTickFormat,
} from "~/utils/graph";

import {
  canSwap,
  ChangeTick,
  datumAt,
  DdlEvent,
  memoryBar,
  placeChangeTicks,
  placeStatusSegments,
  ramLimitPercent,
  ReplicaTimeline,
  seriesWithBarAt,
} from "./resourceUsageModel";
import {
  CHART_MARGIN,
  percentScale,
  useStatusStyles,
} from "./resourceUsageStyles";

export type TimeScale = ReturnType<typeof scaleTime<number>>;

export type PercentScale = ReturnType<typeof scaleLinear<number>>;

export type Layout = ReturnType<typeof buildXYGraphLayoutProps>;

// 0 to 100% keeps 168px on an axis that tops out at 125%.
const PLOT_HEIGHT_PX = 210;
const PLOT_SVG_HEIGHT_PX =
  CHART_MARGIN.top + PLOT_HEIGHT_PX + CHART_MARGIN.bottom;
// Room above a chart with no headroom, such as the status rows, for its
// header.
const INSET_HEADER_PX = 32;
const AXIS_LABEL_FONT_PX = 11;
const AXIS_LABEL_INSET_PX = 4;
const TIME_AXIS_HEIGHT_PX = 18;
const STATUS_ROW_HEIGHT_PX = 18;
const STATUS_ROW_GAP_PX = 10;
const STATUS_MARGIN = { ...CHART_MARGIN, top: 0, bottom: 0 };
const MIN_STATUS_SEGMENT_PX = 4;
const CHANGES_ROW_HEIGHT_PX = 18;
// Creations closer than this share a tick, which is also each tick's hover
// target width.
const CHANGE_MERGE_PX = 8;
const CHANGE_TICK_WIDTH_PX = 3;
const CHANGE_TICK_MIN_HEIGHT_PX = 6;
const CHANGE_TICK_STEP_PX = 2;
// The most objects a change tick's tooltip names before summarizing the rest.
const CHANGE_TOOLTIP_OBJECT_LIMIT = 5;
const TIME_TICK_COUNT = 8;
// Replicas' bars at one time overlap by this share of a bar, so each group
// reads as one point in time.
const BAR_OVERLAP = 0.5;
const BADGE_FONT_PX = 11;
// Monospace glyphs are about 0.6em wide, which sizes a badge to its label.
const BADGE_CHAR_PX = BADGE_FONT_PX * 0.6;
const BADGE_PADDING_PX = 4;
// Shorter than a status row, so the row's state shows above and below it.
const BADGE_HEIGHT_PX = 14;
const STATUS_LABEL_INSET_PX = 6;

// The square swatch is 8px wide, so more stripes are too thin to tell apart.
const MAX_SWATCH_STRIPES = 3;

interface LegendSwatchProps {
  /**
   * Several colors split the swatch, one stripe per replica it stands for, up
   * to the first `MAX_SWATCH_STRIPES`.
   */
  color: string | string[];
  variant: "square" | "line" | "dashed" | "diamond" | "tick";
}

export const LegendSwatch = ({ color, variant }: LegendSwatchProps) => {
  const palette = (typeof color === "string" ? [color] : color).slice(
    0,
    MAX_SWATCH_STRIPES,
  );
  if (variant === "square") {
    if (palette.length === 1) return <TooltipColorSwatch color={palette[0]} />;
    return (
      <Flex width="2" height="2" borderRadius="sm" overflow="hidden">
        {palette.map((stripe, stripeIndex) => (
          <Box
            key={stripeIndex}
            flex="1"
            background={stripe}
            data-testid="swatch-stripe"
          />
        ))}
      </Flex>
    );
  }
  if (variant === "diamond") {
    return (
      <Box boxSize="2" background={palette[0]} transform="rotate(45deg)" />
    );
  }
  if (variant === "tick") {
    return (
      <Box
        width={`${CHANGE_TICK_WIDTH_PX}px`}
        height="2.5"
        borderRadius="1px"
        background={palette[0]}
      />
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

export interface ChartHeaderProps {
  title: string;
  description?: string;
  legend?: Array<{ label: string } & LegendSwatchProps>;
  /** A control after the legend, such as a button opening more detail. */
  action?: React.ReactNode;
}

/** A title and a legend, baseline-aligned across the top of a chart. */
export const ChartHeader = ({
  title,
  description,
  legend,
  action,
}: ChartHeaderProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  // Only the title and legend take the pointer, so hovering between them
  // reaches the chart below.
  const headerItem = { px: 1.5, pointerEvents: "auto" } as const;
  return (
    <Flex
      justifyContent="space-between"
      alignItems="baseline"
      flexWrap="wrap"
      columnGap={6}
      rowGap={1}
      width="100%"
      pointerEvents="none"
    >
      <HStack spacing={2} alignItems="baseline" {...headerItem}>
        <Text textStyle="text-ui-med">{title}</Text>
        {description && (
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {description}
          </Text>
        )}
      </HStack>
      {(legend || action) && (
        <Flex
          flexWrap="wrap"
          justifyContent="flex-end"
          alignItems="center"
          columnGap={4}
          rowGap={1}
          minWidth={0}
          {...headerItem}
        >
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
          {action}
        </Flex>
      )}
    </Flex>
  );
};

export interface ChartFrameProps extends ChartHeaderProps {
  /**
   * Leaves room above the chart for the header. Without it, the header floats
   * over the chart's headroom.
   */
  isHeaderInset?: boolean;
  children: React.ReactNode;
}

/** A chart between rules, with its header in the top corners. */
export const ChartFrame = ({
  isHeaderInset = false,
  children,
  ...header
}: ChartFrameProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Box
      position="relative"
      borderTopWidth="1px"
      borderBottomWidth="1px"
      borderColor={colors.border.secondary}
      pt={isHeaderInset ? `${INSET_HEADER_PX}px` : undefined}
      pb={isHeaderInset ? 2 : undefined}
    >
      {children}
      <Box position="absolute" top={1} left={0} right={0} pointerEvents="none">
        <ChartHeader {...header} />
      </Box>
    </Box>
  );
};

/** Dashed lines at `ticks`, labeled in the right margin just below each. */
export const PercentAxis = ({
  layout,
  yScale,
  ticks,
}: {
  layout: Layout;
  yScale: PercentScale;
  ticks: number[];
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [rangeStart, rangeEnd] = layout.xScaleRange;
  return (
    <>
      <GridRows
        {...layout.gridRowsProps}
        // Through the label margin, so each label sits on its line.
        width={layout.svgProps.width - rangeStart}
        scale={yScale}
        tickValues={ticks}
        stroke={colors.border.primary}
        strokeDasharray="3 3"
        pointerEvents="none"
      />
      <g pointerEvents="none">
        {ticks.map((tick) => (
          <text
            key={tick}
            x={rangeEnd + AXIS_LABEL_INSET_PX}
            y={yScale(tick) + AXIS_LABEL_INSET_PX}
            dominantBaseline="hanging"
            fill={colors.foreground.secondary}
            fontSize={AXIS_LABEL_FONT_PX}
          >
            {tick}%
          </text>
        ))}
      </g>
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

/** A faint line at a hovered or focused change tick's time. */
const ChangeGuide = ({
  layout,
  x,
}: {
  layout: Layout;
  x: number | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (x === undefined) return null;
  const { graphTop, graphBottom } = layout.graphLineCursorProps;
  return (
    <Line
      data-testid="change-guide"
      from={{ x, y: graphTop }}
      to={{ x, y: graphBottom }}
      stroke={colors.foreground.tertiary}
      strokeWidth={1}
      strokeDasharray="2 3"
      pointerEvents="none"
    />
  );
};

interface PlotProps {
  width: number;
  xScale: TimeScale;
  series: ReplicaData[];
  colorMap: Map<string, string>;
  /** The time of the change tick being hovered or focused. */
  changeGuideMs?: number;
  hoverTimeMs?: number;
  onPointerMove: (timeMs: number, event: React.PointerEvent) => void;
  onPointerLeave: () => void;
}

export const HoverOverlay = ({
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

// Memoized so pointer moves, which only change the cursor, skip redrawing the
// lines.
const CpuMarks = React.memo(
  ({
    layout,
    xScale,
    yScale,
    ticks,
    series,
    colorMap,
    showLimit,
  }: {
    layout: Layout;
    xScale: TimeScale;
    yScale: PercentScale;
    ticks: number[];
    series: ReplicaData[];
    colorMap: Map<string, string>;
    showLimit: boolean;
  }) => {
    const { colors } = useTheme<MaterializeTheme>();
    return (
      <>
        <PercentAxis layout={layout} yScale={yScale} ticks={ticks} />
        {series.map((replica) => (
          <LinePath
            key={replica.id}
            data={replica.data}
            defined={(point) => point.cpuPercent !== null}
            x={(point) => xScale(point.bucketStart)}
            y={(point) => yScale(point.cpuPercent ?? 0)}
            stroke={colorMap.get(replica.id)}
            strokeWidth={2}
            pointerEvents="none"
          />
        ))}
        {/* A reading with no defined neighbor draws no line segment, such as a
            short-lived replica merged into one bucket, so it gets a dot. */}
        {series.flatMap((replica) =>
          replica.data.flatMap((point, index, data) => {
            const hasReading = (neighbor: DataPoint | undefined) =>
              neighbor !== undefined && neighbor.cpuPercent !== null;
            if (
              point.cpuPercent === null ||
              hasReading(data[index - 1]) ||
              hasReading(data[index + 1])
            ) {
              return [];
            }
            return [
              <circle
                key={`${replica.id}-${point.bucketStart}`}
                cx={xScale(point.bucketStart)}
                cy={yScale(point.cpuPercent)}
                r={2.5}
                fill={colorMap.get(replica.id)}
                pointerEvents="none"
              />,
            ];
          }),
        )}
        {showLimit && (
          <ThresholdLine
            layout={layout}
            y={yScale(100)}
            color={colors.accent.red}
          />
        )}
      </>
    );
  },
);

export type CpuChartProps = PlotProps & {
  headerAction?: React.ReactNode;
};

/**
 * A line per replica as a share of its CPU allocation. Readings past 100%
 * extend the axis, with a dashed line marking the allocation.
 */
export const CpuChart = ({
  width,
  xScale,
  series,
  colorMap,
  changeGuideMs,
  hoverTimeMs,
  onPointerMove,
  onPointerLeave,
  headerAction,
}: CpuChartProps) => {
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
  const peak = React.useMemo(
    () =>
      max(series, (replica) =>
        max(replica.data, (point) => point.cpuPercent),
      ) ?? 0,
    [series],
  );
  const isOverflowing = peak > 100;
  const { yScale, ticks } = React.useMemo(
    () => percentScale(peak, layout),
    [peak, layout],
  );

  return (
    <ChartFrame
      title="CPU"
      legend={
        isOverflowing
          ? [
              {
                label: "CPU allocation",
                color: colors.accent.red,
                variant: "dashed",
              },
            ]
          : undefined
      }
      action={headerAction}
    >
      <svg
        {...layout.svgProps}
        display="block"
        role="img"
        aria-label="CPU usage"
      >
        <CpuMarks
          layout={layout}
          xScale={xScale}
          yScale={yScale}
          ticks={ticks}
          series={series}
          colorMap={colorMap}
          showLimit={isOverflowing}
        />
        <ChangeGuide
          layout={layout}
          x={changeGuideMs === undefined ? undefined : xScale(changeGuideMs)}
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
                const value = point?.cpuPercent;
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
    </ChartFrame>
  );
};

const MemoryMarks = React.memo(
  ({
    layout,
    xScale,
    yScale,
    ticks,
    series,
    colorMap,
    ramLimit,
  }: {
    layout: Layout;
    xScale: TimeScale;
    yScale: PercentScale;
    ticks: number[];
    series: ReplicaData[];
    colorMap: Map<string, string>;
    ramLimit: number | undefined;
  }) => {
    const { colors } = useTheme<MaterializeTheme>();
    const { graphBottom } = layout.graphLineCursorProps;
    // A surface ring keeps overlapping bars apart.
    const ring = { stroke: colors.background.primary, strokeWidth: 2 };
    const idsByBucketStart = seriesWithBarAt(series);
    const bars = series.map((replica) =>
      replica.data.map((point) => {
        const bar = memoryBar(point);
        if (!bar) return null;
        const barIds = idsByBucketStart.get(point.bucketStart) ?? [replica.id];
        const slotIndex = barIds.indexOf(replica.id);
        const bucketLeft = xScale(point.bucketStart);
        // 2px gap between neighboring times.
        const groupWidth = Math.max(
          1,
          xScale(point.bucketEnd) - bucketLeft - 2,
        );
        const step = 1 - BAR_OVERLAP;
        const barWidth = groupWidth / (1 + (barIds.length - 1) * step);
        const x = bucketLeft + 1 + slotIndex * barWidth * step;
        const radius = Math.min(2, barWidth / 2);
        const ramTop = yScale(bar.heap - bar.swap);
        const barRing = barIds.length > 1 ? ring : {};
        return (
          <g key={`${replica.id}-${point.bucketStart}`}>
            <rect
              x={x}
              y={ramTop}
              width={barWidth}
              height={Math.max(0, graphBottom - ramTop)}
              rx={radius}
              fill={colorMap.get(replica.id) ?? colors.foreground.tertiary}
              {...barRing}
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
                {...barRing}
              />
            )}
          </g>
        );
      }),
    );
    return (
      <>
        <PercentAxis layout={layout} yScale={yScale} ticks={ticks} />
        <g pointerEvents="none">{bars}</g>
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
  /** False when readings are shares of the memory allocation instead. */
  hasHeapLimit: boolean;
};

export const MemoryChart = ({
  width,
  xScale,
  series,
  colorMap,
  changeGuideMs,
  hoverTimeMs,
  onPointerMove,
  onPointerLeave,
  heapLimitBytes,
  hasHeapLimit,
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
  const peak = React.useMemo(
    () =>
      max(series, (replica) =>
        max(replica.data, (point) => memoryBar(point)?.heap),
      ) ?? 0,
    [series],
  );
  const { yScale, ticks } = React.useMemo(
    () => percentScale(peak, layout),
    [peak, layout],
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
      label: hasSplit || !hasHeapLimit ? "memory" : "heap",
      color: replicaColors(series, colorMap, colors.foreground.tertiary),
      variant: "square",
    },
  ];
  if (hasSwap) {
    legend.push({
      label: "disk",
      color: colors.accent.orange,
      variant: "square",
    });
  }
  if (ramLimit !== undefined) {
    legend.push({
      label: limitLabel("memory limit", heapLimitBytes, ramLimit),
      color: colors.accent.orange,
      variant: "dashed",
    });
  }
  legend.push({
    label: limitLabel(
      hasHeapLimit ? "heap limit" : "memory limit",
      heapLimitBytes,
      100,
    ),
    color: colors.accent.red,
    variant: "dashed",
  });

  return (
    <ChartFrame title="Memory" legend={legend}>
      <svg
        {...layout.svgProps}
        display="block"
        role="img"
        aria-label="Memory usage"
      >
        <MemoryMarks
          layout={layout}
          xScale={xScale}
          yScale={yScale}
          ticks={ticks}
          series={series}
          colorMap={colorMap}
          ramLimit={ramLimit}
        />
        <ChangeGuide
          layout={layout}
          x={changeGuideMs === undefined ? undefined : xScale(changeGuideMs)}
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
    </ChartFrame>
  );
};

export interface TimeAxisProps {
  width: number;
  xScale: TimeScale;
}

/**
 * Evenly spaced from the start of `xScale`'s domain, then its end, matching
 * `UtilizationGraph`.
 */
const timeTickValues = (xScale: TimeScale) => {
  const [startMs, endMs] = xScale.domain().map((date) => date.getTime());
  return [
    ...Array.from(
      { length: TIME_TICK_COUNT },
      (_, tickIndex) =>
        startMs + (tickIndex * (endMs - startMs)) / TIME_TICK_COUNT,
    ),
    endMs,
  ];
};

/** Each tick's time before now, just right of its gridline. */
export const TimeAxis = ({ width, xScale }: TimeAxisProps) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const [startDate, endDate] = xScale.domain();
  const endMs = endDate.getTime();
  return (
    <svg width={width} height={TIME_AXIS_HEIGHT_PX} display="block" aria-hidden>
      {timeTickValues(xScale).map((value) => (
        <text
          key={value}
          x={xScale(value) + AXIS_LABEL_INSET_PX}
          y={TIME_AXIS_HEIGHT_PX / 2}
          dominantBaseline="central"
          fill={colors.foreground.secondary}
          fontFamily={fonts.mono}
          fontSize={AXIS_LABEL_FONT_PX}
        >
          {value === endMs
            ? "now"
            : relativeTimeTickFormat(value, startDate, endDate)}
        </text>
      ))}
    </svg>
  );
};

/**
 * Dashed lines at the time ticks and the right edge, for behind a stack of
 * charts and their time axes. Its parent must be positioned, and the stack
 * positioned too so it paints over the lines.
 */
export const TimeGridLines = ({ width, xScale }: TimeAxisProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  // Inset half a pixel so the edge lines aren't clipped to half their width.
  const xs = [
    ...timeTickValues(xScale).map((value) => xScale(value)),
    width,
  ].map((x) => Math.min(width - 0.5, Math.max(0.5, x)));
  return (
    <Box
      as="svg"
      position="absolute"
      inset={0}
      width="100%"
      height="100%"
      pointerEvents="none"
      aria-hidden
    >
      {xs.map((x) => (
        <line
          key={x}
          x1={x}
          x2={x}
          y1="0"
          y2="100%"
          stroke={colors.border.primary}
          strokeDasharray="3 3"
        />
      ))}
    </Box>
  );
};

export interface StatusTimelineRow {
  id: string;
  name: string;
  timeline: ReplicaTimeline;
  /** Objects the replica has yet to hydrate. */
  unhydratedCount?: number;
}

// An in-progress hydration has no recorded start, so it's a badge at the now
// edge, not a stretch.
const HydratingBadge = ({
  count,
  right,
  center,
}: {
  count: number;
  right: number;
  center: number;
}) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const statusStyles = useStatusStyles();
  const label = `Hydrating · ${count} ${pluralize(count, "object", "objects")}`;
  const width = label.length * BADGE_CHAR_PX + 2 * BADGE_PADDING_PX;
  return (
    <g>
      <rect
        x={right - width}
        y={center - BADGE_HEIGHT_PX / 2}
        width={width}
        height={BADGE_HEIGHT_PX}
        rx={3}
        fill={colors.background.primary}
        stroke={statusStyles.hydrating.color}
      />
      <text
        x={right - BADGE_PADDING_PX}
        y={center + 4}
        textAnchor="end"
        fill={colors.foreground.primary}
        fontFamily={fonts.mono}
        fontSize={BADGE_FONT_PX}
      >
        {label}
      </text>
    </g>
  );
};

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
              <rect
                x={rangeStart}
                y={top}
                width={Math.max(0, rangeEnd - rangeStart)}
                height={STATUS_ROW_HEIGHT_PX}
                fill={colors.background.secondary}
              />
              {placeStatusSegments(
                row.timeline.segments,
                xScale,
                rangeEnd,
                MIN_STATUS_SEGMENT_PX,
              ).map(({ segment, x, width: segmentWidth, isWidened }) => {
                const style = statusStyles[segment.state];
                const rect = {
                  x,
                  y: top,
                  width: segmentWidth,
                  height: STATUS_ROW_HEIGHT_PX,
                };
                return (
                  <g key={segment.startMs}>
                    {/* Segments are translucent, so a widened one masks the
                        stretch it overlaps instead of blending with it. */}
                    {isWidened && (
                      <rect {...rect} fill={colors.background.secondary} />
                    )}
                    <rect
                      {...rect}
                      fill={style.color}
                      fillOpacity={style.opacity}
                    />
                  </g>
                );
              })}
              <text
                x={rangeStart + STATUS_LABEL_INSET_PX}
                y={center}
                dominantBaseline="central"
                fill={colors.foreground.primary}
                fontFamily={fonts.mono}
                fontSize={AXIS_LABEL_FONT_PX}
              >
                {row.name}
              </text>
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
              {row.unhydratedCount !== undefined &&
                row.unhydratedCount > 0 &&
                row.timeline.segments.at(-1)?.state !== "offline" && (
                  <HydratingBadge
                    count={row.unhydratedCount}
                    right={rangeEnd}
                    center={center}
                  />
                )}
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
    <ChartFrame
      isHeaderInset
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
    >
      <svg
        {...layout.svgProps}
        display="block"
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
    </ChartFrame>
  );
};

/** "5 objects created". */
const changeCountLabel = (count: number) =>
  `${count} ${pluralize(count, "object", "objects")} created`;

/** A tick's time, or the span of its events' times. */
const changeTimeLabel = ({ startMs, endMs }: ChangeTick) => {
  if (startMs === endMs) return formatDate(startMs, FRIENDLY_DATETIME_FORMAT);
  if (formatDate(startMs, DATE_FORMAT) !== formatDate(endMs, DATE_FORMAT)) {
    return `${formatDate(startMs, FRIENDLY_DATETIME_FORMAT)} - ${formatDate(endMs, FRIENDLY_DATETIME_FORMAT)}`;
  }
  return `${formatDate(startMs, `${DATE_FORMAT} ${TIME_FORMAT}`)}-${formatDate(endMs, `${TIME_FORMAT} z`)}`;
};

/** The time, count and objects of a change tick. */
export const ChangeTooltipContent = ({ tick }: { tick: ChangeTick }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const rest = tick.events.length - CHANGE_TOOLTIP_OBJECT_LIMIT;
  return (
    <VStack align="start" spacing="1" width="100%" paddingX="4" paddingY="2">
      <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
        {changeTimeLabel(tick)}
      </Text>
      <Text textStyle="text-ui-med">
        {changeCountLabel(tick.events.length)}
      </Text>
      {tick.events
        .slice(0, CHANGE_TOOLTIP_OBJECT_LIMIT)
        .map(({ id, name, objectType }) => (
          <HStack key={id} spacing={2}>
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              {kebabToSentenceCase(objectType)}
            </Text>
            <Text textStyle="text-ui-reg">{name}</Text>
          </HStack>
        ))}
      {rest > 0 && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          and {rest} more
        </Text>
      )}
    </VStack>
  );
};

export interface ChangesLaneProps {
  width: number;
  xScale: TimeScale;
  events: DdlEvent[];
  /** The `startMs` of the tick whose tooltip is open. */
  activeStartMs?: number;
  onActivate: (tick: ChangeTick, target: Element) => void;
  onDeactivate: () => void;
}

/**
 * A tick where indexes and materialized views were created, taller for more
 * objects, in a row that lines up with the status rows. Hovering or focusing a
 * tick activates it.
 */
export const ChangesLane = ({
  width,
  xScale,
  events,
  activeStartMs,
  onActivate,
  onDeactivate,
}: ChangesLaneProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const layout = React.useMemo(
    () =>
      buildXYGraphLayoutProps({
        width,
        height: CHANGES_ROW_HEIGHT_PX + STATUS_ROW_GAP_PX,
        margin: STATUS_MARGIN,
      }),
    [width],
  );
  const [rangeStart, rangeEnd] = layout.xScaleRange;
  const ticks = React.useMemo(
    () =>
      placeChangeTicks(events, xScale, [rangeStart, rangeEnd], CHANGE_MERGE_PX),
    [events, xScale, rangeStart, rangeEnd],
  );
  const top = STATUS_ROW_GAP_PX / 2;
  const bottom = top + CHANGES_ROW_HEIGHT_PX;

  return (
    <ChartFrame
      isHeaderInset
      title="Objects Created"
      legend={[
        {
          label: "index or materialized view created",
          color: colors.foreground.secondary,
          variant: "tick",
        },
      ]}
    >
      <svg
        {...layout.svgProps}
        display="block"
        role="group"
        aria-label="Objects created"
      >
        <rect
          x={rangeStart}
          y={top}
          width={Math.max(0, rangeEnd - rangeStart)}
          height={CHANGES_ROW_HEIGHT_PX}
          fill={colors.background.secondary}
        />
        {ticks.map((tick) => {
          const x = xScale(tick.atMs);
          const height = Math.min(
            CHANGES_ROW_HEIGHT_PX,
            CHANGE_TICK_MIN_HEIGHT_PX +
              (tick.events.length - 1) * CHANGE_TICK_STEP_PX,
          );
          const activate = (event: React.SyntheticEvent<SVGGElement>) =>
            onActivate(tick, event.currentTarget);
          return (
            <g
              key={tick.startMs}
              role="img"
              aria-label={`${changeCountLabel(tick.events.length)}, ${changeTimeLabel(tick)}`}
              tabIndex={0}
              onPointerEnter={activate}
              onPointerLeave={onDeactivate}
              onFocus={activate}
              onBlur={onDeactivate}
            >
              <rect
                x={x - CHANGE_MERGE_PX / 2}
                y={top}
                width={CHANGE_MERGE_PX}
                height={CHANGES_ROW_HEIGHT_PX}
                fill="transparent"
              />
              <rect
                x={x - CHANGE_TICK_WIDTH_PX / 2}
                y={bottom - height}
                width={CHANGE_TICK_WIDTH_PX}
                height={height}
                rx={1}
                fill={
                  tick.startMs === activeStartMs
                    ? colors.foreground.primary
                    : colors.foreground.secondary
                }
              />
            </g>
          );
        })}
      </svg>
    </ChartFrame>
  );
};
