// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { useParentSize } from "@visx/responsive";
import { scaleLinear, scaleTime } from "@visx/scale";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import { max } from "d3";
import React from "react";

import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import { GraphTooltip, TooltipColorSwatch } from "~/components/graphComponents";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatDurationExact, formatDurationForAxis } from "~/utils/format";

import { TimeAxis } from "../ResourceUsage/ResourceUsageCharts";
import { CHART_MARGIN } from "../ResourceUsage/resourceUsageStyles";
import { InfoTip } from "./hydrationComponents";
import { HYDRATION_HELP, HydrationRow, rowTime } from "./hydrationView";

const PLOT_HEIGHT_PX = 180;
// The lane under the plot for hydrations that never finished.
const LANE_HEIGHT_PX = 22;
const SVG_HEIGHT_PX =
  CHART_MARGIN.top + PLOT_HEIGHT_PX + LANE_HEIGHT_PX + CHART_MARGIN.bottom;
const DURATION_TICK_COUNT = 4;
// Replicas often hydrate at the same moment, so each is drawn this far from
// the next to keep both visible.
const REPLICA_OFFSET_PX = 4;

export interface InProgressHydration {
  replicaId: string;
  startedAt: Date;
  elapsedMs: number;
}

export interface HydrationHistoryChartProps {
  rows: HydrationRow[];
  inProgress: InProgressHydration[];
  /** The replicas to draw, in display order. */
  replicaIds: string[];
  colorById: Map<string, string>;
  nameById: Map<string, string>;
  startMs: number;
  endMs: number;
  typicalMs: number | undefined;
  selectedId: string | undefined;
  onSelect: (rowId: string) => void;
}

type TooltipDatum =
  | { type: "row"; row: HydrationRow }
  | { type: "inProgress"; hydration: InProgressHydration };

const describeRow = (row: HydrationRow, name: string) => {
  if (row.type === "unfinished") {
    return {
      title: `${name} restarted before finishing`,
      detail:
        row.reason === OOM_REASON
          ? "Out of memory"
          : "The replica went offline",
      time: row.endedAt,
    };
  }
  return {
    title: `${name} · ${row.kind === "full" ? "Full hydration" : "New objects"}`,
    detail: `${formatDurationExact(row.durationMs)} · ${row.objectCount} ${
      row.objectCount === 1 ? "object" : "objects"
    }`,
    time: row.startedAt,
  };
};

/**
 * How long each hydration of the cluster's replicas took over the window,
 * with restarts that never finished in a lane below. Each mark selects its
 * hydration.
 */
export const HydrationHistoryChart = (props: HydrationHistoryChartProps) => {
  const { parentRef, width } = useParentSize({ debounceTime: 10 });
  return (
    <Box ref={parentRef} width="100%" minWidth={0}>
      {width > 0 && <HistoryPlot width={width} {...props} />}
    </Box>
  );
};

const HistoryPlot = ({
  width,
  rows,
  inProgress,
  replicaIds,
  colorById,
  nameById,
  startMs,
  endMs,
  typicalMs,
  selectedId,
  onSelect,
}: HydrationHistoryChartProps & { width: number }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { tooltipData, tooltipLeft, tooltipTop, showTooltip, hideTooltip } =
    useTooltip<TooltipDatum>();
  const { containerRef, TooltipInPortal } = useTooltipInPortal({
    detectBounds: true,
    scroll: true,
  });

  const plotRight = width - CHART_MARGIN.right;
  const plotBottom = CHART_MARGIN.top + PLOT_HEIGHT_PX;
  const xScale = React.useMemo(
    () =>
      scaleTime<number>({
        domain: [new Date(startMs), new Date(endMs)],
        range: [CHART_MARGIN.left, plotRight],
      }),
    [startMs, endMs, plotRight],
  );
  const yScale = React.useMemo(() => {
    const longestMs = max([
      ...rows.map((row) => (row.type === "hydration" ? row.durationMs : 0)),
      ...inProgress.map(({ elapsedMs }) => elapsedMs),
      typicalMs ?? 0,
      // An empty or near-instant window still gets a readable axis.
      1000,
    ]);
    return scaleLinear<number>({
      domain: [0, longestMs ?? 1000],
      range: [plotBottom, CHART_MARGIN.top],
      nice: DURATION_TICK_COUNT,
    });
  }, [rows, inProgress, typicalMs, plotBottom]);

  const offsetOf = (replicaId: string) => {
    const index = replicaIds.indexOf(replicaId);
    return (index - (replicaIds.length - 1) / 2) * REPLICA_OFFSET_PX;
  };
  const laneY = plotBottom + LANE_HEIGHT_PX / 2;
  const show = (datum: TooltipDatum, x: number, y: number) =>
    showTooltip({ tooltipData: datum, tooltipLeft: x, tooltipTop: y });
  const markProps = (rowId: string, label: string) => ({
    role: "button",
    tabIndex: 0,
    "aria-label": label,
    "aria-pressed": rowId === selectedId,
    style: { cursor: "pointer", outline: "none" },
    onClick: () => onSelect(rowId),
    onKeyDown: (event: React.KeyboardEvent) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        onSelect(rowId);
      }
    },
    onPointerLeave: hideTooltip,
    onBlur: hideTooltip,
  });

  return (
    <VStack spacing={0} alignItems="stretch" ref={containerRef}>
      <svg
        width={width}
        height={SVG_HEIGHT_PX}
        role="img"
        aria-label="Hydration durations"
      >
        {yScale.ticks(DURATION_TICK_COUNT).map((tickMs) => (
          <g key={tickMs}>
            <line
              x1={CHART_MARGIN.left}
              x2={plotRight}
              y1={yScale(tickMs)}
              y2={yScale(tickMs)}
              stroke={colors.border.primary}
            />
            <text
              x={CHART_MARGIN.left - 8}
              y={yScale(tickMs) + 4}
              textAnchor="end"
              fontSize={12}
              fill={colors.foreground.secondary}
            >
              {formatDurationForAxis(tickMs)}
            </text>
          </g>
        ))}
        <line
          x1={CHART_MARGIN.left}
          x2={plotRight}
          y1={plotBottom + LANE_HEIGHT_PX}
          y2={plotBottom + LANE_HEIGHT_PX}
          stroke={colors.border.primary}
        />
        {typicalMs !== undefined && (
          <line
            x1={CHART_MARGIN.left}
            x2={plotRight}
            y1={yScale(typicalMs)}
            y2={yScale(typicalMs)}
            stroke={colors.accent.purple}
            strokeDasharray="2 4"
            pointerEvents="none"
          />
        )}
        {rows.map((row) => {
          const color =
            colorById.get(row.replicaId) ?? colors.foreground.tertiary;
          const name = nameById.get(row.replicaId) ?? row.replicaId;
          const cx = xScale(rowTime(row)) + offsetOf(row.replicaId);
          const isSelected = row.id === selectedId;
          const { title, detail, time } = describeRow(row, name);
          const label = `${title}, ${detail}, ${formatDate(time, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}`;
          if (row.type === "unfinished") {
            return (
              <g
                key={row.id}
                {...markProps(row.id, label)}
                onPointerEnter={() => show({ type: "row", row }, cx, laneY)}
                onFocus={() => show({ type: "row", row }, cx, laneY)}
              >
                <circle cx={cx} cy={laneY} r={10} fill="transparent" />
                {isSelected && (
                  <circle
                    cx={cx}
                    cy={laneY}
                    r={8}
                    fill="none"
                    stroke={colors.foreground.primary}
                    strokeWidth={1.5}
                  />
                )}
                <path
                  d={`M${cx - 4} ${laneY - 4}l8 8M${cx + 4} ${laneY - 4}l-8 8`}
                  stroke={colors.accent.red}
                  strokeWidth={2.2}
                  strokeLinecap="round"
                />
              </g>
            );
          }
          const cy = yScale(row.durationMs);
          const isFull = row.kind === "full";
          return (
            <g
              key={row.id}
              {...markProps(row.id, label)}
              onPointerEnter={() => show({ type: "row", row }, cx, cy)}
              onFocus={() => show({ type: "row", row }, cx, cy)}
            >
              <circle cx={cx} cy={cy} r={12} fill="transparent" />
              {isSelected && (
                <circle
                  cx={cx}
                  cy={cy}
                  r={isFull ? 10 : 8.5}
                  fill="none"
                  stroke={colors.foreground.primary}
                  strokeWidth={1.5}
                />
              )}
              <circle
                cx={cx}
                cy={cy}
                r={isFull ? 6 : 4.5}
                fill={isFull ? color : colors.background.primary}
                stroke={isFull ? colors.background.primary : color}
                strokeWidth={2}
              />
            </g>
          );
        })}
        {inProgress.map((hydration) => {
          const color =
            colorById.get(hydration.replicaId) ?? colors.foreground.tertiary;
          const cx =
            xScale(hydration.startedAt) + offsetOf(hydration.replicaId);
          const cy = yScale(hydration.elapsedMs);
          return (
            <g
              key={`in-progress-${hydration.replicaId}`}
              role="img"
              aria-label={`${nameById.get(hydration.replicaId) ?? hydration.replicaId} is hydrating, ${formatDurationExact(hydration.elapsedMs)} so far`}
              onPointerEnter={() =>
                show({ type: "inProgress", hydration }, cx, cy)
              }
              onPointerLeave={hideTooltip}
            >
              <circle cx={cx} cy={cy} r={12} fill="transparent" />
              <circle
                cx={cx}
                cy={cy}
                r={5.5}
                fill={colors.background.primary}
                stroke={color}
                strokeWidth={2}
                strokeDasharray="3 2"
              />
            </g>
          );
        })}
      </svg>
      <TimeAxis width={width} xScale={xScale} />
      {tooltipData && tooltipLeft !== undefined && tooltipTop !== undefined && (
        <GraphTooltip
          component={TooltipInPortal}
          top={tooltipTop}
          left={tooltipLeft}
        >
          <HistoryTooltipContent
            datum={tooltipData}
            nameById={nameById}
            colorById={colorById}
          />
        </GraphTooltip>
      )}
    </VStack>
  );
};

const HistoryTooltipContent = ({
  datum,
  nameById,
  colorById,
}: {
  datum: TooltipDatum;
  nameById: Map<string, string>;
  colorById: Map<string, string>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const replicaId =
    datum.type === "row" ? datum.row.replicaId : datum.hydration.replicaId;
  const name = nameById.get(replicaId) ?? replicaId;
  const { title, detail, time } =
    datum.type === "row"
      ? describeRow(datum.row, name)
      : {
          title: `${name} is hydrating`,
          detail: `${formatDurationExact(datum.hydration.elapsedMs)} so far`,
          time: datum.hydration.startedAt,
        };
  return (
    <VStack align="start" spacing={1} px={4} py={2}>
      <HStack spacing={2}>
        <TooltipColorSwatch
          color={colorById.get(replicaId) ?? colors.foreground.tertiary}
        />
        <Text textStyle="text-ui-med">{title}</Text>
      </HStack>
      <Text textStyle="text-ui-reg">{detail}</Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {formatDate(time, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}
      </Text>
    </VStack>
  );
};

export interface HydrationHistoryLegendProps {
  replicas: Array<{ id: string; name: string }>;
  colorById: Map<string, string>;
  hasTypical: boolean;
  hasInProgress: boolean;
}

/** Names each replica's color and each kind of mark in the chart. */
export const HydrationHistoryLegend = ({
  replicas,
  colorById,
  hasTypical,
  hasInProgress,
}: HydrationHistoryLegendProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const glyph = (children: React.ReactNode, glyphWidth = 12) => (
    <svg width={glyphWidth} height={12} aria-hidden>
      {children}
    </svg>
  );
  const items: Array<{
    key: string;
    swatch: React.ReactNode;
    label: string;
    help?: string;
  }> = [
    ...replicas.map(({ id, name }) => ({
      key: id,
      swatch: (
        <TooltipColorSwatch
          color={colorById.get(id) ?? colors.foreground.tertiary}
        />
      ),
      label: name,
    })),
    {
      key: "full",
      swatch: glyph(
        <circle cx={6} cy={6} r={5} fill={colors.foreground.secondary} />,
      ),
      label: "Full hydration",
      help: HYDRATION_HELP.full,
    },
    {
      key: "new",
      swatch: glyph(
        <circle
          cx={6}
          cy={6}
          r={4}
          fill="none"
          stroke={colors.foreground.secondary}
          strokeWidth={2}
        />,
      ),
      label: "New objects",
      help: HYDRATION_HELP.newObjects,
    },
    {
      key: "unfinished",
      swatch: glyph(
        <path
          d="M2 2l8 8M10 2l-8 8"
          stroke={colors.accent.red}
          strokeWidth={2}
          strokeLinecap="round"
        />,
      ),
      label: "Restarted before finishing",
      help: HYDRATION_HELP.unfinished,
    },
    ...(hasInProgress
      ? [
          {
            key: "in-progress",
            swatch: glyph(
              <circle
                cx={6}
                cy={6}
                r={4.5}
                fill="none"
                stroke={colors.foreground.secondary}
                strokeWidth={1.5}
                strokeDasharray="2 1.5"
              />,
            ),
            label: "In progress",
            help: HYDRATION_HELP.inProgress,
          },
        ]
      : []),
    ...(hasTypical
      ? [
          {
            key: "typical",
            swatch: glyph(
              <line
                x1={0}
                x2={18}
                y1={6}
                y2={6}
                stroke={colors.accent.purple}
                strokeWidth={1.5}
                strokeDasharray="2 3"
              />,
              18,
            ),
            label: "Typical full hydration",
            help: HYDRATION_HELP.typical,
          },
        ]
      : []),
  ];
  return (
    <HStack
      as="ul"
      aria-label="Legend"
      listStyleType="none"
      spacing={4}
      flexWrap="wrap"
      rowGap={1}
    >
      {items.map(({ key, swatch, label, help }) => (
        <HStack as="li" key={key} spacing={1.5}>
          {swatch}
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {label}
          </Text>
          {help && <InfoTip subject={label} label={help} />}
        </HStack>
      ))}
    </HStack>
  );
};
