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
import { pluralize } from "~/util";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatDurationExact } from "~/utils/format";

import { TimeAxis } from "../ResourceUsage/ResourceUsageCharts";
import { CHART_MARGIN } from "../ResourceUsage/resourceUsageStyles";
import { InfoTip } from "./hydrationComponents";
import {
  HYDRATION_HELP,
  HydrationRow,
  ROW_KIND_NAMES,
  rowTime,
} from "./hydrationView";

const PLOT_HEIGHT_PX = 180;
// The lane under the plot for hydrations that never finished.
const LANE_HEIGHT_PX = 22;
const SVG_HEIGHT_PX =
  CHART_MARGIN.top + PLOT_HEIGHT_PX + LANE_HEIGHT_PX + CHART_MARGIN.bottom;
const DURATION_TICK_COUNT = 4;
// Durations the axis ticks at, so each label is a round duration.
const DURATION_STEPS_MS = [
  100, 200, 500, 1_000, 2_000, 5_000, 10_000, 15_000, 30_000, 60_000, 120_000,
  300_000, 600_000, 900_000, 1_800_000, 3_600_000, 7_200_000, 21_600_000,
  43_200_000, 86_400_000,
];
// Replicas often hydrate at the same moment, so each is drawn this far from
// the next to keep both visible.
const REPLICA_OFFSET_PX = 4;
// A replica restarting in a loop restarts too often to draw each restart, so
// restarts closer than this are one mark with a count.
const RESTART_GROUP_PX = 12;

/** Ticks from zero at a round duration, at most `DURATION_TICK_COUNT` steps. */
function durationTicks(maxMs: number) {
  const stepMs =
    DURATION_STEPS_MS.find(
      (step) => Math.ceil(maxMs / step) <= DURATION_TICK_COUNT,
    ) ?? DURATION_STEPS_MS[DURATION_STEPS_MS.length - 1];
  const count = Math.max(1, Math.ceil(maxMs / stepMs));
  return Array.from({ length: count + 1 }, (_, index) => index * stepMs);
}

type UnfinishedRow = Extract<HydrationRow, { type: "unfinished" }>;

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
  selectedId: string | undefined;
  onSelect: (rowId: string) => void;
}

type TooltipDatum =
  | { type: "row"; row: HydrationRow }
  | { type: "restarts"; rows: UnfinishedRow[] }
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
    title: `${name} · ${ROW_KIND_NAMES[row.kind].label}`,
    detail: `${formatDurationExact(row.durationMs)} · ${row.objectCount} ${pluralize(row.objectCount, "object", "objects")}`,
    time: row.startedAt,
  };
};

const describeRestarts = (rows: UnfinishedRow[], name: string) => {
  const oomCount = rows.filter(({ reason }) => reason === OOM_REASON).length;
  return {
    title: `${name} restarted ${rows.length} times before finishing`,
    detail:
      oomCount === rows.length
        ? "Each after running out of memory"
        : oomCount === 0
          ? "Each after going offline"
          : `${oomCount} after running out of memory`,
    time: rows[rows.length - 1].endedAt,
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
        // A hydration that began before the window and finished in it sits
        // at the window's start.
        clamp: true,
      }),
    [startMs, endMs, plotRight],
  );
  const ticksMs = React.useMemo(
    () =>
      durationTicks(
        max([
          ...rows.map((row) => (row.type === "hydration" ? row.durationMs : 0)),
          ...inProgress.map(({ elapsedMs }) => elapsedMs),
          // An empty or near-instant window still gets a readable axis.
          1000,
        ]) ?? 1000,
      ),
    [rows, inProgress],
  );
  const yScale = React.useMemo(
    () =>
      scaleLinear<number>({
        domain: [0, ticksMs[ticksMs.length - 1]],
        range: [plotBottom, CHART_MARGIN.top],
      }),
    [ticksMs, plotBottom],
  );

  const offsetOf = (replicaId: string) => {
    const index = replicaIds.indexOf(replicaId);
    return (index - (replicaIds.length - 1) / 2) * REPLICA_OFFSET_PX;
  };
  const laneY = plotBottom + LANE_HEIGHT_PX / 2;
  const restartGroups: Array<{ x: number; rows: UnfinishedRow[] }> = [];
  for (const row of rows
    .filter((each): each is UnfinishedRow => each.type === "unfinished")
    .sort((a, b) => a.endedAt.getTime() - b.endedAt.getTime())) {
    const x = xScale(row.endedAt) + offsetOf(row.replicaId);
    const group = restartGroups.findLast(
      (each) => each.rows[0].replicaId === row.replicaId,
    );
    if (group && x - group.x < RESTART_GROUP_PX) {
      group.rows.push(row);
      group.x = x;
    } else {
      restartGroups.push({ x, rows: [row] });
    }
  }
  const show = (datum: TooltipDatum, x: number, y: number) =>
    showTooltip({ tooltipData: datum, tooltipLeft: x, tooltipTop: y });
  const markProps = (rowId: string, label: string) => ({
    role: "button",
    tabIndex: 0,
    "aria-label": label,
    "aria-pressed": rowId === selectedId,
    style: { cursor: "pointer" },
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
    <VStack spacing={0} alignItems="stretch">
      <Text
        textStyle="text-small"
        color={colors.foreground.secondary}
        aria-hidden
      >
        Hydration time
      </Text>
      {/* The tooltip is placed in the svg's coordinates, so its container
          starts where the svg does. */}
      <Box ref={containerRef}>
        <svg
          width={width}
          height={SVG_HEIGHT_PX}
          role="img"
          aria-label="Hydration times"
        >
          {ticksMs.map((tickMs) => (
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
                {tickMs === 0 ? "0" : formatDurationExact(tickMs)}
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
          {restartGroups.map(({ x, rows: groupRows }) => {
            const latest = groupRows[groupRows.length - 1];
            const name = nameById.get(latest.replicaId) ?? latest.replicaId;
            const isSelected = groupRows.some(({ id }) => id === selectedId);
            const { title, detail, time } =
              groupRows.length === 1
                ? describeRow(latest, name)
                : describeRestarts(groupRows, name);
            const label = `${title}, ${detail}, ${formatDate(time, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}`;
            const datum: TooltipDatum =
              groupRows.length === 1
                ? { type: "row", row: latest }
                : { type: "restarts", rows: groupRows };
            return (
              <g
                key={latest.id}
                {...markProps(latest.id, label)}
                aria-pressed={isSelected}
                onPointerEnter={() => show(datum, x, laneY)}
                onFocus={() => show(datum, x, laneY)}
              >
                <circle cx={x} cy={laneY} r={10} fill="transparent" />
                {isSelected && (
                  <circle
                    cx={x}
                    cy={laneY}
                    r={8}
                    fill="none"
                    stroke={colors.foreground.primary}
                    strokeWidth={1.5}
                  />
                )}
                <path
                  d={`M${x - 4} ${laneY - 4}l8 8M${x + 4} ${laneY - 4}l-8 8`}
                  stroke={colors.accent.red}
                  strokeWidth={2.2}
                  strokeLinecap="round"
                />
                {groupRows.length > 1 && (
                  <text
                    x={x - 9}
                    y={laneY + 4}
                    textAnchor="end"
                    fontSize={11}
                    fontWeight={600}
                    fill={colors.accent.red}
                  >
                    {groupRows.length}
                  </text>
                )}
              </g>
            );
          })}
          {rows.flatMap((row) => {
            if (row.type === "unfinished") return [];
            const color =
              colorById.get(row.replicaId) ?? colors.foreground.tertiary;
            const name = nameById.get(row.replicaId) ?? row.replicaId;
            const cx = xScale(rowTime(row)) + offsetOf(row.replicaId);
            const isSelected = row.id === selectedId;
            const { title, detail, time } = describeRow(row, name);
            const label = `${title}, ${detail}, ${formatDate(time, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}`;
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
      </Box>
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
    datum.type === "row"
      ? datum.row.replicaId
      : datum.type === "restarts"
        ? datum.rows[0].replicaId
        : datum.hydration.replicaId;
  const name = nameById.get(replicaId) ?? replicaId;
  const { title, detail, time } =
    datum.type === "row"
      ? describeRow(datum.row, name)
      : datum.type === "restarts"
        ? describeRestarts(datum.rows, name)
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
  hasInProgress: boolean;
}

/** Names each replica's color and each kind of mark in the chart. */
export const HydrationHistoryLegend = ({
  replicas,
  colorById,
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
      label: ROW_KIND_NAMES.full.label,
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
      label: ROW_KIND_NAMES.additive.label,
      help: HYDRATION_HELP.additive,
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
      label: ROW_KIND_NAMES.unfinished.label,
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
