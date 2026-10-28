// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, HStack, Text, useTheme, VStack } from "@chakra-ui/react";
import { scaleLinear } from "@visx/scale";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import { mean } from "d3";
import React from "react";

import { GraphTooltip } from "~/components/graphComponents";
import { MaterializeTheme } from "~/theme";
import { formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import {
  ChartHeader,
  PercentAxis,
  PercentScale,
  ThresholdLine,
} from "../ResourceUsage/ResourceUsageCharts";
import {
  CHART_MARGIN,
  PERCENT_TICKS,
} from "../ResourceUsage/resourceUsageStyles";
import {
  Measurement,
  processOfWorker,
  workerLabel,
  WorkerReplica,
} from "./workerCpuModel";
import { useWorkerBarColors } from "./workerCpuStyles";

const PLOT_HEIGHT_PX = 160;
const SVG_HEIGHT_PX = CHART_MARGIN.top + PLOT_HEIGHT_PX + CHART_MARGIN.bottom;
const LABEL_ROW_PX = 18;
const PROCESS_ROW_PX = 22;
const PROCESS_GAP_PX = 16;
// As between the memory chart's bars.
const BAR_GAP_PX = 2;
const MAX_BAR_WIDTH_PX = 28;
// Labels are 12px text, about 7px a character, with room between them.
// Narrower slots label only every few workers, so labels don't collide.
const LABEL_CHAR_WIDTH = 7;
const LABEL_GAP = 8;

export interface WorkerBarsProps {
  width: number;
  measurement: Measurement;
  /** The object whose part of each worker's time is drawn darker. */
  picked: { name: string; perWorkerNs: number[] } | undefined;
  /** The worker to call out, with its legend label, from `markedWorker`. */
  hotWorker: { workerId: number; label: string } | undefined;
  /** The worker whose time the objects table breaks down. */
  selectedWorkerId: number;
  onSelectWorker: (workerId: number) => void;
  replica: WorkerReplica;
}

interface Slots {
  workerCount: number;
  slot: number;
  barWidth: number;
  /** A worker's slot's left edge. */
  x: (workerId: number) => number;
}

interface BarsProps {
  layout: ReturnType<typeof buildXYGraphLayoutProps>;
  yScale: PercentScale;
  slots: Slots;
  /** Each worker's busy share of a core, by worker id. */
  busy: number[];
  /** Each worker's busy share that went to the picked object. */
  pickedBusy: number[];
  hotWorkerId: number | undefined;
  labeled: Set<number>;
  othersAverage: number | undefined;
}

// Memoized like the memory chart's marks, so hovering, which only moves the
// column highlight and tooltip, skips redrawing the bars.
const BarMarks = React.memo(
  ({
    layout,
    yScale,
    slots,
    busy,
    pickedBusy,
    hotWorkerId,
    labeled,
    othersAverage,
  }: BarsProps) => {
    const { colors, fonts } = useTheme<MaterializeTheme>();
    const barColors = useWorkerBarColors();
    const { graphBottom } = layout.graphLineCursorProps;
    return (
      <g pointerEvents="none">
        <PercentAxis layout={layout} yScale={yScale} ticks={PERCENT_TICKS} />
        {busy.map((fraction, workerId) => {
          const isHot = workerId === hotWorkerId;
          const x = slots.x(workerId) + (slots.slot - slots.barWidth) / 2;
          const top = yScale(fraction * 100);
          const pickedTop = yScale(pickedBusy[workerId] * 100);
          const radius = Math.min(2, slots.barWidth / 2);
          return (
            <g key={workerId} data-bar={workerId}>
              <rect
                x={x}
                y={top}
                width={slots.barWidth}
                height={Math.max(0, graphBottom - top)}
                rx={radius}
                fill={isHot ? barColors.hotRest : barColors.rest}
              />
              {pickedBusy[workerId] > 0 && (
                <rect
                  x={x}
                  y={pickedTop}
                  width={slots.barWidth}
                  height={Math.max(0, graphBottom - pickedTop)}
                  rx={radius}
                  fill={isHot ? barColors.hotPicked : barColors.picked}
                />
              )}
              {labeled.has(workerId) && (
                <text
                  x={x + slots.barWidth / 2}
                  y={top - 4}
                  textAnchor="middle"
                  fontFamily={fonts.body}
                  fontSize={12}
                  fontWeight={600}
                  fill={isHot ? colors.accent.red : colors.foreground.primary}
                >
                  {formatPercentage(fraction, 0)}
                </text>
              )}
            </g>
          );
        })}
        {othersAverage !== undefined && (
          <ThresholdLine
            layout={layout}
            y={yScale(othersAverage * 100)}
            color={colors.foreground.primary}
          />
        )}
      </g>
    );
  },
);

/**
 * One bar per worker for how busy it was over a measurement, as a share of
 * one core, with a dashed line at the other workers' average. Hovering a
 * worker shows its numbers, and clicking it, or moving with the arrow keys
 * while the chart has focus, selects it.
 */
export const WorkerBars = ({
  width,
  measurement,
  picked,
  hotWorker,
  selectedWorkerId,
  onSelectWorker,
  replica,
}: WorkerBarsProps) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const hotWorkerId = hotWorker?.workerId;
  const {
    tooltipData: hoveredWorkerId,
    tooltipLeft,
    tooltipTop,
    showTooltip,
    hideTooltip,
  } = useTooltip<number>();
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
  const yScale = React.useMemo(
    () =>
      scaleLinear<number>({
        domain: [0, 100],
        range: layout.yScaleRange,
        clamp: true,
      }),
    [layout],
  );
  const spanNs = (measurement.endMs - measurement.startMs) * 1e6;
  const busy = React.useMemo(
    () =>
      measurement.workerTotalsNs.map((ns) =>
        spanNs > 0 ? Math.min(1, ns / spanNs) : 0,
      ),
    [measurement, spanNs],
  );
  const pickedBusy = React.useMemo(
    () =>
      busy.map((fraction, workerId) =>
        picked && spanNs > 0
          ? Math.min(fraction, (picked.perWorkerNs[workerId] ?? 0) / spanNs)
          : 0,
      ),
    [busy, picked, spanNs],
  );
  const workerCount = busy.length;
  const busiest = measurement.workers.at(0);
  // With one worker there are no others to average.
  const othersAverage =
    workerCount > 1
      ? (mean(
          busy.filter((_, workerId) => workerId !== busiest?.workerId),
          (fraction) => fraction,
        ) ?? 0)
      : undefined;

  const { processes, workers: perProcess } = replica.allocation;
  const processCount = Math.max(1, processes);
  const [plotLeft, plotRight] = layout.xScaleRange;
  const { graphTop, graphBottom } = layout.graphLineCursorProps;
  const slots = React.useMemo((): Slots => {
    const slot =
      workerCount > 0
        ? (plotRight - plotLeft - PROCESS_GAP_PX * (processCount - 1)) /
          workerCount
        : 0;
    return {
      workerCount,
      slot,
      barWidth: Math.max(1, Math.min(MAX_BAR_WIDTH_PX, slot - BAR_GAP_PX)),
      x: (workerId) =>
        plotLeft +
        workerId * slot +
        processOfWorker(workerId, perProcess) * PROCESS_GAP_PX,
    };
  }, [workerCount, plotLeft, plotRight, processCount, perProcess]);

  // These always get a label, and the spaced labels near them give way.
  const labeledAlways = [
    busiest?.workerId,
    hotWorkerId,
    selectedWorkerId,
  ].filter((workerId): workerId is number => workerId !== undefined);
  const labeledAlwaysKey = labeledAlways.join(",");
  const valueLabeled = React.useMemo(
    () => new Set(labeledAlwaysKey.split(",").map(Number)),
    [labeledAlwaysKey],
  );
  const labelWidth =
    `w${Math.max(0, workerCount - 1)}`.length * LABEL_CHAR_WIDTH + LABEL_GAP;
  const labelEvery = Math.max(
    1,
    Math.ceil(labelWidth / Math.max(1, slots.slot)),
  );

  const showWorker = (workerId: number, target: Element) => {
    const bounds = target.getBoundingClientRect();
    showTooltip({
      tooltipData: workerId,
      tooltipLeft: bounds.left + bounds.width / 2 - containerBounds.left,
      tooltipTop: bounds.top - containerBounds.top,
    });
  };
  const columnHighlight = (workerId: number, color: string) => (
    <rect
      x={slots.x(workerId)}
      y={graphTop}
      width={slots.slot}
      height={graphBottom - graphTop}
      fill={color}
      pointerEvents="none"
    />
  );
  const summary = busiest
    ? othersAverage !== undefined
      ? `${workerCount} workers. The busiest, w${busiest.workerId}, was ${formatPercentage(busiest.busyFraction, 0)} busy, and the other workers averaged ${formatPercentage(othersAverage, 0)}.`
      : `1 worker. w${busiest.workerId} was ${formatPercentage(busiest.busyFraction, 0)} busy.`
    : `${workerCount} workers.`;
  const textProps = {
    fontFamily: fonts.body,
    fontSize: 12,
    fill: colors.foreground.secondary,
  };
  const barColors = useWorkerBarColors();

  return (
    <VStack spacing={3} alignItems="stretch">
      <ChartHeader
        title="Workers"
        description={
          workerCount > 1
            ? "% of one core over the measured span. Click a worker to see what it spent its time on, or an object below to see its part of each bar."
            : "% of one core over the measured span. Pick an object below to see its part of the bar."
        }
        legend={[
          ...(picked
            ? [
                {
                  label: picked.name,
                  color: barColors.picked,
                  variant: "square" as const,
                },
              ]
            : []),
          {
            label: picked
              ? "The rest of each worker's time"
              : "Each worker's time",
            color: barColors.rest,
            variant: "square",
          },
          ...(hotWorker
            ? [
                {
                  label: hotWorker.label,
                  color: barColors.hotPicked,
                  variant: "square" as const,
                },
              ]
            : []),
          ...(othersAverage !== undefined
            ? [
                {
                  label: `The other workers' average (${formatPercentage(othersAverage, 0)})`,
                  color: colors.foreground.primary,
                  variant: "dashed" as const,
                },
              ]
            : []),
        ]}
      />
      <Box
        ref={containerRef}
        position="relative"
        tabIndex={0}
        role="group"
        aria-label={`${summary} w${selectedWorkerId} is selected. The left and right arrow keys pick another worker.`}
        borderRadius="md"
        outline="none"
        _focusVisible={{
          boxShadow: `0 0 0 2px ${colors.accent.brightPurple}`,
        }}
        onKeyDown={(event) => {
          const step =
            event.key === "ArrowRight" || event.key === "ArrowDown"
              ? 1
              : event.key === "ArrowLeft" || event.key === "ArrowUp"
                ? -1
                : 0;
          if (step === 0 || workerCount === 0) return;
          event.preventDefault();
          onSelectWorker((selectedWorkerId + step + workerCount) % workerCount);
        }}
      >
        <svg {...layout.svgProps} role="img" aria-label={summary}>
          {columnHighlight(selectedWorkerId, colors.background.accent)}
          {hoveredWorkerId !== undefined &&
            hoveredWorkerId !== selectedWorkerId &&
            columnHighlight(hoveredWorkerId, colors.background.secondary)}
          <BarMarks
            layout={layout}
            yScale={yScale}
            slots={slots}
            busy={busy}
            pickedBusy={pickedBusy}
            hotWorkerId={hotWorkerId}
            labeled={valueLabeled}
            othersAverage={othersAverage}
          />
          {busy.map((_, workerId) => (
            <rect
              key={workerId}
              data-worker-id={workerId}
              x={slots.x(workerId)}
              y={graphTop}
              width={slots.slot}
              height={graphBottom - graphTop}
              fill="transparent"
              cursor="pointer"
              onPointerEnter={(event) =>
                showWorker(workerId, event.target as Element)
              }
              onPointerLeave={hideTooltip}
              onClick={() => onSelectWorker(workerId)}
            />
          ))}
        </svg>
        <svg
          width={width}
          height={LABEL_ROW_PX + (processCount > 1 ? PROCESS_ROW_PX : 0)}
          aria-hidden
        >
          {busy.map((_, workerId) => {
            const labelX = slots.x(workerId) + slots.slot / 2;
            // A label centered on the last bars can run past the chart's edge.
            const overflows =
              labelX + (`w${workerId}`.length * LABEL_CHAR_WIDTH) / 2 > width;
            const isAlways = labeledAlways.includes(workerId);
            const show =
              isAlways ||
              (workerId % labelEvery === 0 &&
                !overflows &&
                labeledAlways.every(
                  (labeled) => Math.abs(labeled - workerId) >= labelEvery,
                ));
            if (!show) return null;
            const isHot = workerId === hotWorkerId;
            const isSelected = workerId === selectedWorkerId;
            return (
              <text
                key={workerId}
                {...textProps}
                x={overflows ? width : labelX}
                y={13}
                textAnchor={overflows ? "end" : "middle"}
                fontWeight={isHot || isSelected ? 600 : undefined}
                fill={
                  isHot
                    ? colors.accent.red
                    : isSelected
                      ? colors.foreground.primary
                      : textProps.fill
                }
              >
                w{workerId}
              </text>
            );
          })}
          {processCount > 1 &&
            Array.from({ length: processCount }, (_, process) => {
              const left = slots.x(process * perProcess);
              const right =
                slots.x((process + 1) * perProcess - 1) + slots.slot;
              const top = LABEL_ROW_PX + 2;
              return (
                <g key={process}>
                  <line
                    x1={left}
                    x2={right}
                    y1={top}
                    y2={top}
                    stroke={colors.border.secondary}
                  />
                  <text
                    {...textProps}
                    x={(left + right) / 2}
                    y={top + 14}
                    textAnchor="middle"
                  >
                    Process {process}
                  </text>
                </g>
              );
            })}
        </svg>
      </Box>
      {hoveredWorkerId !== undefined &&
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
              minWidth="200px"
            >
              <Text textStyle="text-ui-med">
                {workerLabel(hoveredWorkerId, replica)}
              </Text>
              <TooltipRow
                label="Busy"
                value={`${formatPercentage(busy[hoveredWorkerId], 0)} of a core`}
              />
              {picked && (
                <TooltipRow
                  label={picked.name}
                  value={formatPercentage(pickedBusy[hoveredWorkerId], 0)}
                />
              )}
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                {hoveredWorkerId === selectedWorkerId
                  ? "Its objects are ranked below."
                  : "Click to rank the objects below by its time."}
              </Text>
            </VStack>
          </GraphTooltip>
        )}
    </VStack>
  );
};

/** One label and value row of a Workers tab chart's tooltip. */
export const TooltipRow = ({
  label,
  value,
}: {
  label: string;
  value: string;
}) => (
  <HStack justifyContent="space-between" spacing={3}>
    <Text textStyle="text-small" noOfLines={1}>
      {label}
    </Text>
    <Text textStyle="text-small" whiteSpace="nowrap">
      {value}
    </Text>
  </HStack>
);
