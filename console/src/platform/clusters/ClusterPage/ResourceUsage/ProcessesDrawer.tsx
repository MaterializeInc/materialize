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
  Checkbox,
  Flex,
  HStack,
  Select,
  Spinner,
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { createColumnHelper, RowSelectionState } from "@tanstack/react-table";
import { useParentSize } from "@visx/responsive";
import { scaleTime } from "@visx/scale";
import { LinePath } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import { max } from "d3";
import React from "react";
import { Link, To } from "react-router-dom";

import { SizeAllocation } from "~/api/materialize/cluster/replicaProcessMetrics";
import Alert from "~/components/Alert";
import {
  GraphLineCursor,
  GraphTooltip,
  GraphTooltipBucketRange,
  TooltipColorSwatch,
} from "~/components/graphComponents";
import SideDrawer from "~/components/SideDrawer";
import { sortingFunctions } from "~/components/Table/tableColumnBuilders";
import { UniversalTable } from "~/components/Table/UniversalTable";
import { useUniversalTable } from "~/components/Table/useUniversalTable";
import TextLink from "~/components/TextLink";
import { MIN_BUCKET_SIZE_MS } from "~/platform/clusters/constants";
import { MaterializeTheme } from "~/theme";
import { formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import {
  bucketProcessReadings,
  BusiestProcess,
  busiestProcess,
  otherProcessesLabel,
  ProcessBucket,
  processIdsOf,
  ProcessMetric,
  ProcessStatistic,
  ProcessSummary,
  readProcessSamples,
  reportsHeapLimit,
  standsOut,
  summarizeProcesses,
} from "./processReadingsModel";
import {
  ChartHeader,
  ChartHeaderProps,
  HoverOverlay,
  Layout,
  LegendSwatch,
  PercentAxis,
  PercentScale,
  TimeAxis,
  TimeScale,
} from "./ResourceUsageCharts";
import { CHART_MARGIN, percentScale } from "./resourceUsageStyles";
import { useReplicaProcessSamples } from "./useReplicaProcessSamples";

// The per-process history is raw per-minute samples, so the drawer reads at
// most a day of them, whatever the card's window.
const MAX_WINDOW_MINUTES = 24 * 60;
// The theme's chart palette has five strong colors. Past five processes, only
// the ones that stand out get a color; the rest stay grey but named.
const MAX_COLORED_PROCESSES = 5;
const PLOT_HEIGHT_PX = 150;
// The most processes the tooltip lists before summarizing the rest.
const TOOLTIP_PROCESS_LIMIT = 6;

export interface ProcessReplica {
  id: string;
  name: string;
  size: string;
  /** One process's allocation. */
  allocation: SizeAllocation;
}

export interface ProcessesDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  /** Replicas whose size runs more than one process. */
  replicas: ProcessReplica[];
  timePeriodMinutes: number;
  /** The Workers tab on a replica. */
  workersLink?: (replicaName: string) => To;
  /** Where focus goes when the drawer closes. */
  finalFocusRef?: React.RefObject<HTMLElement>;
}

/**
 * How each multi-process replica's processes share its work: which process is
 * busiest, a line per process over time, and each one's latest, peak and
 * average reading, read live while open. One tab per replica, and only the
 * open tab reads.
 */
export const ProcessesDrawer = ({
  isOpen,
  onClose,
  replicas,
  timePeriodMinutes,
  workersLink,
  finalFocusRef,
}: ProcessesDrawerProps) => (
  <SideDrawer
    isOpen={isOpen}
    onClose={onClose}
    finalFocusRef={finalFocusRef}
    title="Processes"
    size="xl"
    width="min(1120px, 94vw)"
  >
    <Tabs isLazy lazyBehavior="unmount">
      <TabList px={6}>
        {replicas.map((replica) => (
          <Tab key={replica.id}>{replica.name}</Tab>
        ))}
      </TabList>
      <TabPanels>
        {replicas.map((replica) => (
          <TabPanel key={replica.id} px={6} py={5}>
            <ProcessesPanel
              replica={replica}
              windowMinutes={Math.min(timePeriodMinutes, MAX_WINDOW_MINUTES)}
              isWindowCapped={timePeriodMinutes > MAX_WINDOW_MINUTES}
              workersLink={workersLink?.(replica.name)}
            />
          </TabPanel>
        ))}
      </TabPanels>
    </Tabs>
  </SideDrawer>
);

const formatReading = (percent: number | null) =>
  percent === null ? "-" : formatPercentage(percent / 100, 1);

/**
 * How each statistic is written. `menu` names it where a reader chooses
 * between them; `reading` introduces a process's value in the findings.
 */
const STATISTIC_LABELS: Record<
  ProcessStatistic,
  { menu: string; reading: string }
> = {
  current: { menu: "right now", reading: "is at" },
  peak: { menu: "at any moment (peak)", reading: "peaked at" },
  average: { menu: "on average", reading: "averaged" },
};

const windowLabel = (minutes: number) => {
  if (minutes === 60) return "hour";
  if (minutes < 60) return `${minutes} minutes`;
  return `${minutes / 60} hours`;
};

// A process standing out at a lower reading than this is noted, not flagged.
const BUSY_PERCENT = 50;

const evidence = (
  label: string,
  statistic: ProcessStatistic,
  busiest: BusiestProcess,
) => {
  const others = otherProcessesLabel(busiest.otherCount);
  const reading = `process ${busiest.processId} ${STATISTIC_LABELS[statistic].reading} ${formatReading(busiest.value)}`;
  if (!standsOut(busiest) || busiest.othersMean <= 0) {
    return `${label}: ${reading}, against ${formatReading(busiest.othersMean)} for ${others}.`;
  }
  return `${label}: ${reading}, ${(busiest.value / busiest.othersMean).toFixed(1)}× ${others} (${formatReading(busiest.othersMean)}).`;
};

const headline = (
  cpu: BusiestProcess | undefined,
  memory: BusiestProcess | undefined,
) => {
  const cpuId = standsOut(cpu) ? cpu?.processId : undefined;
  const memoryId = standsOut(memory) ? memory?.processId : undefined;
  // Only a metric with readings from two processes can be called even.
  const isEven = (busiest: BusiestProcess | undefined) =>
    busiest !== undefined && !standsOut(busiest);
  if (cpuId !== undefined && cpuId === memoryId) {
    return `Process ${cpuId} used the most CPU and memory.`;
  }
  if (cpuId !== undefined && memoryId !== undefined) {
    return `Process ${cpuId} used the most CPU, and process ${memoryId} the most memory.`;
  }
  if (cpuId !== undefined) {
    return `Process ${cpuId} used the most CPU.${isEven(memory) ? " Memory is spread evenly." : ""}`;
  }
  if (memoryId !== undefined) {
    return `Process ${memoryId} used the most memory.${isEven(cpu) ? " CPU is spread evenly." : ""}`;
  }
  if (isEven(cpu) && isEven(memory)) {
    return "CPU and memory are spread evenly across the processes.";
  }
  return isEven(cpu)
    ? "CPU is spread evenly across the processes."
    : "Memory is spread evenly across the processes.";
};

/** What the window's process readings say, with the numbers behind it. */
const ProcessFindings = ({
  replica,
  busiestCpu,
  busiestMemory,
  statistic,
  windowMinutes,
  isWindowCapped,
  unreadNowIds,
  memoryLimit,
  workersLink,
}: {
  replica: ProcessReplica;
  busiestCpu: BusiestProcess | undefined;
  busiestMemory: BusiestProcess | undefined;
  statistic: ProcessStatistic;
  windowMinutes: number;
  /** The card shows more than the drawer reads. */
  isWindowCapped: boolean;
  /** Processes with no reading in the newest round. */
  unreadNowIds: string[];
  /** What memory reads as a share of. */
  memoryLimit: string;
  workersLink: To | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const isBusy = (busiest: BusiestProcess | undefined) =>
    standsOut(busiest) && (busiest?.value ?? 0) >= BUSY_PERCENT;
  const note = (
    <Text textStyle="text-small" color={colors.foreground.secondary}>
      {replica.size} runs each replica as {replica.allocation.processes}{" "}
      processes. CPU reads as a share of one process&apos;s CPU allocation and
      memory as a share of its {memoryLimit}, from the last{" "}
      {windowLabel(windowMinutes)}
      {isWindowCapped ? ", the most this drawer reads" : ""}.
    </Text>
  );
  const unread = statistic === "current" && unreadNowIds.length > 0 && (
    <Text>
      No reading right now from{" "}
      {unreadNowIds.map((id) => `process ${id}`).join(", ")}.
    </Text>
  );
  if (!busiestCpu && !busiestMemory) {
    return (
      <VStack spacing={1} alignItems="start">
        {unread}
        {note}
      </VStack>
    );
  }
  return (
    <Alert
      variant={isBusy(busiestCpu) || isBusy(busiestMemory) ? "warning" : "info"}
      label={headline(busiestCpu, busiestMemory)}
      message={
        <VStack spacing={2} alignItems="stretch">
          <VStack spacing={0.5} alignItems="stretch">
            {busiestCpu && (
              <Text>{evidence("CPU", statistic, busiestCpu)}</Text>
            )}
            {busiestMemory && (
              <Text>{evidence("Memory", statistic, busiestMemory)}</Text>
            )}
            {unread}
          </VStack>
          {note}
          {workersLink && (
            <TextLink as={Link} to={workersLink} alignSelf="flex-start">
              Which workers did the work
            </TextLink>
          )}
        </VStack>
      }
    />
  );
};

const ProcessesPanel = ({
  replica,
  windowMinutes,
  isWindowCapped,
  workersLink,
}: {
  replica: ProcessReplica;
  windowMinutes: number;
  /** The card shows more than the drawer reads. */
  isWindowCapped: boolean;
  workersLink: To | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [statistic, setStatistic] = React.useState<ProcessStatistic>("peak");
  const state = useReplicaProcessSamples({
    replicaId: replica.id,
    windowMinutes,
  });
  const { parentRef, width } = useParentSize({ debounceTime: 10 });
  const data = React.useMemo(() => {
    const endMs = Date.now();
    const startMs = endMs - windowMinutes * 60_000;
    const readings = readProcessSamples({
      samples: state.data,
      allocation: replica.allocation,
      startMs,
    });
    const processIds = processIdsOf(readings);
    const summaries = summarizeProcesses(processIds, readings);
    return {
      startMs,
      endMs,
      processIds,
      buckets: bucketProcessReadings(
        readings,
        Math.max(windowMinutes * 1000, MIN_BUCKET_SIZE_MS),
        startMs,
      ),
      summaries,
      hasHeapLimit: reportsHeapLimit(state.data),
      unreadNowIds: summaries.flatMap(({ processId, cpu, memory }) =>
        cpu.current === null && memory.current === null ? [processId] : [],
      ),
    };
  }, [state.data, replica.allocation, windowMinutes]);
  const view = React.useMemo(() => {
    const busiestCpu = busiestProcess(data.summaries, "cpu", statistic);
    const busiestMemory = busiestProcess(data.summaries, "memory", statistic);
    return {
      ...data,
      busiestCpu,
      busiestMemory,
      busiestCpuId: standsOut(busiestCpu) ? busiestCpu?.processId : undefined,
      busiestMemoryId: standsOut(busiestMemory)
        ? busiestMemory?.processId
        : undefined,
    };
  }, [data, statistic]);
  const { processIds, busiestCpuId, busiestMemoryId } = view;
  const colorById = React.useMemo(() => {
    const palette = colors.lineGraph.slice(0, MAX_COLORED_PROCESSES);
    const coloredIds =
      processIds.length <= MAX_COLORED_PROCESSES
        ? processIds
        : [
            ...new Set(
              [busiestCpuId, busiestMemoryId].flatMap((id) =>
                id === undefined ? [] : [id],
              ),
            ),
          ];
    return new Map(coloredIds.map((id, index) => [id, palette[index]]));
  }, [processIds, busiestCpuId, busiestMemoryId, colors.lineGraph]);

  // Every process starts selected, and a new set of processes resets that.
  const processKey = view.processIds.join(",");
  const [selection, setSelection] = React.useState({
    key: processKey,
    ids: new Set(view.processIds),
  });
  const selectedIds =
    selection.key === processKey ? selection.ids : new Set(view.processIds);
  const setSelected = (ids: Set<string>) =>
    setSelection({ key: processKey, ids });
  const [hoveredId, setHoveredId] = React.useState<string>();

  if (state.error) {
    return (
      <Alert variant="error" message="Process readings are unavailable." />
    );
  }
  if (!state.snapshotComplete) {
    return (
      <Flex justifyContent="center" py={10}>
        <Spinner />
      </Flex>
    );
  }
  if (view.buckets.length === 0) {
    return (
      <Text textStyle="text-base" color={colors.foreground.secondary}>
        No process readings in this time period.
      </Text>
    );
  }
  const memoryLimit = view.hasHeapLimit ? "heap limit" : "memory allocation";
  return (
    <VStack spacing={6} alignItems="stretch">
      <VStack spacing={3} alignItems="stretch">
        <HStack spacing={2} alignItems="center" flexWrap="wrap">
          <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
            Compare processes
          </Text>
          <Select
            size="sm"
            width="auto"
            aria-label="Which reading to compare processes by"
            value={statistic}
            onChange={(event) =>
              setStatistic(event.target.value as ProcessStatistic)
            }
          >
            {(Object.keys(STATISTIC_LABELS) as ProcessStatistic[]).map(
              (key) => (
                <option key={key} value={key}>
                  {STATISTIC_LABELS[key].menu}
                </option>
              ),
            )}
          </Select>
          {statistic !== "current" && (
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              over the last {windowLabel(windowMinutes)}
            </Text>
          )}
        </HStack>
        <ProcessFindings
          replica={replica}
          busiestCpu={view.busiestCpu}
          busiestMemory={view.busiestMemory}
          statistic={statistic}
          windowMinutes={windowMinutes}
          isWindowCapped={isWindowCapped}
          unreadNowIds={view.unreadNowIds}
          memoryLimit={memoryLimit}
          workersLink={workersLink}
        />
      </VStack>
      <Box ref={parentRef} width="100%" minWidth={0}>
        {width > 0 && (
          <ProcessLineCharts
            replicaName={replica.name}
            width={width}
            startMs={view.startMs}
            endMs={view.endMs}
            processIds={view.processIds.filter((id) => selectedIds.has(id))}
            buckets={view.buckets}
            colorById={colorById}
            highlightedId={hoveredId}
            memoryLimit={memoryLimit}
          />
        )}
      </Box>
      <ProcessTable
        summaries={view.summaries}
        colorById={colorById}
        busiestCpuId={view.busiestCpuId}
        busiestMemoryId={view.busiestMemoryId}
        statistic={statistic}
        memoryLimit={memoryLimit}
        selectedIds={selectedIds}
        onSelect={setSelected}
        onHover={setHoveredId}
      />
    </VStack>
  );
};

const ProcessLineCharts = ({
  replicaName,
  width,
  startMs,
  endMs,
  processIds,
  buckets,
  colorById,
  highlightedId,
  memoryLimit,
}: {
  replicaName: string;
  width: number;
  startMs: number;
  endMs: number;
  /** The selected processes, in process order. */
  processIds: string[];
  buckets: ProcessBucket[];
  colorById: Map<string, string>;
  highlightedId: string | undefined;
  memoryLimit: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { tooltipData, tooltipLeft, tooltipTop, showTooltip, hideTooltip } =
    useTooltip<{ timeMs: number; metric: ProcessMetric }>();
  const { containerRef, containerBounds, TooltipInPortal } = useTooltipInPortal(
    { detectBounds: true, scroll: true },
  );
  const xScale = React.useMemo(
    () =>
      scaleTime<number>({
        domain: [new Date(startMs), new Date(endMs)],
        range: buildXYGraphLayoutProps({
          width,
          height: 0,
          margin: CHART_MARGIN,
        }).xScaleRange,
      }),
    [startMs, endMs, width],
  );
  const hoveredBucket =
    tooltipData === undefined
      ? undefined
      : buckets.find(
          (bucket) =>
            bucket.bucketStart <= tooltipData.timeMs &&
            tooltipData.timeMs < bucket.bucketEnd,
        );
  const chartProps = {
    width,
    xScale,
    processIds,
    buckets,
    colorById,
    highlightedId,
    hoverTimeMs: tooltipData?.timeMs,
    onHover: (
      hover: { timeMs: number; metric: ProcessMetric },
      event: React.PointerEvent,
    ) =>
      showTooltip({
        tooltipData: hover,
        tooltipLeft: event.clientX - containerBounds.left,
        tooltipTop: event.clientY - containerBounds.top,
      }),
    onLeave: hideTooltip,
  };

  // Both charts draw the same processes in the same colors, so the CPU chart
  // carries the one legend.
  const legend: ChartHeaderProps["legend"] = [
    ...processIds.flatMap((id) => {
      const color = colorById.get(id);
      return color
        ? [
            {
              label: `${replicaName} · process ${id}`,
              color,
              variant: "line" as const,
            },
          ]
        : [];
    }),
    ...(processIds.some((id) => !colorById.has(id))
      ? [
          {
            label: "Other processes",
            color: colors.foreground.tertiary,
            variant: "line" as const,
          },
        ]
      : []),
  ];

  return (
    <Box ref={containerRef} position="relative" width="100%">
      <VStack spacing={5} alignItems="stretch">
        <ProcessLineChart
          {...chartProps}
          metric="cpu"
          title="CPU by Process"
          description="% of each process's CPU allocation"
          legend={legend}
        />
        <VStack spacing={0} alignItems="stretch">
          <ProcessLineChart
            {...chartProps}
            metric="memory"
            title="Memory by Process"
            description={`% of each process's ${memoryLimit}`}
          />
          <TimeAxis width={width} xScale={xScale} />
        </VStack>
      </VStack>
      {hoveredBucket &&
        tooltipData &&
        tooltipLeft !== undefined &&
        tooltipTop !== undefined && (
          <GraphTooltip
            component={TooltipInPortal}
            top={tooltipTop}
            left={tooltipLeft}
          >
            <LinesTooltipContent
              bucket={hoveredBucket}
              metric={tooltipData.metric}
              processIds={processIds}
              colorById={colorById}
            />
          </GraphTooltip>
        )}
    </Box>
  );
};

interface ProcessLine {
  processId: string;
  points: Array<{ atMs: number; value: number | null }>;
}

const hasReading = (point: ProcessLine["points"][number] | undefined) =>
  point !== undefined && point.value !== null;

// Memoized so pointer moves, which only move the cursor, skip redrawing the
// lines.
const ProcessLines = React.memo(
  ({
    layout,
    xScale,
    yScale,
    ticks,
    lines,
    colorById,
    highlightedId,
  }: {
    layout: Layout;
    xScale: TimeScale;
    yScale: PercentScale;
    ticks: number[];
    lines: ProcessLine[];
    colorById: Map<string, string>;
    highlightedId: string | undefined;
  }) => {
    const { colors, fonts } = useTheme<MaterializeTheme>();
    // Outside the palette, so a highlighted grey process can't be read as
    // a colored one.
    const highlightColor = colors.foreground.primary;
    const coloredOrder = [...colorById.keys()];
    // Grey lines first, then colored ones widest first: where processes read
    // the same, the narrower line on top leaves the wider one's edges showing,
    // so every color stays visible.
    const order = [
      ...lines.filter(({ processId }) => !colorById.has(processId)),
      ...coloredOrder.flatMap((id) =>
        lines.filter(({ processId }) => processId === id),
      ),
    ];
    const strokeWidth = (processId: string) => {
      const index = coloredOrder.indexOf(processId);
      return index < 0 ? 1 : Math.max(1.5, 4.5 - index * 0.75);
    };
    const highlighted = lines.find(
      ({ processId }) => processId === highlightedId,
    );
    const highlightedLast = highlighted?.points.findLast(
      ({ value }) => value !== null,
    );
    const [, rightEdge] = layout.xScaleRange;
    const draw = (
      { processId, points }: ProcessLine,
      style: { color: string; opacity?: number; strokeWidth: number },
    ) => (
      <g key={processId} pointerEvents="none" opacity={style.opacity}>
        <LinePath
          data={points}
          defined={({ value }) => value !== null}
          x={({ atMs }) => xScale(atMs)}
          y={({ value }) => yScale(value ?? 0)}
          stroke={style.color}
          strokeWidth={style.strokeWidth}
        />
        {/* A reading with no defined neighbor draws no line segment, so it
            gets a dot. */}
        {points.flatMap(({ atMs, value }, index) =>
          value === null ||
          hasReading(points[index - 1]) ||
          hasReading(points[index + 1])
            ? []
            : [
                <circle
                  key={atMs}
                  cx={xScale(atMs)}
                  cy={yScale(value)}
                  r={2.5}
                  fill={style.color}
                />,
              ],
        )}
      </g>
    );

    return (
      <>
        <PercentAxis layout={layout} yScale={yScale} ticks={ticks} />
        {order.map((processLine) =>
          draw(processLine, {
            color:
              colorById.get(processLine.processId) ??
              colors.foreground.tertiary,
            opacity:
              highlighted && processLine !== highlighted
                ? 0.2
                : colorById.has(processLine.processId)
                  ? 1
                  : 0.5,
            strokeWidth: strokeWidth(processLine.processId),
          }),
        )}
        {highlighted &&
          draw(highlighted, {
            color: colorById.get(highlighted.processId) ?? highlightColor,
            strokeWidth: 2.5,
          })}
        {highlighted && highlightedLast && highlightedLast.value !== null && (
          <text
            x={Math.min(xScale(highlightedLast.atMs) + 6, rightEdge + 4)}
            y={yScale(highlightedLast.value) + 4}
            fill={colorById.get(highlighted.processId) ?? highlightColor}
            fontFamily={fonts.mono}
            fontSize={11}
          >
            p{highlighted.processId}
          </text>
        )}
      </>
    );
  },
);

const ProcessLineChart = ({
  width,
  xScale,
  processIds,
  buckets,
  colorById,
  highlightedId,
  metric,
  title,
  description,
  legend,
  hoverTimeMs,
  onHover,
  onLeave,
}: {
  width: number;
  xScale: TimeScale;
  processIds: string[];
  buckets: ProcessBucket[];
  colorById: Map<string, string>;
  highlightedId: string | undefined;
  metric: ProcessMetric;
  title: string;
  description: string;
  legend?: ChartHeaderProps["legend"];
  hoverTimeMs: number | undefined;
  onHover: (
    hover: { timeMs: number; metric: ProcessMetric },
    event: React.PointerEvent,
  ) => void;
  onLeave: () => void;
}) => {
  const layout = React.useMemo(
    () =>
      buildXYGraphLayoutProps({
        width,
        height: CHART_MARGIN.top + PLOT_HEIGHT_PX + CHART_MARGIN.bottom,
        margin: CHART_MARGIN,
      }),
    [width],
  );
  const lines = React.useMemo(
    () =>
      processIds.map((processId) => ({
        processId,
        points: buckets.map((bucket) => ({
          atMs: bucket.bucketStart,
          value: bucket.readings.get(processId)?.[metric] ?? null,
        })),
      })),
    [processIds, buckets, metric],
  );
  const peak = React.useMemo(
    () => max(lines, ({ points }) => max(points, ({ value }) => value)) ?? 0,
    [lines],
  );
  const { yScale, ticks } = React.useMemo(
    () => percentScale(peak, layout),
    [peak, layout],
  );

  return (
    <VStack spacing={2} alignItems="stretch" width="100%">
      <ChartHeader title={title} description={description} legend={legend} />
      <svg {...layout.svgProps} role="img" aria-label={title}>
        <ProcessLines
          layout={layout}
          xScale={xScale}
          yScale={yScale}
          ticks={ticks}
          lines={lines}
          colorById={colorById}
          highlightedId={highlightedId}
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
          onPointerMove={(timeMs, event) => onHover({ timeMs, metric }, event)}
          onPointerLeave={onLeave}
        />
      </svg>
    </VStack>
  );
};

const LinesTooltipContent = ({
  bucket,
  metric,
  processIds,
  colorById,
}: {
  bucket: ProcessBucket;
  metric: ProcessMetric;
  processIds: string[];
  colorById: Map<string, string>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const ranked = processIds
    .flatMap((processId) => {
      const value = bucket.readings.get(processId)?.[metric] ?? null;
      return value === null ? [] : [{ processId, value }];
    })
    .sort((a, b) => b.value - a.value);
  const missing = processIds.filter(
    (processId) => (bucket.readings.get(processId)?.[metric] ?? null) === null,
  );
  const rest = ranked.length - TOOLTIP_PROCESS_LIMIT;
  const missingRest = missing.length - TOOLTIP_PROCESS_LIMIT;
  return (
    <>
      <VStack align="start" spacing="1" width="100%" paddingY="1">
        <Text textStyle="text-ui-med" paddingX="4">
          {metric === "cpu" ? "CPU" : "Memory"}
        </Text>
        {ranked.slice(0, TOOLTIP_PROCESS_LIMIT).map(({ processId, value }) => (
          <HStack
            key={processId}
            justifyContent="space-between"
            width="100%"
            paddingX="4"
            spacing={6}
          >
            <HStack spacing={2}>
              <TooltipColorSwatch
                color={colorById.get(processId) ?? colors.foreground.tertiary}
              />
              <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
                Process {processId}
              </Text>
            </HStack>
            <Text textStyle="text-ui-reg">{formatReading(value)}</Text>
          </HStack>
        ))}
        {rest > 0 && (
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            paddingX="4"
          >
            {rest} more {rest === 1 ? "process" : "processes"}
          </Text>
        )}
        {missing.length > 0 && (
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            paddingX="4"
          >
            No reading from{" "}
            {missing
              .slice(0, TOOLTIP_PROCESS_LIMIT)
              .map((processId) => `process ${processId}`)
              .join(", ")}
            {missingRest > 0 && ` and ${missingRest} more`}
          </Text>
        )}
      </VStack>
      <GraphTooltipBucketRange
        bucketStart={new Date(bucket.bucketStart)}
        bucketEnd={new Date(bucket.bucketEnd)}
      />
    </>
  );
};

interface ProcessRow extends ProcessSummary {
  color: string | undefined;
  isBusiestCpu: boolean;
  isBusiestMemory: boolean;
}

const columnHelper = createColumnHelper<ProcessRow>();

interface ProcessTableMeta {
  /** The statistic the findings compare by, whose column reads bold. */
  statistic: ProcessStatistic;
}

const STATISTIC_HEADERS: Record<ProcessStatistic, string> = {
  current: "now",
  peak: "peak",
  average: "average",
};

const readingColumns = (metric: ProcessMetric, label: string) =>
  (Object.keys(STATISTIC_HEADERS) as ProcessStatistic[]).map((key) =>
    columnHelper.accessor((row) => row[metric][key], {
      id: `${metric}-${key}`,
      header: `${label} ${STATISTIC_HEADERS[key]}`,
      sortingFn: sortingFunctions.numericNullsLast,
      meta: { isNumeric: true },
      cell: (info) => {
        const { statistic } = info.table.options.meta as ProcessTableMeta;
        const row = info.row.original;
        const isBusiest =
          key === statistic &&
          (metric === "cpu" ? row.isBusiestCpu : row.isBusiestMemory);
        return (
          <Text
            as="span"
            fontFamily="mono"
            fontWeight={isBusiest ? 600 : undefined}
          >
            {formatReading(info.getValue())}
          </Text>
        );
      },
    }),
  );

const ProcessName = ({ row }: { row: ProcessRow }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack spacing={2} data-process-id={row.processId}>
      <LegendSwatch
        color={row.color ?? colors.foreground.tertiary}
        variant="line"
      />
      <Text as="span">Process {row.processId}</Text>
      {(row.isBusiestCpu || row.isBusiestMemory) && (
        <Text
          as="span"
          textStyle="text-small"
          color={colors.foreground.secondary}
        >
          {row.isBusiestCpu && row.isBusiestMemory
            ? "busiest"
            : row.isBusiestCpu
              ? "busiest CPU"
              : "busiest memory"}
        </Text>
      )}
    </HStack>
  );
};

const processColumns = [
  columnHelper.display({
    id: "select",
    size: 40,
    header: ({ table }) => (
      <Checkbox
        aria-label="Show every process"
        isChecked={table.getIsAllRowsSelected()}
        isIndeterminate={table.getIsSomeRowsSelected()}
        onChange={table.getToggleAllRowsSelectedHandler()}
      />
    ),
    cell: ({ row }) => (
      <Checkbox
        aria-label={`Show process ${row.original.processId}`}
        isChecked={row.getIsSelected()}
        onChange={row.getToggleSelectedHandler()}
      />
    ),
  }),
  columnHelper.accessor((row) => Number(row.processId), {
    id: "process",
    header: "Process",
    cell: (info) => <ProcessName row={info.row.original} />,
  }),
  ...readingColumns("cpu", "CPU"),
  ...readingColumns("memory", "Memory"),
];

// The process whose row an event happened in.
const processIdAt = (target: EventTarget) =>
  target instanceof Element
    ? (target
        .closest("tr")
        ?.querySelector("[data-process-id]")
        ?.getAttribute("data-process-id") ?? undefined)
    : undefined;

const ProcessTable = ({
  summaries,
  colorById,
  busiestCpuId,
  busiestMemoryId,
  statistic,
  memoryLimit,
  selectedIds,
  onSelect,
  onHover,
}: {
  summaries: ProcessSummary[];
  colorById: Map<string, string>;
  busiestCpuId: string | undefined;
  busiestMemoryId: string | undefined;
  statistic: ProcessStatistic;
  memoryLimit: string;
  /** The processes whose lines are drawn. */
  selectedIds: Set<string>;
  onSelect: (ids: Set<string>) => void;
  onHover: (id: string | undefined) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const rows = React.useMemo(
    () =>
      summaries.map((summary): ProcessRow => ({
        ...summary,
        color: colorById.get(summary.processId),
        isBusiestCpu: summary.processId === busiestCpuId,
        isBusiestMemory: summary.processId === busiestMemoryId,
      })),
    [summaries, colorById, busiestCpuId, busiestMemoryId],
  );
  const rowSelection: RowSelectionState = Object.fromEntries(
    [...selectedIds].map((id) => [id, true]),
  );
  const table = useUniversalTable({
    data: rows,
    columns: processColumns,
    getRowId: ({ processId }) => processId,
    state: { rowSelection },
    onRowSelectionChange: (updater) => {
      const next =
        typeof updater === "function" ? updater(rowSelection) : updater;
      onSelect(new Set(Object.keys(next).filter((id) => next[id])));
    },
    meta: { statistic } satisfies ProcessTableMeta,
    // A replica runs at most a few dozen processes, so one page holds them.
    pageSize: 1000,
  });
  return (
    <VStack spacing={2} alignItems="stretch">
      <HStack spacing={2} alignItems="baseline" flexWrap="wrap">
        <Text textStyle="text-ui-med">Processes</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Check a process to draw its line. Memory is a share of each
          process&apos;s {memoryLimit}.
        </Text>
      </HStack>
      <Box
        // Hovering a drawn process's row highlights its line.
        onMouseOver={(event) => {
          const id = processIdAt(event.target);
          onHover(id !== undefined && selectedIds.has(id) ? id : undefined);
        }}
        onMouseLeave={() => onHover(undefined)}
      >
        <UniversalTable table={table} variant="rounded" />
      </Box>
    </VStack>
  );
};
