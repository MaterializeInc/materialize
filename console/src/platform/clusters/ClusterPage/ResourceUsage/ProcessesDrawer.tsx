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
  Button,
  Flex,
  HStack,
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
import { createColumnHelper } from "@tanstack/react-table";
import { useParentSize } from "@visx/responsive";
import { scaleLinear, scaleTime } from "@visx/scale";
import { LinePath } from "@visx/shape";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import React from "react";

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
import { MIN_BUCKET_SIZE_MS } from "~/platform/clusters/constants";
import { MaterializeTheme } from "~/theme";
import { formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import {
  bucketProcessReadings,
  BusiestProcess,
  busiestProcess,
  ProcessBucket,
  processIdsOf,
  ProcessMetric,
  ProcessSummary,
  readProcessSamples,
  summarizeProcesses,
} from "./processReadingsModel";
import {
  ChartHeader,
  HoverOverlay,
  PercentAxis,
  TimeAxis,
  TimeScale,
} from "./ResourceUsageCharts";
import { CHART_MARGIN, PERCENT_TICKS } from "./resourceUsageStyles";
import { useReplicaProcessSamples } from "./useReplicaProcessSamples";

// The per-process history is raw per-minute samples, so the drawer reads at
// most a day of them, whatever the card's window.
const MAX_WINDOW_MINUTES = 24 * 60;
// A busiest process within this factor of the others' mean reads as even.
const EVEN_RATIO = 1.25;
// The theme's chart palette has five strong colors. Past five processes, only
// the ones that stand out get a color; the rest stay grey but named.
const MAX_COLORED_PROCESSES = 5;
const PLOT_HEIGHT_PX = 150;
const OVERFLOW_TICK_COUNT = 4;
// The most processes the tooltip lists before summarizing the rest.
const TOOLTIP_PROCESS_LIMIT = 6;

export interface ProcessReplica {
  id: string;
  name: string;
  size: string;
  color: string;
  /** One process's allocation. */
  allocation: SizeAllocation;
}

export interface ProcessesDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  /** Replicas whose size runs more than one process. */
  replicas: ProcessReplica[];
  timePeriodMinutes: number;
}

/**
 * How each multi-process replica's processes share its work: which process is
 * busiest, a line per process over time, and each one's average and peak,
 * read live while open. One tab per replica, and only the open tab reads.
 */
export const ProcessesDrawer = ({
  isOpen,
  onClose,
  replicas,
  timePeriodMinutes,
}: ProcessesDrawerProps) => (
  <SideDrawer
    isOpen={isOpen}
    onClose={onClose}
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
            />
          </TabPanel>
        ))}
      </TabPanels>
    </Tabs>
  </SideDrawer>
);

const formatReading = (percent: number | null) =>
  percent === null ? "-" : formatPercentage(percent / 100, 1);

const standsOut = (busiest: BusiestProcess | undefined) =>
  busiest !== undefined &&
  (busiest.othersAverage <= 0 ||
    busiest.average / busiest.othersAverage >= EVEN_RATIO);

const finding = (
  label: string,
  noun: string,
  busiest: BusiestProcess | undefined,
) => {
  if (!busiest) return undefined;
  const others =
    busiest.otherCount === 1
      ? "the other process"
      : `the other ${busiest.otherCount} processes`;
  if (!standsOut(busiest)) {
    return `${label} is spread evenly: process ${busiest.processId} averaged ${formatReading(busiest.average)}, against ${formatReading(busiest.othersAverage)} for ${others}.`;
  }
  const ratio =
    busiest.othersAverage > 0
      ? `, ${(busiest.average / busiest.othersAverage).toFixed(1)}× ${others} (${formatReading(busiest.othersAverage)})`
      : "";
  return `Process ${busiest.processId} used the most ${noun}: ${formatReading(busiest.average)} on average${ratio}.`;
};

const ProcessesPanel = ({
  replica,
  windowMinutes,
}: {
  replica: ProcessReplica;
  windowMinutes: number;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const state = useReplicaProcessSamples({
    replicaId: replica.id,
    windowMinutes,
  });
  const { parentRef, width } = useParentSize({ debounceTime: 10 });
  const view = React.useMemo(() => {
    const endMs = Date.now();
    const startMs = endMs - windowMinutes * 60_000;
    const readings = readProcessSamples({
      samples: state.data,
      allocation: replica.allocation,
      startMs,
    });
    const processIds = processIdsOf(readings);
    const buckets = bucketProcessReadings(
      readings,
      Math.max(windowMinutes * 1000, MIN_BUCKET_SIZE_MS),
    );
    const summaries = summarizeProcesses(processIds, readings);
    const busiestCpu = busiestProcess(summaries, "cpu");
    const busiestMemory = busiestProcess(summaries, "memory");
    return {
      startMs,
      endMs,
      processIds,
      buckets,
      summaries,
      busiestCpu,
      busiestMemory,
      busiestCpuId: standsOut(busiestCpu) ? busiestCpu?.processId : undefined,
      busiestMemoryId: standsOut(busiestMemory)
        ? busiestMemory?.processId
        : undefined,
      hasHeapLimit: state.data.some(({ heapLimit }) => heapLimit !== null),
    };
  }, [state.data, replica.allocation, windowMinutes]);
  const colorById = React.useMemo(() => {
    const palette = colors.lineGraph.slice(0, MAX_COLORED_PROCESSES);
    const coloredIds =
      view.processIds.length <= MAX_COLORED_PROCESSES
        ? view.processIds
        : [
            ...new Set(
              [view.busiestCpuId, view.busiestMemoryId].flatMap((id) =>
                id === undefined ? [] : [id],
              ),
            ),
          ];
    return new Map(coloredIds.map((id, index) => [id, palette[index]]));
  }, [view, colors.lineGraph]);

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
  const memoryShare = view.hasHeapLimit
    ? "% of each process's heap limit"
    : "% of each process's memory allocation";
  const isBusiest = (id: string) =>
    id === view.busiestCpuId || id === view.busiestMemoryId;
  return (
    <VStack spacing={6} alignItems="stretch">
      <VStack spacing={1} alignItems="start">
        {[
          finding("CPU", "CPU", view.busiestCpu),
          finding("Memory", "memory", view.busiestMemory),
        ].map(
          (text) =>
            text && (
              <Text key={text} textStyle="text-base">
                {text}
              </Text>
            ),
        )}
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {replica.size} runs each replica as {replica.allocation.processes}{" "}
          processes. Readings are shares of one process&apos;s allocation.
        </Text>
      </VStack>
      <VStack spacing={3} alignItems="stretch">
        <ProcessLegend
          processIds={view.processIds}
          selectedIds={selectedIds}
          colorById={colorById}
          isBusiest={isBusiest}
          onToggle={(id) => {
            const next = new Set(selectedIds);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            setSelected(next);
          }}
          onSelectAll={() => setSelected(new Set(view.processIds))}
          onClearAll={() => setSelected(new Set())}
          onHover={setHoveredId}
        />
        <Box ref={parentRef} width="100%" minWidth={0}>
          {width > 0 && (
            <ProcessLineCharts
              width={width}
              startMs={view.startMs}
              endMs={view.endMs}
              processIds={view.processIds.filter((id) => selectedIds.has(id))}
              buckets={view.buckets}
              colorById={colorById}
              highlightColor={replica.color}
              highlightedId={hoveredId}
              memoryShare={memoryShare}
            />
          )}
        </Box>
      </VStack>
      <ProcessTable
        summaries={view.summaries}
        colorById={colorById}
        busiestCpuId={view.busiestCpuId}
        busiestMemoryId={view.busiestMemoryId}
        memoryShare={memoryShare}
      />
    </VStack>
  );
};

const LineSwatch = ({ color }: { color: string }) => (
  <Box
    as="span"
    display="inline-block"
    width="14px"
    height="3px"
    borderRadius="full"
    background={color}
    flexShrink={0}
  />
);

const ProcessLegend = ({
  processIds,
  selectedIds,
  colorById,
  isBusiest,
  onToggle,
  onSelectAll,
  onClearAll,
  onHover,
}: {
  processIds: string[];
  selectedIds: Set<string>;
  colorById: Map<string, string>;
  isBusiest: (id: string) => boolean;
  onToggle: (id: string) => void;
  onSelectAll: () => void;
  onClearAll: () => void;
  onHover: (id: string | undefined) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Flex
      as="ul"
      aria-label="Processes"
      listStyleType="none"
      flexWrap="wrap"
      gap={2}
      alignItems="center"
    >
      {processIds.map((id) => {
        const isSelected = selectedIds.has(id);
        return (
          <Box as="li" key={id}>
            <Button
              size="xs"
              variant="outline"
              borderRadius="full"
              aria-pressed={isSelected}
              opacity={isSelected ? 1 : 0.5}
              onClick={() => onToggle(id)}
              onMouseEnter={() => onHover(isSelected ? id : undefined)}
              onMouseLeave={() => onHover(undefined)}
              onFocus={() => onHover(isSelected ? id : undefined)}
              onBlur={() => onHover(undefined)}
              leftIcon={
                <LineSwatch
                  color={colorById.get(id) ?? colors.foreground.tertiary}
                />
              }
            >
              Process {id}
              {isBusiest(id) && (
                <Text
                  as="span"
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                  marginLeft={1.5}
                >
                  busiest
                </Text>
              )}
            </Button>
          </Box>
        );
      })}
      <Box as="li">
        <HStack spacing={1}>
          <Button
            size="xs"
            variant="ghost"
            onClick={onSelectAll}
            isDisabled={selectedIds.size === processIds.length}
          >
            Select all
          </Button>
          <Button
            size="xs"
            variant="ghost"
            onClick={onClearAll}
            isDisabled={selectedIds.size === 0}
          >
            Clear all
          </Button>
        </HStack>
      </Box>
    </Flex>
  );
};

const ProcessLineCharts = ({
  width,
  startMs,
  endMs,
  processIds,
  buckets,
  colorById,
  highlightColor,
  highlightedId,
  memoryShare,
}: {
  width: number;
  startMs: number;
  endMs: number;
  /** The selected processes, in process order. */
  processIds: string[];
  buckets: ProcessBucket[];
  colorById: Map<string, string>;
  /** The color for a highlighted process that has none of its own. */
  highlightColor: string;
  highlightedId: string | undefined;
  memoryShare: string;
}) => {
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
    highlightColor,
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

  return (
    <Box ref={containerRef} position="relative" width="100%">
      <VStack spacing={5} alignItems="stretch">
        <ProcessLineChart
          {...chartProps}
          metric="cpu"
          title="CPU by process"
          description="% of each process's CPU allocation"
        />
        <VStack spacing={0} alignItems="stretch">
          <ProcessLineChart
            {...chartProps}
            metric="memory"
            title="Memory by process"
            description={memoryShare}
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

const ProcessLineChart = ({
  width,
  xScale,
  processIds,
  buckets,
  colorById,
  highlightColor,
  highlightedId,
  metric,
  title,
  description,
  hoverTimeMs,
  onHover,
  onLeave,
}: {
  width: number;
  xScale: TimeScale;
  processIds: string[];
  buckets: ProcessBucket[];
  colorById: Map<string, string>;
  highlightColor: string;
  highlightedId: string | undefined;
  metric: ProcessMetric;
  title: string;
  description: string;
  hoverTimeMs: number | undefined;
  onHover: (
    hover: { timeMs: number; metric: ProcessMetric },
    event: React.PointerEvent,
  ) => void;
  onLeave: () => void;
}) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
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
  const peak = Math.max(
    0,
    ...lines.flatMap(({ points }) =>
      points.flatMap(({ value }) => (value === null ? [] : [value])),
    ),
  );
  const { yScale, ticks } = React.useMemo(() => {
    if (peak <= 100) {
      return {
        yScale: scaleLinear<number>({
          domain: [0, 100],
          range: layout.yScaleRange,
          clamp: true,
        }),
        ticks: PERCENT_TICKS,
      };
    }
    const scale = scaleLinear<number>({
      domain: [0, peak],
      range: layout.yScaleRange,
      nice: OVERFLOW_TICK_COUNT,
    });
    return { yScale: scale, ticks: scale.ticks(OVERFLOW_TICK_COUNT) };
  }, [layout, peak]);
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
  const line = (
    { processId, points }: (typeof lines)[number],
    props: { stroke: string; strokeOpacity?: number; strokeWidth: number },
  ) => (
    <LinePath
      key={processId}
      data={points}
      defined={({ value }) => value !== null}
      x={({ atMs }) => xScale(atMs)}
      y={({ value }) => yScale(value ?? 0)}
      pointerEvents="none"
      {...props}
    />
  );

  return (
    <VStack spacing={2} alignItems="stretch" width="100%">
      <ChartHeader title={title} description={description} />
      <svg {...layout.svgProps} role="img" aria-label={title}>
        <PercentAxis layout={layout} yScale={yScale} ticks={ticks} />
        {order.map((processLine) =>
          line(processLine, {
            stroke:
              colorById.get(processLine.processId) ??
              colors.foreground.tertiary,
            strokeOpacity:
              highlighted && processLine !== highlighted
                ? 0.2
                : colorById.has(processLine.processId)
                  ? 1
                  : 0.5,
            strokeWidth: strokeWidth(processLine.processId),
          }),
        )}
        {highlighted &&
          line(highlighted, {
            stroke: colorById.get(highlighted.processId) ?? highlightColor,
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
            {missing.map((processId) => `process ${processId}`).join(", ")}
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

const readingColumn = (
  id: string,
  header: string,
  read: (row: ProcessRow) => number | null,
  isBusiest?: (row: ProcessRow) => boolean,
) =>
  columnHelper.accessor(read, {
    id,
    header,
    sortingFn: sortingFunctions.numericNullsLast,
    meta: { isNumeric: true },
    cell: (info) => (
      <Text
        as="span"
        fontFamily="mono"
        fontWeight={isBusiest?.(info.row.original) ? 600 : undefined}
      >
        {formatReading(info.getValue())}
      </Text>
    ),
  });

const ProcessName = ({ row }: { row: ProcessRow }) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack spacing={2}>
      <LineSwatch color={row.color ?? colors.foreground.tertiary} />
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
  columnHelper.accessor((row) => Number(row.processId), {
    id: "process",
    header: "Process",
    cell: (info) => <ProcessName row={info.row.original} />,
  }),
  readingColumn(
    "cpuAverage",
    "CPU average",
    (row) => row.cpu.average,
    (row) => row.isBusiestCpu,
  ),
  readingColumn("cpuPeak", "CPU peak", (row) => row.cpu.peak),
  readingColumn(
    "memoryAverage",
    "Memory average",
    (row) => row.memory.average,
    (row) => row.isBusiestMemory,
  ),
  readingColumn("memoryPeak", "Memory peak", (row) => row.memory.peak),
];

const ProcessTable = ({
  summaries,
  colorById,
  busiestCpuId,
  busiestMemoryId,
  memoryShare,
}: {
  summaries: ProcessSummary[];
  colorById: Map<string, string>;
  busiestCpuId: string | undefined;
  busiestMemoryId: string | undefined;
  memoryShare: string;
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
  const table = useUniversalTable({
    data: rows,
    columns: processColumns,
    // A replica runs at most a few dozen processes, so one page holds them.
    pageSize: 1000,
  });
  return (
    <VStack spacing={2} alignItems="stretch">
      <HStack spacing={2} alignItems="baseline" flexWrap="wrap">
        <Text textStyle="text-ui-med">Processes</Text>
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Average and peak over the window. Memory is {memoryShare}.
        </Text>
      </HStack>
      <UniversalTable table={table} variant="rounded" />
    </VStack>
  );
};
