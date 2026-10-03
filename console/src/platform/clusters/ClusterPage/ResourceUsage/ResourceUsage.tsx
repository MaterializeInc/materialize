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
  Card,
  Flex,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { ParentSize } from "@visx/responsive";
import { scaleTime } from "@visx/scale";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { hasNoUtilizationMetrics } from "~/api/materialize/cluster/replicaUtilizationBinning";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import {
  GraphTooltip,
  GraphTooltipBucketRange,
  TooltipColorSwatch,
} from "~/components/graphComponents";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useChartLegend } from "~/hooks/useChartLegend";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import {
  DataPoint,
  ReplicaData,
} from "~/platform/clusters/ClusterOverview/types";
import {
  CLUSTER_METRICS_UNAVAILABLE_MESSAGE,
  CLUSTERS_FETCH_ERROR_MESSAGE,
} from "~/platform/clusters/constants";
import { MaterializeTheme } from "~/theme";
import { capitalizeSentence } from "~/util";
import { formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import {
  CpuChart,
  DiskChart,
  MemoryChart,
  ReplicaStatusTimeline,
  StatusTimelineRow,
  TimeAxis,
} from "./ResourceUsageCharts";
import {
  canSwap,
  datumAt,
  DdlEvent,
  hasOomBetween,
  memoryBar,
  mergeBuckets,
  mergedBucketMs,
  sharedHeapLimitBytes,
  stateAt,
} from "./resourceUsageModel";
import {
  CHART_MARGIN,
  MIN_BAR_SLOT_PX,
  useStatusStyles,
} from "./resourceUsageStyles";
import { ColoredReplica, useResourceUsageData } from "./useResourceUsageData";

// Readings in percent, 0 to 100. Memory and swap are the bar's two segments,
// so they add up to its height.
const readingRows = (point: DataPoint) => {
  const bar = memoryBar(point);
  return [
    { label: "CPU", percent: point.cpuPercent },
    point.swapPercent === null
      ? { label: "Heap", percent: point.heapPercent }
      : { label: "Memory", percent: bar ? bar.heap - bar.swap : null },
    ...(canSwap(point)
      ? [
          {
            label: "Swap",
            percent: point.swapPercent === null ? null : (bar?.swap ?? null),
          },
        ]
      : []),
    ...(point.diskPercent === null
      ? []
      : [{ label: "Disk", percent: point.diskPercent }]),
  ];
};

/** Each replica's reading and status at `timeMs`, skipping replicas with neither. */
const tooltipSectionsAt = (
  series: ReplicaData[],
  timelines: Map<string, StatusTimelineRow["timeline"]>,
  timeMs: number,
) =>
  series.flatMap(({ id, data }) => {
    const point = datumAt(data, timeMs);
    const timeline = timelines.get(id);
    const state = timeline ? stateAt(timeline, timeMs) : undefined;
    if (!point && !state) return [];
    const isOom = Boolean(
      point &&
      timeline &&
      hasOomBetween(timeline, point.bucketStart, point.bucketEnd),
    );
    return [{ id, point, state, isOom }];
  });

const ResourceUsageTooltipContent = ({
  sections,
  replicasById,
}: {
  sections: ReturnType<typeof tooltipSectionsAt>;
  replicasById: Map<string, ColoredReplica>;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const statusStyles = useStatusStyles();
  const bucket = sections.find((section) => section.point)?.point;

  return (
    <>
      {sections.map(({ id, point, state, isOom }) => {
        const replica = replicasById.get(id);
        return (
          <VStack key={id} align="start" spacing="1" width="100%">
            <Flex
              background={colors.background.secondary}
              borderColor={colors.border.primary}
              borderBottomWidth="1px"
              justifyContent="space-between"
              alignItems="center"
              width="100%"
              gap={4}
              paddingX="4"
              paddingY="1"
            >
              <HStack spacing="2">
                <TooltipColorSwatch color={replica?.color ?? ""} />
                <Text as="span" textStyle="text-ui-med">
                  {replica?.name ?? id}
                </Text>
                {replica?.size && (
                  <Text
                    as="span"
                    textStyle="text-ui-med"
                    color={colors.foreground.secondary}
                  >
                    {replica.size}
                  </Text>
                )}
              </HStack>
              <Text
                textStyle="text-ui-med"
                color={isOom ? colors.accent.red : undefined}
              >
                {isOom
                  ? "Out of Memory"
                  : state &&
                    capitalizeSentence(statusStyles[state].label, false)}
              </Text>
            </Flex>
            {point &&
              readingRows(point).map(({ label, percent }) => (
                <HStack
                  key={label}
                  justifyContent="space-between"
                  width="100%"
                  paddingX="4"
                >
                  <Text
                    textStyle="text-ui-reg"
                    color={colors.foreground.secondary}
                  >
                    {label}
                  </Text>
                  <Text textStyle="text-ui-reg">
                    {percent === null
                      ? "-"
                      : formatPercentage(percent / 100, 1)}
                  </Text>
                </HStack>
              ))}
          </VStack>
        );
      })}
      {bucket && (
        <GraphTooltipBucketRange
          bucketStart={new Date(bucket.bucketStart)}
          bucketEnd={new Date(bucket.bucketEnd)}
        />
      )}
    </>
  );
};

interface ResourceUsageChartsProps {
  width: number;
  startDate: Date;
  endDate: Date;
  series: ReplicaData[];
  replicasById: Map<string, ColoredReplica>;
  colorMap: Map<string, string>;
  timelines: Map<string, StatusTimelineRow["timeline"]>;
  ddlEvents: DdlEvent[];
  heapLimitBytes: number | undefined;
  showDiskChart: boolean;
  isDdlError: boolean;
  isStatusLoading: boolean;
  isStatusError: boolean;
  isHydrationError: boolean;
}

const ResourceUsageCharts = ({
  width,
  startDate,
  endDate,
  series,
  replicasById,
  colorMap,
  timelines,
  ddlEvents,
  heapLimitBytes,
  showDiskChart,
  isDdlError,
  isStatusLoading,
  isStatusError,
  isHydrationError,
}: ResourceUsageChartsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { tooltipData, tooltipLeft, tooltipTop, showTooltip, hideTooltip } =
    useTooltip<number>();
  const { containerRef, containerBounds, TooltipInPortal } = useTooltipInPortal(
    { detectBounds: true, scroll: true },
  );
  const xScale = React.useMemo(
    () =>
      scaleTime<number>({
        domain: [startDate, endDate],
        range: buildXYGraphLayoutProps({
          width,
          height: 0,
          margin: CHART_MARGIN,
        }).xScaleRange,
      }),
    [startDate, endDate, width],
  );
  // Wide windows have more buckets than the plot fits as bars, so they merge
  // into wider ones. The charts and the tooltip all read the merged series.
  const chartSeries = React.useMemo(() => {
    const [left, right] = xScale.range();
    const sourceBucketMs = series.reduce(
      (widest, { data }) =>
        data.reduce(
          (max, point) => Math.max(max, point.bucketEnd - point.bucketStart),
          widest,
        ),
      0,
    );
    const groupMs = mergedBucketMs({
      sourceBucketMs,
      domainMs: endDate.getTime() - startDate.getTime(),
      plotWidthPx: right - left,
      replicaCount: series.length,
      minSlotPx: MIN_BAR_SLOT_PX,
    });
    if (groupMs === sourceBucketMs) return series;
    return series.map((replica) => ({
      ...replica,
      data: mergeBuckets(replica.data, groupMs),
    }));
  }, [series, xScale, startDate, endDate]);
  const statusRows = React.useMemo(
    () =>
      series.flatMap(({ id }) => {
        const timeline = timelines.get(id);
        return timeline
          ? [{ id, name: replicasById.get(id)?.name ?? id, timeline }]
          : [];
      }),
    [series, timelines, replicasById],
  );
  const onPointerMove = (timeMs: number, event: React.PointerEvent) =>
    showTooltip({
      tooltipData: timeMs,
      tooltipLeft: event.clientX - containerBounds.left,
      tooltipTop: event.clientY - containerBounds.top,
    });
  const tooltipSections =
    tooltipData === undefined
      ? []
      : tooltipSectionsAt(chartSeries, timelines, tooltipData);
  const plotProps = {
    width,
    xScale,
    series: chartSeries,
    colorMap,
    ddlEvents,
    hoverTimeMs: tooltipData,
    onPointerMove,
    onPointerLeave: hideTooltip,
  };

  return (
    <Box ref={containerRef} position="relative" width="100%">
      <VStack spacing={6} alignItems="stretch">
        <VStack spacing={1} alignItems="stretch">
          <CpuChart {...plotProps} />
          {isDdlError && (
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Object creation markers are unavailable.
            </Text>
          )}
        </VStack>
        <VStack spacing={0} alignItems="stretch">
          <VStack spacing={6} alignItems="stretch">
            <MemoryChart {...plotProps} heapLimitBytes={heapLimitBytes} />
            {showDiskChart && <DiskChart {...plotProps} />}
          </VStack>
          <TimeAxis width={width} xScale={xScale} />
        </VStack>
        {isStatusError ? (
          <Alert variant="error" message="Replica status is unavailable." />
        ) : isStatusLoading ? (
          <Flex justifyContent="center">
            <Spinner size="sm" />
          </Flex>
        ) : (
          <VStack spacing={1} alignItems="stretch">
            <ReplicaStatusTimeline
              width={width}
              xScale={xScale}
              rows={statusRows}
              hoverTimeMs={tooltipData}
              onPointerMove={onPointerMove}
              onPointerLeave={hideTooltip}
            />
            {isHydrationError && (
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                Hydration history is unavailable.
              </Text>
            )}
          </VStack>
        )}
      </VStack>
      {tooltipSections.length > 0 &&
        tooltipLeft !== undefined &&
        tooltipTop !== undefined && (
          <GraphTooltip
            component={TooltipInPortal}
            top={tooltipTop}
            left={tooltipLeft}
          >
            <ResourceUsageTooltipContent
              sections={tooltipSections}
              replicasById={replicasById}
            />
          </GraphTooltip>
        )}
    </Box>
  );
};

export interface ResourceUsageProps {
  cluster: Cluster;
}

/**
 * CPU, memory and replica status for a cluster's replicas over a shared time
 * axis, with one crosshair and tooltip across all three.
 */
export const ResourceUsage = ({ cluster }: ResourceUsageProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-cluster-graph-time-period",
  });
  const {
    history,
    replicasById,
    replicaIds,
    colorMap,
    timelines,
    ddlEvents,
    heapLimitBytesBySize,
    isDdlError,
    isStatusLoading,
    isStatusError,
    isHydrationError,
  } = useResourceUsageData({ cluster, timePeriodMinutes });
  const { visibleLegendItems, toggleLegendItem } = useChartLegend({
    allLegendItems: replicaIds,
  });
  const graphData = history.data?.graphData;
  const series = React.useMemo(
    () => (graphData ?? []).filter(({ id }) => visibleLegendItems.has(id)),
    [graphData, visibleLegendItems],
  );
  // Swap sizes run without a scratch disk and report no disk usage, so only
  // older disk sizes have readings. Decided over all replicas, so hiding one
  // doesn't remove the chart.
  const showDiskChart = (graphData ?? []).some(({ data }) =>
    data.some((point) => point.diskPercent !== null),
  );
  const heapLimitBytes = sharedHeapLimitBytes(
    series.map(({ id }) => replicasById.get(id)?.size),
    heapLimitBytesBySize,
  );

  let body: React.ReactNode;
  if (history.isError) {
    body = <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />;
  } else if (history.isLoading || !history.data) {
    body = (
      <Flex height="md" alignItems="center" justifyContent="center">
        <Spinner data-testid="loading-spinner" />
      </Flex>
    );
  } else if (history.data.graphData.length === 0) {
    body = (
      <Text textStyle="text-base" color={colors.foreground.secondary}>
        No resource usage in this time period.
      </Text>
    );
  } else if (hasNoUtilizationMetrics(history.data.graphData)) {
    body = (
      <Alert variant="info" message={CLUSTER_METRICS_UNAVAILABLE_MESSAGE} />
    );
  } else {
    const { startDate, endDate } = history.data;
    body = (
      <ParentSize debounceTime={10} style={{ width: "100%", minWidth: 0 }}>
        {({ width }) =>
          width > 0 && (
            <ResourceUsageCharts
              width={width}
              startDate={startDate}
              endDate={endDate}
              series={series}
              showDiskChart={showDiskChart}
              replicasById={replicasById}
              colorMap={colorMap}
              timelines={timelines}
              ddlEvents={ddlEvents}
              heapLimitBytes={heapLimitBytes}
              isDdlError={isDdlError}
              isStatusLoading={isStatusLoading}
              isStatusError={isStatusError}
              isHydrationError={isHydrationError}
            />
          )
        }
      </ParentSize>
    );
  }

  return (
    <Card
      p={5}
      width="100%"
      borderRadius="md"
      borderWidth="1px"
      borderColor={colors.border.primary}
    >
      <VStack spacing={6} alignItems="stretch">
        <HStack justifyContent="space-between" alignItems="start" spacing={4}>
          <Text as="h3" textStyle="heading-sm">
            Resource Usage
          </Text>
          <HStack spacing={4} flexWrap="wrap" justifyContent="flex-end">
            {history.isRefreshing && (
              <Spinner size="sm" data-testid="refreshing-spinner" />
            )}
            <HStack
              as="ul"
              aria-label="Replicas"
              spacing={4}
              listStyleType="none"
            >
              {[...replicasById.values()].map((replica) => (
                <Box as="li" key={replica.id}>
                  <HStack
                    as="button"
                    type="button"
                    aria-pressed={visibleLegendItems.has(replica.id)}
                    spacing={1}
                    onClick={(event) => toggleLegendItem(replica.id, event)}
                    opacity={visibleLegendItems.has(replica.id) ? 1 : 0.5}
                  >
                    <TooltipColorSwatch color={replica.color} />
                    <Text
                      textStyle="text-small"
                      color={colors.foreground.primary}
                    >
                      {replica.isCurrent
                        ? replica.name
                        : `${replica.name} (dropped)`}
                    </Text>
                    {replica.size && (
                      <Text
                        textStyle="text-small"
                        color={colors.foreground.secondary}
                      >
                        {replica.size}
                      </Text>
                    )}
                  </HStack>
                </Box>
              ))}
            </HStack>
            <TimePeriodSelect
              timePeriodMinutes={timePeriodMinutes}
              setTimePeriodMinutes={setTimePeriodMinutes}
            />
          </HStack>
        </HStack>
        {body}
      </VStack>
    </Card>
  );
};
