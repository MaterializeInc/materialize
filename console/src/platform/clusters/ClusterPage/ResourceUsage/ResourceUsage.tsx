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
  Card,
  Flex,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useParentSize } from "@visx/responsive";
import { scaleTime } from "@visx/scale";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { hasUtilizationReading } from "~/api/materialize/cluster/replicaUtilizationBinning";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import {
  GraphTooltip,
  GraphTooltipBucketRange,
  TooltipColorSwatch,
} from "~/components/graphComponents";
import TextLink from "~/components/TextLink";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useAppConfig } from "~/config/useAppConfig";
import { useChartLegend } from "~/hooks/useChartLegend";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import docUrls from "~/mz-doc-urls.json";
import {
  DataPoint,
  ReplicaData,
} from "~/platform/clusters/ClusterOverview/types";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "~/platform/clusters/constants";
import { useReplicaSizeAllocations } from "~/platform/clusters/queries";
import { MaterializeTheme } from "~/theme";
import { capitalizeSentence } from "~/util";
import { formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import { WorkerCpuDrawer } from "../WorkerCpu/WorkerCpuDrawer";
import { WorkerReplica } from "../WorkerCpu/workerCpuModel";
import { ProcessesDrawer, ProcessReplica } from "./ProcessesDrawer";
import {
  CpuChart,
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
  MissingUsageCause,
  mostBarsAtOnce,
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
const readingRows = (point: DataPoint, hasHeapLimit: boolean) => {
  const bar = memoryBar(point);
  return [
    { label: "CPU", percent: point.cpuPercent },
    point.swapPercent === null
      ? { label: hasHeapLimit ? "Heap" : "Memory", percent: point.heapPercent }
      : { label: "Memory", percent: bar ? bar.heap - bar.swap : null },
    ...(canSwap(point)
      ? [
          {
            label: "Swap",
            percent: point.swapPercent === null ? null : (bar?.swap ?? null),
          },
        ]
      : []),
  ];
};

/**
 * One section per replica group at `timeMs`: each member with a reading then,
 * or the group's status alone when none has one. Groups with neither are
 * skipped.
 */
const tooltipSectionsAt = (
  series: ReplicaData[],
  timelines: Map<string, StatusTimelineRow["timeline"]>,
  groupIdById: Map<string, string>,
  timeMs: number,
) => {
  const sections = series.map(({ id, data }) => {
    const groupId = groupIdById.get(id) ?? id;
    const point = datumAt(data, timeMs);
    const timeline = timelines.get(groupId);
    const state = timeline ? stateAt(timeline, timeMs) : undefined;
    const isOom = Boolean(
      point &&
      timeline &&
      hasOomBetween(timeline, point.bucketStart, point.bucketEnd),
    );
    return { id, groupId, point, state, isOom };
  });
  const groupsWithReading = new Set(
    sections.flatMap(({ groupId, point }) => (point ? [groupId] : [])),
  );
  return sections.filter(
    ({ id, groupId, point, state }) =>
      point ||
      (state !== undefined &&
        id === groupId &&
        !groupsWithReading.has(groupId)),
  );
};

const ResourceUsageTooltipContent = ({
  sections,
  replicasById,
  hasHeapLimit,
}: {
  sections: ReturnType<typeof tooltipSectionsAt>;
  replicasById: Map<string, ColoredReplica>;
  hasHeapLimit: boolean;
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
              readingRows(point, hasHeapLimit).map(({ label, percent }) => (
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

// Where readings stay empty, the next step depends on the deployment: cloud
// customers can't install metrics collection themselves.
const MissingUsageAdvice = () => {
  const { mode } = useAppConfig();
  return mode === "cloud" ? (
    <>
      If this continues,{" "}
      <TextLink href={docUrls["/docs/support/"]} isExternal>
        contact support
      </TextLink>
      .
    </>
  ) : (
    <>Check that metrics-server is installed in your Kubernetes cluster.</>
  );
};

const MissingUsageMessage = ({
  cause,
  hasReplicas,
}: {
  cause: MissingUsageCause;
  hasReplicas: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  let message: React.ReactNode;
  if (!hasReplicas) {
    message = "No resource usage in this time period.";
  } else if (cause === "unmeasured") {
    message = (
      <>
        Your replicas were running, but no resource usage was recorded for this
        period. <MissingUsageAdvice />
      </>
    );
  } else if (cause === "offlineOom") {
    message = (
      <>
        All replicas were offline for this period. They last went offline after
        running out of memory. See{" "}
        <TextLink
          href={docUrls["/docs/clusters/troubleshoot-clusters/memory-spike/"]}
          isExternal
        >
          Troubleshoot memory usage
        </TextLink>
        .
      </>
    );
  } else if (cause === "offline") {
    message = (
      <>
        All replicas were offline for this period, and Materialize didn&apos;t
        record why. If they keep restarting, see{" "}
        <TextLink
          href={
            docUrls["/docs/clusters/troubleshoot-clusters/hydration-failures/"]
          }
          isExternal
        >
          Troubleshoot hydration failures
        </TextLink>
        .
      </>
    );
  } else {
    message = (
      <>
        No resource usage was recorded for this period. The replicas may have
        been offline, or metrics may be unavailable. <MissingUsageAdvice />
      </>
    );
  }
  return (
    <Text textStyle="text-base" color={colors.foreground.secondary}>
      {message}
    </Text>
  );
};

interface ResourceUsageContentProps {
  width: number;
  startDate: Date;
  endDate: Date;
  series: ReplicaData[];
  hasUsage: boolean;
  /** Replicas whose status rows show, which may have no samples. */
  statusReplicaIds: string[];
  replicasById: Map<string, ColoredReplica>;
  groupIdById: Map<string, string>;
  colorMap: Map<string, string>;
  /** By group id from `groupIdById`. */
  timelines: Map<string, StatusTimelineRow["timeline"]>;
  ddlEvents: DdlEvent[];
  heapLimitBytes: number | undefined;
  hasHeapLimit: boolean;
  unhydratedObjectCounts: Map<string, number> | undefined;
  missingUsage: MissingUsageCause;
  isDdlError: boolean;
  cpuHeaderAction?: React.ReactNode;
  isStatusLoading: boolean;
  isStatusError: boolean;
  isHydrationError: boolean;
}

const ResourceUsageContent = ({
  width,
  startDate,
  endDate,
  series,
  hasUsage,
  statusReplicaIds,
  replicasById,
  groupIdById,
  colorMap,
  timelines,
  ddlEvents,
  heapLimitBytes,
  hasHeapLimit,
  unhydratedObjectCounts,
  missingUsage,
  isDdlError,
  isStatusLoading,
  isStatusError,
  isHydrationError,
  cpuHeaderAction,
}: ResourceUsageContentProps) => {
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
      // Replicas that never ran together don't share a time's slot.
      replicaCount: mostBarsAtOnce(series),
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
      statusReplicaIds.flatMap((id) => {
        const timeline = timelines.get(id);
        return timeline
          ? [
              {
                id,
                name: replicasById.get(id)?.name ?? id,
                timeline,
                unhydratedCount: unhydratedObjectCounts?.get(id),
              },
            ]
          : [];
      }),
    [statusReplicaIds, timelines, replicasById, unhydratedObjectCounts],
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
      : tooltipSectionsAt(chartSeries, timelines, groupIdById, tooltipData);
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
        {hasUsage ? (
          <>
            <VStack spacing={1} alignItems="stretch">
              <CpuChart {...plotProps} headerAction={cpuHeaderAction} />
              {isDdlError && (
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  Object creation markers are unavailable.
                </Text>
              )}
            </VStack>
            <VStack spacing={0} alignItems="stretch">
              <MemoryChart
                {...plotProps}
                heapLimitBytes={heapLimitBytes}
                hasHeapLimit={hasHeapLimit}
              />
              <TimeAxis width={width} xScale={xScale} />
            </VStack>
          </>
        ) : (
          <VStack spacing={2} alignItems="stretch">
            <MissingUsageMessage
              cause={missingUsage}
              hasReplicas={replicasById.size > 0}
            />
            <TimeAxis width={width} xScale={xScale} />
          </VStack>
        )}
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
              hasHeapLimit={hasHeapLimit}
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
    replicaGroups,
    groupIdById,
    colorMap,
    timelines,
    ddlEvents,
    heapLimitBytesBySize,
    hasHeapLimit,
    unhydratedObjectCounts,
    missingUsage,
    isDdlError,
    isStatusLoading,
    isStatusError,
    isHydrationError,
    chartData,
  } = useResourceUsageData({ cluster, timePeriodMinutes });
  // Measure only the width. visx's `ParentSize` collapses to 0px tall here,
  // since the charts set their own height instead of filling a parent.
  const { parentRef, width } = useParentSize({ debounceTime: 10 });
  const groupIds = React.useMemo(
    () => replicaGroups.map(({ id }) => id),
    [replicaGroups],
  );
  const { visibleLegendItems, toggleLegendItem } = useChartLegend({
    allLegendItems: groupIds,
  });
  const series = React.useMemo(
    () =>
      chartData.filter(({ id }) =>
        visibleLegendItems.has(groupIdById.get(id) ?? id),
      ),
    [chartData, groupIdById, visibleLegendItems],
  );
  const statusReplicaIds = React.useMemo(
    () => groupIds.filter((id) => visibleLegendItems.has(id)),
    [groupIds, visibleLegendItems],
  );
  const heapLimitBytes = sharedHeapLimitBytes(
    series.map(({ id }) => replicasById.get(id)?.size),
    heapLimitBytesBySize,
  );
  const hasUsage = Boolean(
    history.data?.graphData.some(({ data }) =>
      data.some(hasUtilizationReading),
    ),
  );

  const [isProcessesOpen, setIsProcessesOpen] = React.useState(false);
  const { data: allocationsBySize } = useReplicaSizeAllocations(
    [
      ...new Set(cluster.replicas.flatMap(({ size }) => (size ? [size] : []))),
    ].sort(),
  );
  // Only a size that runs more than one process has processes to compare.
  const processReplicas = React.useMemo(
    () =>
      cluster.replicas.flatMap(({ id, name, size }): ProcessReplica[] => {
        const allocation = size ? allocationsBySize?.get(size) : undefined;
        return size && allocation && allocation.processes > 1
          ? [
              {
                id,
                name,
                size,
                allocation,
                color: colorMap.get(id) ?? colors.lineGraph[0],
              },
            ]
          : [];
      }),
    [cluster.replicas, allocationsBySize, colorMap, colors.lineGraph],
  );

  const [isWorkersOpen, setIsWorkersOpen] = React.useState(false);
  // Skew needs more than one worker, so single-worker sizes get no button.
  // Memoized because the drawer's object table is memoized on it.
  const workerReplicas = React.useMemo(
    () =>
      cluster.replicas.flatMap(({ id, name, size }): WorkerReplica[] => {
        const allocation = size ? allocationsBySize?.get(size) : undefined;
        if (!size || !allocation) return [];
        return allocation.processes * allocation.workers > 1
          ? [
              {
                id,
                name,
                size,
                processes: allocation.processes,
                workersPerProcess: allocation.workers,
                cpuNanoCoresPerProcess: allocation.cpuNanoCores,
              },
            ]
          : [];
      }),
    [cluster.replicas, allocationsBySize],
  );

  let body: React.ReactNode;
  if (history.isError) {
    body = <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />;
  } else if (
    history.isLoading ||
    !history.data ||
    // Without readings, replica status picks which message explains why.
    (!hasUsage && isStatusLoading)
  ) {
    body = (
      <Flex height="md" alignItems="center" justifyContent="center">
        <Spinner data-testid="loading-spinner" />
      </Flex>
    );
  } else {
    const { startDate, endDate } = history.data;
    body = (
      <Box ref={parentRef} width="100%" minWidth={0}>
        {width > 0 && (
          <ResourceUsageContent
            width={width}
            startDate={startDate}
            endDate={endDate}
            series={series}
            hasUsage={hasUsage}
            statusReplicaIds={statusReplicaIds}
            replicasById={replicasById}
            groupIdById={groupIdById}
            colorMap={colorMap}
            timelines={timelines}
            ddlEvents={ddlEvents}
            heapLimitBytes={heapLimitBytes}
            hasHeapLimit={hasHeapLimit}
            unhydratedObjectCounts={unhydratedObjectCounts}
            missingUsage={missingUsage}
            isDdlError={isDdlError}
            isStatusLoading={isStatusLoading}
            isStatusError={isStatusError}
            isHydrationError={isHydrationError}
            cpuHeaderAction={
              (processReplicas.length > 0 || workerReplicas.length > 0) && (
                <HStack spacing={2}>
                  {processReplicas.length > 0 && (
                    <Button
                      size="xs"
                      variant="outline"
                      onClick={() => setIsProcessesOpen(true)}
                    >
                      Processes
                    </Button>
                  )}
                  {workerReplicas.length > 0 && (
                    <Button
                      size="xs"
                      variant="outline"
                      onClick={() => setIsWorkersOpen(true)}
                    >
                      Workers
                    </Button>
                  )}
                </HStack>
              )
            }
          />
        )}
      </Box>
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
              {replicaGroups.map((replica) => (
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
      <ProcessesDrawer
        isOpen={isProcessesOpen}
        onClose={() => setIsProcessesOpen(false)}
        replicas={processReplicas}
        timePeriodMinutes={timePeriodMinutes}
      />
      <WorkerCpuDrawer
        isOpen={isWorkersOpen}
        onClose={() => setIsWorkersOpen(false)}
        clusterId={cluster.id}
        clusterName={cluster.name}
        replicas={workerReplicas}
        unhydratedObjectCounts={unhydratedObjectCounts}
      />
    </Card>
  );
};
