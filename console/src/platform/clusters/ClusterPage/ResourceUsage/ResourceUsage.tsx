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
  forwardRef,
  HStack,
  Spinner,
  StackProps,
  Text,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useParentSize } from "@visx/responsive";
import { scaleTime } from "@visx/scale";
import { useTooltip, useTooltipInPortal } from "@visx/tooltip";
import React from "react";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { SizeAllocation } from "~/api/materialize/cluster/replicaProcessMetrics";
import { hasUtilizationReading } from "~/api/materialize/cluster/replicaUtilizationBinning";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import {
  GraphTooltip,
  GraphTooltipBucketRange,
  TooltipColorSwatch,
} from "~/components/graphComponents";
import TextLink from "~/components/TextLink";
import { useAppConfig } from "~/config/useAppConfig";
import { useChartLegend } from "~/hooks/useChartLegend";
import docUrls from "~/mz-doc-urls.json";
import {
  DataPoint,
  ReplicaData,
} from "~/platform/clusters/ClusterOverview/types";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "~/platform/clusters/constants";
import { useReplicaSizeAllocations } from "~/platform/clusters/queries";
import { MaterializeTheme } from "~/theme";
import { capitalizeSentence, pluralize } from "~/util";
import { formatBytesShort, formatPercentage } from "~/utils/format";
import { buildXYGraphLayoutProps } from "~/utils/graph";

import { ProcessesDrawer, ProcessReplica } from "./ProcessesDrawer";
import {
  ChangesLane,
  ChangeTooltipContent,
  CpuChart,
  LegendSwatch,
  MemoryChart,
  ReplicaStatusTimeline,
  StatusTimelineRow,
  TimeAxis,
} from "./ResourceUsageCharts";
import {
  canSwap,
  ChangeTick,
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

interface ReadingRow {
  label: string;
  percent: number | null;
  /** The reading in bytes or cores, when its limit is known. */
  amount?: string;
  /** What the percentage is a share of, when the row names it. */
  limit?: string;
}

const formatBytes = (bytes: number) =>
  formatBytesShort(BigInt(Math.round(bytes)));

// NOTE: a size without a CPU limit reports `CpuLimit::MAX`, about u64::MAX
// nanocores (`pack_all_replica_size_updates` in mz-adapter), which isn't an
// allocation to count cores against.
const isCpuLimited = (allocation: SizeAllocation) =>
  allocation.cpuNanoCores <= Number.MAX_SAFE_INTEGER;

/**
 * A replica's readings at one time, in percent, 0 to 100. CPU, memory and disk
 * are shares of the replica's allocation of each, summed over its processes,
 * as the Clusters list shows them. Heap and the total are shares of one
 * process's heap limit, `heapLimitBytes`, as the memory bar draws them.
 */
const readingRows = (
  point: DataPoint,
  hasHeapLimit: boolean,
  heapLimitBytes: number | undefined,
  allocation: SizeAllocation | undefined,
): ReadingRow[] => {
  const ofHeapLimit = (
    label: string,
    percent: number | null,
    limit?: string,
  ): ReadingRow => ({
    label,
    percent,
    amount:
      percent === null || heapLimitBytes === undefined
        ? undefined
        : formatBytes((heapLimitBytes * percent) / 100),
    limit,
  });
  const ofAllocation = (
    label: string,
    percent: number | null,
    bytesPerProcess: number | null | undefined,
  ): ReadingRow => {
    const limitBytes =
      allocation && bytesPerProcess
        ? bytesPerProcess * allocation.processes
        : undefined;
    return {
      label,
      percent,
      amount:
        percent === null || limitBytes === undefined
          ? undefined
          : formatBytes((limitBytes * percent) / 100),
      limit: limitBytes === undefined ? undefined : formatBytes(limitBytes),
    };
  };
  const cores =
    point.cpuPercent !== null && allocation && isCpuLimited(allocation)
      ? (point.cpuPercent / 100) *
        ((allocation.cpuNanoCores * allocation.processes) / 1e9)
      : undefined;
  // NOTE: a 0% disk reading arrives as null, since
  // `toReplicaUtilizationGraphData` reads readings by truthiness. `disk_bytes`
  // counts swap (`process_metrics.disk_bytes` in mz-orchestrator-kubernetes),
  // so a sample that read swap read disk, and null there on a size with disk
  // is 0.
  const diskPercent =
    point.diskPercent ??
    (point.swapPercent !== null && allocation?.diskBytes ? 0 : null);
  const bar = memoryBar(point);
  return [
    {
      label: "CPU",
      percent: point.cpuPercent,
      amount:
        cores === undefined
          ? undefined
          : `${cores.toFixed(cores < 0.1 ? 2 : 1)} cores`,
    },
    point.swapPercent === null && hasHeapLimit
      ? ofHeapLimit("Heap", point.heapPercent)
      : ofAllocation("Memory", point.memoryPercent, allocation?.memoryBytes),
    ...(canSwap(point) || diskPercent !== null
      ? [ofAllocation("Disk", diskPercent, allocation?.diskBytes)]
      : []),
    ...(canSwap(point)
      ? [ofHeapLimit("Total", bar?.heap ?? null, "heap limit")]
      : []),
  ];
};

const formatReadingRow = ({ percent, amount, limit }: ReadingRow) => {
  if (percent === null) return "-";
  const share = formatPercentage(percent / 100, 1);
  return [
    ...(amount === undefined ? [] : [amount]),
    limit ? `${share} of ${limit}` : share,
  ].join(" · ");
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
  heapLimitBytesBySize,
  allocationsBySize,
}: {
  sections: ReturnType<typeof tooltipSectionsAt>;
  replicasById: Map<string, ColoredReplica>;
  hasHeapLimit: boolean;
  heapLimitBytesBySize: Map<string, number>;
  allocationsBySize: Map<string, SizeAllocation> | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const statusStyles = useStatusStyles();
  const bucket = sections.find((section) => section.point)?.point;

  return (
    <>
      {sections.map(({ id, point, state, isOom }) => {
        const replica = replicasById.get(id);
        // Without a heap limit, readings are shares of the memory allocation.
        const heapLimitBytes =
          hasHeapLimit && replica?.size
            ? heapLimitBytesBySize.get(replica.size)
            : undefined;
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
              readingRows(
                point,
                hasHeapLimit,
                heapLimitBytes,
                replica?.size
                  ? allocationsBySize?.get(replica.size)
                  : undefined,
              ).map((row) => (
                <HStack
                  key={row.label}
                  justifyContent="space-between"
                  width="100%"
                  paddingX="4"
                  spacing={6}
                >
                  <Text
                    textStyle="text-ui-reg"
                    color={colors.foreground.secondary}
                  >
                    {row.label}
                  </Text>
                  <Text textStyle="text-ui-reg">{formatReadingRow(row)}</Text>
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
  heapLimitBytesBySize: Map<string, number>;
  allocationsBySize: Map<string, SizeAllocation> | undefined;
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
  heapLimitBytesBySize,
  allocationsBySize,
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
  const changeTooltip = useTooltip<ChangeTick>();
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
      data: mergeBuckets(replica.data, groupMs, {
        startMs: startDate.getTime(),
        endMs: endDate.getTime(),
      }),
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
  // Anchored to the tick, since a focused tick has no pointer position.
  const onChangeActivate = (tick: ChangeTick, target: Element) => {
    const bounds = target.getBoundingClientRect();
    changeTooltip.showTooltip({
      tooltipData: tick,
      tooltipLeft: bounds.left + bounds.width / 2 - containerBounds.left,
      tooltipTop: bounds.top - containerBounds.top,
    });
  };
  const tooltipSections =
    tooltipData === undefined
      ? []
      : tooltipSectionsAt(chartSeries, timelines, groupIdById, tooltipData);
  const plotProps = {
    width,
    xScale,
    series: chartSeries,
    colorMap,
    changeGuideMs: changeTooltip.tooltipData?.atMs,
    hoverTimeMs: tooltipData,
    onPointerMove,
    onPointerLeave: hideTooltip,
  };

  return (
    <Box ref={containerRef} position="relative" width="100%">
      <VStack spacing={6} alignItems="stretch">
        {hasUsage ? (
          <>
            <CpuChart {...plotProps} headerAction={cpuHeaderAction} />
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
        <VStack spacing={1} alignItems="stretch">
          {isStatusError ? (
            <Alert variant="error" message="Replica status is unavailable." />
          ) : isStatusLoading ? (
            <Flex justifyContent="center">
              <Spinner size="sm" />
            </Flex>
          ) : (
            <ReplicaStatusTimeline
              width={width}
              xScale={xScale}
              rows={statusRows}
              hoverTimeMs={tooltipData}
              onPointerMove={onPointerMove}
              onPointerLeave={hideTooltip}
            />
          )}
          {!isStatusError && !isStatusLoading && isHydrationError && (
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Hydration history is unavailable.
            </Text>
          )}
        </VStack>
        <VStack spacing={1} alignItems="stretch">
          <ChangesLane
            width={width}
            xScale={xScale}
            events={ddlEvents}
            activeStartMs={changeTooltip.tooltipData?.startMs}
            onActivate={onChangeActivate}
            onDeactivate={changeTooltip.hideTooltip}
          />
          {isDdlError && (
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Object creation markers are unavailable.
            </Text>
          )}
        </VStack>
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
              heapLimitBytesBySize={heapLimitBytesBySize}
              allocationsBySize={allocationsBySize}
            />
          </GraphTooltip>
        )}
      {changeTooltip.tooltipData &&
        changeTooltip.tooltipLeft !== undefined &&
        changeTooltip.tooltipTop !== undefined && (
          <GraphTooltip
            component={TooltipInPortal}
            top={changeTooltip.tooltipTop}
            left={changeTooltip.tooltipLeft}
          >
            <ChangeTooltipContent tick={changeTooltip.tooltipData} />
          </GraphTooltip>
        )}
    </Box>
  );
};

// The legend key that stands for every past replica.
const PAST_REPLICAS_KEY = "past-replicas";
// The most past replicas their toggle's tooltip names before summarizing the
// rest.
const PAST_REPLICA_TOOLTIP_LIMIT = 10;

interface ReplicaToggleProps extends StackProps {
  isPressed: boolean;
  swatchColor: string | string[];
}

// Spreads its props and forwards its ref, so a `Tooltip` can wrap it.
const ReplicaToggle = forwardRef<ReplicaToggleProps, "button">(
  ({ isPressed, swatchColor, children, ...props }, ref) => (
    <HStack
      ref={ref}
      as="button"
      type="button"
      aria-pressed={isPressed}
      spacing={1}
      opacity={isPressed ? 1 : 0.5}
      whiteSpace="nowrap"
      {...props}
    >
      <LegendSwatch color={swatchColor} variant="square" />
      {children}
    </HStack>
  ),
);

const PastReplicaList = ({ replicas }: { replicas: ColoredReplica[] }) => {
  const rest = replicas.length - PAST_REPLICA_TOOLTIP_LIMIT;
  return (
    <VStack as="ul" align="start" spacing={1} listStyleType="none">
      {replicas.slice(0, PAST_REPLICA_TOOLTIP_LIMIT).map((replica) => (
        <HStack as="li" key={replica.id} spacing={2}>
          <TooltipColorSwatch color={replica.color} />
          <Text as="span" textStyle="text-small">
            {replica.name}
          </Text>
          {replica.size && (
            <Text as="span" textStyle="text-small" opacity={0.7}>
              {replica.size}
            </Text>
          )}
        </HStack>
      ))}
      {rest > 0 && (
        <Text as="li" textStyle="text-small">
          and {rest} more
        </Text>
      )}
    </VStack>
  );
};

/**
 * A toggle per current replica, and one for every past replica at once, which
 * names them in a tooltip.
 */
const ReplicaToggles = ({
  currentGroups,
  pastGroups,
  visibleKeys,
  onToggle,
}: {
  currentGroups: ColoredReplica[];
  pastGroups: ColoredReplica[];
  visibleKeys: Set<string>;
  onToggle: (key: string, event: React.MouseEvent) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Flex
      as="ul"
      aria-label="Replicas"
      flexWrap="wrap"
      justifyContent="flex-end"
      columnGap={4}
      rowGap={1}
      minWidth={0}
      listStyleType="none"
    >
      {currentGroups.map((replica) => (
        <Box as="li" key={replica.id}>
          <ReplicaToggle
            isPressed={visibleKeys.has(replica.id)}
            swatchColor={replica.color}
            onClick={(event) => onToggle(replica.id, event)}
          >
            <Text textStyle="text-small" color={colors.foreground.primary}>
              {replica.name}
            </Text>
            {replica.size && (
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                {replica.size}
              </Text>
            )}
          </ReplicaToggle>
        </Box>
      ))}
      {pastGroups.length > 0 && (
        <Box as="li">
          <Tooltip
            label={<PastReplicaList replicas={pastGroups} />}
            placement="bottom-end"
            px={3}
            py={2}
            rounded="md"
          >
            <ReplicaToggle
              isPressed={visibleKeys.has(PAST_REPLICAS_KEY)}
              swatchColor={pastGroups.map(({ color }) => color)}
              onClick={(event) => onToggle(PAST_REPLICAS_KEY, event)}
            >
              <Text textStyle="text-small" color={colors.foreground.primary}>
                +{pastGroups.length} past{" "}
                {pluralize(pastGroups.length, "replica", "replicas")}
              </Text>
            </ReplicaToggle>
          </Tooltip>
        </Box>
      )}
    </Flex>
  );
};

export interface ResourceUsageProps {
  cluster: Cluster;
  timePeriodMinutes: number;
  /**
   * A current replica to show alone, with the dropped replicas of its name.
   * Without one, every replica shows, with a toggle per current replica and
   * one for the past replicas.
   */
  selectedReplicaId?: string;
}

/**
 * CPU, memory and replica status for a cluster's replicas over a shared time
 * axis, with one crosshair and tooltip across all three.
 */
export const ResourceUsage = ({
  cluster,
  timePeriodMinutes,
  selectedReplicaId,
}: ResourceUsageProps) => {
  const { colors } = useTheme<MaterializeTheme>();
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
  const { currentGroups, pastGroups, legendKeys } = React.useMemo(() => {
    const current = replicaGroups.filter(({ isCurrent }) => isCurrent);
    const past = replicaGroups.filter(({ isCurrent }) => !isCurrent);
    return {
      currentGroups: current,
      pastGroups: past,
      legendKeys: [
        ...current.map(({ id }) => id),
        ...(past.length > 0 ? [PAST_REPLICAS_KEY] : []),
      ],
    };
  }, [replicaGroups]);
  const { visibleLegendItems, toggleLegendItem } = useChartLegend({
    allLegendItems: legendKeys,
  });
  // A current replica's id is its group's id.
  const visibleGroupIds = React.useMemo(
    () =>
      selectedReplicaId === undefined
        ? new Set(
            replicaGroups.flatMap(({ id, isCurrent }) =>
              visibleLegendItems.has(isCurrent ? id : PAST_REPLICAS_KEY)
                ? [id]
                : [],
            ),
          )
        : new Set([selectedReplicaId]),
    [replicaGroups, selectedReplicaId, visibleLegendItems],
  );
  const series = React.useMemo(
    () =>
      chartData.filter(({ id }) =>
        visibleGroupIds.has(groupIdById.get(id) ?? id),
      ),
    [chartData, groupIdById, visibleGroupIds],
  );
  const statusReplicaIds = React.useMemo(
    () => groupIds.filter((id) => visibleGroupIds.has(id)),
    [groupIds, visibleGroupIds],
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
  // Past replicas' sizes too, for their tooltip readings.
  const { data: allocationsBySize } = useReplicaSizeAllocations(
    [
      ...new Set(
        [...replicasById.values()].flatMap(({ size }) => (size ? [size] : [])),
      ),
    ].sort(),
  );
  // Only a size that runs more than one process has processes to compare.
  const processReplicas = cluster.replicas.flatMap(
    ({ id, name, size }): ProcessReplica[] => {
      if (selectedReplicaId !== undefined && id !== selectedReplicaId) {
        return [];
      }
      const allocation = size ? allocationsBySize?.get(size) : undefined;
      return size && allocation && allocation.processes > 1
        ? [{ id, name, size, allocation }]
        : [];
    },
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
            heapLimitBytesBySize={heapLimitBytesBySize}
            allocationsBySize={allocationsBySize}
            hasHeapLimit={hasHeapLimit}
            unhydratedObjectCounts={unhydratedObjectCounts}
            missingUsage={missingUsage}
            isDdlError={isDdlError}
            isStatusLoading={isStatusLoading}
            isStatusError={isStatusError}
            isHydrationError={isHydrationError}
            cpuHeaderAction={
              processReplicas.length > 0 && (
                <Button
                  size="xs"
                  variant="outline"
                  onClick={() => setIsProcessesOpen(true)}
                >
                  Processes
                </Button>
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
        <Flex
          justifyContent="space-between"
          alignItems="start"
          flexWrap="wrap"
          columnGap={4}
          rowGap={2}
        >
          <Text as="h3" textStyle="heading-sm" whiteSpace="nowrap">
            Resource Usage
          </Text>
          <Flex
            flex="1"
            minWidth={0}
            justifyContent="flex-end"
            alignItems="center"
            columnGap={4}
            rowGap={1}
          >
            {history.isRefreshing && (
              <Spinner size="sm" data-testid="refreshing-spinner" />
            )}
            {selectedReplicaId === undefined && (
              <ReplicaToggles
                currentGroups={currentGroups}
                pastGroups={pastGroups}
                visibleKeys={visibleLegendItems}
                onToggle={toggleLegendItem}
              />
            )}
          </Flex>
        </Flex>
        {body}
      </VStack>
      <ProcessesDrawer
        isOpen={isProcessesOpen}
        onClose={() => setIsProcessesOpen(false)}
        replicas={processReplicas}
        timePeriodMinutes={timePeriodMinutes}
      />
    </Card>
  );
};
