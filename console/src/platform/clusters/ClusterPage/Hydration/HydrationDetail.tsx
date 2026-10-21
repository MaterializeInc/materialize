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
  Flex,
  HStack,
  SimpleGrid,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useParentSize } from "@visx/responsive";
import { scaleLinear } from "@visx/scale";
import { sort } from "d3";
import React from "react";

import { SizeAllocation } from "~/api/materialize/cluster/replicaProcessMetrics";
import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import Alert from "~/components/Alert";
import { PaginationControls } from "~/components/Table/TablePagination";
import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";
import { formatDurationExact, formatPercentage } from "~/utils/format";

import { percentOf } from "../ResourceUsage/processReadingsModel";
import { InfoTip } from "./hydrationComponents";
import { Hydration, ObjectHydrationSpan } from "./hydrationModel";
import { formatPeakBytes, HYDRATION_HELP, HydrationRow } from "./hydrationView";
import { useHydrationDetail } from "./useClusterHydrationData";

// A page of objects, enough to see the ones that set an episode's length
// without mounting a row per object on a cluster with hundreds.
const WATERFALL_PAGE_SIZE = 30;
const ROW_HEIGHT_PX = 26;
// The waterfall's legend row and pagination around its rows.
const WATERFALL_CHROME_HEIGHT_PX = 72;
const LABEL_WIDTH_PX = 200;
const DURATION_LABEL_WIDTH_PX = 64;
// A peak this close to the limit is close to running out of memory.
const NEAR_LIMIT_PERCENT = 90;

export interface HydrationDetailProps {
  clusterId: string;
  row: HydrationRow;
  replicaName: string;
  /** The replica's per-process heap limit. */
  heapLimitBytes: number | undefined;
  /** The replica's per-process CPU, memory and disk. */
  allocation: SizeAllocation | undefined;
}

/** What one hydration took, or why it never finished. */
export const HydrationDetail = (props: HydrationDetailProps) =>
  props.row.type === "unfinished" ? (
    <UnfinishedDetail row={props.row} replicaName={props.replicaName} />
  ) : (
    <FinishedDetail {...props} hydration={props.row} />
  );

const UnfinishedDetail = ({
  row,
  replicaName,
}: {
  row: Extract<HydrationRow, { type: "unfinished" }>;
  replicaName: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack spacing={2} alignItems="start">
      <Text textStyle="text-base">
        {row.reason === OOM_REASON
          ? `${replicaName} ran out of memory and restarted before it finished hydrating.`
          : `${replicaName} went offline before it finished hydrating.`}
      </Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        Materialize records object timings only for hydrations that finish.
      </Text>
      <Text textStyle="text-small">
        See our{" "}
        <TextLink
          href={
            docUrls["/docs/clusters/troubleshoot-clusters/hydration-failures/"]
          }
          isExternal
        >
          docs on troubleshooting hydration failures
        </TextLink>
        .
      </Text>
    </VStack>
  );
};

const Stat = ({
  label,
  value,
  note,
  help,
  isNearLimit = false,
}: {
  label: string;
  value: string;
  note?: string;
  help: string;
  isNearLimit?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const labelId = React.useId();
  return (
    <VStack
      spacing={0.5}
      alignItems="start"
      p={3}
      borderRadius="md"
      borderWidth="1px"
      borderColor={isNearLimit ? colors.accent.red : colors.border.primary}
      role="group"
      aria-labelledby={labelId}
    >
      <HStack spacing={1}>
        <Text
          id={labelId}
          textStyle="text-small-heavy"
          color={colors.foreground.secondary}
        >
          {label}
        </Text>
        <InfoTip subject={label} label={help} />
      </HStack>
      <Text
        textStyle="heading-xs"
        color={isNearLimit ? colors.accent.red : undefined}
      >
        {value}
      </Text>
      {note && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {note}
        </Text>
      )}
    </VStack>
  );
};

const formatShare = (percent: number | null) =>
  percent === null ? "-" : formatPercentage(percent / 100, 0);

/** A peak as a share of its limit, with the bytes it reached. */
const peakStat = (peakBytes: number | null, limitBytes: number | undefined) => {
  const percent = percentOf(peakBytes, limitBytes ?? null);
  return {
    value:
      percent === null
        ? (formatPeakBytes(peakBytes) ?? "-")
        : formatShare(percent),
    note: percent === null ? undefined : formatPeakBytes(peakBytes),
    percent,
  };
};

const FinishedDetail = ({
  clusterId,
  hydration,
  heapLimitBytes,
  allocation,
}: HydrationDetailProps & { hydration: Hydration }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const detail = useHydrationDetail({ clusterId, hydration });
  const memory = peakStat(hydration.peakMemoryBytes, allocation?.memoryBytes);
  const disk = peakStat(
    hydration.peakDiskBytes,
    allocation?.diskBytes ?? undefined,
  );
  const heap = peakStat(hydration.peakHeapBytes, heapLimitBytes);
  const cpuPercent =
    detail.peakCpuNanoCores === undefined
      ? null
      : percentOf(detail.peakCpuNanoCores, allocation?.cpuNanoCores ?? null);
  return (
    <VStack spacing={4} alignItems="stretch">
      <SimpleGrid columns={{ base: 2, md: 3, xl: 5 }} spacing={3}>
        <Stat
          label="Duration"
          help={HYDRATION_HELP.duration}
          value={formatDurationExact(hydration.durationMs)}
          note={`${hydration.objectCount} ${pluralize(hydration.objectCount, "object", "objects")}`}
        />
        <Stat
          label="Peak memory"
          help={HYDRATION_HELP.peakMemory}
          value={memory.value}
          note={memory.note}
        />
        <Stat
          label="Peak disk"
          help={HYDRATION_HELP.peakDisk}
          value={disk.value}
          note={disk.note}
        />
        <Stat
          label="Peak heap"
          help={HYDRATION_HELP.peakHeap}
          value={heap.value}
          note={heap.note}
          isNearLimit={
            heap.percent !== null && heap.percent >= NEAR_LIMIT_PERCENT
          }
        />
        <Stat
          label="Peak CPU"
          help={HYDRATION_HELP.cpu}
          value={detail.isCpuLoading ? "-" : formatShare(cpuPercent)}
          note={
            detail.isCpuError
              ? "Couldn't be loaded"
              : detail.peakCpuNanoCores === null
                ? "No sample during this hydration"
                : undefined
          }
        />
      </SimpleGrid>
      {detail.isSpansError ? (
        <Alert variant="error" message="Object timings couldn't be loaded." />
      ) : detail.isSpansLoading || !detail.spans ? (
        // Holds about the waterfall's height, so picking another hydration
        // doesn't move the table below.
        <Flex
          justifyContent="center"
          alignItems="center"
          minHeight={`${
            Math.min(Math.max(hydration.objectCount, 1), WATERFALL_PAGE_SIZE) *
              ROW_HEIGHT_PX +
            WATERFALL_CHROME_HEIGHT_PX
          }px`}
        >
          <Spinner />
        </Flex>
      ) : detail.spans.length === 0 ? (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          No object timings for this hydration. Materialize keeps them for 30
          days.
        </Text>
      ) : (
        <ObjectWaterfall
          // Its page and order belong to one hydration.
          key={hydration.id}
          spans={detail.spans}
          durationMs={hydration.durationMs}
        />
      )}
    </VStack>
  );
};

type NamedSpan = ObjectHydrationSpan & { name: string };

type WaterfallOrder = "started" | "slowest";

const WATERFALL_ORDERS: Record<WaterfallOrder, string> = {
  started: "By start",
  slowest: "Slowest first",
};

const ObjectWaterfall = ({
  spans,
  durationMs,
}: {
  spans: NamedSpan[];
  durationMs: number;
}) => {
  const { colors, fonts } = useTheme<MaterializeTheme>();
  const { parentRef, width } = useParentSize({ debounceTime: 10 });
  const [order, setOrder] = React.useState<WaterfallOrder>("started");
  const [pageIndex, setPageIndex] = React.useState(0);
  const ordered = React.useMemo(
    () =>
      order === "slowest"
        ? sort(spans, (a, b) => (b.workingMs ?? 0) - (a.workingMs ?? 0))
        : spans,
    [spans, order],
  );
  const pageCount = Math.ceil(ordered.length / WATERFALL_PAGE_SIZE);
  const shown = ordered.slice(
    pageIndex * WATERFALL_PAGE_SIZE,
    (pageIndex + 1) * WATERFALL_PAGE_SIZE,
  );
  const height = shown.length * ROW_HEIGHT_PX;
  const xScale = React.useMemo(
    () =>
      scaleLinear<number>({
        domain: [0, Math.max(durationMs, 1)],
        range: [
          LABEL_WIDTH_PX,
          Math.max(LABEL_WIDTH_PX + 1, width - DURATION_LABEL_WIDTH_PX),
        ],
        clamp: true,
      }),
    [durationMs, width],
  );
  return (
    <VStack spacing={2} alignItems="stretch">
      <Flex justifyContent="space-between" flexWrap="wrap" gap={2}>
        <HStack spacing={4} as="ul" aria-label="Legend" listStyleType="none">
          <HStack as="li" spacing={1.5}>
            <Box
              width="4"
              height="2"
              borderRadius="sm"
              background={colors.accent.brightPurple}
            />
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Working
            </Text>
          </HStack>
          <HStack as="li" spacing={1.5}>
            <Box
              width="4"
              height="2"
              borderRadius="sm"
              background={colors.background.accent}
              borderWidth="1px"
              borderColor={colors.accent.brightPurple}
            />
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Waiting
            </Text>
            <InfoTip subject="waiting" label={HYDRATION_HELP.hydrationSlot} />
          </HStack>
        </HStack>
        <HStack
          as="ul"
          aria-label="Order objects"
          spacing={4}
          listStyleType="none"
        >
          {(Object.keys(WATERFALL_ORDERS) as WaterfallOrder[]).map((each) => (
            <li key={each}>
              <Text
                as="button"
                type="button"
                textStyle="text-small"
                aria-pressed={order === each}
                opacity={order === each ? 1 : 0.5}
                onClick={() => {
                  setOrder(each);
                  setPageIndex(0);
                }}
              >
                {WATERFALL_ORDERS[each]}
              </Text>
            </li>
          ))}
        </HStack>
      </Flex>
      <Box ref={parentRef} width="100%" minWidth={0}>
        {width > 0 && (
          <svg
            width={width}
            height={height}
            role="img"
            aria-label="Object hydration times"
          >
            {shown.map((span, rowIndex) => {
              const y = rowIndex * ROW_HEIGHT_PX;
              const barY = y + 6;
              const workingEnd = span.hydratedMs ?? durationMs;
              return (
                <g key={span.globalId}>
                  <text
                    x={0}
                    y={y + 17}
                    fontFamily={fonts.mono}
                    fontSize={12}
                    fill={colors.foreground.primary}
                  >
                    {span.name.length > 26
                      ? `${span.name.slice(0, 25)}…`
                      : span.name}
                    <title>{span.name}</title>
                  </text>
                  {span.queuedMs > 0 && (
                    <rect
                      x={xScale(span.installedMs)}
                      y={barY}
                      width={Math.max(
                        1,
                        xScale(span.startedMs) - xScale(span.installedMs),
                      )}
                      height={14}
                      rx={3}
                      fill={colors.background.accent}
                      stroke={colors.accent.brightPurple}
                      strokeOpacity={0.5}
                    />
                  )}
                  <rect
                    x={xScale(span.startedMs)}
                    y={barY}
                    width={Math.max(
                      2,
                      xScale(workingEnd) - xScale(span.startedMs),
                    )}
                    height={14}
                    rx={3}
                    fill={colors.accent.brightPurple}
                  />
                  <text
                    x={xScale(workingEnd) + 6}
                    y={y + 17}
                    fontSize={12}
                    fill={colors.foreground.secondary}
                  >
                    {span.workingMs === undefined
                      ? "-"
                      : formatDurationExact(span.workingMs)}
                  </text>
                </g>
              );
            })}
          </svg>
        )}
      </Box>
      <PaginationControls
        pageIndex={pageIndex}
        pageSize={WATERFALL_PAGE_SIZE}
        pageCount={pageCount}
        totalItems={ordered.length}
        itemLabel="objects"
        onPrevious={() => setPageIndex((index) => Math.max(0, index - 1))}
        onNext={() =>
          setPageIndex((index) => Math.min(pageCount - 1, index + 1))
        }
      />
    </VStack>
  );
};
