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
import React from "react";

import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import Alert from "~/components/Alert";
import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";
import { formatBytesShort, formatDurationExact } from "~/utils/format";

import { percentOf } from "../ResourceUsage/processReadingsModel";
import { InfoTip } from "./hydrationComponents";
import { Hydration, ObjectHydrationSpan } from "./hydrationModel";
import { HYDRATION_HELP, HydrationRow } from "./hydrationView";
import { useHydrationDetail } from "./useClusterHydrationData";

// Enough for the objects that set an episode's length, without mounting a row
// per object on a cluster with hundreds.
const MAX_WATERFALL_ROWS = 30;
const ROW_HEIGHT_PX = 26;
const LABEL_WIDTH_PX = 200;
const DURATION_LABEL_WIDTH_PX = 64;

export interface HydrationDetailProps {
  clusterId: string;
  row: HydrationRow;
  replicaName: string;
  /** The replica's per-process heap limit. */
  heapLimitBytes: number | undefined;
  /** The replica's per-process CPU allocation. */
  cpuNanoCores: number | undefined;
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
      <TextLink
        href={
          docUrls["/docs/clusters/troubleshoot-clusters/hydration-failures/"]
        }
        isExternal
      >
        Troubleshoot hydration failures
      </TextLink>
    </VStack>
  );
};

const Stat = ({
  label,
  value,
  note,
  help,
}: {
  label: string;
  value: string;
  note: string;
  help: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack
      spacing={0.5}
      alignItems="start"
      p={3}
      borderRadius="md"
      background={colors.background.secondary}
    >
      <HStack spacing={1}>
        <Text textStyle="text-small-heavy" color={colors.foreground.secondary}>
          {label}
        </Text>
        <InfoTip subject={label} label={help} />
      </HStack>
      <Text textStyle="heading-xs">{value}</Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {note}
      </Text>
    </VStack>
  );
};

const formatShare = (percent: number | null) =>
  percent === null ? "-" : `${Math.round(percent)}%`;

const FinishedDetail = ({
  clusterId,
  hydration,
  heapLimitBytes,
  cpuNanoCores,
}: HydrationDetailProps & { hydration: Hydration }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const detail = useHydrationDetail({ clusterId, hydration });
  const heapPercent = percentOf(
    hydration.peakHeapBytes,
    heapLimitBytes ?? null,
  );
  const cpuPercent =
    detail.peakCpuNanoCores === undefined
      ? null
      : percentOf(detail.peakCpuNanoCores, cpuNanoCores ?? null);
  return (
    <VStack spacing={4} alignItems="stretch">
      <SimpleGrid columns={{ base: 2, md: 4 }} spacing={3}>
        <Stat
          label="Duration"
          help={HYDRATION_HELP.duration}
          value={formatDurationExact(hydration.durationMs)}
          note={`${hydration.objectCount} ${hydration.objectCount === 1 ? "object" : "objects"}`}
        />
        <Stat
          label="Peak memory"
          help={HYDRATION_HELP.peakMemory}
          value={formatShare(heapPercent)}
          note={
            hydration.peakHeapBytes === null
              ? "Not recorded"
              : `${formatBytesShort(BigInt(Math.round(hydration.peakHeapBytes)))}, since the process started`
          }
        />
        <Stat
          label="Peak disk or swap"
          help={HYDRATION_HELP.peakDisk}
          value={
            hydration.peakDiskBytes === null
              ? "-"
              : formatBytesShort(BigInt(Math.round(hydration.peakDiskBytes)))
          }
          note="Since the process started"
        />
        <Stat
          label="CPU"
          help={HYDRATION_HELP.cpu}
          value={formatShare(cpuPercent)}
          note={
            detail.peakCpuNanoCores === null
              ? "No sample during this hydration"
              : "Sampled once a minute"
          }
        />
      </SimpleGrid>
      {detail.isError ? (
        <Alert variant="error" message="Object timings couldn't be loaded." />
      ) : detail.isLoading || !detail.spans || !detail.summary ? (
        <Flex justifyContent="center" py={6}>
          <Spinner />
        </Flex>
      ) : detail.spans.length === 0 ? (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          No object timings for this hydration. Materialize keeps them for 30
          days.
        </Text>
      ) : (
        <>
          <ObjectsInsight
            spans={detail.spans}
            summary={detail.summary}
            durationMs={hydration.durationMs}
          />
          <ObjectWaterfall
            spans={detail.spans}
            durationMs={hydration.durationMs}
          />
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            From Materialize&apos;s object hydration history, kept 30 days.
          </Text>
        </>
      )}
    </VStack>
  );
};

type NamedSpan = ObjectHydrationSpan & { name: string };

const ObjectsInsight = ({
  spans,
  summary,
  durationMs,
}: {
  spans: NamedSpan[];
  summary: NonNullable<ReturnType<typeof useHydrationDetail>["summary"]>;
  durationMs: number;
}) => {
  const { slowest, waitedCount, maxQueuedMs } = summary;
  if (spans.length === 1 || !slowest?.workingMs) {
    return (
      <Text textStyle="text-base">
        {spans.length === 1
          ? `One object hydrated, in ${formatDurationExact(durationMs)}.`
          : `${spans.length} objects hydrated.`}
      </Text>
    );
  }
  const setsLength =
    slowest.hydratedMs !== undefined &&
    Math.abs(slowest.hydratedMs - durationMs) < 1000;
  const waiting =
    waitedCount > 0
      ? ` ${waitedCount} ${waitedCount === 1 ? "object" : "objects"} waited up to ${formatDurationExact(maxQueuedMs)} for a hydration slot.`
      : "";
  return (
    <Text textStyle="text-base">
      <Text as="span" fontWeight="600">
        {slowest.name}
      </Text>
      {` worked longest, for ${formatDurationExact(slowest.workingMs)}${
        setsLength ? ", until the hydration finished." : "."
      }${waiting}`}
    </Text>
  );
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
  const shown = spans.slice(0, MAX_WATERFALL_ROWS);
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
            Waiting for a hydration slot
          </Text>
          <InfoTip
            subject="hydration slots"
            label={HYDRATION_HELP.hydrationSlot}
          />
        </HStack>
      </HStack>
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
      {spans.length > shown.length && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Showing the first {shown.length} of {spans.length} objects to start.
        </Text>
      )}
    </VStack>
  );
};
