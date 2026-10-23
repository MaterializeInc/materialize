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
  Grid,
  HStack,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import { TooltipColorSwatch } from "~/components/graphComponents";
import { LoadingContainer } from "~/components/LoadingContainer";
import TextLink from "~/components/TextLink";
import TimePeriodSelect from "~/components/TimePeriodSelect";
import { useTimePeriodMinutes } from "~/hooks/useTimePeriodSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import docUrls from "~/mz-doc-urls.json";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { CLUSTERS_FETCH_ERROR_MESSAGE } from "~/platform/clusters/constants";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";
import { assert } from "~/util";
import { formatDurationExact } from "~/utils/format";

import { ReplicaSelect } from "../ReplicaSelect";
import { percentOf } from "../ResourceUsage/processReadingsModel";
import {
  assignReplicaColors,
  groupReplicasByName,
} from "../ResourceUsage/resourceUsageModel";
import { useStatusStyles } from "../ResourceUsage/resourceUsageStyles";
import { useSelectedReplica } from "../useSelectedReplica";
import { InfoTip } from "./hydrationComponents";
import { HydrationDetail } from "./HydrationDetail";
import {
  HydrationHistoryChart,
  HydrationHistoryLegend,
} from "./HydrationHistoryChart";
import {
  Hydration,
  HydrationReplica,
  typicalFullHydration,
} from "./hydrationModel";
import { HydrationTable } from "./HydrationTable";
import {
  HYDRATION_HELP,
  hydrationVerdict,
  ReplicaNow,
  summarizeReplicasNow,
  toHydrationRows,
} from "./hydrationView";
import {
  CreatedObject,
  useClusterHydrationData,
} from "./useClusterHydrationData";
import { useLiveObjectHydrations } from "./useLiveHydration";

// Status history, which unfinished hydrations come from, is kept 30 days.
const HYDRATION_TIME_PERIODS = {
  "1440": "Last 24 hours",
  "10080": "Last 7 days",
  "43200": "Last 30 days",
};
const MAX_LIVE_OBJECTS = 8;

const SectionCard = ({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Card
      p={5}
      width="100%"
      minWidth={0}
      borderRadius="md"
      borderWidth="1px"
      borderColor={colors.border.primary}
    >
      <VStack spacing={4} alignItems="stretch">
        <VStack spacing={0.5} alignItems="start">
          <Text as="h3" textStyle="heading-sm">
            {title}
          </Text>
          {subtitle && (
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              {subtitle}
            </Text>
          )}
        </VStack>
        {children}
      </VStack>
    </Card>
  );
};

/** Each replica keeps the color the Metrics tab gives it. */
const useReplicaColors = (clusterId: string, replicas: HydrationReplica[]) => {
  const { colors } = useTheme<MaterializeTheme>();
  return React.useMemo(() => {
    const withCluster = replicas.map((replica) => ({ ...replica, clusterId }));
    const groupIds = groupReplicasByName(
      withCluster,
      new Map(
        replicas.map(({ id, createdAt }) => [id, createdAt?.getTime() ?? 0]),
      ),
    );
    const groupColors = assignReplicaColors(
      withCluster.filter(({ id }) => groupIds.get(id) === id),
      colors.lineGraph,
    );
    return new Map(
      replicas.map(({ id }) => [
        id,
        groupColors.get(groupIds.get(id) ?? id) ?? colors.lineGraph[0],
      ]),
    );
  }, [clusterId, replicas, colors.lineGraph]);
};

const ClusterHydrationContent = ({ cluster }: { cluster: Cluster }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { selectedReplica, setSelectedReplicaName } =
    useSelectedReplica(cluster);
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-cluster-hydration-time-period",
    defaultValue: "10080",
    timePeriodOptions: HYDRATION_TIME_PERIODS,
  });
  const data = useClusterHydrationData({ cluster, timePeriodMinutes });
  const colorById = useReplicaColors(cluster.id, data.replicas);
  const [selectedRowId, setSelectedRowId] = React.useState<string>();

  // Narrowing keeps every replica of the selected name, such as the ones a
  // resize replaced.
  const replicas = selectedReplica
    ? data.replicas.filter(({ name }) => name === selectedReplica.name)
    : data.replicas;
  const replicaIds = new Set(replicas.map(({ id }) => id));
  const hydrations = data.hydrations.filter(({ replicaId }) =>
    replicaIds.has(replicaId),
  );
  const unfinished = data.unfinished.filter(({ replicaId }) =>
    replicaIds.has(replicaId),
  );
  const rows = toHydrationRows(hydrations, unfinished);
  const nowMs = Date.now();
  const isHistoryReady = !data.isHistoryLoading;
  const replicasNow = summarizeReplicasNow({
    replicas,
    progressByReplicaId: data.progressByReplicaId,
    bootTimes: data.bootTimes,
    hydrations,
    // Restarts are only unfinished once the history shows nothing finished
    // after them, so none count before it loads.
    unfinished: isHistoryReady ? unfinished : [],
    createdObjects: data.createdObjects,
  });
  const nameById = new Map(data.replicas.map(({ id, name }) => [id, name]));
  const selectedRow =
    rows.find(({ id }) => id === selectedRowId) ??
    rows.find((row) => row.type === "hydration" && row.kind === "full") ??
    rows[0];
  const currentSizes = new Set(
    replicasNow.map(({ replica: { size } }) => size),
  );
  const [currentSize] = currentSizes;
  const typical =
    currentSizes.size === 1
      ? typicalFullHydration(hydrations, replicas, currentSize)
      : undefined;
  const inProgress = replicasNow.flatMap(({ replica, state, startedAt }) =>
    state !== "hydrated" && startedAt
      ? [
          {
            replicaId: replica.id,
            startedAt,
            elapsedMs: nowMs - startedAt.getTime(),
          },
        ]
      : [],
  );

  return (
    <MainContentContainer mt="10">
      <VStack spacing="6" alignItems="stretch">
        <HStack justifyContent="space-between" flexWrap="wrap" spacing={4}>
          <ReplicaSelect
            replicas={cluster.replicas}
            value={selectedReplica?.name}
            onChange={setSelectedReplicaName}
            includeAll
          />
          <TimePeriodSelect
            timePeriodMinutes={timePeriodMinutes}
            setTimePeriodMinutes={setTimePeriodMinutes}
            options={HYDRATION_TIME_PERIODS}
          />
        </HStack>
        <>
          {!data.isProgressLoading &&
            isHistoryReady &&
            !data.isProgressError &&
            !data.isHistoryError && (
              <Verdict replicasNow={replicasNow} nowMs={nowMs} />
            )}
          <SectionCard title="Replicas Now" subtitle="Live from each replica">
            {data.isProgressError ? (
              <Alert
                variant="error"
                message="Live hydration progress couldn't be loaded."
              />
            ) : data.isProgressLoading ? (
              <Flex justifyContent="center" py={4}>
                <Spinner />
              </Flex>
            ) : (
              <ReplicasNow
                clusterName={cluster.name}
                replicasNow={replicasNow}
                colorById={colorById}
                nowMs={nowMs}
                isHistoryReady={isHistoryReady}
              />
            )}
          </SectionCard>
          {data.hasHistory === false ? (
            <Text textStyle="text-base" color={colors.foreground.secondary}>
              Hydration history needs Materialize v26.43 or later.
            </Text>
          ) : data.isHistoryError ? (
            <Alert
              variant="error"
              message="Hydration history couldn't be loaded."
            />
          ) : !isHistoryReady ? (
            <LoadingContainer />
          ) : (
            <>
              <Grid
                templateColumns={{ base: "minmax(0, 1fr)", xl: "2fr 1fr" }}
                gap={6}
              >
                <SectionCard
                  title="Hydration History"
                  subtitle="From Materialize's hydration history"
                >
                  <HydrationHistoryLegend
                    replicas={replicas}
                    colorById={colorById}
                    hasTypical={typical !== undefined}
                    hasInProgress={inProgress.length > 0}
                  />
                  {rows.length === 0 && inProgress.length === 0 ? (
                    <Text
                      textStyle="text-small"
                      color={colors.foreground.secondary}
                    >
                      No hydrations in this period.
                    </Text>
                  ) : (
                    <HydrationHistoryChart
                      rows={rows}
                      inProgress={inProgress}
                      replicaIds={replicas.map(({ id }) => id)}
                      colorById={colorById}
                      nameById={nameById}
                      startMs={data.startMs}
                      endMs={nowMs}
                      typicalMs={typical?.medianMs}
                      selectedId={selectedRow?.id}
                      onSelect={setSelectedRowId}
                    />
                  )}
                </SectionCard>
                <SectionCard
                  title="Before the Next Full Hydration"
                  subtitle="Based on past hydrations, not a forecast"
                >
                  <NextHydration
                    createdObjects={data.createdObjects}
                    isCreatedObjectsError={data.isCreatedObjectsError}
                    size={currentSizes.size === 1 ? currentSize : undefined}
                    typical={typical}
                    lastFull={hydrations.findLast(
                      ({ kind }) => kind === "full",
                    )}
                    heapLimitBytesById={data.heapLimitBytesById}
                  />
                </SectionCard>
              </Grid>
              {selectedRow && (
                <SectionCard
                  title={
                    selectedRow.type === "unfinished"
                      ? `Restart on ${nameById.get(selectedRow.replicaId) ?? selectedRow.replicaId}`
                      : `${selectedRow.kind === "full" ? "Full Hydration" : "New Objects"} on ${nameById.get(selectedRow.replicaId) ?? selectedRow.replicaId}`
                  }
                >
                  <HydrationDetail
                    clusterId={cluster.id}
                    row={selectedRow}
                    replicaName={
                      nameById.get(selectedRow.replicaId) ??
                      selectedRow.replicaId
                    }
                    heapLimitBytes={data.heapLimitBytesById.get(
                      selectedRow.replicaId,
                    )}
                    cpuNanoCores={data.cpuNanoCoresById.get(
                      selectedRow.replicaId,
                    )}
                  />
                </SectionCard>
              )}
              <SectionCard title="Hydrations">
                <HydrationTable
                  rows={rows}
                  colorById={colorById}
                  nameById={nameById}
                  heapLimitById={data.heapLimitBytesById}
                  selectedId={selectedRow?.id}
                  onSelect={setSelectedRowId}
                />
              </SectionCard>
            </>
          )}
        </>
      </VStack>
    </MainContentContainer>
  );
};

const Verdict = ({
  replicasNow,
  nowMs,
}: {
  replicasNow: ReplicaNow[];
  nowMs: number;
}) => {
  const verdict = hydrationVerdict(replicasNow, nowMs);
  return (
    <Alert
      variant={verdict.variant}
      label={verdict.label}
      message={
        verdict.isFailing ? (
          <VStack spacing={1} alignItems="start">
            <Text>{verdict.message}</Text>
            <TextLink
              href={
                docUrls[
                  "/docs/clusters/troubleshoot-clusters/hydration-failures/"
                ]
              }
              isExternal
            >
              Troubleshoot hydration failures
            </TextLink>
          </VStack>
        ) : (
          verdict.message
        )
      }
    />
  );
};

const ReplicasNow = ({
  clusterName,
  replicasNow,
  colorById,
  nowMs,
  isHistoryReady,
}: {
  clusterName: string;
  replicasNow: ReplicaNow[];
  colorById: Map<string, string>;
  nowMs: number;
  /** Until the history loads, nothing is said about past hydrations. */
  isHistoryReady: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const statusStyles = useStatusStyles();
  if (replicasNow.length === 0) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        This cluster has no replicas.
      </Text>
    );
  }
  const stateStyles = {
    hydrated: { label: "Hydrated", color: statusStyles.running.color },
    hydrating: { label: "Hydrating", color: statusStyles.hydrating.color },
    restarting: { label: "Restarting", color: colors.accent.red },
  };
  return (
    <VStack as="ul" spacing={4} alignItems="stretch" listStyleType="none">
      {replicasNow.map((replicaNow) => {
        const { replica, state, progress, startedAt, lastFull, restarts } =
          replicaNow;
        const { label, color } = stateStyles[state];
        const share =
          progress.objectCount === 0
            ? 1
            : progress.hydratedCount / progress.objectCount;
        const lastFullDuration =
          isHistoryReady && lastFull
            ? formatDurationExact(lastFull.durationMs)
            : undefined;
        const detail =
          state === "restarting"
            ? `Restarted ${restarts.length === 1 ? "once" : `${restarts.length} times`} without finishing`
            : state === "hydrating" && startedAt
              ? `${formatDurationExact(nowMs - startedAt.getTime())} so far${
                  lastFullDuration
                    ? ` · last full hydration took ${lastFullDuration}`
                    : ""
                }`
              : !isHistoryReady
                ? ""
                : lastFullDuration
                  ? `Last full hydration took ${lastFullDuration}`
                  : "No full hydration in this period";
        return (
          <VStack as="li" key={replica.id} spacing={2} alignItems="stretch">
            <Flex
              justifyContent="space-between"
              flexWrap="wrap"
              columnGap={4}
              rowGap={1}
            >
              <HStack spacing={2}>
                <TooltipColorSwatch
                  color={
                    colorById.get(replica.id) ?? colors.foreground.tertiary
                  }
                />
                <Text textStyle="text-ui-med">{replica.name}</Text>
                {replica.size && (
                  <Text
                    textStyle="text-small"
                    color={colors.foreground.secondary}
                  >
                    {replica.size}
                  </Text>
                )}
                <HStack spacing={1.5}>
                  <Box
                    width="2"
                    height="2"
                    borderRadius="full"
                    background={color}
                  />
                  <Text textStyle="text-small">{label}</Text>
                  {state === "restarting" && (
                    <InfoTip
                      subject="restarting"
                      label={HYDRATION_HELP.restarting}
                    />
                  )}
                </HStack>
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  {progress.hydratedCount} of {progress.objectCount} objects
                </Text>
              </HStack>
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                {detail}
              </Text>
            </Flex>
            <Box
              height="2"
              borderRadius="full"
              background={colors.background.tertiary}
              overflow="hidden"
              role="progressbar"
              aria-label={`${replica.name} objects hydrated`}
              aria-valuemin={0}
              aria-valuemax={progress.objectCount}
              aria-valuenow={progress.hydratedCount}
            >
              <Box height="100%" width={`${share * 100}%`} background={color} />
            </Box>
            {state !== "hydrated" && (
              <LiveObjects
                clusterName={clusterName}
                replicaId={replica.id}
                replicaName={replica.name}
                nowMs={nowMs}
              />
            )}
          </VStack>
        );
      })}
    </VStack>
  );
};

/** The objects a hydrating replica is working on, read from the replica. */
const LiveObjects = ({
  clusterName,
  replicaId,
  replicaName,
  nowMs,
}: {
  clusterName: string;
  replicaId: string;
  replicaName: string;
  nowMs: number;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { objects, snapshotComplete, isError } = useLiveObjectHydrations({
    clusterName,
    replicaId,
    replicaName,
  });
  if (isError) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        Couldn&apos;t read which objects {replicaName} is working on.
      </Text>
    );
  }
  if (!snapshotComplete) return null;
  const working = objects.filter(({ state }) => state === "working");
  const queuedCount = objects.filter(({ state }) => state === "queued").length;
  if (working.length === 0 && queuedCount === 0) return null;
  return (
    <Flex
      as="ul"
      aria-label={`Objects ${replicaName} is hydrating`}
      listStyleType="none"
      flexWrap="wrap"
      gap={2}
    >
      {working.slice(0, MAX_LIVE_OBJECTS).map((object) => (
        <Text
          as="li"
          key={object.globalId}
          textStyle="text-small"
          fontFamily="mono"
          px={2}
          py={0.5}
          borderRadius="md"
          borderWidth="1px"
          borderColor={colors.border.primary}
        >
          {object.name} · working{" "}
          {object.startedAt
            ? formatDurationExact(nowMs - object.startedAt.getTime())
            : ""}
        </Text>
      ))}
      {working.length > MAX_LIVE_OBJECTS && (
        <Text
          as="li"
          textStyle="text-small"
          color={colors.foreground.secondary}
        >
          and {working.length - MAX_LIVE_OBJECTS} more working
        </Text>
      )}
      {queuedCount > 0 && (
        <HStack as="li" spacing={1}>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            {queuedCount} waiting for a hydration slot
          </Text>
          <InfoTip
            subject="hydration slots"
            label={HYDRATION_HELP.hydrationSlot}
          />
        </HStack>
      )}
    </Flex>
  );
};

const Fact = ({
  label,
  value,
  note,
  help,
}: {
  label: string;
  value: string;
  note?: string;
  help: string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const labelId = React.useId();
  return (
    <VStack
      spacing={0}
      alignItems="start"
      role="group"
      aria-labelledby={labelId}
    >
      <HStack spacing={1}>
        <Text
          id={labelId}
          textStyle="text-small"
          color={colors.foreground.secondary}
        >
          {label}
        </Text>
        <InfoTip subject={label} label={help} />
      </HStack>
      <Text textStyle="text-ui-med">{value}</Text>
      {note && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {note}
        </Text>
      )}
    </VStack>
  );
};

/** What the last full hydrations say about the next one, without a forecast. */
const NextHydration = ({
  size,
  typical,
  lastFull,
  heapLimitBytesById,
  createdObjects,
  isCreatedObjectsError,
}: {
  size: string | undefined;
  typical: ReturnType<typeof typicalFullHydration>;
  lastFull: Hydration | undefined;
  heapLimitBytesById: Map<string, number>;
  createdObjects: CreatedObject[] | undefined;
  isCreatedObjectsError: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  if (!lastFull) {
    return (
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        No full hydration finished in this period.
      </Text>
    );
  }
  const createdSince = createdObjects?.filter(
    ({ occurredAt }) => occurredAt.getTime() > lastFull.finishedAt.getTime(),
  );
  const peakPercent = percentOf(
    lastFull.peakHeapBytes,
    heapLimitBytesById.get(lastFull.replicaId) ?? null,
  );
  return (
    <VStack spacing={4} alignItems="start">
      {typical && (
        <Fact
          label={
            size
              ? `Typical full hydration on ${size}`
              : "Typical full hydration"
          }
          help={HYDRATION_HELP.typical}
          value={formatDurationExact(typical.medianMs)}
          note={
            typical.count === 1
              ? "From the one full hydration in this period"
              : `Median of the last ${typical.count}, from ${formatDurationExact(typical.minMs)} to ${formatDurationExact(typical.maxMs)}`
          }
        />
      )}
      <Fact
        label="Created since the last full hydration"
        help={HYDRATION_HELP.createdSince}
        value={
          createdSince === undefined
            ? "-"
            : createdSince.length === 0
              ? "No objects"
              : `${createdSince.length} ${createdSince.length === 1 ? "object" : "objects"}`
        }
        note={
          isCreatedObjectsError
            ? "Creation times couldn't be loaded"
            : createdSince === undefined || createdSince.length === 0
              ? undefined
              : createdSince
                  .flatMap(({ name }) => (name ? [name] : []))
                  .slice(0, 3)
                  .join(", ")
        }
      />
      {peakPercent !== null && (
        <Fact
          label="Memory at the last full hydration"
          help={HYDRATION_HELP.lastFullMemory}
          value={`${Math.round(peakPercent)}% of the heap limit`}
          note={`${Math.max(0, Math.round(100 - peakPercent))}% headroom at its peak`}
        />
      )}
    </VStack>
  );
};

/** The Hydration tab of the redesigned cluster page. */
export const ClusterHydration = () => {
  const { clusterId } = useParams<ClusterParams>();
  assert(clusterId);
  const { getClusterById, isError } = useAllClusters();
  const cluster = getClusterById(clusterId);

  // `ClusterRoutes` redirects away from a cluster that doesn't exist, so a
  // missing one is still loading, unless the subscribe failed first.
  if (!cluster) {
    return isError ? (
      <ErrorBox message={CLUSTERS_FETCH_ERROR_MESSAGE} />
    ) : (
      <LoadingContainer />
    );
  }
  return <ClusterHydrationContent cluster={cluster} />;
};
