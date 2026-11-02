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
  Text,
  useInterval,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { greatest } from "d3";
import { millisecondsInSecond } from "date-fns/constants";
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
import { REPLICA_HYDRATION_HISTORY_MIN_VERSION } from "~/platform/clusters/queries";
import { useAllClusters } from "~/store/allClusters";
import { MaterializeTheme } from "~/theme";
import { assert, pluralize } from "~/util";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatDurationExact } from "~/utils/format";

import { replicaStatus } from "../Configuration/clusterConfigurationModel";
import { ReplicaSelect } from "../ReplicaSelect";
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
import { HydrationReplica } from "./hydrationModel";
import { HydrationTable } from "./HydrationTable";
import {
  HYDRATION_HELP,
  HydrationVerdict,
  hydrationVerdict,
  ReplicaNow,
  ROW_KIND_NAMES,
  rowKind,
  rowTime,
  summarizeReplicasNow,
  toHydrationRows,
} from "./hydrationView";
import { useClusterHydrationData } from "./useClusterHydrationData";
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

// "26.43.0" reads "v26.43".
const HISTORY_MIN_VERSION_TEXT = `v${REPLICA_HYDRATION_HISTORY_MIN_VERSION.split(".").slice(0, 2).join(".")}`;

const ClusterHydrationContent = ({ cluster }: { cluster: Cluster }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { selectedReplicaName, setSelectedReplicaName } =
    useSelectedReplica(cluster);
  const [timePeriodMinutes, setTimePeriodMinutes] = useTimePeriodMinutes({
    localStorageKey: "mz-cluster-hydration-time-period",
    defaultValue: "10080",
    timePeriodOptions: HYDRATION_TIME_PERIODS,
  });
  const data = useClusterHydrationData({ cluster, timePeriodMinutes });
  const colorById = useReplicaColors(cluster.id, data.replicas);
  const [selectedRowId, setSelectedRowId] = React.useState<string>();

  // A retired replica can be selected while its history is in the window.
  // Narrowing keeps every replica of the selected name, such as the ones a
  // resize replaced.
  const selectedName = data.replicas.some(
    ({ name }) => name === selectedReplicaName,
  )
    ? selectedReplicaName
    : undefined;
  const replicas = React.useMemo(
    () =>
      selectedName
        ? data.replicas.filter(({ name }) => name === selectedName)
        : data.replicas,
    [data.replicas, selectedName],
  );
  const currentNames = new Set(cluster.replicas.map(({ name }) => name));
  const retired = data.replicas.filter(
    ({ isCurrent, name }) => !isCurrent && !currentNames.has(name),
  );
  const replicaOptions = [
    ...cluster.replicas,
    // The newest retired replica of each name.
    ...retired
      .filter(
        ({ name }, index) =>
          retired.findIndex((other) => other.name === name) === index,
      )
      .map(({ id, name, size }) => ({
        id,
        name,
        size: size ?? null,
        isRetired: true,
      })),
  ];
  // Held across renders, which come every second while a replica hydrates, so
  // the table's and chart's memos keyed on them hold too.
  const { replicaIds, hydrations, unfinished, rows } = React.useMemo(() => {
    const ids = new Set(replicas.map(({ id }) => id));
    const shownHydrations = data.hydrations.filter(({ replicaId }) =>
      ids.has(replicaId),
    );
    const shownUnfinished = data.unfinished.filter(({ replicaId }) =>
      ids.has(replicaId),
    );
    return {
      replicaIds: replicas.map(({ id }) => id),
      hydrations: shownHydrations,
      unfinished: shownUnfinished,
      rows: toHydrationRows(shownHydrations, shownUnfinished),
    };
  }, [replicas, data.hydrations, data.unfinished]);
  const nowMs = Date.now();
  // Only to rerender, so elapsed times move while a replica hydrates.
  const [, setTickMs] = React.useState(0);
  const isHistoryReady = !data.isHistoryLoading;
  const replicasNow = summarizeReplicasNow({
    replicas,
    statusById: new Map(
      cluster.replicas.map(({ id, statuses }) => [id, replicaStatus(statuses)]),
    ),
    clusterObjectCount: data.objectCount,
    progressByReplicaId: data.progressByReplicaId,
    bootTimes: data.bootTimes,
    hydrations,
    // Restarts are only unfinished once the history shows nothing finished
    // after them, so none count before it loads.
    unfinished: isHistoryReady ? unfinished : [],
    createdObjects: data.createdObjects,
    nowMs,
  });
  const notHydrated = replicasNow.filter(({ state }) => state !== "hydrated");
  // A retired replica's view is its history alone, so it gets no alert about
  // the cluster's current replicas.
  const isRetiredSelected =
    selectedName !== undefined && !currentNames.has(selectedName);
  const verdict = isRetiredSelected
    ? undefined
    : hydrationVerdict(replicasNow, nowMs);
  const nameById = new Map(data.replicas.map(({ id, name }) => [id, name]));
  const lastFinished = greatest(hydrations, ({ finishedAt }) =>
    finishedAt.getTime(),
  );
  const explicitRow = rows.find(({ id }) => id === selectedRowId);
  const selectedRow =
    explicitRow ?? rows.find(({ id }) => id === lastFinished?.id) ?? rows[0];
  const isLastFinishedShown =
    !explicitRow && lastFinished !== undefined && selectedRow !== undefined;
  // A replica's boot comes from its status, so its start waits for it too.
  const inProgress = replicasNow.flatMap(({ replica, isWorking, startedAt }) =>
    isWorking && startedAt && !data.isProgressLoading
      ? [
          {
            replicaId: replica.id,
            startedAt,
            elapsedMs: nowMs - startedAt.getTime(),
          },
        ]
      : [],
  );
  const nameOf = (replicaId: string) => nameById.get(replicaId) ?? replicaId;
  useInterval(
    () => setTickMs(Date.now()),
    inProgress.length > 0 ? millisecondsInSecond : null,
  );

  return (
    <MainContentContainer mt="10">
      <VStack spacing="6" alignItems="stretch">
        <HStack justifyContent="space-between" flexWrap="wrap" spacing={4}>
          <ReplicaSelect
            replicas={replicaOptions}
            value={selectedName}
            onChange={setSelectedReplicaName}
            includeAll
          />
          <TimePeriodSelect
            timePeriodMinutes={timePeriodMinutes}
            setTimePeriodMinutes={setTimePeriodMinutes}
            options={HYDRATION_TIME_PERIODS}
          />
        </HStack>
        {data.objectCount === 0 && cluster.replicas.length > 0 && (
          <Text textStyle="text-base" color={colors.foreground.secondary}>
            This cluster has no indexes or materialized views, so it has nothing
            to hydrate.
          </Text>
        )}
        {verdict &&
          !data.isProgressLoading &&
          isHistoryReady &&
          !data.isProgressError &&
          !data.isHistoryError && <Verdict verdict={verdict} />}
        {data.isProgressError ? (
          <SectionCard title="Hydration in Progress">
            <Alert
              variant="error"
              message="Live hydration progress couldn't be loaded."
            />
          </SectionCard>
        ) : (
          !data.isProgressLoading &&
          notHydrated.length > 0 && (
            <SectionCard title="Hydration in Progress">
              <ReplicasNow
                clusterName={cluster.name}
                replicasNow={notHydrated}
                colorById={colorById}
                nowMs={nowMs}
                isHistoryReady={isHistoryReady}
              />
            </SectionCard>
          )
        )}
        {data.hasHistory === false ? (
          <Text textStyle="text-base" color={colors.foreground.secondary}>
            Hydration history needs Materialize {HISTORY_MIN_VERSION_TEXT} or
            later.
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
            <SectionCard title="Hydration History">
              <HydrationHistoryLegend
                replicas={replicas}
                colorById={colorById}
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
                  replicaIds={replicaIds}
                  colorById={colorById}
                  nameById={nameById}
                  startMs={data.startMs}
                  endMs={nowMs}
                  selectedId={selectedRow?.id}
                  onSelect={setSelectedRowId}
                />
              )}
            </SectionCard>
            {selectedRow && (
              <SectionCard
                title={
                  isLastFinishedShown
                    ? "Last Successful Hydration"
                    : `${ROW_KIND_NAMES[rowKind(selectedRow)].title} on ${nameOf(selectedRow.replicaId)}`
                }
                subtitle={
                  isLastFinishedShown
                    ? `${ROW_KIND_NAMES[rowKind(selectedRow)].label} on ${nameOf(selectedRow.replicaId)}, ${formatDate(rowTime(selectedRow), FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}`
                    : formatDate(
                        rowTime(selectedRow),
                        FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
                      )
                }
              >
                <HydrationDetail
                  clusterId={cluster.id}
                  row={selectedRow}
                  replicaName={nameOf(selectedRow.replicaId)}
                  heapLimitBytes={data.heapLimitBytesById.get(
                    selectedRow.replicaId,
                  )}
                  allocation={data.allocationById.get(selectedRow.replicaId)}
                />
              </SectionCard>
            )}
            <SectionCard title="Past Hydrations">
              <HydrationTable
                rows={rows}
                colorById={colorById}
                nameById={nameById}
                heapLimitById={data.heapLimitBytesById}
                allocationById={data.allocationById}
                selectedId={selectedRow?.id}
                onSelect={setSelectedRowId}
              />
            </SectionCard>
          </>
        )}
      </VStack>
    </MainContentContainer>
  );
};

const Verdict = ({ verdict }: { verdict: HydrationVerdict }) => (
  <Alert
    role="status"
    variant={verdict.variant}
    label={verdict.label}
    message={
      verdict.isFailing ? (
        <Text>
          {verdict.message} See our{" "}
          <TextLink
            href={
              docUrls[
                "/docs/clusters/troubleshoot-clusters/hydration-failures/"
              ]
            }
            isExternal
          >
            docs on troubleshooting hydration failures
          </TextLink>
          .
        </Text>
      ) : (
        verdict.message
      )
    }
  />
);

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
  const stateStyles = {
    hydrated: { label: "Hydrated", color: statusStyles.running.color },
    hydrating: { label: "Hydrating", color: statusStyles.hydrating.color },
    restarting: { label: "Restarting", color: colors.accent.red },
    offline: { label: "Offline", color: statusStyles.offline.color },
    starting: { label: "Starting", color: statusStyles.hydrating.color },
  };
  const stateHelp: Partial<Record<ReplicaNow["state"], string>> = {
    restarting: HYDRATION_HELP.restarting,
    offline: HYDRATION_HELP.offline,
    starting: HYDRATION_HELP.starting,
  };
  return (
    <VStack as="ul" spacing={4} alignItems="stretch" listStyleType="none">
      {replicasNow.map((replicaNow) => {
        const { replica, state, progress, isWorking, startedAt, lastFull } =
          replicaNow;
        const { label, color } = stateStyles[state];
        const help = stateHelp[state];
        const share =
          progress.objectCount === 0
            ? 1
            : progress.hydratedCount / progress.objectCount;
        const lastFullDuration =
          isHistoryReady && lastFull
            ? formatDurationExact(lastFull.durationMs)
            : undefined;
        const detail =
          isWorking && startedAt
            ? `${formatDurationExact(nowMs - startedAt.getTime())} so far${
                lastFullDuration
                  ? ` · last full hydration took ${lastFullDuration}`
                  : ""
              }`
            : "";
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
                {/* The progress bar already says a replica is hydrating. */}
                {state !== "hydrating" && (
                  <HStack spacing={1.5}>
                    <Box
                      width="2"
                      height="2"
                      borderRadius="full"
                      background={color}
                    />
                    <Text textStyle="text-small">{label}</Text>
                    {help && <InfoTip subject={label} label={help} />}
                  </HStack>
                )}
                {isWorking && (
                  <Text
                    textStyle="text-small"
                    color={colors.foreground.secondary}
                  >
                    Hydrated so far: {progress.hydratedCount} of{" "}
                    {progress.objectCount}{" "}
                    {pluralize(progress.objectCount, "object", "objects")}
                  </Text>
                )}
              </HStack>
              {detail && (
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  {detail}
                </Text>
              )}
            </Flex>
            {isWorking && (
              <>
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
                  <Box
                    height="100%"
                    width={`${share * 100}%`}
                    background={color}
                  />
                </Box>
                <LiveObjects
                  clusterName={clusterName}
                  replicaId={replica.id}
                  replicaName={replica.name}
                  nowMs={nowMs}
                />
              </>
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
  // The progress above already says how far the replica has got.
  if (isError || !snapshotComplete) return null;
  const working = objects.filter(({ state }) => state === "working");
  if (working.length === 0) return null;
  return (
    <VStack
      as="ul"
      aria-label={`Objects ${replicaName} is hydrating`}
      listStyleType="none"
      spacing={1}
      alignItems="start"
    >
      {working.slice(0, MAX_LIVE_OBJECTS).map((object) => (
        <Text as="li" key={object.globalId} textStyle="text-small">
          Working on{" "}
          <Text as="span" fontFamily="mono">
            {object.name}
          </Text>
          {object.startedAt &&
            ` for ${formatDurationExact(nowMs - object.startedAt.getTime())}`}
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
