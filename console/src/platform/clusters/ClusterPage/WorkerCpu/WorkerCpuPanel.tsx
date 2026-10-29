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
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useParentSize } from "@visx/responsive";
import { mean } from "d3";
import { startOfMinute } from "date-fns";
import { millisecondsInDay, millisecondsInMinute } from "date-fns/constants";
import React from "react";
import { Link } from "react-router-dom";

import { ProcessMetricsSample } from "~/api/materialize/cluster/replicaProcessMetrics";
import Alert from "~/components/Alert";
import TextLink from "~/components/TextLink";
import { useMaterializationLag } from "~/platform/clusters/queries";
import { queryHistoryPath } from "~/platform/query-history/queryHistoryUtils";
import { useClusterObjectsLive } from "~/store/allObjectsCollection";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";
import { formatDate, TIME_FORMAT } from "~/utils/dateFormat";
import { formatDurationExact, formatPercentage } from "~/utils/format";

import {
  busiestProcess,
  processIdsOf,
  readProcessSamples,
  summarizeProcesses,
} from "../ResourceUsage/processReadingsModel";
import { ChartHeader } from "../ResourceUsage/ResourceUsageCharts";
import { useReplicaProcessSamples } from "../ResourceUsage/useReplicaProcessSamples";
import { TabLink } from "../tabLinks";
import { ObjectCpuTable, QueryHistoryLinks } from "./ObjectCpuTable";
import { WorkerBars } from "./WorkerBars";
import { WorkerCpuCharts } from "./WorkerCpuCharts";
import {
  ADHOC_ROW_ID,
  CPU_DOCS,
  cpuVerdict,
  isPooledRow,
  isSkewVerdict,
  markedWorker,
  Measurement,
  MEASUREMENT_MS,
  objectCpuRows,
  replicaCpuPoints,
  runsStorageDataflow,
  samplesInMeasurement,
  SYSTEM_ROW_ID,
  topObjectsOnWorker,
  verdictObjectId,
  workerBusySeries,
  workerLabel,
  WorkerReplica,
  workersPerCore,
} from "./workerCpuModel";
import {
  LAG_NOTE_MS,
  READ_INTERVAL_MS,
  SAMPLING_LIMIT_MS,
  useWorkerCpuSampler,
  WorkerCpuState,
} from "./workerCpuSampler";
import { FirstLook, Verdict } from "./WorkerCpuVerdict";

const CHART_WINDOW_MS = 15 * millisecondsInMinute;
// Room on either side of the readings: the replica's CPU just before opening,
// and space the readings fill in, so the first minutes aren't a sliver.
const CHART_LEAD_MS = 2 * millisecondsInMinute;
const CHART_RUNWAY_MS = 3 * millisecondsInMinute;
// The cluster's objects that can run dataflows on its replicas.
const DATAFLOW_OBJECT_TYPES = [
  "index",
  "materialized-view",
  "source",
  "sink",
] as const;
// The replica's own CPU covers the chart's window and its lead.
const REPLICA_CPU_WINDOW_MINUTES = 20;

const chartWindow = (
  firstMs: number | undefined,
  lastMs: number | undefined,
) => {
  const openedAtMs = firstMs ?? Date.now();
  const endMs = Math.max(lastMs ?? openedAtMs, openedAtMs + CHART_RUNWAY_MS);
  return {
    openedAtMs,
    startMs: Math.max(openedAtMs - CHART_LEAD_MS, endMs - CHART_WINDOW_MS),
    endMs,
  };
};

const clockTime = (ms: number) => formatDate(new Date(ms), TIME_FORMAT);

/**
 * Query History for the cluster's statements behind a measurement's queries
 * row: those that began from a minute before the span to its end, longest
 * first, and those that began up to a day before its end and still run,
 * oldest first. The list filters on when a statement began, so a long-running
 * SUBSCRIBE is only in the second.
 */
const measuredQueryHistory = (
  regionSlug: string,
  clusterId: string,
  { startMs, endMs }: Measurement,
): QueryHistoryLinks => {
  const rangeEnd = startOfMinute(endMs + millisecondsInMinute);
  return {
    startedPath: queryHistoryPath(regionSlug, {
      clusterId,
      dateRange: [startOfMinute(startMs - millisecondsInMinute), rangeEnd],
      sortField: "duration",
    }),
    runningPath: queryHistoryPath(regionSlug, {
      clusterId,
      dateRange: [startOfMinute(endMs - millisecondsInDay), rangeEnd],
      finishedStatuses: ["running"],
      sortField: "start_time",
      sortOrder: "asc",
    }),
  };
};

/** The replica's CPU over a measurement, from the process samples covering it. */
const measuredReplicaCpu = (
  samples: ProcessMetricsSample[],
  measurement: Measurement,
  allocation: WorkerReplica["allocation"],
): NonNullable<Measurement["replicaCpu"]> => {
  const measured = samplesInMeasurement(samples, measurement);
  const processReadings = readProcessSamples({
    samples: measured,
    allocation,
    startMs: measurement.startMs - millisecondsInMinute,
  });
  return {
    fraction: mean(
      replicaCpuPoints(measured, allocation),
      ({ percent }) => percent / 100,
    ),
    busiestProcess:
      allocation.processes > 1
        ? busiestProcess(
            summarizeProcesses(processIdsOf(processReadings), processReadings),
            "cpu",
            "average",
          )
        : undefined,
  };
};

// The objects the breakdown under the bars names before "everything else".
const BREAKDOWN_OBJECTS = 3;

/** Where the selected worker's time went, right under the bars it's picked on. */
const WorkerBreakdown = ({
  measurement,
  workerId,
  busyFraction,
  replica,
  objectName,
}: {
  measurement: Measurement;
  workerId: number;
  busyFraction: number;
  replica: WorkerReplica;
  objectName: (objectId: string) => string;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const top = topObjectsOnWorker(
    measurement.rows,
    measurement.workerTotalsNs,
    workerId,
    BREAKDOWN_OBJECTS,
  );
  const rest = 1 - top.reduce((sum, { share }) => sum + share, 0);
  const parts = top.map(
    ({ objectId, share }) =>
      `${formatPercentage(share, 0)} on ${objectName(objectId)}`,
  );
  if (rest >= 0.005) {
    parts.push(`${formatPercentage(rest, 0)} on everything else`);
  }
  return (
    <Text
      textStyle="text-small"
      color={colors.foreground.secondary}
      aria-live="polite"
    >
      <Text as="span" textStyle="text-ui-med" color={colors.foreground.primary}>
        {workerLabel(workerId, replica)}
      </Text>{" "}
      was {formatPercentage(busyFraction, 0)} busy
      {parts.length > 0 ? `: ${parts.join(", ")}.` : ", on no object's work."}
    </Text>
  );
};

export interface WorkerCpuPanelProps {
  clusterId: string;
  clusterName: string;
  replica: WorkerReplica;
  unhydratedCount: number | undefined;
  /** The replica's processes, for a replica that runs several. */
  processesLink: TabLink | undefined;
}

/**
 * How a replica's workers share its CPU over one measured minute, and which
 * objects that time went to, read live from the replica while mounted.
 */
export const WorkerCpuPanel = ({
  clusterId,
  clusterName,
  replica,
  unhydratedCount,
  processesLink,
}: WorkerCpuPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const regionSlug = useRegionSlug();
  const bars = useParentSize({ debounceTime: 10 });
  const chart = useParentSize({ debounceTime: 10 });
  const { processes, workers: workersPerProcess } = replica.allocation;
  const workerCount = processes * workersPerProcess;
  const { state, start, pause, resume, measure, annotateMeasurement } =
    useWorkerCpuSampler({
      clusterName,
      replicaId: replica.id,
      replicaName: replica.name,
      workerCount,
    });
  const processSamples = useReplicaProcessSamples({
    replicaId: replica.id,
    windowMinutes: REPLICA_CPU_WINDOW_MINUTES,
  });
  const replicaCpu = React.useMemo(
    () => replicaCpuPoints(processSamples.data, replica.allocation),
    [processSamples.data, replica.allocation],
  );
  const { data: clusterObjects, isError: isObjectsError } =
    useClusterObjectsLive(clusterId, DATAFLOW_OBJECT_TYPES);
  const objectsById = React.useMemo(
    () => new Map(clusterObjects.map((object) => [object.id, object])),
    [clusterObjects],
  );
  const hasSourcesOrSinks = clusterObjects.some(runsStorageDataflow);
  const objectName = React.useCallback(
    (objectId: string) =>
      objectId === ADHOC_ROW_ID
        ? "Queries and subscribes"
        : objectId === SYSTEM_ROW_ID
          ? "Materialize's own dataflows"
          : (objectsById.get(objectId)?.name ?? objectId),
    [objectsById],
  );

  const { readings, measurement } = state;
  const first = readings.at(0);
  const last = readings.at(-1);
  const series = React.useMemo(() => workerBusySeries(readings), [readings]);
  // Recorded on the measurement once the samples covering it have loaded,
  // and read live only until then.
  const liveReplicaCpu = React.useMemo(
    () =>
      measurement && !measurement.replicaCpu && processSamples.snapshotComplete
        ? measuredReplicaCpu(
            processSamples.data,
            measurement,
            replica.allocation,
          )
        : undefined,
    [
      measurement,
      processSamples.data,
      processSamples.snapshotComplete,
      replica.allocation,
    ],
  );
  React.useEffect(() => {
    if (measurement && liveReplicaCpu) {
      annotateMeasurement?.(measurement.endMs, { replicaCpu: liveReplicaCpu });
    }
  }, [measurement, liveReplicaCpu, annotateMeasurement]);
  const measuredCpu = measurement?.replicaCpu ?? liveReplicaCpu;
  const replicaCpuFraction = measuredCpu?.fraction;
  const verdict =
    measurement && measurement.workers.length > 0
      ? cpuVerdict(
          measurement.workers,
          replicaCpuFraction,
          workersPerCore(replica.allocation),
        )
      : undefined;
  const isSkewed = isSkewVerdict(verdict);
  const focusId =
    verdict && measurement ? verdictObjectId(verdict, measurement) : undefined;
  // A pick holds for the measurement it was made on. The next one picks its
  // own object.
  const [pick, setPick] = React.useState<{ endMs: number; objectId: string }>();
  const pickedId =
    pick && pick.endMs === measurement?.endMs ? pick.objectId : focusId;
  const pickedRow = measurement?.rows.find(
    ({ objectId }) => objectId === pickedId,
  );
  const onPick = (objectId: string) =>
    measurement && setPick({ endMs: measurement.endMs, objectId });
  const picked = React.useMemo(
    () =>
      pickedRow && {
        name: objectName(pickedRow.objectId),
        perWorkerNs: pickedRow.perWorkerNs,
      },
    [pickedRow, objectName],
  );

  const lag = useMaterializationLag({
    objectIds: (measurement?.rows ?? [])
      .map(({ objectId }) => objectId)
      .filter((objectId) => !isPooledRow(objectId))
      .sort(),
  });
  // Read once a measurement lands and recorded on it, so freshness is as of
  // the span's end, and a later refetch, such as on window focus, doesn't
  // change the report. Measuring the same objects again keeps the query key,
  // so this reads again rather than taking the cached result.
  const { refetch: refetchLag } = lag;
  const measuredEndMs = measurement?.endMs;
  const isLagRecorded = Boolean(measurement?.lag);
  React.useEffect(() => {
    if (measuredEndMs === undefined || isLagRecorded) return;
    let isCurrent = true;
    void refetchLag().then(({ data, isSuccess }) => {
      if (isCurrent && isSuccess && data) {
        annotateMeasurement?.(measuredEndMs, { lag: [...data.lagMap] });
      }
    });
    return () => {
      isCurrent = false;
    };
  }, [measuredEndMs, isLagRecorded, refetchLag, annotateMeasurement]);
  const recordedLag = measurement?.lag;
  const liveLagMap = lag.data?.lagMap;
  const lagMap = React.useMemo(
    () => (recordedLag ? new Map(recordedLag) : liveLagMap),
    [recordedLag, liveLagMap],
  );
  const queryHistory = React.useMemo(
    () =>
      measurement && measuredQueryHistory(regionSlug, clusterId, measurement),
    [measurement, regionSlug, clusterId],
  );
  const isMeasuringAgain = state.isMeasuring && state.status === "reading";
  const secondsLeft =
    state.measuringFromMs !== undefined && last
      ? Math.max(
          0,
          Math.ceil(
            (MEASUREMENT_MS - (last.atMs - state.measuringFromMs)) / 1000,
          ),
        )
      : undefined;

  // Which process used the most CPU over the measurement, to place a skewed
  // worker among the replica's processes.
  const busiestProcessInWindow = isSkewed
    ? measuredCpu?.busiestProcess
    : undefined;

  const latestRead = state.objectReads.at(-1);
  const lifetimeObjects = React.useMemo(
    () =>
      latestRead &&
      objectCpuRows({
        latest: latestRead.rows,
        exports: latestRead.exports,
        workerCount,
      }),
    [latestRead, workerCount],
  );
  const allocationNs = measurement
    ? (measurement.endMs - measurement.startMs) *
      1e6 *
      ((processes * replica.allocation.cpuNanoCores) / 1e9)
    : 0;
  const busiestWorkerId = measurement?.workers.at(0)?.workerId ?? 0;
  // Like the picked object, a selected worker holds for its measurement.
  const [workerPick, setWorkerPick] = React.useState<{
    endMs: number;
    workerId: number;
  }>();
  const selectedWorkerId =
    workerPick && workerPick.endMs === measurement?.endMs
      ? workerPick.workerId
      : busiestWorkerId;
  const onSelectWorker = (workerId: number) =>
    measurement && setWorkerPick({ endMs: measurement.endMs, workerId });
  const selectedBusy =
    measurement &&
    measurement.endMs > measurement.startMs &&
    Math.min(
      1,
      (measurement.workerTotalsNs[selectedWorkerId] ?? 0) /
        ((measurement.endMs - measurement.startMs) * 1e6),
    );

  return (
    <VStack spacing={7} alignItems="stretch">
      <Flex
        justifyContent="space-between"
        alignItems="center"
        gap={3}
        flexWrap="wrap"
      >
        <HStack spacing={3} alignItems="baseline" flexWrap="wrap">
          <Text textStyle="text-ui-med">
            {workerCount} {pluralize(workerCount, "worker", "workers")}
            {processes > 1 && (
              <Text as="span" color={colors.foreground.secondary}>
                {" "}
                · {processes} processes × {workersPerProcess} workers
              </Text>
            )}
          </Text>
          {processesLink && (
            <TextLink as={Link} {...processesLink} textStyle="text-small">
              CPU and memory by process
            </TextLink>
          )}
        </HStack>
        <HStack spacing={3}>
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            aria-live="polite"
          >
            {/* Before the first measurement, the first look says it's measuring. */}
            {measurement &&
              (isMeasuringAgain
                ? `Measuring a new minute…${secondsLeft ? ` ${secondsLeft}s left` : ""}`
                : `Measured ${clockTime(measurement.startMs)} to ${clockTime(measurement.endMs)}`)}
          </Text>
          {/* Before the first measurement, the first look starts it. */}
          {measurement && (
            <Button
              size="sm"
              variant="outline"
              onClick={measure}
              isDisabled={!measure || isMeasuringAgain}
            >
              Measure again
            </Button>
          )}
        </HStack>
      </Flex>
      {unhydratedCount !== undefined && unhydratedCount > 0 && (
        <Alert
          variant="info"
          label={`${replica.name} is hydrating ${unhydratedCount} ${pluralize(unhydratedCount, "object", "objects")}.`}
          message={
            <>
              Hydration reprocesses each object&apos;s inputs from scratch, so
              busy workers are expected until it finishes.{" "}
              <TextLink href={`${CPU_DOCS}#hydration`} isExternal>
                Troubleshoot hydration
              </TextLink>
            </>
          }
        />
      )}
      {state.status === "error" && readings.length === 0 ? (
        <Alert
          variant="error"
          message={`Couldn't read worker CPU on ${replica.name}: ${state.error}`}
        />
      ) : measurement && verdict && queryHistory ? (
        <Verdict
          clusterName={clusterName}
          replica={replica}
          verdict={verdict}
          measurement={measurement}
          replicaCpuFraction={replicaCpuFraction}
          lagMap={lagMap}
          objectName={objectName}
          objectsById={objectsById}
          hasSourcesOrSinks={hasSourcesOrSinks}
          busiestProcess={busiestProcessInWindow}
          processesLink={processesLink}
          queriesPath={queryHistory.startedPath}
        />
      ) : (
        <FirstLook
          replica={replica}
          workerCount={workerCount}
          measuredMs={
            last && state.stretchStartMs !== undefined
              ? last.atMs - state.stretchStartMs
              : 0
          }
          status={state.status}
          latestCpu={replicaCpu.at(-1)}
          lastReading={last}
          lifetimeObjects={lifetimeObjects}
          objectName={objectName}
          onStart={start}
        />
      )}
      {measurement && (
        <Box ref={bars.parentRef} width="100%" minWidth={0}>
          {bars.width > 0 && (
            <WorkerBars
              width={bars.width}
              measurement={measurement}
              picked={picked}
              hotWorker={markedWorker(verdict)}
              selectedWorkerId={selectedWorkerId}
              onSelectWorker={onSelectWorker}
              replica={replica}
            />
          )}
          <Box marginTop={2}>
            <WorkerBreakdown
              measurement={measurement}
              workerId={selectedWorkerId}
              busyFraction={selectedBusy || 0}
              replica={replica}
              objectName={objectName}
            />
          </Box>
        </Box>
      )}
      {measurement && queryHistory && (
        <VStack spacing={3} alignItems="stretch">
          <ChartHeader
            title="Objects"
            description={`Ranked by their time on w${selectedWorkerId}${
              workerCount === 1
                ? ", the only worker"
                : selectedWorkerId === busiestWorkerId
                  ? ", the busiest worker"
                  : ""
            }, which was ${formatPercentage(selectedBusy || 0, 0)} busy`}
          />
          <ObjectCpuTable
            measurement={measurement}
            workerId={selectedWorkerId}
            pickedId={pickedId}
            onPick={onPick}
            allocationNs={allocationNs}
            lagMap={lagMap}
            objectName={objectName}
            objectsById={objectsById}
            isSkewed={isSkewed}
            queryHistory={queryHistory}
          />
        </VStack>
      )}
      <Box ref={chart.parentRef} width="100%" minWidth={0}>
        {chart.width > 0 && (
          <WorkerCpuCharts
            width={chart.width}
            {...chartWindow(first?.atMs, last?.atMs)}
            replicaCpu={replicaCpu}
            series={series}
            replica={replica}
            measured={measurement}
          />
        )}
      </Box>
      {(processSamples.error || isObjectsError) && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {processSamples.error &&
            `Couldn't read ${replica.name}'s CPU metrics, so the finding leaves its CPU out. `}
          {isObjectsError &&
            `Couldn't load ${clusterName}'s objects, so rows show ids instead of names.`}
        </Text>
      )}
      {state.status !== "idle" && (
        <SamplingStatus
          replicaName={replica.name}
          state={state}
          sinceMs={first?.atMs}
          onPause={pause}
          onResume={resume}
        />
      )}
    </VStack>
  );
};

const SamplingStatus = ({
  replicaName,
  state,
  sinceMs,
  onPause,
  onResume,
}: {
  replicaName: string;
  state: WorkerCpuState;
  sinceMs: number | undefined;
  onPause: (() => void) | undefined;
  onResume: (() => void) | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  let message: string;
  if (state.status === "error") {
    // With no readings, the error shows in place of the report instead.
    message =
      state.readings.length > 0
        ? `Couldn't read worker CPU on ${replicaName}: ${state.error ?? "unknown error"}. Readings so far are kept.`
        : `Stopped reading worker CPU on ${replicaName}.`;
  } else if (state.status === "paused") {
    message =
      state.pauseReason === "limit"
        ? `Paused after ${SAMPLING_LIMIT_MS / millisecondsInMinute} minutes so the readings stop adding load to ${replicaName}.`
        : state.pauseReason === "hidden"
          ? "Paused while this browser tab is hidden."
          : "Paused.";
  } else if (state.waitingMs !== undefined) {
    message = `Waiting for ${replicaName}'s counters${
      state.waitingMs > 0
        ? `, ${formatDurationExact(state.waitingMs)} so far`
        : ""
    }.`;
  } else if (state.lagMs !== undefined && state.lagMs > LAG_NOTE_MS) {
    message = `${replicaName}'s counters are ${formatDurationExact(state.lagMs)} behind, as happens when its workers are busy.`;
  } else {
    message = `Reading worker CPU on ${replicaName} every ${READ_INTERVAL_MS / 1000}s${
      sinceMs !== undefined
        ? ` since ${formatDate(new Date(sinceMs), TIME_FORMAT)}`
        : ""
    }. Stops when you leave the Workers tab, and pauses while the browser tab is hidden or after ${SAMPLING_LIMIT_MS / millisecondsInMinute} minutes.`;
  }
  const isReading = state.status === "reading";
  return (
    <Flex
      justifyContent="space-between"
      alignItems="center"
      gap={3}
      paddingTop={4}
      borderTopWidth="1px"
      borderColor={colors.border.primary}
    >
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {message}
      </Text>
      <Button
        size="sm"
        variant="outline"
        onClick={isReading ? onPause : onResume}
        isDisabled={!(isReading ? onPause : onResume)}
      >
        {isReading
          ? "Pause"
          : state.status === "error"
            ? "Try again"
            : "Resume"}
      </Button>
    </Flex>
  );
};
