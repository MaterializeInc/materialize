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
  Tab,
  TabList,
  TabPanel,
  TabPanels,
  Tabs,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useParentSize } from "@visx/responsive";
import { mean } from "d3";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import Alert from "~/components/Alert";
import SideDrawer from "~/components/SideDrawer";
import TextLink from "~/components/TextLink";
import { useClusterObjectsLive } from "~/store/allObjectsCollection";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";
import { formatDate, TIME_FORMAT } from "~/utils/dateFormat";
import { formatDurationExact } from "~/utils/format";

import { BusiestWorkers } from "./BusiestWorkers";
import { ObjectCpuTable, ObjectMode } from "./ObjectCpuTable";
import { useReplicaCpu } from "./useReplicaCpu";
import { WorkerCpuCharts } from "./WorkerCpuCharts";
import {
  ADHOC_ROW_ID,
  CPU_DOCS,
  cpuVerdict,
  isSkewVerdict,
  objectCpuRows,
  SMOOTHING_WINDOW_MS,
  verdictWindowStart,
  workerBusyBetween,
  workerBusySeries,
  WorkerReplica,
  workersPerCore,
} from "./workerCpuModel";
import {
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
// The verdict judges recent readings, so a reopened drawer reports the present
// rather than averaging in readings from before it closed.
const VERDICT_WINDOW_MS = 5 * millisecondsInMinute;
// Objects whose dataflows run on a cluster's replicas.
const EXPORT_OBJECT_TYPES = ["index", "materialized-view"] as const;

export interface WorkerCpuDrawerProps {
  isOpen: boolean;
  onClose: () => void;
  clusterId: string;
  clusterName: string;
  /** Replicas with more than one worker, the only ones skew applies to. */
  replicas: WorkerReplica[];
  unhydratedObjectCounts: Map<string, number> | undefined;
}

/**
 * How a cluster's replicas spend their CPU across workers, read live from the
 * replica while open.
 */
export const WorkerCpuDrawer = ({
  isOpen,
  onClose,
  clusterId,
  clusterName,
  replicas,
  unhydratedObjectCounts,
}: WorkerCpuDrawerProps) => (
  <SideDrawer
    isOpen={isOpen}
    onClose={onClose}
    title="CPU and workers"
    size="xl"
    width="min(1120px, 94vw)"
  >
    <Tabs isLazy>
      <TabList px={6}>
        {replicas.map((replica) => (
          <Tab key={replica.id}>{replica.name}</Tab>
        ))}
      </TabList>
      <TabPanels>
        {replicas.map((replica) => (
          <TabPanel key={replica.id} px={6} py={5}>
            <WorkerCpuPanel
              clusterId={clusterId}
              clusterName={clusterName}
              replica={replica}
              unhydratedCount={unhydratedObjectCounts?.get(replica.id)}
            />
          </TabPanel>
        ))}
      </TabPanels>
    </Tabs>
  </SideDrawer>
);

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

const WorkerCpuPanel = ({
  clusterId,
  clusterName,
  replica,
  unhydratedCount,
}: {
  clusterId: string;
  clusterName: string;
  replica: WorkerReplica;
  unhydratedCount: number | undefined;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { parentRef, width } = useParentSize({ debounceTime: 10 });
  const { state, pause, resume } = useWorkerCpuSampler({
    clusterName,
    replicaId: replica.id,
    replicaName: replica.name,
  });
  const replicaCpu = useReplicaCpu({
    replicaId: replica.id,
    cpuNanoCoresPerProcess: replica.cpuNanoCoresPerProcess,
  });
  const [objectMode, setObjectMode] = React.useState<ObjectMode>("auto");
  const { data: clusterObjects } = useClusterObjectsLive(
    clusterId,
    EXPORT_OBJECT_TYPES,
  );
  const objectsById = React.useMemo(
    () => new Map(clusterObjects.map((object) => [object.id, object])),
    [clusterObjects],
  );
  const objectName = React.useCallback(
    (objectId: string) =>
      objectId === ADHOC_ROW_ID
        ? "Ad-hoc queries and subscribes"
        : (objectsById.get(objectId)?.name ?? objectId),
    [objectsById],
  );
  const workerCount = replica.processes * replica.workersPerProcess;

  const { readings } = state;
  const first = readings.at(0);
  const last = readings.at(-1);
  const series = React.useMemo(() => workerBusySeries(readings), [readings]);
  const verdictStart = verdictWindowStart(readings, VERDICT_WINDOW_MS);
  const windowMs = verdictStart && last ? last.atMs - verdictStart.atMs : 0;
  const windowWorkers =
    verdictStart && last && windowMs > 0
      ? workerBusyBetween(verdictStart, last)
      : [];
  // Metrics land about once a minute, so include the one before the window.
  const windowCpu = mean(
    verdictStart
      ? replicaCpu.filter(
          (point) => point.atMs >= verdictStart.atMs - millisecondsInMinute,
        )
      : [],
    (point) => point.percent / 100,
  );
  // Shorter windows swing with the counters' one-second ticks.
  const verdict =
    windowMs >= SMOOTHING_WINDOW_MS && windowWorkers.length > 0
      ? cpuVerdict(
          windowWorkers,
          windowCpu,
          workersPerCore(
            replica.workersPerProcess,
            replica.cpuNanoCoresPerProcess,
          ),
        )
      : undefined;
  const isSkewed = isSkewVerdict(verdict);

  const hasWindowObjects = Boolean(
    state.firstObjects &&
    state.latestObjects &&
    state.latestObjects.atMs > state.firstObjects.atMs,
  );
  const lifetimeObjects = React.useMemo(
    () =>
      state.latestObjects &&
      objectCpuRows({
        latest: state.latestObjects.rows,
        exports: state.exports,
        workerCount,
      }),
    [state.latestObjects, state.exports, workerCount],
  );
  const windowObjects = React.useMemo(
    () =>
      hasWindowObjects && state.firstObjects && state.latestObjects
        ? objectCpuRows({
            baseline: state.firstObjects.rows,
            latest: state.latestObjects.rows,
            exports: state.exports,
            workerCount,
          })
        : undefined,
    [
      hasWindowObjects,
      state.firstObjects,
      state.latestObjects,
      state.exports,
      workerCount,
    ],
  );
  const tableMode =
    objectMode === "auto"
      ? windowObjects
        ? "window"
        : "lifetime"
      : objectMode;
  const tableObjects =
    tableMode === "window"
      ? (windowObjects ?? lifetimeObjects)
      : lifetimeObjects;
  // A window's objects measure against the allocation over it, the CPU
  // chart's unit.
  const { firstObjects, latestObjects } = state;
  const allocation = React.useMemo(() => {
    if (tableMode !== "window" || !windowObjects) return undefined;
    if (!firstObjects || !latestObjects) return undefined;
    const cores = (replica.processes * replica.cpuNanoCoresPerProcess) / 1e9;
    const ns = (latestObjects.atMs - firstObjects.atMs) * 1e6 * cores;
    if (ns <= 0) return undefined;
    return {
      ns,
      // Metrics land about once a minute, so include the one before.
      replicaCpuFraction: mean(
        replicaCpu.filter(
          (point) =>
            point.atMs >= firstObjects.atMs - millisecondsInMinute &&
            point.atMs <= latestObjects.atMs,
        ),
        (point) => point.percent / 100,
      ),
    };
  }, [
    tableMode,
    windowObjects,
    firstObjects,
    latestObjects,
    replica,
    replicaCpu,
  ]);

  return (
    <VStack spacing={7} alignItems="stretch">
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {clusterName} · {replica.name} · {replica.size} · {workerCount} workers
        {replica.processes > 1 ? ` on ${replica.processes} processes` : ""}
      </Text>
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
      ) : verdict ? (
        <Verdict
          clusterName={clusterName}
          replica={replica}
          verdict={verdict}
          windowMs={windowMs}
          replicaCpuFraction={windowCpu}
          // Whichever span the table shows.
          objects={windowObjects ?? lifetimeObjects}
          objectsSinceMs={windowObjects ? state.firstObjects?.atMs : undefined}
          objectName={objectName}
          objectsById={objectsById}
        />
      ) : (
        <FirstLook
          replica={replica}
          workerCount={workerCount}
          measuredMs={windowMs}
          latestCpu={replicaCpu.at(-1)}
          lastReading={last}
          lifetimeObjects={lifetimeObjects}
          objectName={objectName}
        />
      )}
      <Box ref={parentRef} width="100%" minWidth={0}>
        {width > 0 && (
          <WorkerCpuCharts
            width={width}
            {...chartWindow(first?.atMs, last?.atMs)}
            replicaCpu={replicaCpu}
            series={series}
            replica={replica}
          />
        )}
      </Box>
      {verdict && verdictStart && (
        <BusiestWorkers
          workers={windowWorkers}
          sinceMs={verdictStart.atMs}
          isSkewed={isSkewed}
          objects={windowObjects}
          objectName={objectName}
          replica={replica}
        />
      )}
      {tableObjects && (
        <ObjectCpuTable
          rows={tableObjects.rows}
          // Without skew, extra time ranks noise, so rank by what uses the CPU.
          sortBy={isSkewed ? "extra" : "total"}
          mode={tableMode}
          canMeasureWindow={hasWindowObjects}
          sinceMs={state.firstObjects?.atMs}
          onModeChange={setObjectMode}
          dataflowTotalNs={tableObjects.workerTotalsNs.reduce(
            (sum, ns) => sum + ns,
            0,
          )}
          allocation={allocation}
          replica={replica}
          objectName={objectName}
          objectsById={objectsById}
        />
      )}
      <SamplingStatus
        replicaName={replica.name}
        state={state}
        sinceMs={first?.atMs}
        onPause={pause}
        onResume={resume}
      />
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
    message = `Couldn't read worker CPU on ${replicaName}: ${state.error ?? "unknown error"}. Readings so far are kept.`;
  } else if (state.status === "paused") {
    message = state.pausedForLimit
      ? `Paused after ${SAMPLING_LIMIT_MS / millisecondsInMinute} minutes so the readings stop adding load to ${replicaName}.`
      : "Paused.";
  } else if (state.intervalMs > READ_INTERVAL_MS) {
    message = `${replicaName} answered slowly, so readings now run every ${formatDurationExact(state.intervalMs)} to keep load off it.`;
  } else {
    message = `Reading worker CPU on ${replicaName} every ${READ_INTERVAL_MS / 1000}s${
      sinceMs !== undefined
        ? ` since ${formatDate(new Date(sinceMs), TIME_FORMAT)}`
        : ""
    }. Stops when you close this, pauses after ${SAMPLING_LIMIT_MS / millisecondsInMinute} minutes.`;
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
