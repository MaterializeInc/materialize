// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Spinner, Text, useTheme, VStack } from "@chakra-ui/react";
import { greatest, mean } from "d3";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";
import { Link } from "react-router-dom";

import {
  formatFullyQualifiedObjectName,
  quoteIdentifier,
} from "~/api/materialize";
import { LagMap } from "~/api/materialize/cluster/materializationLag";
import { DatabaseObject } from "~/api/materialize/objects";
import Alert from "~/components/Alert";
import { CopyableBox } from "~/components/copyableComponents";
import TextLink from "~/components/TextLink";
import { formatLagInfoDetailed } from "~/platform/clusters/format";
import { MaterializeTheme } from "~/theme";
import {
  kebabToScreamingSpaceCase,
  pluralize,
  sumPostgresIntervalMs,
} from "~/util";
import { formatPercentage } from "~/utils/format";

import {
  BusiestProcess,
  otherProcessesLabel,
  standsOut,
} from "../ResourceUsage/processReadingsModel";
import { TabLink } from "../tabLinks";
import {
  ALL_BUSY_FRACTION,
  CPU_DOCS,
  CpuPoint,
  CpuVerdict,
  EVEN_RATIO,
  formatMultiple,
  isPooledRow,
  largestShare,
  lifetimeWorkerSkew,
  Measurement,
  MEASUREMENT_MS,
  objectCpuRows,
  ONE_WORKER_RATIO,
  processOfWorker,
  SKEW_RATIO,
  topObjectOnWorker,
  WorkerCpuReading,
  workerLabel,
  WorkerReplica,
  workersPerCore,
} from "./workerCpuModel";
import { WorkerCpuState } from "./workerCpuSampler";

// Below this share of the replica's CPU, work outside the workers' dataflows
// goes unmentioned.
const NOTABLE_OUTSIDE_FRACTION = 0.05;

// Below this, an overloaded worker hides behind a replica CPU that reads fine.
const LOW_CPU_FRACTION = 0.5;
// An object with this much of a worker's time is what the worker is doing.
const MOST_OF_WORKER = 0.5;
const EXPLAINABLE_TYPES = new Set(["index", "materialized-view"]);

type ObjectCpu = ReturnType<typeof objectCpuRows>;

// EXPLAIN ANALYZE reads the session's cluster and replica, so the statement
// pins both to the replica the readings came from.
const explainStatement = (
  object: DatabaseObject,
  clusterName: string,
  replicaName: string,
) =>
  EXPLAINABLE_TYPES.has(object.objectType)
    ? [
        `SET cluster = ${quoteIdentifier(clusterName)};`,
        `SET cluster_replica = ${quoteIdentifier(replicaName)};`,
        `EXPLAIN ANALYZE CPU WITH SKEW FOR ${kebabToScreamingSpaceCase(object.objectType)} ${formatFullyQualifiedObjectName(object, { quoteIdentifiers: true })};`,
      ].join("\n")
    : undefined;

const Note = ({ children }: React.PropsWithChildren) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Text textStyle="text-small" color={colors.foreground.secondary}>
      {children}
    </Text>
  );
};

const DocLink = ({
  anchor,
  children,
}: React.PropsWithChildren<{ anchor: string }>) => (
  <TextLink href={`${CPU_DOCS}#${anchor}`} isExternal>
    {children}
  </TextLink>
);

/**
 * How much more of an object's work runs on a worker than on the others, as
 * the end of a sentence about it. `ratio` is the worker's time on the object
 * over the other workers' average, infinite when they spent none.
 */
const spreadPhrase = (ratio: number, worker: string) =>
  Number.isFinite(ratio)
    ? `runs ${formatMultiple(ratio)} as much work on ${worker} as on the other workers`
    : `runs all of its work on ${worker}`;

const ratioOnWorker = ({
  ns,
  othersAverageNs,
}: {
  ns: number;
  othersAverageNs: number;
}) => (othersAverageNs > 0 ? ns / othersAverageNs : Infinity);

export interface FirstLookProps {
  replica: WorkerReplica;
  workerCount: number;
  measuredMs: number;
  status: WorkerCpuState["status"];
  latestCpu: CpuPoint | undefined;
  lastReading: WorkerCpuReading | undefined;
  lifetimeObjects: ObjectCpu | undefined;
  objectName: (objectId: string) => string;
}

/** What's known before the first minute is measured. */
export const FirstLook = ({
  replica,
  workerCount,
  measuredMs,
  status,
  latestCpu,
  lastReading,
  lifetimeObjects,
  objectName,
}: FirstLookProps) => {
  // Metrics land about once a minute, so an older point is stale.
  const isCpuCurrent =
    latestCpu && Date.now() - latestCpu.atMs < 3 * millisecondsInMinute;
  const lifetimeSkew = lastReading && lifetimeWorkerSkew(lastReading);
  const topObject =
    lifetimeObjects &&
    largestShare(lifetimeObjects.rows, lifetimeObjects.workerTotalsNs);
  const workers = `${replica.name}'s ${workerCount} ${pluralize(workerCount, "worker", "workers")}`;
  const progress = `${Math.floor(Math.min(measuredMs, MEASUREMENT_MS) / 1000)}s of ${MEASUREMENT_MS / 1000}s`;
  return (
    <Alert
      variant={status === "error" ? "warning" : "info"}
      label={
        status === "reading" ? (
          <HStack spacing={2}>
            <Spinner size="xs" />
            <span>
              Measuring {workers} for a minute ({progress})
            </span>
          </HStack>
        ) : status === "paused" ? (
          `Paused measuring ${workers} (${progress})`
        ) : (
          `Couldn't finish measuring ${workers} (${progress})`
        )
      }
      message={
        <VStack spacing={1} alignItems="stretch">
          {isCpuCurrent && (
            <Text>
              {replica.name} is at{" "}
              {formatPercentage(latestCpu.percent / 100, 0)} of its CPU.
            </Text>
          )}
          {workerCount === 1 && topObject && (
            <Note>
              Since each object started, {objectName(topObject.objectId)}{" "}
              accounts for {formatPercentage(topObject.share, 0)} of its
              worker&apos;s time.
            </Note>
          )}
          {workerCount > 1 && (lifetimeSkew || topObject) && (
            <Note>
              Since each object started,{" "}
              {lifetimeSkew
                ? lifetimeSkew.ratio >= SKEW_RATIO
                  ? Number.isFinite(lifetimeSkew.ratio)
                    ? `${workerLabel(lifetimeSkew.workerId, replica)} has done ${formatMultiple(lifetimeSkew.ratio)} as much as the other workers on average`
                    : `${workerLabel(lifetimeSkew.workerId, replica)} has done all of the work`
                  : lifetimeSkew.ratio < EVEN_RATIO
                    ? "the work is spread evenly across workers"
                    : `no worker carries much more than the others (the busiest has done ${formatMultiple(lifetimeSkew.ratio)} as much)`
                : "the per-worker split isn't read yet"}
              {topObject &&
                `, and ${objectName(topObject.objectId)} accounts for ${formatPercentage(topObject.share, 0)} of the workers' time`}
              .
            </Note>
          )}
        </VStack>
      }
    />
  );
};

export interface VerdictProps {
  clusterName: string;
  replica: WorkerReplica;
  verdict: CpuVerdict;
  measurement: Measurement;
  /** The replica's CPU over the measurement, from its metrics. */
  replicaCpuFraction: number | undefined;
  lagMap: LagMap | undefined;
  objectName: (objectId: string) => string;
  objectsById: Map<string, DatabaseObject>;
  /** Whether any of the cluster's objects `runsStorageDataflow`. */
  hasSourcesOrSinks: boolean;
  /** The process that used the most CPU over the measurement. */
  busiestProcess?: BusiestProcess;
  /** The replica's processes, for a replica that runs several. */
  processesLink?: TabLink;
  /** Where the cluster's statements that began around the span are listed. */
  queriesPath: string;
}

/**
 * The plain-language finding for one measurement, naming the worker and the
 * object behind it together.
 */
export const Verdict = ({
  clusterName,
  replica,
  verdict,
  measurement,
  replicaCpuFraction,
  lagMap,
  objectName,
  objectsById,
  hasSourcesOrSinks,
  busiestProcess,
  processesLink,
  queriesPath,
}: VerdictProps) => {
  const { rows, workerTotalsNs } = measurement;
  const workerCount = workerTotalsNs.length;
  const behind = (objectId: string) => {
    const lag = lagMap?.get(objectId);
    return lag?.isOutdated
      ? `${objectName(objectId)} is ${formatLagInfoDetailed(lag)} behind its inputs.`
      : undefined;
  };
  // The object furthest behind, of those past the outdated threshold.
  const outdated = greatest(
    rows.flatMap(({ objectId }) => {
      const lag = lagMap?.get(objectId);
      return lag?.isOutdated && lag.lag
        ? [{ objectId, lagMs: sumPostgresIntervalMs(lag.lag) }]
        : [];
    }),
    ({ lagMs }) => lagMs,
  );

  if (verdict.kind === "idle") {
    return (
      <Alert
        variant="info"
        label="Workers are idle"
        message={`No object did measurable work on ${replica.name} in this span. Skew only shows while workers are busy.`}
      />
    );
  }

  // One worker's work can't be uneven, so the finding is how busy it was and
  // what on, with the replica's CPU when it's high.
  if (workerCount === 1) {
    const only = verdict.busiest;
    const largest = largestShare(rows, workerTotalsNs);
    const isBusy = only.busyFraction >= ALL_BUSY_FRACTION;
    const highCpu = verdict.kind === "highCpu" ? verdict : undefined;
    return (
      <Alert
        variant={isBusy || highCpu ? "warning" : "info"}
        label={
          isBusy
            ? `Not skew: ${replica.name}'s only worker is busy`
            : highCpu
              ? `Not skew: ${replica.name} is at ${formatPercentage(highCpu.replicaCpuFraction, 0)} of its CPU`
              : `${replica.name} runs one worker, so its work can't be uneven`
        }
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              w{only.workerId} was {formatPercentage(only.busyFraction, 0)} busy
              {largest
                ? `, and ${objectName(largest.objectId)} took the most of its time, ${formatPercentage(largest.share, 0)}.`
                : "."}
              {isBusy &&
                ` Less than ${formatPercentage(1 - ALL_BUSY_FRACTION, 0)} of its core is left idle.`}
              {highCpu &&
                highCpu.outsideFraction >= NOTABLE_OUTSIDE_FRACTION &&
                ` Another ${formatPercentage(highCpu.outsideFraction, 0)} of the replica's CPU went to work outside its worker's dataflows.`}
            </Text>
            {outdated && <Note>{behind(outdated.objectId)}</Note>}
            {(isBusy || highCpu) && (
              <HStack spacing={4} flexWrap="wrap">
                <DocLink anchor="an-expensive-object">
                  Troubleshoot an expensive object
                </DocLink>
                <DocLink anchor="an-undersized-cluster">
                  Troubleshoot an undersized cluster
                </DocLink>
              </HStack>
            )}
          </VStack>
        }
      />
    );
  }

  if (verdict.kind === "highCpu") {
    const { dataflowFraction, outsideFraction, busiest, uneven } = verdict;
    const cpu = formatPercentage(verdict.replicaCpuFraction, 0);
    const largest = largestShare(rows, workerTotalsNs);
    const startThere =
      largest &&
      `${objectName(largest.objectId)} took the most, ${formatPercentage(largest.share, 0)} of the workers' time, so start there.`;
    const workersSentence = `Its workers spent ${formatPercentage(dataflowFraction, 0)} of the allocation running work, and the busiest, ${workerLabel(busiest.workerId, replica)}, ${formatPercentage(busiest.busyFraction, 0)} of its core${
      uneven
        ? `. Work leans on ${workerLabel(uneven.busiest.workerId, replica)}, but no worker is at its limit, so skew isn't what uses the CPU.`
        : ", so this isn't skew."
    }`;
    // Sizing up helps only when the workers use the CPU. Work outside them
    // grows with the operator count on every worker, which a bigger size
    // raises.
    if (outsideFraction > dataflowFraction) {
      return (
        <Alert
          variant="warning"
          label={`Not skew: ${replica.name} is at ${cpu} of its CPU, mostly outside dataflows`}
          message={
            <VStack spacing={2} alignItems="stretch">
              <Text>
                {workersSentence} The other{" "}
                {formatPercentage(outsideFraction, 0)} went to work outside the
                workers&apos; dataflows:{" "}
                {hasSourcesOrSinks &&
                  "reading from sources and writing to sinks, "}
                answering SELECTs straight from indexes, queries that started
                and ended between readings, and scheduling, progress tracking
                and networking.
              </Text>
              <Note>
                {startThere ? `${startThere} ` : ""}
                Work outside dataflows grows with the number of operators on
                every worker, so sizing up doesn&apos;t always lower it.
                Splitting objects across smaller clusters can.
              </Note>
              <HStack spacing={4} flexWrap="wrap">
                {hasSourcesOrSinks && (
                  <DocLink anchor="upstream-data-volume">
                    Troubleshoot upstream data volume
                  </DocLink>
                )}
                <DocLink anchor="ad-hoc-query-load">
                  Troubleshoot ad-hoc queries
                </DocLink>
                <TextLink as={Link} to={queriesPath}>
                  Queries started on {clusterName} in these minutes
                </TextLink>
                <DocLink anchor="an-undersized-cluster">
                  Troubleshoot an undersized cluster
                </DocLink>
              </HStack>
            </VStack>
          }
        />
      );
    }
    return (
      <Alert
        variant="warning"
        label={
          dataflowFraction >= ALL_BUSY_FRACTION
            ? "Not skew: every worker is busy"
            : `Not skew: ${replica.name} is at ${cpu} of its CPU${uneven ? "" : ", spread evenly across its workers"}`
        }
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              {workersSentence}
              {outsideFraction >= NOTABLE_OUTSIDE_FRACTION &&
                ` Another ${formatPercentage(outsideFraction, 0)} went to work outside the workers' dataflows.`}
            </Text>
            {startThere && <Note>{startThere}</Note>}
            {outdated && <Note>{behind(outdated.objectId)}</Note>}
            <HStack spacing={4} flexWrap="wrap">
              <DocLink anchor="an-expensive-object">
                Troubleshoot an expensive object
              </DocLink>
              <DocLink anchor="an-undersized-cluster">
                Troubleshoot an undersized cluster
              </DocLink>
            </HStack>
          </VStack>
        }
      />
    );
  }

  const { busiest, othersBusyFraction } = verdict;

  const worker = `w${busiest.workerId}`;
  // The process once, in the first sentence about the worker.
  const workerIntro =
    replica.allocation.processes > 1
      ? `${worker}, in process ${processOfWorker(busiest.workerId, replica.allocation.workers)},`
      : worker;
  const busyText = formatPercentage(busiest.busyFraction, 0);
  const othersText = formatPercentage(othersBusyFraction, 0);
  const top = topObjectOnWorker(rows, workerTotalsNs, busiest.workerId);
  // Of the object's time on the busiest worker, the one the finding names.
  const topRatio = top && ratioOnWorker(top);
  const topName = top && objectName(top.objectId);

  if (verdict.kind === "saturated" || verdict.kind === "uneven") {
    const isSaturated = verdict.kind === "saturated";
    const isMostlyOne = top !== undefined && top.share >= MOST_OF_WORKER;
    const topObject = top && objectsById.get(top.objectId);
    const statement =
      isMostlyOne &&
      topObject &&
      explainStatement(topObject, clusterName, replica.name);
    // NOTE: a replica with missing metrics reads 0% of its CPU. CPU under half
    // the workers' own time is taken as missing, so neither it nor the
    // processes' CPU is quoted then.
    const workersShare =
      (mean(measurement.workers, ({ busyFraction }) => busyFraction) ?? 0) *
      workersPerCore(replica.allocation);
    const hasCpuMetrics =
      replicaCpuFraction !== undefined &&
      replicaCpuFraction >= workersShare / 2;
    const isCpuLow =
      hasCpuMetrics &&
      replicaCpuFraction !== undefined &&
      replicaCpuFraction < LOW_CPU_FRACTION;
    const label = isMostlyOne
      ? isSaturated
        ? `${worker} is overloaded, and most of its time goes to ${topName}`
        : `Work leans on ${worker}, mostly from ${topName}`
      : isSaturated
        ? `${worker} is overloaded, with no single object behind it`
        : `Work leans on ${worker}`;
    const workerProcess = String(
      processOfWorker(busiest.workerId, replica.allocation.workers),
    );
    const processPercent = (percent: number) =>
      formatPercentage(percent / 100, 0);
    // Name a process only by the processes drawer's rule, or one hot worker
    // among many would credit whichever process noise puts a point ahead.
    const processSentence =
      hasCpuMetrics &&
      busiestProcess &&
      (!standsOut(busiestProcess)
        ? `The processes used CPU about evenly, ${processPercent(busiestProcess.value)} at most against ${processPercent(busiestProcess.othersMean)} for the others, so the skew is between workers, not processes.`
        : busiestProcess.processId === workerProcess
          ? `Its process, process ${workerProcess}, used the most CPU: ${processPercent(busiestProcess.value)} against ${processPercent(busiestProcess.othersMean)} for ${otherProcessesLabel(busiestProcess.otherCount)}.`
          : `Process ${busiestProcess.processId} used the most CPU, ${processPercent(busiestProcess.value)} against ${processPercent(busiestProcess.othersMean)} for ${otherProcessesLabel(busiestProcess.otherCount)}.`);
    const cause =
      topRatio !== undefined &&
      (topRatio >= ONE_WORKER_RATIO
        ? "Nearly all of an object's work on one worker is what a cross join does, since it has no key to split the work by. A key value that almost every row shares does the same."
        : "That pattern usually means a hot key: one value in a join or GROUP BY key holds most of the rows, so all of them land on one worker.");
    const freshness = [
      top && behind(top.objectId),
      isCpuLow &&
        `Replica CPU reads only ${formatPercentage(replicaCpuFraction, 0)}, because the average across workers hides ${worker}.`,
    ]
      .filter(Boolean)
      .join(" ");
    return (
      <Alert
        variant={isSaturated ? "error" : "warning"}
        label={label}
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              {workerIntro} is {busyText} busy.{" "}
              {workerCount === 2
                ? `The other worker is at ${othersText}`
                : `The other ${workerCount - 1} workers average ${othersText}`}
              , so {worker} does{" "}
              {othersBusyFraction > 0
                ? `${formatMultiple(busiest.busyFraction / othersBusyFraction)} ${workerCount === 2 ? "its" : "their"} work.`
                : "nearly all of the work."}
              {top &&
                topRatio !== undefined &&
                (isMostlyOne
                  ? ` ${formatPercentage(top.share, 0)} of ${worker}'s time goes to ${topName}, which ${spreadPhrase(topRatio, worker)}.`
                  : ` Its time is split across several objects. The largest, ${topName}, takes ${formatPercentage(top.share, 0)} of it.`)}
              {!isSaturated &&
                " No worker is at its limit yet, so results aren't waiting on it."}
            </Text>
            {isMostlyOne && cause && <Text>{cause}</Text>}
            {freshness && <Text>{freshness}</Text>}
            {statement && (
              <>
                <Note>
                  To find the step in {topName}&apos;s plan, and the key it
                  splits on, run:
                </Note>
                <CopyableBox contents={statement} wrap variant="compact" />
              </>
            )}
            {processSentence && <Note>{processSentence}</Note>}
            <HStack spacing={4} flexWrap="wrap">
              <DocLink anchor="worker-skew">How to fix worker skew</DocLink>
              {processesLink && (
                <TextLink as={Link} {...processesLink}>
                  CPU and memory by process
                </TextLink>
              )}
            </HStack>
          </VStack>
        }
      />
    );
  }

  if (verdict.kind === "allBusy") {
    const busyFractions = measurement.workers.map(
      ({ busyFraction }) => busyFraction,
    );
    const largest = largestShare(rows, workerTotalsNs);
    return (
      <Alert
        variant="warning"
        label="Not skew: every worker is busy"
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              All {workerCount} workers are{" "}
              {formatPercentage(Math.min(...busyFractions), 0)} to{" "}
              {formatPercentage(Math.max(...busyFractions), 0)} busy, so the
              work is already spread evenly and {replica.name} has less than 10%
              headroom.
              {largest &&
                ` ${objectName(largest.objectId)} takes the most, ${formatPercentage(largest.share, 0)} of the workers' time, so start there.`}
            </Text>
            {outdated && <Text>{behind(outdated.objectId)}</Text>}
            <HStack spacing={4} flexWrap="wrap">
              <DocLink anchor="an-expensive-object">
                Troubleshoot an expensive object
              </DocLink>
              <DocLink anchor="an-undersized-cluster">
                Troubleshoot an undersized cluster
              </DocLink>
            </HStack>
          </VStack>
        }
      />
    );
  }

  if (verdict.kind === "light") {
    const leans = topRatio !== undefined && topRatio >= SKEW_RATIO;
    return (
      <Alert
        variant="info"
        label="Nothing to act on"
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              Workers are lightly loaded: the busiest, {worker}, is {busyText}{" "}
              busy.
              {leans &&
                ` ${topName} ${spreadPhrase(topRatio, worker)}, but no worker is near its limit, so it isn't slowing anything down.`}
            </Text>
            {leans && (
              <Note>
                {`If ${topName} grows${top && !isPooledRow(top.objectId) ? " or starts falling behind" : ""}, measure again.`}
              </Note>
            )}
          </VStack>
        }
      />
    );
  }

  return (
    <Alert
      variant="info"
      label="Work is spread evenly"
      message={
        <VStack spacing={2} alignItems="stretch">
          <Text>
            The busiest worker, {worker}, is {busyText} busy, and the other
            workers average {othersText}. Skew isn&apos;t slowing {replica.name}{" "}
            down.
          </Text>
          {outdated && <Note>{behind(outdated.objectId)}</Note>}
        </VStack>
      }
    />
  );
};
