// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Spinner, Text, useTheme, VStack } from "@chakra-ui/react";
import { millisecondsInMinute } from "date-fns/constants";
import React from "react";

import {
  formatFullyQualifiedObjectName,
  quoteIdentifier,
} from "~/api/materialize";
import { DatabaseObject } from "~/api/materialize/objects";
import Alert from "~/components/Alert";
import { CopyableBox } from "~/components/copyableComponents";
import TextLink from "~/components/TextLink";
import { MaterializeTheme } from "~/theme";
import { kebabToScreamingSpaceCase } from "~/util";
import { formatDate, TIME_FORMAT } from "~/utils/dateFormat";
import { formatDurationExact, formatPercentage } from "~/utils/format";

import {
  ALL_BUSY_FRACTION,
  CPU_DOCS,
  CpuPoint,
  CpuVerdict,
  dominantObject,
  largestShare,
  lifetimeWorkerSkew,
  objectCpuRows,
  SKEW_RATIO,
  SMOOTHING_WINDOW_MS,
  topObjectOnWorker,
  WorkerCpuReading,
  workerLabel,
  WorkerReplica,
} from "./workerCpuModel";

// Below this, a saturated worker hides behind a replica CPU that reads fine.
const LOW_CPU_FRACTION = 0.5;
// Less work than this outside dataflows isn't worth a sentence.
const OUTSIDE_NOTE_FRACTION = 0.05;
const EXPLAINABLE_TYPES = new Set(["index", "materialized-view"]);

type ObjectCpu = ReturnType<typeof objectCpuRows>;

// EXPLAIN ANALYZE reads the session's cluster and replica, so the statement
// pins both to the replica the drawer measured.
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

export interface FirstLookProps {
  replica: WorkerReplica;
  workerCount: number;
  measuredMs: number;
  latestCpu: CpuPoint | undefined;
  lastReading: WorkerCpuReading | undefined;
  lifetimeObjects: ObjectCpu | undefined;
  objectName: (objectId: string) => string;
}

/** What's known before the live window is long enough to judge. */
export const FirstLook = ({
  replica,
  workerCount,
  measuredMs,
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
  return (
    <Alert
      variant="info"
      label={
        <HStack spacing={2}>
          <Spinner size="xs" />
          <span>
            Measuring {replica.name}&apos;s {workerCount} workers (
            {formatDurationExact(Math.min(measuredMs, SMOOTHING_WINDOW_MS))} of{" "}
            {SMOOTHING_WINDOW_MS / 1000}s)
          </span>
        </HStack>
      }
      message={
        <VStack spacing={1} alignItems="stretch">
          {isCpuCurrent && (
            <Text>
              {replica.name} is at{" "}
              {formatPercentage(latestCpu.percent / 100, 0)} of its CPU.
            </Text>
          )}
          {(lifetimeSkew || topObject) && (
            <Note>
              Over its dataflows&apos; lifetimes,{" "}
              {lifetimeSkew
                ? lifetimeSkew.ratio >= SKEW_RATIO
                  ? Number.isFinite(lifetimeSkew.ratio)
                    ? `worker ${lifetimeSkew.workerId} has done ${lifetimeSkew.ratio.toFixed(1)}× as much as the other workers on average`
                    : `worker ${lifetimeSkew.workerId} has done all of the work`
                  : `the work is spread evenly across workers (the busiest has done ${lifetimeSkew.ratio.toFixed(2)}× as much as the others on average)`
                : "the per-worker split isn't read yet"}
              {topObject &&
                `, and ${objectName(topObject.objectId)} accounts for ${formatPercentage(topObject.share, 0)} of the dataflow CPU`}
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
  windowMs: number;
  /** The replica's CPU over the verdict's window, from its metrics. */
  replicaCpuFraction: number | undefined;
  objects: ObjectCpu | undefined;
  /** When `objects` starts, or undefined for each dataflow's lifetime. */
  objectsSinceMs: number | undefined;
  objectName: (objectId: string) => string;
  objectsById: Map<string, DatabaseObject>;
}

/** The plain-language finding for the last few minutes of readings. */
export const Verdict = ({
  clusterName,
  replica,
  verdict,
  windowMs,
  replicaCpuFraction,
  objects,
  objectsSinceMs,
  objectName,
  objectsById,
}: VerdictProps) => {
  const windowLength = formatDurationExact(windowMs);
  const objectsSpan =
    objectsSinceMs !== undefined
      ? `since ${formatDate(new Date(objectsSinceMs), TIME_FORMAT)}`
      : "over its dataflows' lifetimes";
  const dominant =
    objects && dominantObject(objects.rows, objects.workerTotalsNs);
  const dominantSentence =
    dominant &&
    `${objectName(dominant.objectId)} used ${formatPercentage(dominant.share, 0)} of the dataflow CPU ${objectsSpan}, so start there.`;

  if (verdict.kind === "idle") {
    return (
      <Alert
        variant="info"
        label={`No dataflow work on ${replica.name} in the last ${windowLength}.`}
        message="Skew only shows while workers are busy. Where the CPU went, since each dataflow started, shows where their time went before."
      />
    );
  }

  if (verdict.kind === "highCpu") {
    const { dataflowFraction, outsideFraction, busiest, uneven } = verdict;
    const cpu = formatPercentage(verdict.replicaCpuFraction, 0);
    const dataflowSentence = `Over the last ${windowLength} its workers spent ${formatPercentage(dataflowFraction, 0)} of the allocation running dataflows, and the busiest worker ${formatPercentage(busiest.busyFraction, 0)} of its core${
      uneven
        ? `. Work is uneven, ${workerLabel(uneven.busiest.workerId, replica)} at ${formatPercentage(uneven.busiest.busyFraction, 0)} of its core against ${formatPercentage(uneven.othersBusyFraction, 0)} for the other workers, but no worker is at its limit, so skew isn't what uses the CPU.`
        : ", so this isn't worker skew."
    }`;
    // Sizing up helps only when dataflows use the CPU. Work outside them
    // grows with the operator count on every worker, which a bigger size
    // raises.
    if (outsideFraction > dataflowFraction) {
      return (
        <Alert
          variant="warning"
          label={`${replica.name} is using ${cpu} of its CPU, mostly outside its running dataflows.`}
          message={
            <VStack spacing={2} alignItems="stretch">
              <Text>
                {dataflowSentence} The other{" "}
                {formatPercentage(outsideFraction, 0)} went to work that
                isn&apos;t counted against a running dataflow: answering
                queries, dataflows that finished between readings, such as those
                built for ad-hoc queries, and scheduling, progress tracking and
                networking.
              </Text>
              <Note>
                {dominantSentence ? `${dominantSentence} ` : ""}
                Work outside dataflows grows with the number of operators on
                every worker, so sizing up doesn&apos;t always lower it.
                Splitting objects across smaller clusters can.
              </Note>
              <HStack spacing={4}>
                <DocLink anchor="ad-hoc-query-load">
                  Troubleshoot ad-hoc queries
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
    return (
      <Alert
        variant="warning"
        label={`${replica.name} is using ${cpu} of its CPU, mostly running dataflows spread evenly across its workers.`}
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              {dataflowSentence}
              {dataflowFraction >= ALL_BUSY_FRACTION &&
                " Less than 10% of the allocation is left idle."}
              {outsideFraction >= OUTSIDE_NOTE_FRACTION &&
                ` Another ${formatPercentage(outsideFraction, 0)} went to work outside running dataflows.`}
            </Text>
            <Note>
              {dominantSentence ??
                "Where the CPU went ranks objects by their CPU time."}
            </Note>
            {dominant ? (
              <DocLink anchor="an-expensive-object">
                Troubleshoot an expensive object
              </DocLink>
            ) : (
              <DocLink anchor="an-undersized-cluster">
                Troubleshoot an undersized cluster
              </DocLink>
            )}
          </VStack>
        }
      />
    );
  }

  const { busiest, averageBusyFraction, othersBusyFraction } = verdict;
  const busiestName = workerLabel(busiest.workerId, replica);

  if (verdict.kind === "saturated" || verdict.kind === "uneven") {
    const top =
      objects &&
      topObjectOnWorker(objects.rows, objects.workerTotalsNs, busiest.workerId);
    const topObject = top && objectsById.get(top.objectId);
    const statement =
      topObject && explainStatement(topObject, clusterName, replica.name);
    const isCpuLow =
      replicaCpuFraction !== undefined && replicaCpuFraction < LOW_CPU_FRACTION;
    return (
      <Alert
        variant="warning"
        label={
          verdict.kind === "saturated"
            ? `${busiestName} is saturated.`
            : "Work is uneven across workers."
        }
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              Over the last {windowLength} {busiestName} ran at{" "}
              {formatPercentage(busiest.busyFraction, 0)} of its core, while the
              other workers averaged {formatPercentage(othersBusyFraction, 0)}.
              {verdict.kind === "saturated"
                ? ` Work routed to it waits${
                    isCpuLow
                      ? `, even though ${replica.name} is only at ${formatPercentage(replicaCpuFraction, 0)} of its CPU`
                      : ""
                  }.`
                : " No worker is at its limit yet."}
            </Text>
            {top && (
              <Note>
                {objectName(top.objectId)} took {formatPercentage(top.share, 0)}{" "}
                of its time {objectsSpan}.
                {statement &&
                  " To find the operator, and the key it hashes on, run:"}
              </Note>
            )}
            {statement && (
              <CopyableBox contents={statement} wrap variant="compact" />
            )}
            <DocLink anchor="worker-skew">Troubleshoot worker skew</DocLink>
          </VStack>
        }
      />
    );
  }

  if (verdict.kind === "allBusy") {
    return (
      <Alert
        variant="warning"
        label="Every worker is busy."
        message={
          <VStack spacing={2} alignItems="stretch">
            <Text>
              Over the last {windowLength} the average worker ran at{" "}
              {formatPercentage(averageBusyFraction, 0)} of its core and{" "}
              {busiestName}, the busiest, at{" "}
              {formatPercentage(busiest.busyFraction, 0)}, so the work is spread
              evenly and {replica.name} has less than 10% headroom.
              {dominantSentence && ` ${dominantSentence}`}
            </Text>
            {dominant ? (
              <DocLink anchor="an-expensive-object">
                Troubleshoot an expensive object
              </DocLink>
            ) : (
              <DocLink anchor="an-undersized-cluster">
                Troubleshoot an undersized cluster
              </DocLink>
            )}
          </VStack>
        }
      />
    );
  }

  if (verdict.kind === "light") {
    return (
      <Alert
        variant="info"
        label={`Load is light on ${replica.name}.`}
        message={`Over the last ${windowLength} the busiest worker, ${busiestName}, ran at ${formatPercentage(busiest.busyFraction, 0)} of its core and the other workers averaged ${formatPercentage(othersBusyFraction, 0)}. Uneven work only slows results as a worker nears its core's limit.`}
      />
    );
  }

  return (
    <Alert
      variant="info"
      label="Work is spread evenly."
      message={`Over the last ${windowLength} the busiest worker, ${busiestName}, ran at ${formatPercentage(busiest.busyFraction, 0)} of its core and the average worker at ${formatPercentage(averageBusyFraction, 0)}.`}
    />
  );
};
