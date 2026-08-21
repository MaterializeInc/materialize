// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, useTheme, VStack } from "@chakra-ui/react";
import React from "react";
import { Link as RouterLink } from "react-router-dom";

import Alert, { AlertVariant } from "~/components/Alert";
import TextLink from "~/components/TextLink";
import { DetailItem } from "~/platform/connectors/AsideBox";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import {
  formatBytesShort,
  formatDurationForAxis,
  formatIntervalShort,
} from "~/utils/format";

import {
  MaintainedObjectCluster,
  MaintainedObjectLag,
  MaintainedObjectSourceStatus,
  ObjectSourceStatistics,
  useClusterBucketsInWindow,
  useObjectSourceStatistics,
} from "./queries";
import {
  diagnoseSourceHealth,
  IngestionSample,
  MIN_RATE_SPAN_MS,
  SourceHealthDiagnosis,
} from "./sourceHealthDiagnosis";

const formatRate = (perSec: number) =>
  perSec >= 10 ? Math.round(perSec).toLocaleString() : perSec.toFixed(1);

/** `offset_known`/`offset_committed` are in a source-defined unit (see the
 * `mz_source_statistics` docs): Kafka counts message offsets, MySQL counts
 * transactions, Postgres counts WAL bytes (LSN). */
const offsetUnits = (sourceType: string | null) => {
  switch (sourceType) {
    case "kafka":
      return { noun: "messages", rate: "msgs/s" };
    case "mysql":
      return { noun: "transactions", rate: "txns/s" };
    case "postgres":
      return { noun: "WAL bytes", rate: "WAL bytes/s" };
    default:
      return { noun: "units", rate: "units/s" };
  }
};

const verdictAlert = (
  diagnosis: SourceHealthDiagnosis,
  lag: MaintainedObjectLag | null,
  hasOffsets: boolean,
  units: { noun: string; rate: string },
): { variant: AlertVariant; message: string } | null => {
  const { verdict, inflowPerSec, drainPerSec, etaMs } = diagnosis;
  switch (verdict) {
    case "healthy":
      return null;
    case "measuring":
      if (!lag) return null;
      return {
        variant: "info",
        message: hasOffsets
          ? `Behind by ${formatIntervalShort(lag.value)}. Measuring ingestion rate, this takes about a minute.`
          : `Behind by ${formatIntervalShort(lag.value)}.`,
      };
    case "stalled":
      return {
        variant: "error",
        message: `Ingestion is stalled: upstream is producing ~${formatRate(inflowPerSec ?? 0)} ${units.rate} but almost nothing is being committed.`,
      };
    case "fallingBehind":
      return {
        variant: "error",
        message: `Falling behind: committing ~${formatRate(drainPerSec ?? 0)} ${units.rate} while ~${formatRate(inflowPerSec ?? 0)} ${units.rate} arrive upstream. At this rate it will not catch up.`,
      };
    case "catchingUp":
      return {
        variant: "warning",
        message: `Catching up: committing ~${formatRate(drainPerSec ?? 0)} ${units.rate} against ~${formatRate(inflowPerSec ?? 0)} ${units.rate} incoming.${etaMs ? ` Estimated catch-up in ~${formatDurationForAxis(etaMs)}.` : ""}`,
      };
  }
};

/** Replica memory above this fraction of its limit makes "scale this cluster
 * up" the confident recommendation for a lagging source. */
const MEMORY_PRESSURE_THRESHOLD = 0.85;

/** Peak memory utilization across the cluster's live replicas over the last
 * few minutes, with the largest replica's size for the message. */
const useClusterMemoryPressure = (clusterId: string | null) => {
  const { data: bucketsByReplicaId } = useClusterBucketsInWindow({
    clusterId,
    lookbackMs: 10 * 60 * 1000,
    bucketSizeMs: 60 * 1000,
    anchorTimestamp: null,
  });
  const latest = Object.values(bucketsByReplicaId ?? {})
    .map((bs) => bs.at(-1))
    .filter((b) => b !== undefined);
  const percents = latest
    .map((b) => b.maxHeap.percent)
    .filter((p): p is number => p !== null);
  return {
    memoryPercent: percents.length > 0 ? Math.max(...percents) : null,
    replicaSize: latest.at(0)?.size ?? null,
  };
};

/** Accumulates offset watermark readings across the 5s poll so rates can be
 * derived client-side; the upstream statistics only tick about once a minute. */
const useIngestionSamples = (
  sourceId: string,
  stats: ObjectSourceStatistics | null | undefined,
) => {
  const [state, setState] = React.useState<{
    sourceId: string;
    samples: IngestionSample[];
  }>({ sourceId, samples: [] });

  const offsetKnown = stats?.offsetKnown ?? null;
  const offsetCommitted = stats?.offsetCommitted ?? null;
  React.useEffect(() => {
    if (offsetKnown === null || offsetCommitted === null) return;
    setState((prev) => {
      const samples = prev.sourceId === sourceId ? prev.samples : [];
      const last = samples.at(-1);
      const moved =
        !last ||
        last.offsetKnown !== offsetKnown ||
        last.offsetCommitted !== offsetCommitted;
      // Record unmoved watermarks too once they are stale, so a fully
      // stalled source still accrues the two samples a verdict needs.
      const stale = last && Date.now() - last.atMs >= MIN_RATE_SPAN_MS;
      if (!moved && !stale) return prev;
      return {
        sourceId,
        samples: [
          ...samples.slice(-19),
          { atMs: Date.now(), offsetKnown, offsetCommitted },
        ],
      };
    });
  }, [sourceId, offsetKnown, offsetCommitted]);

  return state.sourceId === sourceId ? state.samples : [];
};

export interface SourceIngestionHealthProps {
  sourceId: string;
  /** Subtype of the source (e.g. `kafka`, `postgres`); decides offset units. */
  sourceType: string | null;
  /** Null until the source status subscribe delivers a row. */
  sourceStatus: MaintainedObjectSourceStatus | null;
  /** Null until the lag subscribe delivers a row. */
  lag: MaintainedObjectLag | null;
  cluster: MaintainedObjectCluster | null;
}

/** Statuses whose story the status pill and error alert already tell; a
 * verdict computed from their frozen watermarks would only mislead. */
const NON_INGESTING_STATUSES = new Set(["paused", "stalled", "failed"]);

/** Verdict-first ingestion health: is the source keeping up, since when not,
 * why, and which cluster to act on. */
export const SourceIngestionHealth = ({
  sourceId,
  sourceType,
  sourceStatus,
  lag,
  cluster,
}: SourceIngestionHealthProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const regionSlug = useRegionSlug();
  const { data: stats } = useObjectSourceStatistics(sourceId);
  const samples = useIngestionSamples(sourceId, stats);
  const { memoryPercent, replicaSize } = useClusterMemoryPressure(
    cluster?.id ?? null,
  );

  const diagnosis = diagnoseSourceHealth({
    samples,
    lagMs: lag?.ms ?? null,
    nowMs: Date.now(),
  });
  const units = offsetUnits(sourceType);
  const hasOffsets = stats?.offsetKnown != null;
  const alert = verdictAlert(diagnosis, lag, hasOffsets, units);
  const behind = diagnosis.verdict !== "healthy";

  if (sourceStatus && NON_INGESTING_STATUSES.has(sourceStatus.status)) {
    return null;
  }

  // Position through the upstream's offset space, as context on the backlog.
  // NOTE: not "percent of work done": offsets before the source's start
  // offset count as ingested, and `offset_known` can regress, so clamp.
  const ingestedPercent =
    behind &&
    stats?.offsetKnown != null &&
    stats.offsetKnown > 0 &&
    stats.offsetCommitted !== null
      ? Math.max(
          0,
          Math.min(
            100,
            Math.floor((stats.offsetCommitted / stats.offsetKnown) * 100),
          ),
        )
      : null;

  if (!stats) return null;

  return (
    <VStack align="stretch" spacing={2} width="100%">
      {alert && (
        <Alert variant={alert.variant} width="100%" message={alert.message} />
      )}
      {!behind && (
        <Text textStyle="text-small" color={colors.accent.green}>
          Ingestion is keeping up with upstream.
        </Text>
      )}
      {diagnosis.drainPerSec !== null && diagnosis.inflowPerSec !== null && (
        <DetailItem label="Ingestion rate">
          ~{formatRate(diagnosis.drainPerSec)} {units.rate} (upstream ~
          {formatRate(diagnosis.inflowPerSec)} {units.rate})
        </DetailItem>
      )}
      {diagnosis.backlog !== null && (
        <DetailItem
          label="Upstream lag"
          color={behind ? colors.accent.red : colors.foreground.primary}
        >
          {diagnosis.backlog.toLocaleString()} {units.noun}
          {ingestedPercent !== null && ` (${ingestedPercent}% ingested)`}
        </DetailItem>
      )}
      {behind && diagnosis.stalledSinceMs !== null && (
        <DetailItem label="No fully processed data since">
          {formatDate(
            new Date(diagnosis.stalledSinceMs),
            FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
          )}
        </DetailItem>
      )}
      {stats.bytesIndexed > 0 && (
        <DetailItem label="Upsert state">
          {formatBytesShort(BigInt(Math.round(stats.bytesIndexed)))} (
          {stats.recordsIndexed.toLocaleString()} keys)
        </DetailItem>
      )}
      {behind && cluster && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          {memoryPercent !== null &&
          memoryPercent >= MEMORY_PRESSURE_THRESHOLD ? (
            <>
              Replica memory is at {Math.round(memoryPercent * 100)}%
              {replicaSize ? ` (${replicaSize})` : ""} on cluster{" "}
              <TextLink
                as={RouterLink}
                to={absoluteClusterPath(regionSlug, cluster)}
              >
                {cluster.name}
              </TextLink>
              , which this source ingests on. Scaling that cluster up is likely
              to restore throughput.
            </>
          ) : (
            <>
              Ingestion for this source runs on cluster{" "}
              <TextLink
                as={RouterLink}
                to={absoluteClusterPath(regionSlug, cluster)}
              >
                {cluster.name}
              </TextLink>
              {memoryPercent !== null
                ? `, which has memory headroom (${Math.round(memoryPercent * 100)}%), so scaling it up may not help. The bottleneck may be upstream throughput or the source's partition parallelism.`
                : ". If it can't keep up, that cluster is the one to scale."}{" "}
            </>
          )}{" "}
          Resizing clusters downstream of this source won&apos;t help.
        </Text>
      )}
      {!behind && stats.bytesIndexed > 0 && cluster && (
        <Text textStyle="text-small" color={colors.foreground.secondary}>
          Upsert ingestion slows sharply when state approaches the
          replica&apos;s memory. Before a planned bulk load, scale cluster{" "}
          <Text as="span" fontWeight="500">
            {cluster.name}
          </Text>{" "}
          up first, then scale back once the source has caught up.
        </Text>
      )}
    </VStack>
  );
};

export default SourceIngestionHealth;
