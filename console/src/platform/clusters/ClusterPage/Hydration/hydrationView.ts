// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { greatest, min } from "d3";
import { millisecondsInMinute } from "date-fns/constants";

import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import { AlertVariant } from "~/components/Alert";
import { pluralize } from "~/util";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatBytesShort, formatDurationExact } from "~/utils/format";

import { ReplicaStatus } from "../Configuration/clusterConfigurationModel";
import {
  Hydration,
  HydrationReplica,
  latestBoot,
  latestFullHydration,
  UnfinishedHydration,
} from "./hydrationModel";
import { ReplicaHydrationProgress } from "./useLiveHydration";

/** A finished hydration or one a replica restarted before finishing. */
export type HydrationRow =
  | ({ type: "hydration" } & Hydration)
  | ({ type: "unfinished" } & UnfinishedHydration);

export type HydrationRowKind = "full" | "additive" | "unfinished";

export const rowKind = (row: HydrationRow): HydrationRowKind =>
  row.type === "unfinished" ? "unfinished" : row.kind;

/** How the tab names each kind of row: in text, in a heading, and in a column. */
export const ROW_KIND_NAMES: Record<
  HydrationRowKind,
  { label: string; title: string; short: string }
> = {
  full: { label: "Full hydration", title: "Full Hydration", short: "Full" },
  additive: {
    label: "Additive hydration",
    title: "Additive Hydration",
    short: "Additive",
  },
  unfinished: {
    label: "Restarted before finishing",
    title: "Restart",
    short: "Restarted before finishing",
  },
};

/** Bytes for display, undefined when not recorded. */
export const formatPeakBytes = (bytes: number | null) =>
  bytes === null ? undefined : formatBytesShort(BigInt(Math.round(bytes)));

/** When a row happened: a hydration's start, or an unfinished one's end. */
export const rowTime = (row: HydrationRow) =>
  row.type === "hydration" ? row.startedAt : row.endedAt;

/** Hydrations and unfinished ones, newest first. */
export const toHydrationRows = (
  hydrations: Hydration[],
  unfinished: UnfinishedHydration[],
): HydrationRow[] =>
  [
    ...hydrations.map((row) => ({ ...row, type: "hydration" as const })),
    ...unfinished.map((row) => ({ ...row, type: "unfinished" as const })),
  ].sort((a, b) => rowTime(b).getTime() - rowTime(a).getTime());

/**
 * `restarting` is hydrating again after restarting since it last finished.
 * `starting` is up but hasn't reported its objects yet.
 */
export type ReplicaHydrationState =
  "hydrated" | "hydrating" | "restarting" | "offline" | "starting";

export interface ReplicaNow {
  replica: HydrationReplica;
  state: ReplicaHydrationState;
  progress: ReplicaHydrationProgress;
  /** When its current hydration began, if that's known. */
  startedAt: Date | undefined;
  lastFull: Hydration | undefined;
  /** Times it restarted without finishing, since it last finished. */
  restarts: UnfinishedHydration[];
}

/**
 * When a replica's current hydration began. If every object it hasn't hydrated
 * was created after its latest boot, it's hydrating new objects and began with
 * the first of them. Otherwise it's rehydrating since that boot. Asking whether
 * a hydration finished since the boot fails after a zero-downtime upgrade,
 * which records the boot after the replica already hydrated.
 */
function currentHydrationStart(
  bootedAt: Date | undefined,
  unhydratedObjectIds: string[],
  createdAtMsById: Map<string, number> | undefined,
) {
  if (!createdAtMsById) return undefined;
  const bootMs = bootedAt?.getTime() ?? -Infinity;
  const createdMs = unhydratedObjectIds.map((id) => createdAtMsById.get(id));
  const isNewObjects =
    createdMs.length > 0 &&
    createdMs.every((ms) => ms !== undefined && ms > bootMs);
  if (!isNewObjects) return bootedAt;
  const firstMs = min(createdMs, (ms) => ms);
  return firstMs === undefined ? undefined : new Date(firstMs);
}

/**
 * How long after booting a replica that hasn't reported its objects reads as
 * starting. A replica can also never report, as with the cluster's
 * introspection turned off, so after this it's no longer called starting.
 */
const STARTING_GRACE_MS = 2 * millisecondsInMinute;

/**
 * Where each current replica's hydration stands. A replica's live progress
 * outlives its process, so an offline replica's last progress is ignored.
 */
export function summarizeReplicasNow({
  replicas,
  statusById,
  clusterObjectCount,
  progressByReplicaId,
  bootTimes,
  hydrations,
  unfinished,
  createdObjects,
  nowMs,
}: {
  replicas: HydrationReplica[];
  statusById: Map<string, ReplicaStatus>;
  /** The cluster's indexes and materialized views, undefined until known. */
  clusterObjectCount: number | undefined;
  progressByReplicaId: Map<string, ReplicaHydrationProgress>;
  bootTimes: Map<string, number[]>;
  hydrations: Hydration[];
  unfinished: UnfinishedHydration[];
  /** Objects created on the cluster in the window, undefined until known. */
  createdObjects: Array<{ id: string | null; occurredAt: Date }> | undefined;
  nowMs: number;
}): ReplicaNow[] {
  const createdAtMsById =
    createdObjects &&
    new Map(
      createdObjects.flatMap(({ id, occurredAt }) =>
        id ? [[id, occurredAt.getTime()]] : [],
      ),
    );
  return replicas
    .filter(({ isCurrent }) => isCurrent)
    .map((replica) => {
      const progress = progressByReplicaId.get(replica.id) ?? {
        objectCount: 0,
        hydratedCount: 0,
        unhydratedObjectIds: [],
      };
      const lastFinishedMs =
        greatest(
          hydrations.filter(({ replicaId }) => replicaId === replica.id),
          ({ finishedAt }) => finishedAt.getTime(),
        )?.finishedAt.getTime() ?? -Infinity;
      const restarts = unfinished.filter(
        ({ replicaId, endedAt }) =>
          replicaId === replica.id && endedAt.getTime() > lastFinishedMs,
      );
      const isHydrating = progress.hydratedCount < progress.objectCount;
      const isOffline = statusById.get(replica.id)?.kind === "offline";
      // A replica reports nothing until its first process is up, which reads as
      // nothing to hydrate on a cluster that has objects.
      const bootedAt = latestBoot(bootTimes, replica.id);
      const hasNotReported =
        progress.objectCount === 0 &&
        (clusterObjectCount ?? 0) > 0 &&
        bootedAt !== undefined &&
        nowMs - bootedAt.getTime() < STARTING_GRACE_MS;
      const state: ReplicaHydrationState =
        restarts.length > 0 && (isOffline || isHydrating || hasNotReported)
          ? "restarting"
          : isOffline
            ? "offline"
            : hasNotReported
              ? "starting"
              : isHydrating
                ? "hydrating"
                : "hydrated";
      return {
        replica,
        state,
        progress,
        startedAt: isHydrating
          ? currentHydrationStart(
              bootedAt,
              progress.unhydratedObjectIds,
              createdAtMsById,
            )
          : undefined,
        lastFull: latestFullHydration(hydrations, replica.id),
        restarts,
      };
    });
}

export interface HydrationVerdict {
  variant: AlertVariant;
  label: string;
  message: string | undefined;
  /** Whether to point at the hydration troubleshooting guides. */
  isFailing: boolean;
}

const LIST_FORMAT = new Intl.ListFormat("en", { type: "conjunction" });

const restartsSentence = (restarts: UnfinishedHydration[], subject: string) => {
  const times = restarts.length === 1 ? "once" : `${restarts.length} times`;
  const isOom = restarts.every(({ reason }) => reason === OOM_REASON);
  if (!isOom) return `${subject} restarted ${times}.`;
  return restarts.length === 1
    ? `${subject} restarted once, after running out of memory.`
    : `${subject} restarted ${times}, each after running out of memory.`;
};

/**
 * One sentence on what needs attention in the cluster's hydration, with the
 * numbers behind it. Undefined when every replica is hydrated.
 */
export function hydrationVerdict(
  replicasNow: ReplicaNow[],
  nowMs: number,
): HydrationVerdict | undefined {
  if (replicasNow.length === 0) {
    return {
      variant: "info",
      label: "This cluster has no replicas, so nothing on it is hydrated.",
      message: undefined,
      isFailing: false,
    };
  }

  const restarting = replicasNow.filter(({ state }) => state === "restarting");
  if (restarting.length > 0) {
    const restarts = restarting.flatMap(({ restarts: each }) => each);
    // Since the first attempt that never finished began, when that's recorded.
    const sinceMs =
      min(restarts, ({ startedAt, endedAt }) =>
        (startedAt ?? endedAt).getTime(),
      ) ?? nowMs;
    const names = LIST_FORMAT.format(
      restarting.map(({ replica }) => replica.name),
    );
    return {
      variant: "error",
      label: `${names} ${restarting.length === 1 ? "hasn't" : "haven't"} finished hydrating since ${formatDate(sinceMs, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}.`,
      message: restartsSentence(
        restarts,
        restarting.length === 1 ? "It" : "Between them, they",
      ),
      isFailing: true,
    };
  }

  const hydrating = replicasNow.filter(({ state }) => state === "hydrating");
  if (hydrating.length === 1) {
    const [{ replica, progress, startedAt, lastFull }] = hydrating;
    const elapsed = startedAt
      ? `, ${formatDurationExact(nowMs - startedAt.getTime())} so far`
      : "";
    return {
      variant: "warning",
      label: `${replica.name} is hydrating: ${progress.hydratedCount} of ${progress.objectCount} ${pluralize(progress.objectCount, "object", "objects")}${elapsed}.`,
      message: lastFull
        ? `Its last full hydration took ${formatDurationExact(lastFull.durationMs)}.`
        : undefined,
      isFailing: false,
    };
  }
  if (hydrating.length > 1) {
    return {
      variant: "warning",
      label: `${LIST_FORMAT.format(hydrating.map(({ replica }) => replica.name))} are hydrating.`,
      message: undefined,
      isFailing: false,
    };
  }

  const offline = replicasNow.filter(({ state }) => state === "offline");
  if (offline.length > 0) {
    return {
      variant: "warning",
      label: `${LIST_FORMAT.format(offline.map(({ replica }) => replica.name))} ${offline.length === 1 ? "is" : "are"} offline.`,
      message:
        offline.length === 1
          ? "It hydrates once it's back online."
          : "They hydrate once they're back online.",
      isFailing: false,
    };
  }
  const starting = replicasNow.filter(({ state }) => state === "starting");
  if (starting.length > 0) {
    return {
      variant: "warning",
      label: `${LIST_FORMAT.format(starting.map(({ replica }) => replica.name))} ${starting.length === 1 ? "is" : "are"} starting.`,
      message: "Hydration progress shows once the replica reports it.",
      isFailing: false,
    };
  }
  return undefined;
}

/** What the tab's terms mean, shared by the chart, detail and table. */
export const HYDRATION_HELP = {
  full: "Hydrating every object on a replica, as after it starts or restarts. Inferred: Materialize doesn't record why a hydration began.",
  additive:
    "Hydrating new objects, such as a new index, on a replica that already has the others. Inferred: Materialize doesn't record why a hydration began.",
  kind: "Full or additive. Inferred: Materialize doesn't record why a hydration began.",
  unfinished:
    "Inferred from replica status: it went offline or ran out of memory before finishing.",
  inProgress: "Still hydrating, placed at how long it has taken so far.",
  hydrationSlot: "A replica hydrates a few objects at a time, 4 by default.",
  duration: "From the first dataflow installed to the last object hydrated.",
  objects: "Indexes and materialized views in the hydration.",
  peakMemory:
    "The most memory one process used, as a share of its memory. Materialize records the peak since the process started, so it can predate this hydration.",
  peakDisk:
    "The most disk one process used, as a share of its disk. Recorded since the process started.",
  peakHeap:
    "Memory plus disk, as a share of the process's heap limit. A process that reaches the limit runs out of memory. Recorded since the process started.",
  cpu: "The highest CPU sample during the hydration. Materialize samples CPU once a minute.",
  lastFullDuration: "How long the replica's last full hydration took.",
  lastFullObjects:
    "Objects the last full hydration covered, and objects on the cluster now. Objects created since hydrate during the next one.",
  lastFullMemory:
    "Memory plus disk one process used at the last full hydration, as a share of its heap limit.",
  restarting: "Inferred from replica status: it restarted before finishing.",
  offline: "The replica is offline, so it isn't hydrating.",
  starting: "The replica hasn't reported its objects yet.",
};
