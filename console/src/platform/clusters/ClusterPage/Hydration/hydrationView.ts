// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { greatest, max, min } from "d3";
import { millisecondsInMinute } from "date-fns/constants";

import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";
import { AlertVariant } from "~/components/Alert";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";
import { formatBytesShort } from "~/utils/format";

import { ReplicaStatus } from "../Configuration/clusterConfigurationModel";
import {
  Hydration,
  HydrationReplica,
  latestBoot,
  latestFullHydration,
  ReplicaEpisode,
  ReplicaTransition,
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

/** How the tab names each kind of row: in text, and in a column. */
export const ROW_KIND_NAMES: Record<
  HydrationRowKind,
  { label: string; short: string }
> = {
  full: { label: "Full hydration", short: "Full" },
  additive: { label: "Additive hydration", short: "Additive" },
  unfinished: {
    label: "Restarted before finishing",
    short: "Restarted before finishing",
  },
};

/** Bytes for display, undefined when not recorded. */
export const formatPeakBytes = (bytes: number | null) =>
  bytes === null ? undefined : formatBytesShort(BigInt(Math.round(bytes)));

/** When a row happened: a hydration's start, or an unfinished one's end. */
export const rowTime = (row: HydrationRow) =>
  row.type === "hydration" ? row.startedAt : row.endedAt;

/** When a row ended: a hydration's finish, or an unfinished one's restart. */
export const rowEndTime = (row: HydrationRow) =>
  row.type === "hydration" ? row.finishedAt : row.endedAt;

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
  /** A starting replica that hasn't reported has 0 of the cluster's objects. */
  progress: ReplicaHydrationProgress;
  /**
   * Hydrating now: online and short of its objects, or starting. A replica
   * restarting in a loop can be offline between attempts.
   */
  isWorking: boolean;
  /** Whether it has reported its objects. */
  hasReported: boolean;
  /** When its current hydration began, if it's working and that's known. */
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
  /**
   * When the cluster's objects were created, undefined until known. An object
   * missing from it is taken to be older than the replica's boot.
   */
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
      const reported = progressByReplicaId.get(replica.id) ?? {
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
      const isHydrating = reported.hydratedCount < reported.objectCount;
      const isOffline = statusById.get(replica.id)?.kind === "offline";
      // A replica reports nothing until its first process is up, which reads as
      // nothing to hydrate on a cluster that has objects.
      const bootedAt = latestBoot(bootTimes, replica.id);
      const hasNotReported =
        reported.objectCount === 0 &&
        clusterObjectCount !== undefined &&
        clusterObjectCount > 0 &&
        bootedAt !== undefined &&
        nowMs - bootedAt.getTime() < STARTING_GRACE_MS;
      const isWorking = (isHydrating || hasNotReported) && !isOffline;
      const progress = hasNotReported
        ? {
            objectCount: clusterObjectCount,
            hydratedCount: 0,
            unhydratedObjectIds: [],
          }
        : reported;
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
        isWorking,
        hasReported: reported.objectCount > 0,
        startedAt: isWorking
          ? currentHydrationStart(
              bootedAt,
              reported.unhydratedObjectIds,
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
 * numbers behind it. Undefined unless a replica keeps restarting before it
 * finishes or there are no replicas, since the progress card shows the rest.
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
      label: `${names} ${restarting.length === 1 ? "has" : "have"} no finished hydration recorded since ${formatDate(sinceMs, FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}.`,
      message: restartsSentence(
        restarts,
        restarting.length === 1 ? "It" : "Between them, they",
      ),
      isFailing: true,
    };
  }

  return undefined;
}

/** A current replica that may have finished a hydration not recorded yet. */
export interface UnrecordedHydration {
  replica: HydrationReplica;
  /** When it last needed a hydration. */
  since: Date;
}

/**
 * Current replicas with everything hydrated that they last needed, and no
 * hydration recorded since: since they were created or restarted, or got the
 * newest object they have hydrated. One still hydrating newer objects counts,
 * but not one rebuilding after a restart. Materialize records hydrations by
 * visiting replicas in turn, so a finished one can take several minutes to
 * show up. Status alone can't tell a restart from a blip, so
 * `isUnrecordedEpisode` confirms each against the replica.
 */
export function findUnrecordedHydrations({
  replicasNow,
  hydrations,
  restarts,
  createdObjects,
  startMs,
}: {
  replicasNow: ReplicaNow[];
  hydrations: Hydration[];
  /** Every restart of the replicas, as `findRestarts` returns them. */
  restarts: ReplicaTransition[];
  /**
   * When the cluster's objects were created, undefined until known. An object
   * missing from it is taken to be older than the replica's boot.
   */
  createdObjects: Array<{ id: string | null; occurredAt: Date }> | undefined;
  /** Hydrations before this aren't loaded, so earlier events can't be judged. */
  startMs: number;
}): UnrecordedHydration[] {
  const createdAtMsById = new Map(
    (createdObjects ?? []).flatMap(({ id, occurredAt }) =>
      id ? [[id, occurredAt.getTime()]] : [],
    ),
  );
  return replicasNow.flatMap(({ replica, state, progress }) => {
    if (state !== "hydrated" && state !== "hydrating") return [];
    if (progress.objectCount === 0) return [];
    const bootedMs =
      max(
        [
          replica.createdAt?.getTime(),
          ...restarts
            .filter(({ replicaId }) => replicaId === replica.id)
            .map(({ occurredAt }) => occurredAt.getTime()),
        ],
        (ms) => ms,
      ) ?? -Infinity;
    const unhydrated = new Set(progress.unhydratedObjectIds);
    const isRebuilding = [...unhydrated].some(
      (id) => (createdAtMsById.get(id) ?? -Infinity) <= bootedMs,
    );
    if (isRebuilding) return [];
    const lastHydratedCreationMs =
      max(
        [...createdAtMsById].filter(([id]) => !unhydrated.has(id)),
        ([, ms]) => ms,
      ) ?? -Infinity;
    const sinceMs = Math.max(bootedMs, lastHydratedCreationMs);
    if (sinceMs < startMs) return [];
    const isRecorded = hydrations.some(
      ({ replicaId, finishedAt }) =>
        replicaId === replica.id && finishedAt.getTime() > sinceMs,
    );
    return isRecorded ? [] : [{ replica, since: new Date(sinceMs) }];
  });
}

// Episode times come from the replica's clock and `since` from the
// environment's, which drift apart over a long-running process.
const CLOCK_SKEW_MS = millisecondsInMinute;

/**
 * Whether the latest episode a replica reports is a hydration it finished
 * since `since` that isn't recorded. A recorded episode carries the same
 * finish as the replica reports, since both come from its clock.
 */
export function isUnrecordedEpisode({
  episode,
  since,
  hydrations,
}: {
  episode: ReplicaEpisode;
  since: Date;
  /** The replica's recorded hydrations. */
  hydrations: Hydration[];
}) {
  const finishedMs = episode.finishedAt.getTime();
  return (
    finishedMs >= since.getTime() - CLOCK_SKEW_MS &&
    !hydrations.some(({ finishedAt }) => finishedAt.getTime() >= finishedMs)
  );
}

/** What the tab's terms mean, shared by the detail and table. */
export const HYDRATION_HELP = {
  full: "Hydrating every object on a replica, as after it starts or restarts. Inferred: Materialize doesn't record why a hydration began.",
  additive:
    "Hydrating new objects, such as a new index, on a replica that already has the others. Inferred: Materialize doesn't record why a hydration began.",
  kind: "Full or additive. Inferred: Materialize doesn't record why a hydration began.",
  unfinished:
    "Inferred from replica status: it went offline or ran out of memory with no finished hydration recorded. Materialize records hydrations by visiting replicas in turn, so one that finished shortly before the restart can show here.",
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
  ended:
    "Whether the replica ran out of memory or went offline, from its status.",
  ranFor: "From when the replica began hydrating to when it restarted.",
  sampledMemory:
    "The most memory one process used while it hydrated, as a share of its memory. Materialize samples memory once a minute, so the true peak can be higher.",
  sampledHeap:
    "The most memory plus disk or swap one process used while it hydrated, as a share of its heap limit. A process that reaches the limit runs out of memory. Materialize samples these once a minute, so the true peak can be higher.",
  restarting:
    "Inferred from replica status: it restarted with no finished hydration recorded.",
  offline: "The replica is offline, so it isn't hydrating.",
  starting: "The replica hasn't reported its objects yet.",
};
