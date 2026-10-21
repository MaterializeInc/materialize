// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { median } from "d3";
import { millisecondsInMinute } from "date-fns/constants";

import { SizeAllocation } from "~/api/materialize/cluster/replicaProcessMetrics";
import { OOM_REASON } from "~/api/materialize/cluster/replicaStatusTimeline";

/**
 * The database records no cause for a hydration, so one starting this close to
 * the replica coming online or being created counts as a full hydration.
 */
export const BOOT_WINDOW_MS = 2 * millisecondsInMinute;

/** End events of a replica this close together are one restart. */
const RESTART_MERGE_MS = millisecondsInMinute;

/** How many recent full hydrations `typicalFullHydration` reads. */
const TYPICAL_SAMPLE_SIZE = 6;

export interface HydrationReplica {
  id: string;
  name: string;
  size: string | undefined;
  isCurrent: boolean;
  createdAt: Date | undefined;
  droppedAt: Date | undefined;
}

/**
 * Whether a hydration rebuilt everything on a booting replica, or only
 * objects created on a replica that was already hydrated.
 */
export type HydrationKind = "full" | "newObjects";

/** One finished hydration episode of a replica. */
export interface Hydration {
  /** The replica and start, which identify an episode. */
  id: string;
  replicaId: string;
  startedAt: Date;
  finishedAt: Date;
  durationMs: number;
  objectCount: number;
  /** The highest memory plus disk of one process since it started. */
  peakHeapBytes: number | null;
  peakMemoryBytes: number | null;
  peakDiskBytes: number | null;
  kind: HydrationKind;
}

/** A replica that went offline before it finished hydrating. */
export interface UnfinishedHydration {
  id: string;
  replicaId: string;
  /** When the replica came online for this attempt, if it was recorded. */
  startedAt: Date | undefined;
  endedAt: Date;
  /** Why the replica went offline, such as `oom-killed`. */
  reason: string | null;
}

export interface ReplicaTransition {
  replicaId: string;
  occurredAt: Date;
  status: string;
  reason: string | null;
}

/**
 * The cluster's current replicas, then replicas from its history that are
 * gone, newest first. Current replicas keep their current name and size.
 */
export function mergeHydrationReplicas(
  current: Array<{ id: string; name: string; size: string | null }>,
  history: Array<{
    replicaId: string | null;
    name: string | null;
    size: string | null;
    createdAt: Date;
    droppedAt: Date | null;
  }>,
): HydrationReplica[] {
  const historyById = new Map(
    history.flatMap((row) => (row.replicaId ? [[row.replicaId, row]] : [])),
  );
  const currentIds = new Set(current.map(({ id }) => id));
  const currentReplicas = current.map(({ id, name, size }) => ({
    id,
    name,
    size: size ?? undefined,
    isCurrent: true,
    createdAt: historyById.get(id)?.createdAt,
    droppedAt: undefined,
  }));
  const dropped = [...historyById.entries()]
    .filter(([id]) => !currentIds.has(id))
    .map(([id, row]) => ({
      id,
      name: row.name ?? id,
      size: row.size ?? undefined,
      isCurrent: false,
      createdAt: row.createdAt,
      droppedAt: row.droppedAt ?? undefined,
    }))
    .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
  return [...currentReplicas, ...dropped];
}

/** When each replica booted: its creation and each time it came online. */
export function replicaBootTimes(
  transitions: ReplicaTransition[],
  replicas: HydrationReplica[],
) {
  const boots = new Map<string, number[]>();
  const add = (replicaId: string, ms: number) =>
    boots.set(replicaId, [...(boots.get(replicaId) ?? []), ms]);
  for (const { id, createdAt } of replicas) {
    if (createdAt) add(id, createdAt.getTime());
  }
  for (const { replicaId, status, occurredAt } of transitions) {
    if (status === "online") add(replicaId, occurredAt.getTime());
  }
  for (const times of boots.values()) times.sort((a, b) => a - b);
  return boots;
}

/**
 * Hydration episode rows as hydrations, oldest first, each with its kind. A
 * hydration is full when it begins near a boot, or covers at least the objects
 * the cluster had when it began: a zero-downtime upgrade rehydrates on new
 * processes while the environment is read-only, which records no boot.
 * `objectCountAt` is undefined while the cluster's objects are unknown.
 */
export function transformHydrations(
  rows: Array<{
    replicaId: string;
    startedAt: Date;
    finishedAt: Date;
    objectCount: number;
    peakHeapBytes: number | null;
    peakMemoryBytes: number | null;
    peakDiskBytes: number | null;
  }>,
  bootTimes: Map<string, number[]>,
  objectCountAt?: (ms: number) => number,
): Hydration[] {
  return rows
    .map((row) => {
      const startMs = row.startedAt.getTime();
      const isBoot = (bootTimes.get(row.replicaId) ?? []).some(
        (bootMs) => Math.abs(bootMs - startMs) <= BOOT_WINDOW_MS,
      );
      const coversAll =
        objectCountAt !== undefined &&
        row.objectCount > 0 &&
        row.objectCount >= objectCountAt(startMs);
      return {
        ...row,
        id: `${row.replicaId}|${startMs}`,
        durationMs: row.finishedAt.getTime() - startMs,
        kind: isBoot || coversAll ? "full" : "newObjects",
      } satisfies Hydration;
    })
    .sort((a, b) => a.startedAt.getTime() - b.startedAt.getTime());
}

/**
 * Times a replica went offline, or a process was killed for running out of
 * memory, before the replica finished hydrating, from `startMs` on. A replica
 * going offline when it's dropped doesn't count.
 *
 * An attempt runs from the replica's previous restart, or its creation, to the
 * restart. Online rows don't bound it: the one a zero-downtime upgrade writes
 * comes after the replica already hydrated. An attempt that began before
 * `startMs` is skipped, since the hydrations before it aren't read.
 *
 * NOTE: the history can miss an episode that finished, so a restart after
 * one can read as unfinished.
 */
export function findUnfinishedHydrations({
  transitions,
  oomKills,
  hydrations,
  replicas,
  bootTimes,
  startMs,
}: {
  /** Process 0's transitions, which stand for the replica's. */
  transitions: ReplicaTransition[];
  /** OOM kills of any process. */
  oomKills: ReplicaTransition[];
  /** Every finished hydration, including ones with no objects. */
  hydrations: Hydration[];
  replicas: HydrationReplica[];
  bootTimes: Map<string, number[]>;
  startMs: number;
}): UnfinishedHydration[] {
  const replicasById = new Map(
    replicas.map((replica) => [replica.id, replica]),
  );
  // On Kubernetes, a new replica's pod records an offline row before it first
  // comes online, so only an offline row after an online one is a restart.
  const wentOffline: ReplicaTransition[] = [];
  const lastStatusById = new Map<string, string>();
  for (const transition of [...transitions].sort(
    (a, b) => a.occurredAt.getTime() - b.occurredAt.getTime(),
  )) {
    if (
      transition.status === "offline" &&
      lastStatusById.get(transition.replicaId) === "online"
    ) {
      wentOffline.push(transition);
    }
    lastStatusById.set(transition.replicaId, transition.status);
  }
  const ends = [...wentOffline, ...oomKills].sort(
    (a, b) => a.occurredAt.getTime() - b.occurredAt.getTime(),
  );

  // One restart can be recorded as process 0 going offline and as OOM kills
  // on several processes. Keep the first, with an OOM reason if any has one.
  const restarts: ReplicaTransition[] = [];
  for (const end of ends) {
    const previous = restarts.findLast(
      ({ replicaId }) => replicaId === end.replicaId,
    );
    if (
      previous &&
      end.occurredAt.getTime() - previous.occurredAt.getTime() <=
        RESTART_MERGE_MS
    ) {
      if (end.reason === OOM_REASON) previous.reason = OOM_REASON;
      continue;
    }
    restarts.push({ ...end });
  }

  return restarts.flatMap(({ replicaId, occurredAt, reason }, index) => {
    const endMs = occurredAt.getTime();
    if (endMs < startMs) return [];
    const replica = replicasById.get(replicaId);
    const droppedMs = replica?.droppedAt?.getTime();
    if (
      reason !== OOM_REASON &&
      droppedMs !== undefined &&
      endMs >= droppedMs - RESTART_MERGE_MS
    ) {
      return [];
    }
    const previousRestartMs = restarts
      .slice(0, index)
      .findLast((restart) => restart.replicaId === replicaId)
      ?.occurredAt.getTime();
    const attemptStartMs = previousRestartMs ?? replica?.createdAt?.getTime();
    if (attemptStartMs === undefined || attemptStartMs < startMs) return [];
    const finished = hydrations.some(
      (hydration) =>
        hydration.replicaId === replicaId &&
        hydration.finishedAt.getTime() > attemptStartMs &&
        hydration.finishedAt.getTime() <= endMs,
    );
    if (finished) return [];
    // When it came online for this attempt, if that was recorded.
    const bootMs =
      (bootTimes.get(replicaId) ?? []).findLast(
        (ms) => ms >= attemptStartMs && ms < endMs,
      ) ?? attemptStartMs;
    return [
      {
        id: `${replicaId}|${endMs}`,
        replicaId,
        startedAt: new Date(bootMs),
        endedAt: occurredAt,
        reason,
      },
    ];
  });
}

/** When a replica last booted: its creation or the last time it came online. */
export const latestBoot = (
  bootTimes: Map<string, number[]>,
  replicaId: string,
) => {
  const ms = bootTimes.get(replicaId)?.at(-1);
  return ms === undefined ? undefined : new Date(ms);
};

/** The latest full hydration of `replicaId`. */
export const latestFullHydration = (
  hydrations: Hydration[],
  replicaId: string,
) =>
  hydrations.findLast(
    (hydration) =>
      hydration.replicaId === replicaId && hydration.kind === "full",
  );

/**
 * The median duration of the latest full hydrations on replicas of `size`,
 * with the range it was taken over. Undefined without one.
 */
export function typicalFullHydration(
  hydrations: Hydration[],
  replicas: HydrationReplica[],
  size: string | undefined,
) {
  const ofSize = new Set(
    replicas.filter((replica) => replica.size === size).map(({ id }) => id),
  );
  const durations = hydrations
    .filter(({ kind, replicaId }) => kind === "full" && ofSize.has(replicaId))
    .slice(-TYPICAL_SAMPLE_SIZE)
    .map(({ durationMs }) => durationMs);
  const medianMs = median(durations);
  if (medianMs === undefined) return undefined;
  return {
    medianMs,
    minMs: Math.min(...durations),
    maxMs: Math.max(...durations),
    count: durations.length,
  };
}

/**
 * A replica's per-process heap limit: the limit it reports, or its size's
 * memory plus disk for a replica that no longer reports.
 */
export const heapLimitBytes = (
  replica: HydrationReplica,
  reportedLimits: Map<string, number> | undefined,
  allocationsBySize: Map<string, SizeAllocation> | undefined,
) => {
  const reported = reportedLimits?.get(replica.id);
  if (reported !== undefined) return reported;
  const allocation = replica.size
    ? allocationsBySize?.get(replica.size)
    : undefined;
  return allocation
    ? allocation.memoryBytes + (allocation.diskBytes ?? 0)
    : undefined;
};

/** One object's hydration within an episode, in ms from the episode's start. */
export interface ObjectHydrationSpan {
  globalId: string;
  /** The catalog id, null once the object is dropped. */
  objectId: string | null;
  installedMs: number;
  /** When it got a hydration slot. Equals `installedMs` when not recorded. */
  startedMs: number;
  hydratedMs: number | undefined;
  /** Time spent waiting for a hydration slot. */
  queuedMs: number;
  workingMs: number | undefined;
}

/** Object hydration rows of an episode, in the order they started. */
export function transformObjectHydrationSpans(
  rows: Array<{
    globalId: string;
    objectId: string | null;
    installedAt: Date;
    startedAt: Date | null;
    hydratedAt: Date | null;
  }>,
  episodeStartedAt: Date,
): ObjectHydrationSpan[] {
  const origin = episodeStartedAt.getTime();
  return rows
    .map(({ globalId, objectId, installedAt, startedAt, hydratedAt }) => {
      const installedMs = installedAt.getTime() - origin;
      const startedMs = (startedAt ?? installedAt).getTime() - origin;
      const hydratedMs = hydratedAt ? hydratedAt.getTime() - origin : undefined;
      return {
        globalId,
        objectId,
        installedMs,
        startedMs,
        hydratedMs,
        queuedMs: startedMs - installedMs,
        workingMs:
          hydratedMs === undefined ? undefined : hydratedMs - startedMs,
      };
    })
    .sort((a, b) => a.startedMs - b.startedMs || a.installedMs - b.installedMs);
}

/** The object that worked longest, and how much objects waited for a slot. */
export function summarizeObjectSpans<Span extends ObjectHydrationSpan>(
  spans: Span[],
) {
  const slowest = spans.reduce<Span | undefined>(
    (longest, span) =>
      (span.workingMs ?? 0) > (longest?.workingMs ?? -1) ? span : longest,
    undefined,
  );
  const waited = spans.filter(({ queuedMs }) => queuedMs > 0);
  return {
    slowest,
    waitedCount: waited.length,
    maxQueuedMs: Math.max(0, ...waited.map(({ queuedMs }) => queuedMs)),
  };
}
