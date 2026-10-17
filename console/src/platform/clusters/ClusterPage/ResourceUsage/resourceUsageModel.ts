// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { greatest, max, sort } from "d3";
import { millisecondsInHour, millisecondsInMinute } from "date-fns/constants";

import {
  DataPoint,
  ReplicaData,
} from "~/platform/clusters/ClusterOverview/types";

export type ReplicaState = "running" | "hydrating" | "offline";

export interface StatusSegment {
  state: ReplicaState;
  startMs: number;
  endMs: number;
}

export interface ReplicaTimeline {
  segments: StatusSegment[];
  oomAtMs: number[];
}

export interface StatusTransition {
  occurredAt: Date;
  status: string;
  reason: string | null;
}

export interface HydrationEpisode {
  startedAt: Date;
  finishedAt: Date | null;
}

export interface DdlEvent {
  id: string;
  name: string;
  objectType: string;
  occurredAtMs: number;
}

export const OOM_REASON = "oom-killed";

const paintHydrating = (
  segments: StatusSegment[],
  startMs: number,
  endMs: number,
): StatusSegment[] =>
  segments.flatMap((segment) => {
    if (
      segment.state !== "running" ||
      endMs <= segment.startMs ||
      startMs >= segment.endMs
    ) {
      return [segment];
    }
    const pieces: StatusSegment[] = [];
    if (startMs > segment.startMs) {
      pieces.push({
        state: "running",
        startMs: segment.startMs,
        endMs: startMs,
      });
    }
    pieces.push({
      state: "hydrating",
      startMs: Math.max(startMs, segment.startMs),
      endMs: Math.min(endMs, segment.endMs),
    });
    if (endMs < segment.endMs) {
      pieces.push({ state: "running", startMs: endMs, endMs: segment.endMs });
    }
    return pieces;
  });

const mergeAdjacent = (segments: StatusSegment[]) =>
  segments.reduce<StatusSegment[]>((merged, segment) => {
    const last = merged.at(-1);
    if (
      last &&
      last.state === segment.state &&
      last.endMs === segment.startMs
    ) {
      last.endMs = segment.endMs;
    } else {
      merged.push({ ...segment });
    }
    return merged;
  }, []);

/**
 * One replica's status over `[windowStartMs, windowEndMs]`.
 *
 * `transitions` may include transitions before the window, and the last of
 * those sets the starting state. A replica is running while online.
 * Finished hydration episodes paint over running time. A hydration in progress
 * has no recorded start, so it isn't drawn. Without a transition before the
 * window, `sampleRange` (when the replica reported readings) is shown as running
 * up to the first transition.
 */
export function transformReplicaTimeline({
  transitions,
  hydrationEpisodes,
  windowStartMs,
  windowEndMs,
  sampleRange,
}: {
  transitions: StatusTransition[];
  hydrationEpisodes: HydrationEpisode[];
  windowStartMs: number;
  windowEndMs: number;
  sampleRange?: { startMs: number; endMs: number };
}): ReplicaTimeline {
  const sorted = sort(transitions, (transition) =>
    transition.occurredAt.getTime(),
  );
  const oomAtMs: number[] = [];
  let segments: StatusSegment[] = [];

  sorted.forEach((transition, transitionIndex) => {
    const atMs = transition.occurredAt.getTime();
    const isOnline = transition.status === "online";
    if (
      !isOnline &&
      transition.reason === OOM_REASON &&
      atMs >= windowStartMs &&
      atMs <= windowEndMs
    ) {
      oomAtMs.push(atMs);
    }
    const next = sorted.at(transitionIndex + 1);
    const startMs = Math.max(atMs, windowStartMs);
    const endMs = Math.min(
      next ? next.occurredAt.getTime() : windowEndMs,
      windowEndMs,
    );
    if (endMs > startMs) {
      segments.push({
        state: isOnline ? "running" : "offline",
        startMs,
        endMs,
      });
    }
  });

  // Status history older than 30 days is deleted, so a long-running replica's
  // last transition before the window can be gone while its readings remain.
  const firstAtMs = sorted.at(0)?.occurredAt.getTime();
  if (sampleRange && (firstAtMs === undefined || firstAtMs > windowStartMs)) {
    const startMs = Math.max(sampleRange.startMs, windowStartMs);
    const endMs = Math.min(
      sampleRange.endMs,
      firstAtMs ?? windowEndMs,
      windowEndMs,
    );
    if (endMs > startMs) segments.unshift({ state: "running", startMs, endMs });
  }

  segments = mergeAdjacent(segments);

  for (const { startedAt, finishedAt } of hydrationEpisodes) {
    if (finishedAt) {
      segments = paintHydrating(
        segments,
        startedAt.getTime(),
        finishedAt.getTime(),
      );
    }
  }

  return { segments: mergeAdjacent(segments), oomAtMs };
}

/**
 * Current replicas whose latest transition brought them online at or after
 * `sinceMs`, the only ones whose hydration the timeline can place.
 */
export function replicasRestartedSince(
  transitions: Array<StatusTransition & { replicaId: string }>,
  currentReplicaIds: ReadonlySet<string>,
  sinceMs: number,
) {
  const latest = new Map<string, StatusTransition>();
  for (const transition of transitions) {
    const previous = latest.get(transition.replicaId);
    if (!previous || transition.occurredAt > previous.occurredAt) {
      latest.set(transition.replicaId, transition);
    }
  }
  return [...latest.entries()]
    .filter(
      ([replicaId, transition]) =>
        currentReplicaIds.has(replicaId) &&
        transition.status === "online" &&
        transition.occurredAt.getTime() >= sinceMs,
    )
    .map(([replicaId]) => replicaId)
    .sort();
}

/** Joins creation times to the objects they name, oldest first. */
export function transformDdlEvents(
  objects: Array<{ id: string; name: string; objectType: string }>,
  creationTimes: Array<{ id: string | null; occurredAt: Date }>,
): DdlEvent[] {
  const objectsById = new Map(objects.map((object) => [object.id, object]));
  const events = creationTimes.flatMap(({ id, occurredAt }) => {
    const object = id === null ? undefined : objectsById.get(id);
    return object
      ? [
          {
            id: object.id,
            name: object.name,
            objectType: object.objectType,
            occurredAtMs: occurredAt.getTime(),
          },
        ]
      : [];
  });
  return sort(events, (event) => event.occurredAtMs);
}

/**
 * Why a window has no readings. A down replica and missing metrics collection
 * both report empty readings, so replica status tells them apart:
 * `unmeasured` when a replica was up, `offlineOom` or `offline` when every
 * replica was down (by the last offline reason), and `unknown` without status.
 */
export type MissingUsageCause =
  "unmeasured" | "offlineOom" | "offline" | "unknown";

export function missingUsageCause({
  timelines,
  transitions,
  isStatusError,
}: {
  timelines: ReplicaTimeline[];
  transitions: StatusTransition[];
  isStatusError: boolean;
}): MissingUsageCause {
  const states = timelines.flatMap(({ segments }) =>
    segments.map(({ state }) => state),
  );
  if (isStatusError || states.length === 0) return "unknown";
  if (states.some((state) => state !== "offline")) return "unmeasured";
  const lastOffline = greatest(
    transitions.filter(({ status }) => status !== "online"),
    (transition) => transition.occurredAt.getTime(),
  );
  return lastOffline?.reason === OOM_REASON ? "offlineOom" : "offline";
}

export const stateAt = (timeline: ReplicaTimeline, timeMs: number) =>
  timeline.segments.find(
    (segment) => segment.startMs <= timeMs && timeMs < segment.endMs,
  )?.state;

export const hasOomBetween = (
  timeline: ReplicaTimeline,
  startMs: number,
  endMs: number,
) => timeline.oomAtMs.some((atMs) => atMs >= startMs && atMs < endMs);

/** The data point whose bucket contains `timeMs`, if any. */
export const datumAt = (data: DataPoint[], timeMs: number) =>
  data.find((point) => point.bucketStart <= timeMs && timeMs < point.bucketEnd);

/**
 * Merges a replica's buckets into time-aligned groups `groupMs` wide. Each
 * metric keeps its peak, as the source buckets do, and the memory split comes
 * from the group's max-heap bucket so it matches the bar.
 */
export function mergeBuckets(data: DataPoint[], groupMs: number) {
  const groups = new Map<number, DataPoint[]>();
  for (const point of data) {
    const groupStartMs = Math.floor(point.bucketStart / groupMs) * groupMs;
    const group = groups.get(groupStartMs);
    if (group) group.push(point);
    else groups.set(groupStartMs, [point]);
  }
  return [...groups.values()].map((points): DataPoint => {
    const first = points[0];
    const last = points[points.length - 1];
    if (points.length === 1) return first;
    const heapPeak =
      greatest(points, (point) => point.heapPercent ?? -Infinity) ?? last;
    // d3's max skips nulls and is undefined when every value is null.
    const peakOf = (metric: (point: DataPoint) => number | null) =>
      max(points, metric) ?? null;
    return {
      ...last,
      bucketStart: first.bucketStart,
      bucketEnd: last.bucketEnd,
      cpuPercent: peakOf((point) => point.cpuPercent),
      memoryPercent: peakOf((point) => point.memoryPercent),
      diskPercent: peakOf((point) => point.diskPercent),
      maxMemoryAndDiskPercent: peakOf((point) => point.maxMemoryAndDiskPercent),
      heapPercent: heapPeak.heapPercent,
      swapPercent: heapPeak.swapPercent,
      ramLimitPercent: heapPeak.ramLimitPercent,
      offlineEvents: points.flatMap((point) => point.offlineEvents),
    };
  });
}

/**
 * Each replica's group, keyed by the id that stands for its cluster and name:
 * the current replica of that name, else the one that reported last. A
 * scheduled cluster gets a new replica id under the same name on every
 * refresh. A cluster holds one live replica per name (`replica_id_by_name_` in
 * the catalog), so a name's replicas in one cluster ran one after another
 * unless one was renamed. A past blue-green deployment is another cluster, and
 * its replicas run alongside the current ones while those hydrate, so they
 * keep their own groups.
 */
export function groupReplicasByName(
  replicas: Array<{
    id: string;
    name: string;
    clusterId: string;
    isCurrent: boolean;
  }>,
  lastReportedMs: Map<string, number>,
) {
  const groupKey = ({ clusterId, name }: (typeof replicas)[number]) =>
    `${clusterId}\0${name}`;
  const membersByKey = new Map<string, typeof replicas>();
  for (const replica of replicas) {
    membersByKey.set(groupKey(replica), [
      ...(membersByKey.get(groupKey(replica)) ?? []),
      replica,
    ]);
  }
  const groupIdByKey = new Map(
    [...membersByKey].map(([key, members]) => [
      key,
      (
        members.find((replica) => replica.isCurrent) ??
        greatest(members, (replica) => lastReportedMs.get(replica.id) ?? 0) ??
        members[0]
      ).id,
    ]),
  );
  return new Map(
    replicas.map((replica) => [
      replica.id,
      groupIdByKey.get(groupKey(replica)) ?? replica.id,
    ]),
  );
}

/**
 * One status timeline for replicas that ran one after another. Where they
 * overlap, the later segment wins.
 */
export function mergeTimelines(timelines: ReplicaTimeline[]): ReplicaTimeline {
  if (timelines.length === 1) return timelines[0];
  const segments = sort(
    timelines.flatMap((timeline) => timeline.segments),
    (segment) => segment.startMs,
  ).map((segment, index, sorted) => ({
    ...segment,
    endMs: Math.min(segment.endMs, sorted[index + 1]?.startMs ?? Infinity),
  }));
  return {
    segments: mergeAdjacent(
      segments.filter((segment) => segment.endMs > segment.startMs),
    ),
    oomAtMs: sort(timelines.flatMap((timeline) => timeline.oomAtMs)),
  };
}

/**
 * The series with a memory bar in each bucket, in series order, so bars at one
 * time share only that time's slot.
 */
export function seriesWithBarAt(series: ReplicaData[]) {
  const idsByBucketStart = new Map<number, string[]>();
  for (const { id, data } of series) {
    for (const point of data) {
      if (!memoryBar(point)) continue;
      const ids = idsByBucketStart.get(point.bucketStart);
      if (ids) ids.push(id);
      else idsByBucketStart.set(point.bucketStart, [id]);
    }
  }
  return idsByBucketStart;
}

/** The most memory bars at one time, at least 1. */
export const mostBarsAtOnce = (series: ReplicaData[]) =>
  max([...seriesWithBarAt(series).values()], (ids) => ids.length) ?? 1;

const BUCKET_WIDTHS_MS = [
  ...[1, 2, 3, 5, 6, 10, 15, 20, 30].map(
    (minutes) => minutes * millisecondsInMinute,
  ),
  ...[1, 2, 3, 4, 6, 8, 12, 24, 48, 168].map(
    (hours) => hours * millisecondsInHour,
  ),
];

/**
 * The bucket width that gives each bar at least `minSlotPx` when
 * `replicaCount` bars share a time: the source width when that already does,
 * else the narrowest round width that is a multiple of it.
 */
export function mergedBucketMs({
  sourceBucketMs,
  domainMs,
  plotWidthPx,
  replicaCount,
  minSlotPx,
}: {
  sourceBucketMs: number;
  domainMs: number;
  plotWidthPx: number;
  replicaCount: number;
  minSlotPx: number;
}) {
  const slotPx =
    (plotWidthPx * sourceBucketMs) / domainMs / Math.max(1, replicaCount);
  if (slotPx >= minSlotPx || slotPx <= 0) return sourceBucketMs;
  const neededMs = (sourceBucketMs * minSlotPx) / slotPx;
  return (
    BUCKET_WIDTHS_MS.find(
      (widthMs) => widthMs >= neededMs && widthMs % sourceBucketMs === 0,
    ) ?? sourceBucketMs * Math.ceil(minSlotPx / slotPx)
  );
}

/** Current replicas first, then by name. */
export const sortReplicasForDisplay = <
  T extends { id: string; name: string; isCurrent: boolean },
>(
  replicas: T[],
) =>
  sort(
    replicas,
    (replica) => !replica.isCurrent,
    (replica) => replica.name,
    (replica) => replica.id,
  );

/**
 * Colors replicas in display order, so a replica keeps its color when others
 * are hidden.
 */
export function assignReplicaColors(
  replicas: Array<{ id: string; name: string; isCurrent: boolean }>,
  palette: string[],
) {
  return new Map(
    sortReplicasForDisplay(replicas).map((replica, replicaIndex) => [
      replica.id,
      palette[replicaIndex % palette.length],
    ]),
  );
}

/** Whether the point's replica size has swap beyond its RAM. */
export const canSwap = (
  point: DataPoint,
): point is DataPoint & { ramLimitPercent: number } =>
  point.ramLimitPercent !== null && point.ramLimitPercent < 100;

/**
 * A memory bar's height and the swap at its top, both as percentages of the
 * heap limit.
 */
export const memoryBar = (point: DataPoint) => {
  if (point.heapPercent === null) return undefined;
  return {
    heap: point.heapPercent,
    // Heap includes swap, so this only trims sampling skew.
    swap: Math.min(point.swapPercent ?? 0, point.heapPercent),
  };
};

/**
 * Where RAM ends below the heap limit, for one line across the chart. Undefined
 * when no replica can swap, or when replicas of different sizes put it in
 * different places.
 */
export function ramLimitPercent(series: DataPoint[][]) {
  // Keyed on a rounding so float noise doesn't read as a different size.
  const limits = new Map<number, number>();
  for (const data of series) {
    for (const point of data) {
      if (canSwap(point)) {
        limits.set(
          Math.round(point.ramLimitPercent * 10) / 10,
          point.ramLimitPercent,
        );
      }
    }
  }
  return limits.size === 1 ? [...limits.values()][0] : undefined;
}

/**
 * One process's heap limit in bytes, behind the chart's 100% line, when every
 * charted replica has the same size. Undefined otherwise, since the line then
 * stands for different byte counts.
 */
export function sharedHeapLimitBytes(
  chartedSizes: Array<string | undefined>,
  heapLimitBytesBySize: Map<string, number>,
) {
  const sizes = new Set(chartedSizes);
  if (sizes.size !== 1) return undefined;
  const [size] = sizes;
  return size === undefined ? undefined : heapLimitBytesBySize.get(size);
}
