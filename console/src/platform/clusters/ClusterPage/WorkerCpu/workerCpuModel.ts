// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { millisecondsInMinute } from "date-fns/constants";

import { ProcessMetricsSample } from "~/api/materialize/cluster/replicaProcessMetrics";
import docUrls from "~/mz-doc-urls.json";

/**
 * @module
 * Turns cumulative per-worker CPU counters into what the workers drawer shows.
 *
 * A worker's counter is the time it spent in every running dataflow, each
 * from when the dataflow started, so one reading is a lifetime total. The
 * difference between two readings is the time spent in between, and dividing
 * it by the wall-clock time between them gives the share of one core a worker
 * used. A worker is one thread, so that share is at most 1.
 *
 * NOTE: a dataflow's time leaves the counters when it shuts down, so the
 * counters fall when a dataflow is dropped or the replica restarts, and work
 * in dataflows that finished between two readings is never seen.
 */

export const CPU_DOCS =
  docUrls["/docs/clusters/troubleshoot-clusters/cpu-troubleshooting/"];

/** One read of every worker's cumulative dataflow time on a replica. */
export interface WorkerCpuReading {
  atMs: number;
  elapsedNsByWorker: Map<number, number>;
  /** The first reading after a pause, so no point spans the pause. */
  afterGap?: boolean;
  /**
   * A counter fell since the previous reading, so the two don't compare and
   * nothing is measured across them.
   */
  afterReset?: boolean;
}

export interface WorkerBusy {
  workerId: number;
  /** Share of one core spent running dataflows. */
  busyFraction: number;
}

export interface WorkerBusyPoint {
  atMs: number;
  busiest: WorkerBusy;
  averageBusyFraction: number;
  /** The first point after a gap, so a line doesn't bridge it. */
  afterGap?: boolean;
}

// A counter that fell lost a dataflow's time, so its time since the earlier
// reading is unknown. Treating it as zero avoids negative work.
const delta = (before: number | undefined, after: number) =>
  before === undefined ? after : Math.max(0, after - before);

/** Whether any worker's counter fell between two readings. */
export function hasCounterReset(
  before: Map<number, number>,
  after: Map<number, number>,
) {
  for (const [workerId, ns] of after) {
    if (ns < (before.get(workerId) ?? 0)) return true;
  }
  return false;
}

const isSegmentStart = (reading: WorkerCpuReading) =>
  Boolean(reading.afterGap || reading.afterReset);

/** Each worker's busy share between two readings, busiest first. */
export function workerBusyBetween(
  previous: WorkerCpuReading,
  next: WorkerCpuReading,
): WorkerBusy[] {
  const elapsedNs = (next.atMs - previous.atMs) * 1e6;
  if (elapsedNs <= 0) return [];
  return [...next.elapsedNsByWorker]
    .map(([workerId, ns]) => ({
      workerId,
      busyFraction: Math.min(
        1,
        delta(previous.elapsedNsByWorker.get(workerId), ns) / elapsedNs,
      ),
    }))
    .sort((a, b) => b.busyFraction - a.busyFraction || a.workerId - b.workerId);
}

const averageBusy = (workers: WorkerBusy[]) =>
  workers.length === 0
    ? 0
    : workers.reduce((sum, worker) => sum + worker.busyFraction, 0) /
      workers.length;

/**
 * How much time each plotted point averages over. Replicas update the
 * counters once per introspection interval (1s by default), so a short window
 * can catch one tick more or fewer than the next: 20% of a 5s window, 3% of
 * 30s.
 */
export const SMOOTHING_WINDOW_MS = 30_000;
// A point needs at least this much time behind it to be worth drawing.
const MIN_POINT_WINDOW_MS = 10_000;

/**
 * The busiest and average worker at each reading, each over the trailing
 * `windowMs`, or less early on or right after a pause.
 */
export function workerBusySeries(
  readings: WorkerCpuReading[],
  windowMs = SMOOTHING_WINDOW_MS,
): WorkerBusyPoint[] {
  const points: WorkerBusyPoint[] = [];
  let segmentStart = 0;
  let isNewSegment = false;
  for (let i = 1; i < readings.length; i++) {
    if (isSegmentStart(readings[i])) {
      segmentStart = i;
      isNewSegment = points.length > 0;
      continue;
    }
    const atMs = readings[i].atMs;
    let from = segmentStart;
    while (from + 1 < i && atMs - readings[from + 1].atMs >= windowMs) {
      from++;
    }
    if (atMs - readings[from].atMs < Math.min(windowMs, MIN_POINT_WINDOW_MS)) {
      continue;
    }
    const workers = workerBusyBetween(readings[from], readings[i]);
    if (workers.length === 0) continue;
    points.push({
      atMs,
      busiest: workers[0],
      averageBusyFraction: averageBusy(workers),
      ...(isNewSegment && { afterGap: true }),
    });
    isNewSegment = false;
  }
  return points;
}

/**
 * The earliest reading the verdict compares against: within `windowMs` of the
 * last one, and not before a counter reset.
 */
export function verdictWindowStart(
  readings: WorkerCpuReading[],
  windowMs: number,
) {
  const last = readings.at(-1);
  if (!last) return undefined;
  let start = readings.length - 1;
  while (
    start > 0 &&
    !readings[start].afterReset &&
    last.atMs - readings[start - 1].atMs <= windowMs
  ) {
    start--;
  }
  return readings[start];
}

/**
 * A worker past this many times the other workers' average carries skewed
 * work. The dataflow troubleshooting docs use 2 as their example threshold.
 */
export const SKEW_RATIO = 2;
/** A worker this busy is at its core's limit, so work routed to it waits. */
const SATURATED_FRACTION = 0.9;
/**
 * Skew only costs anything as a worker nears its core's limit, so below this
 * the busiest worker's lead over the average isn't worth a warning.
 */
const UNEVEN_FRACTION = 0.5;
/**
 * Workers this busy on average leave the replica under 10% idle, the CPU
 * troubleshooting guide's rule of thumb for having no headroom.
 */
export const ALL_BUSY_FRACTION = 0.9;
/** An object with this share of all dataflow time dominates the replica. */
const DOMINANT_SHARE = 0.5;
// Below these the replica did almost no dataflow work, so ratios are noise.
const IDLE_AVERAGE_FRACTION = 0.01;
const IDLE_BUSIEST_FRACTION = 0.05;

export type WorkerVerdict =
  | { kind: "idle" }
  | {
      /** `light`: uneven, but no worker is near its core's limit. */
      kind: "saturated" | "allBusy" | "uneven" | "light" | "balanced";
      busiest: WorkerBusy;
      averageBusyFraction: number;
      /** Every worker but the busiest, the average skew is judged against. */
      othersBusyFraction: number;
    };

/**
 * What a window's worker shares say, from the most to the least urgent.
 *
 * NOTE: skew compares the busiest worker with the others. An average that
 * includes the busiest worker can't pass `SKEW_RATIO` on two workers unless
 * the other is fully idle.
 */
export function workerVerdict(workers: WorkerBusy[]): WorkerVerdict {
  const busiest = workers[0];
  const averageBusyFraction = averageBusy(workers);
  const othersBusyFraction = averageBusy(workers.slice(1));
  if (
    !busiest ||
    (averageBusyFraction < IDLE_AVERAGE_FRACTION &&
      busiest.busyFraction < IDLE_BUSIEST_FRACTION)
  ) {
    return { kind: "idle" };
  }
  const result = { busiest, averageBusyFraction, othersBusyFraction };
  if (averageBusyFraction >= ALL_BUSY_FRACTION) {
    return { kind: "allBusy", ...result };
  }
  const isSkewed =
    workers.length > 1 &&
    busiest.busyFraction >= SKEW_RATIO * othersBusyFraction;
  if (isSkewed && busiest.busyFraction >= SATURATED_FRACTION) {
    return { kind: "saturated", ...result };
  }
  if (isSkewed && busiest.busyFraction >= UNEVEN_FRACTION) {
    return { kind: "uneven", ...result };
  }
  if (isSkewed) return { kind: "light", ...result };
  return { kind: "balanced", ...result };
}

/** Replica CPU this high is worth explaining, whatever the workers show. */
const HIGH_CPU_FRACTION = 0.75;

export type CpuVerdict =
  | WorkerVerdict
  | {
      kind: "highCpu";
      replicaCpuFraction: number;
      /** The share of the replica's allocation spent running dataflows. */
      dataflowFraction: number;
      /**
       * The rest of the replica's CPU. It includes dataflows that finished
       * between readings, since a dataflow's time leaves the counters when it
       * shuts down.
       */
      outsideFraction: number;
      busiest: WorkerBusy;
      /** Set when the work is also uneven, short of saturating a worker. */
      uneven?: { busiest: WorkerBusy; othersBusyFraction: number };
    };

/** Whether a verdict says one worker carries more than its share. */
export const isSkewVerdict = (verdict: CpuVerdict | undefined) =>
  verdict?.kind === "saturated" || verdict?.kind === "uneven";

/**
 * A saturated worker is the most specific finding, so it wins. Otherwise a
 * replica near its allocation leads with that, even with idle workers, since
 * its CPU then goes to work outside the running dataflows, and uneven work
 * short of a saturated worker becomes a note on it.
 * `replicaCpuFraction` is the replica's CPU as a share of its allocation over
 * the same window, from its metrics.
 */
export function cpuVerdict(
  workers: WorkerBusy[],
  replicaCpuFraction: number | undefined,
  workersPerCore: number,
): CpuVerdict {
  const verdict = workerVerdict(workers);
  if (
    workers.length > 0 &&
    replicaCpuFraction !== undefined &&
    replicaCpuFraction >= HIGH_CPU_FRACTION &&
    verdict.kind !== "saturated"
  ) {
    const dataflowFraction = averageBusy(workers) * workersPerCore;
    return {
      kind: "highCpu",
      replicaCpuFraction,
      dataflowFraction,
      outsideFraction: Math.max(0, replicaCpuFraction - dataflowFraction),
      busiest: workers[0],
      ...(verdict.kind === "uneven" && {
        uneven: {
          busiest: verdict.busiest,
          othersBusyFraction: verdict.othersBusyFraction,
        },
      }),
    };
  }
  return verdict;
}

/**
 * Workers per core of the replica's CPU allocation, which turns the average
 * worker's share of a core into a share of the allocation. Production sizes
 * run one worker per core, and some test sizes run fewer.
 */
export const workersPerCore = (
  workersPerProcess: number,
  cpuNanoCoresPerProcess: number,
) =>
  cpuNanoCoresPerProcess > 0
    ? workersPerProcess / (cpuNanoCoresPerProcess / 1e9)
    : 1;

/** Below this ratio an object's work counts as even across its workers. */
export const EVEN_RATIO = 1.25;
/** An object with less of the dataflow time than this is too small to flag. */
export const NOTABLE_SHARE = 0.01;

/**
 * The busiest worker's cumulative time over the other workers' average, from
 * one reading. Counters start with each dataflow, so this is a lifetime skew,
 * available before a second reading can give a rate. `ratio` is infinite when
 * the others have done nothing.
 */
export function lifetimeWorkerSkew(reading: WorkerCpuReading) {
  let busiest: { workerId: number; elapsedNs: number } | undefined;
  let totalNs = 0;
  for (const [workerId, elapsedNs] of reading.elapsedNsByWorker) {
    totalNs += elapsedNs;
    if (!busiest || elapsedNs > busiest.elapsedNs) {
      busiest = { workerId, elapsedNs };
    }
  }
  const workerCount = reading.elapsedNsByWorker.size;
  if (!busiest || totalNs === 0 || workerCount < 2) return undefined;
  const othersAverageNs = (totalNs - busiest.elapsedNs) / (workerCount - 1);
  return {
    workerId: busiest.workerId,
    ratio: othersAverageNs > 0 ? busiest.elapsedNs / othersAverageNs : Infinity,
  };
}

/** One dataflow's cumulative time on one worker. */
export interface DataflowWorkerCpu {
  dataflowId: string;
  workerId: number;
  elapsedNs: number;
}

export interface DataflowExport {
  objectId: string;
  dataflowId: string;
}

/** The row that pools the temporary dataflows built for ad-hoc queries. */
export const ADHOC_ROW_ID = "adhoc";

export interface ObjectCpuRow {
  /** A user object's id, or `ADHOC_ROW_ID`. */
  objectId: string;
  /** How many dataflows the row pools: 1 for an object. */
  dataflowCount: number;
  /** Indexed by worker id. */
  perWorkerNs: number[];
  totalNs: number;
  averageNs: number;
  hottestWorkerId: number;
  /** The hottest worker's time over the average worker's. */
  ratio: number;
  /** Time the hottest worker spent beyond the average worker. */
  extraNs: number;
}

const key = (row: { dataflowId: string; workerId: number }) =>
  `${row.dataflowId}|${row.workerId}`;

/**
 * Whether any dataflow's counter on any worker fell between two per-dataflow
 * reads. Dataflow ids restart with the replica, so a restart reads as a fall.
 */
export function hasDataflowCounterReset(
  baseline: DataflowWorkerCpu[],
  latest: DataflowWorkerCpu[],
) {
  const before = new Map(baseline.map((row) => [key(row), row.elapsedNs]));
  return latest.some((row) => row.elapsedNs < (before.get(key(row)) ?? 0));
}

/**
 * Each user object's time per worker between two reads, or over its
 * lifetime without `baseline`, plus every worker's total across all
 * dataflows, system and transient ones included.
 */
export function objectCpuRows({
  baseline,
  latest,
  exports,
  workerCount,
}: {
  baseline?: DataflowWorkerCpu[];
  latest: DataflowWorkerCpu[];
  exports: DataflowExport[];
  workerCount: number;
}) {
  const before = new Map((baseline ?? []).map((row) => [key(row), row]));
  // A dataflow can export more than one object. The row is named after the
  // first by id.
  const objectByDataflow = new Map<string, string>();
  for (const { dataflowId, objectId } of [...exports].sort((a, b) =>
    a.objectId.localeCompare(b.objectId),
  )) {
    if (!objectByDataflow.has(dataflowId)) {
      objectByDataflow.set(dataflowId, objectId);
    }
  }
  const workerTotalsNs = new Array<number>(workerCount).fill(0);
  const perWorkerByObject = new Map<string, number[]>();
  const dataflowsByObject = new Map<string, Set<string>>();
  for (const row of latest) {
    if (row.workerId >= workerCount) continue;
    const ns = delta(before.get(key(row))?.elapsedNs, row.elapsedNs);
    workerTotalsNs[row.workerId] += ns;
    const exported = objectByDataflow.get(row.dataflowId);
    if (!exported) continue;
    const objectId = exported.startsWith("t") ? ADHOC_ROW_ID : exported;
    let perWorker = perWorkerByObject.get(objectId);
    if (!perWorker) {
      perWorker = new Array<number>(workerCount).fill(0);
      perWorkerByObject.set(objectId, perWorker);
      dataflowsByObject.set(objectId, new Set());
    }
    perWorker[row.workerId] += ns;
    if (ns > 0) dataflowsByObject.get(objectId)?.add(row.dataflowId);
  }
  const rows: ObjectCpuRow[] = [];
  for (const [objectId, perWorkerNs] of perWorkerByObject) {
    const totalNs = perWorkerNs.reduce((sum, ns) => sum + ns, 0);
    if (totalNs === 0) continue;
    const averageNs = totalNs / workerCount;
    let hottestWorkerId = 0;
    perWorkerNs.forEach((ns, workerId) => {
      if (ns > perWorkerNs[hottestWorkerId]) hottestWorkerId = workerId;
    });
    const hottestNs = perWorkerNs[hottestWorkerId];
    rows.push({
      objectId,
      dataflowCount: dataflowsByObject.get(objectId)?.size ?? 0,
      perWorkerNs,
      totalNs,
      averageNs,
      hottestWorkerId,
      ratio: hottestNs / averageNs,
      extraNs: hottestNs - averageNs,
    });
  }
  return { rows, workerTotalsNs };
}

/** The object with the most dataflow time, with its share of all of it. */
export function largestShare(rows: ObjectCpuRow[], workerTotalsNs: number[]) {
  const totalNs = workerTotalsNs.reduce((sum, ns) => sum + ns, 0);
  const top = rows
    .filter((row) => row.objectId !== ADHOC_ROW_ID)
    .reduce<ObjectCpuRow | undefined>(
      (best, row) => (!best || row.totalNs > best.totalNs ? row : best),
      undefined,
    );
  if (!top || totalNs === 0) return undefined;
  return { objectId: top.objectId, share: top.totalNs / totalNs };
}

/**
 * The object that took at least `DOMINANT_SHARE` of all dataflow time, with
 * its share, if one did.
 */
export function dominantObject(rows: ObjectCpuRow[], workerTotalsNs: number[]) {
  const largest = largestShare(rows, workerTotalsNs);
  return largest && largest.share >= DOMINANT_SHARE ? largest : undefined;
}

/** The object that took most of a worker's time, with its share of it. */
export function topObjectOnWorker(
  rows: ObjectCpuRow[],
  workerTotalsNs: number[],
  workerId: number,
) {
  const totalNs = workerTotalsNs[workerId] ?? 0;
  if (totalNs === 0) return undefined;
  let top: ObjectCpuRow | undefined;
  for (const row of rows) {
    if (!top || row.perWorkerNs[workerId] > top.perWorkerNs[workerId]) {
      top = row;
    }
  }
  if (!top || top.perWorkerNs[workerId] === 0) return undefined;
  return {
    objectId: top.objectId,
    share: top.perWorkerNs[workerId] / totalNs,
  };
}

/**
 * The process a worker runs in. Timely numbers workers process by process,
 * `my_process * threads + index` in its zero-copy allocator.
 */
export const processOfWorker = (workerId: number, workersPerProcess: number) =>
  Math.floor(workerId / Math.max(1, workersPerProcess));

/** A replica the workers drawer reads, with its size's shape. */
export interface WorkerReplica {
  id: string;
  name: string;
  size: string;
  processes: number;
  workersPerProcess: number;
  cpuNanoCoresPerProcess: number;
}

/** A worker's name, with its process when the replica runs more than one. */
export const workerLabel = (
  workerId: number,
  { workersPerProcess, processes }: WorkerReplica,
) =>
  processes > 1
    ? `w${workerId} (process ${processOfWorker(workerId, workersPerProcess)})`
    : `w${workerId}`;

/** The replica's CPU at one time, as a percent of its allocation. */
export interface CpuPoint {
  atMs: number;
  percent: number;
}

/**
 * A replica's CPU per minute, as a percent of its allocation. A multi-process
 * replica averages the processes that reported, so a process whose metrics
 * fetch failed doesn't halve the line.
 */
export function replicaCpuPoints(
  samples: Pick<ProcessMetricsSample, "occurredAt" | "cpuNanoCores">[],
  cpuNanoCoresPerProcess: number,
): CpuPoint[] {
  if (cpuNanoCoresPerProcess <= 0) return [];
  const byMinute = new Map<number, number[]>();
  for (const { occurredAt, cpuNanoCores } of samples) {
    if (cpuNanoCores === null) continue;
    // Grouped by minute, the scrape interval, so one scrape's processes
    // average together whether or not their timestamps match exactly.
    const atMs =
      Math.floor(occurredAt.getTime() / millisecondsInMinute) *
      millisecondsInMinute;
    const values = byMinute.get(atMs) ?? [];
    values.push(cpuNanoCores);
    byMinute.set(atMs, values);
  }
  return [...byMinute]
    .sort(([a], [b]) => a - b)
    .map(([atMs, values]) => ({
      atMs,
      percent:
        (values.reduce((sum, value) => sum + value, 0) /
          values.length /
          cpuNanoCoresPerProcess) *
        100,
    }));
}
