// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { mean } from "d3";
import { millisecondsInMinute } from "date-fns/constants";

import { LagInfo } from "~/api/materialize/cluster/materializationLag";
import {
  ProcessMetricsSample,
  SizeAllocation,
} from "~/api/materialize/cluster/replicaProcessMetrics";
import {
  DataflowExportRow,
  DataflowWorkerCpuRow,
} from "~/api/materialize/cluster/workerCpu";
import { DatabaseObject } from "~/api/materialize/objects";
import docUrls from "~/mz-doc-urls.json";

import {
  bucketProcessReadings,
  BusiestProcess,
  EVEN_RATIO,
  readProcessSamples,
} from "../ResourceUsage/processReadingsModel";

/**
 * @module
 * Turns cumulative per-worker CPU counters into what the Workers tab shows.
 *
 * The replica counts each dataflow's time on each worker from when the
 * dataflow started. A reading carries each worker's total on from the reading
 * before it by how much each dataflow's counter grew since, so it doesn't fall
 * when a dataflow shuts down and its counter leaves. The difference between
 * two readings is the time spent in between, and dividing it by the
 * wall-clock time between them gives the share of one core a worker used. A
 * worker is one thread, so that share is at most 1.
 *
 * Each row of the objects table is carried on the same way, under the row of
 * the dataflow's export.
 *
 * NOTE: work a dataflow did after the last reading before it shut down is
 * never seen, so a span with ad-hoc queries' dataflows finishing in it
 * undercounts, and one that started and ended between two readings is missed
 * entirely. A run of readings that starts without the one before it
 * starts from the running dataflows' lifetimes, which can fall below that
 * reading, and no span is measured across such a fall.
 */

export const CPU_DOCS =
  docUrls["/docs/clusters/troubleshoot-clusters/cpu-troubleshooting/"];

/** One read of every worker's cumulative dataflow time on a replica. */
export interface WorkerCpuReading {
  atMs: number;
  elapsedNsByWorker: Map<number, number>;
  /** The first reading after a pause, so no point spans the pause. */
  afterGap?: boolean;
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

/** Whether any worker's counter at `after` is below its counter at `before`. */
export function hasCounterReset(
  before: Map<number, number>,
  after: Map<number, number>,
) {
  for (const [workerId, ns] of after) {
    if (ns < (before.get(workerId) ?? 0)) return true;
  }
  return false;
}

const isComparable = (start: WorkerCpuReading, end: WorkerCpuReading) =>
  !hasCounterReset(start.elapsedNsByWorker, end.elapsedNsByWorker);

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
  mean(workers, (worker) => worker.busyFraction) ?? 0;

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
    if (readings[i].afterGap) {
      segmentStart = i;
      isNewSegment = points.length > 0;
      continue;
    }
    const atMs = readings[i].atMs;
    // The earliest reading this one compares with, back to the first at least
    // `windowMs` before it.
    let from = i;
    for (let k = i - 1; k >= segmentStart; k--) {
      if (isComparable(readings[k], readings[i])) from = k;
      if (atMs - readings[k].atMs >= windowMs) break;
    }
    if (atMs - readings[from].atMs < Math.min(windowMs, MIN_POINT_WINDOW_MS)) {
      // Too short to draw, as just after a restart. The line breaks here
      // instead of bridging the span.
      isNewSegment = points.length > 0;
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
 * A worker past this many times the other workers' average carries skewed
 * work. The dataflow troubleshooting docs use 2 as their example threshold.
 */
export const SKEW_RATIO = 2;
/** A worker this busy is at its core's limit, so work routed to it waits. */
export const SATURATED_FRACTION = 0.9;
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
// Below these the replica did almost no dataflow work, so ratios are noise.
const IDLE_AVERAGE_FRACTION = 0.01;
const IDLE_BUSIEST_FRACTION = 0.05;

export type WorkerVerdict =
  | { kind: "idle" }
  | {
      /**
       * The most urgent finding. `pegged`: the busiest worker is at its core's
       * limit, ahead of the others but short of `SKEW_RATIO`. `light`: uneven,
       * but no worker is near its core's limit.
       */
      kind:
        "saturated" | "pegged" | "allBusy" | "uneven" | "light" | "balanced";
      busiest: WorkerBusy;
      averageBusyFraction: number;
      /** Every worker but the busiest, the average skew is judged against. */
      othersBusyFraction: number;
      /** The workers at their core's limit, busiest first, whatever `kind`. */
      atLimit: WorkerBusy[];
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
  const result = {
    busiest,
    averageBusyFraction,
    othersBusyFraction,
    atLimit: workers.filter(
      ({ busyFraction }) => busyFraction >= SATURATED_FRACTION,
    ),
  };
  if (averageBusyFraction >= ALL_BUSY_FRACTION) {
    return { kind: "allBusy", ...result };
  }
  const leadsBy = (ratio: number) =>
    workers.length > 1 && busiest.busyFraction >= ratio * othersBusyFraction;
  const isSkewed = leadsBy(SKEW_RATIO);
  const isAtLimit = busiest.busyFraction >= SATURATED_FRACTION;
  if (isSkewed && isAtLimit) return { kind: "saturated", ...result };
  // Short of skew, a worker at its limit still holds up the work routed to
  // it, so the work can't read as spread evenly.
  if (isAtLimit && leadsBy(EVEN_RATIO)) return { kind: "pegged", ...result };
  if (isSkewed && busiest.busyFraction >= UNEVEN_FRACTION) {
    return { kind: "uneven", ...result };
  }
  if (isSkewed) return { kind: "light", ...result };
  return { kind: "balanced", ...result };
}

/** Replica CPU this high is worth explaining, whatever the workers show. */
export const HIGH_CPU_FRACTION = 0.75;

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
      /** The workers at their core's limit, busiest first. */
      atLimit: WorkerBusy[];
      /** Set when the work is also uneven, short of saturating a worker. */
      uneven?: { busiest: WorkerBusy; othersBusyFraction: number };
    };

/** Whether a verdict says one worker carries more than its share. */
export const isSkewVerdict = (verdict: CpuVerdict | undefined) =>
  verdict?.kind === "saturated" || verdict?.kind === "uneven";

/**
 * A worker at its limit is the most specific finding, so it wins. Otherwise a
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
    verdict.kind !== "saturated" &&
    verdict.kind !== "pegged"
  ) {
    const dataflowFraction = averageBusy(workers) * workersPerCore;
    return {
      kind: "highCpu",
      replicaCpuFraction,
      dataflowFraction,
      outsideFraction: Math.max(0, replicaCpuFraction - dataflowFraction),
      busiest: workers[0],
      atLimit: verdict.kind === "idle" ? [] : verdict.atLimit,
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
export const workersPerCore = ({ workers, cpuNanoCores }: SizeAllocation) =>
  cpuNanoCores > 0 ? workers / (cpuNanoCores / 1e9) : 1;

// One threshold for even, whether processes or workers.
export { EVEN_RATIO };
/** Past this many times the other workers' average, skew reads as severe. */
export const SEVERE_SKEW_RATIO = 3;

/**
 * A multiple of the other workers' average, which is infinite when they spent
 * nothing.
 */
export const formatMultiple = (ratio: number) =>
  Number.isFinite(ratio)
    ? `${ratio.toFixed(ratio < 0.1 ? 2 : 1)}×`
    : "all of it";
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
export type DataflowWorkerCpu = DataflowWorkerCpuRow;

export interface DataflowExport {
  objectId: string;
  dataflowId: string;
  /** The dataflow's name, for a transient (`t`) export. */
  dataflowName?: string;
}

/**
 * Whether a cluster object runs a storage dataflow on the cluster's replicas.
 * Webhook sources are written by environmentd, not a replica.
 *
 * NOTE: on v26.48, a replica running a load generator source showed no
 * dataflow for it in the compute introspection the workers' time comes from,
 * so a source's or sink's work reads as CPU outside the workers' dataflows.
 */
export const runsStorageDataflow = ({
  objectType,
  sourceType,
}: DatabaseObject) =>
  objectType === "sink" ||
  (objectType === "source" && sourceType !== "webhook");

/** The row that pools the temporary dataflows built for ad-hoc queries. */
export const ADHOC_ROW_ID = "adhoc";
/** The row that pools the dataflows the coordinator runs on every replica. */
export const SYSTEM_ROW_ID = "system";

// The names of the transient dataflows the coordinator runs on every replica
// for itself (`introspection-subscribe-` in `coord/introspection.rs` and
// `metric-sink-` in `coord/metric_sink.rs`, in `src/adapter`). Every other
// transient dataflow runs a session's statement: a SUBSCRIBE, SELECT or
// COPY TO, or the read of an UPDATE, DELETE or INSERT ... SELECT.
const COORDINATOR_DATAFLOW_NAME =
  /^Dataflow: (introspection-subscribe|metric-sink)-/;

/**
 * The object-table row an export's dataflow counts toward: a user object's
 * own, `ADHOC_ROW_ID` for a session's transient dataflow, `SYSTEM_ROW_ID` for
 * the coordinator's own, or none for a transient dataflow whose name isn't
 * known.
 */
export const objectRowId = ({ objectId, dataflowName }: DataflowExport) => {
  if (!objectId.startsWith("t")) return objectId;
  if (dataflowName === undefined) return undefined;
  return COORDINATOR_DATAFLOW_NAME.test(dataflowName)
    ? SYSTEM_ROW_ID
    : ADHOC_ROW_ID;
};

/** Whether a row pools dataflows rather than standing for one object. */
export const isPooledRow = (objectId: string) =>
  objectId === ADHOC_ROW_ID || objectId === SYSTEM_ROW_ID;

export interface ObjectCpuRow {
  /** An object's id, or a pooled row's (`isPooledRow`). */
  objectId: string;
  /** How many dataflows the row pools: 1 for an object. */
  dataflowCount: number;
  /** Indexed by worker id. */
  perWorkerNs: number[];
  totalNs: number;
  hottestWorkerId: number;
  /**
   * The hottest worker's time over the other workers' average, infinite when
   * they spent none. An average that includes the hottest worker would cap
   * this at the worker count.
   */
  ratio: number;
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

/** Each dataflow's counter on each worker at one reading. */
export type DataflowCounters = Map<string, number>;

export const dataflowCounters = (rows: DataflowWorkerCpu[]): DataflowCounters =>
  new Map(rows.map((row) => [key(row), row.elapsedNs]));

/**
 * How much a dataflow's counter on a worker grew since `counters`. A dataflow
 * missing from them started since, so all of its time is new.
 */
const growthSince = (
  row: DataflowWorkerCpu,
  counters: DataflowCounters | undefined,
) => {
  const before = counters?.get(key(row));
  // A counter below its last value is a new dataflow under a reused id, as
  // after a restart.
  return before === undefined || row.elapsedNs < before
    ? row.elapsedNs
    : row.elapsedNs - before;
};

/**
 * Each worker's total from `rows`, carried on from `previous` by each
 * dataflow's growth since it, or each worker's total over the running
 * dataflows' lifetimes without it.
 */
export function accumulateWorkerElapsedNs(
  rows: DataflowWorkerCpu[],
  previous?: {
    elapsedNsByWorker: Map<number, number>;
    counters: DataflowCounters;
  },
) {
  const totals = new Map(previous?.elapsedNsByWorker);
  for (const row of rows) {
    totals.set(
      row.workerId,
      (totals.get(row.workerId) ?? 0) + growthSince(row, previous?.counters),
    );
  }
  return totals;
}

/** The object-table row each running dataflow counts toward, by dataflow id. */
export type DataflowRowIds = Map<string, string>;

/**
 * The row each of `dataflows` counts toward, from its exports. One that no
 * longer exports anything keeps its row from `remembered` while it runs, so
 * its last readings still count toward it.
 */
export function dataflowRowIds(
  dataflows: DataflowWorkerCpu[],
  exports: DataflowExport[],
  remembered?: DataflowRowIds,
): DataflowRowIds {
  // A dataflow can export more than one object. The row is named after the
  // first by id.
  const exported = new Map<string, string>();
  for (const dataflowExport of [...exports].sort((a, b) =>
    a.objectId.localeCompare(b.objectId),
  )) {
    const rowId = objectRowId(dataflowExport);
    if (rowId !== undefined && !exported.has(dataflowExport.dataflowId)) {
      exported.set(dataflowExport.dataflowId, rowId);
    }
  }
  const rowIds: DataflowRowIds = new Map();
  for (const { dataflowId } of dataflows) {
    const rowId = exported.get(dataflowId) ?? remembered?.get(dataflowId);
    if (rowId !== undefined) rowIds.set(dataflowId, rowId);
  }
  return rowIds;
}

/** Each row's time on each worker, indexed by worker id. */
export type RowTotals = Map<string, number[]>;

/**
 * Each row's totals from `rows`, carried on from `previous` as
 * `accumulateWorkerElapsedNs` carries worker totals, with the dataflows that
 * grew since by row. Workers past `workerCount` are left out.
 */
export function accumulateRowElapsedNs({
  rows,
  rowIds,
  workerCount,
  previous,
}: {
  rows: DataflowWorkerCpu[];
  rowIds: DataflowRowIds;
  workerCount: number;
  previous?: { counters: DataflowCounters; rowTotals: RowTotals };
}) {
  const rowTotals: RowTotals = new Map(
    [...(previous?.rowTotals ?? [])].map(([rowId, ns]) => [rowId, [...ns]]),
  );
  const grown = new Map<string, Set<string>>();
  for (const row of rows) {
    const rowId = rowIds.get(row.dataflowId);
    if (rowId === undefined || row.workerId >= workerCount) continue;
    const ns = growthSince(row, previous?.counters);
    if (ns === 0) continue;
    let perWorkerNs = rowTotals.get(rowId);
    if (!perWorkerNs) {
      perWorkerNs = new Array<number>(workerCount).fill(0);
      rowTotals.set(rowId, perWorkerNs);
    }
    perWorkerNs[row.workerId] += ns;
    let dataflowIds = grown.get(rowId);
    if (!dataflowIds) {
      dataflowIds = new Set();
      grown.set(rowId, dataflowIds);
    }
    dataflowIds.add(row.dataflowId);
  }
  return { rowTotals, grown };
}

/** An export of one of the reader's own subscribes. */
export type OwnExport = Pick<DataflowExport, "objectId" | "dataflowId">;

/**
 * The dataflows of the reader's own subscribes: those exported under its
 * session's `connectionId`, and those of `remembered` exports from its earlier
 * sessions that still run. Returns the exports to remember from here on.
 */
export function ownDataflows({
  dataflows,
  exports,
  connectionId,
  remembered,
}: {
  dataflows: DataflowWorkerCpu[];
  exports: DataflowExportRow[];
  connectionId: number | undefined;
  remembered: OwnExport[];
}) {
  const exportIdsByDataflow = new Map<string, Set<string>>();
  for (const { dataflowId, objectId } of exports) {
    const ids = exportIdsByDataflow.get(dataflowId) ?? new Set<string>();
    ids.add(objectId);
    exportIdsByDataflow.set(dataflowId, ids);
  }
  const running = new Set(dataflows.map(({ dataflowId }) => dataflowId));
  const current = exports.flatMap(
    ({ objectId, dataflowId, connectionId: id }) =>
      id !== null && id === connectionId ? [{ objectId, dataflowId }] : [],
  );
  // NOTE: a closed session's tag leaves before its dataflow does, so a
  // remembered dataflow stays the reader's while it runs, exported or not. One
  // that exports something else is a reused id, as after a restart.
  const stillRunning = remembered.filter(({ objectId, dataflowId }) => {
    const ids = exportIdsByDataflow.get(dataflowId);
    return ids ? ids.has(objectId) : running.has(dataflowId);
  });
  const own = [
    ...current,
    ...stillRunning.filter(
      ({ objectId }) => !current.some((e) => e.objectId === objectId),
    ),
  ];
  return {
    dataflowIds: new Set(own.map(({ dataflowId }) => dataflowId)),
    remembered: own,
  };
}

/** A row from its time on each worker, or none when it spent no time. */
const toObjectCpuRow = (
  objectId: string,
  perWorkerNs: number[],
  dataflowCount: number,
): ObjectCpuRow | undefined => {
  const workerCount = perWorkerNs.length;
  const totalNs = perWorkerNs.reduce((sum, ns) => sum + ns, 0);
  if (totalNs === 0) return undefined;
  let hottestWorkerId = 0;
  perWorkerNs.forEach((ns, workerId) => {
    if (ns > perWorkerNs[hottestWorkerId]) hottestWorkerId = workerId;
  });
  const hottestNs = perWorkerNs[hottestWorkerId];
  const othersAverageNs =
    workerCount > 1 ? (totalNs - hottestNs) / (workerCount - 1) : hottestNs;
  return {
    objectId,
    dataflowCount,
    perWorkerNs,
    totalNs,
    hottestWorkerId,
    ratio: othersAverageNs > 0 ? hottestNs / othersAverageNs : Infinity,
  };
};

/**
 * Each row's time per worker over the running dataflows' lifetimes, plus
 * every worker's total across all dataflows, system and transient ones
 * included.
 */
export function objectCpuRows({
  latest,
  exports,
  workerCount,
}: {
  latest: DataflowWorkerCpu[];
  exports: DataflowExport[];
  workerCount: number;
}) {
  const workerTotalsNs = new Array<number>(workerCount).fill(0);
  for (const row of latest) {
    if (row.workerId < workerCount)
      workerTotalsNs[row.workerId] += row.elapsedNs;
  }
  const { rowTotals, grown } = accumulateRowElapsedNs({
    rows: latest,
    rowIds: dataflowRowIds(latest, exports),
    workerCount,
  });
  const rows = [...rowTotals].flatMap(
    ([objectId, perWorkerNs]) =>
      toObjectCpuRow(objectId, perWorkerNs, grown.get(objectId)?.size ?? 0) ??
      [],
  );
  return { rows, workerTotalsNs };
}

/**
 * One read of each dataflow's counters, with what each dataflow exports, and
 * each row's carried-on totals at the reading it was taken with.
 */
export interface ObjectCpuRead {
  atMs: number;
  rows: DataflowWorkerCpu[];
  exports: DataflowExport[];
  rowTotals: Array<[string, number[]]>;
  /** The dataflows that grew since the previous read, by row. */
  grownDataflows: Array<[string, string[]]>;
}

/**
 * Each row's time from `start` to the last of `after`, the reads that follow
 * it, from their carried-on totals, counting the dataflows that grew in
 * `after`.
 */
function rowsBetween(start: ObjectCpuRead, after: ObjectCpuRead[]) {
  const end = after.at(-1);
  if (!end) return [];
  const before = new Map(start.rowTotals);
  const dataflowsByRow = new Map<string, Set<string>>();
  for (const read of after) {
    for (const [rowId, dataflowIds] of read.grownDataflows) {
      const ids = dataflowsByRow.get(rowId) ?? new Set<string>();
      for (const id of dataflowIds) ids.add(id);
      dataflowsByRow.set(rowId, ids);
    }
  }
  return end.rowTotals.flatMap(([rowId, endNs]) => {
    const startNs = before.get(rowId);
    return (
      toObjectCpuRow(
        rowId,
        endNs.map((ns, workerId) => delta(startNs?.[workerId], ns)),
        dataflowsByRow.get(rowId)?.size ?? 0,
      ) ?? []
    );
  });
}

/** The shortest span a measurement covers. */
export const MEASUREMENT_MS = millisecondsInMinute;
/**
 * The longest. Per-object reads come every 30 s of the counters' time, at the
 * first 5 s tick past it, so a span is usually a minute. Counters that advance
 * in longer steps, as on a replica whose workers are busy, space the reads
 * further apart.
 */
export const MAX_MEASUREMENT_MS = 3 * millisecondsInMinute;

/** How a replica's workers spent one measured span, fixed once taken. */
export interface Measurement {
  startMs: number;
  endMs: number;
  /** Busiest first. */
  workers: WorkerBusy[];
  rows: ObjectCpuRow[];
  /** Each worker's time over the span across every dataflow, by worker id. */
  workerTotalsNs: number[];
  /**
   * The replica's CPU over the span from its metrics, recorded once so a
   * sample landing later doesn't change the finding: its share of the
   * allocation, unset when the metrics had none, and its busiest process.
   */
  replicaCpu?: { fraction?: number; busiestProcess?: BusiestProcess };
  /** Each object's freshness, recorded once read for this measurement. */
  lag?: Array<[string, LagInfo]>;
}

/**
 * Measures the shortest span of `MEASUREMENT_MS` to `MAX_MEASUREMENT_MS` that
 * ends at the last of `reads`, starts at or after `startsFromMs`, and has no
 * pause or counter falling across it, or returns undefined without one.
 * `reads` and `readings` are sorted by time.
 *
 * Worker totals come from the readings taken at the reads' times, and each
 * row's time from the totals the reads carry. Both grow by the same counters
 * at the same readings, so a row's time is never more than its worker's, and
 * a dataflow that finished keeps its time up to the reading before it ended.
 */
export function measureLatest({
  reads,
  readings,
  workerCount,
  startsFromMs = 0,
}: {
  reads: ObjectCpuRead[];
  readings: WorkerCpuReading[];
  workerCount: number;
  startsFromMs?: number;
}): Measurement | undefined {
  const end = reads.at(-1);
  if (!end) return undefined;
  const endReading = readings.find(({ atMs }) => atMs === end.atMs);
  if (!endReading) return undefined;
  for (let i = reads.length - 2; i >= 0; i--) {
    const start = reads[i];
    const spanMs = end.atMs - start.atMs;
    if (spanMs < MEASUREMENT_MS) continue;
    if (spanMs > MAX_MEASUREMENT_MS || start.atMs < startsFromMs) break;
    // Readings carried on across a pause miss the dataflows that started and
    // ended in it.
    if (
      readings.some(
        ({ atMs, afterGap }) =>
          afterGap && atMs > start.atMs && atMs <= end.atMs,
      )
    ) {
      break;
    }
    const startReading = readings.find(({ atMs }) => atMs === start.atMs);
    if (
      !startReading ||
      !isComparable(startReading, endReading) ||
      hasDataflowCounterReset(start.rows, end.rows)
    ) {
      continue;
    }
    const workers = workerBusyBetween(startReading, endReading);
    // From the counters, not the busy shares, which stop at a whole core: a
    // tick that lands just inside the span can put a worker past it.
    const workerTotalsNs = new Array<number>(workerCount).fill(0);
    for (const [workerId, ns] of endReading.elapsedNsByWorker) {
      if (workerId < workerCount) {
        workerTotalsNs[workerId] = delta(
          startReading.elapsedNsByWorker.get(workerId),
          ns,
        );
      }
    }
    const rows = rowsBetween(start, reads.slice(i + 1));
    return {
      startMs: start.atMs,
      endMs: end.atMs,
      workers,
      rows,
      workerTotalsNs,
    };
  }
  return undefined;
}

/**
 * The process samples a measurement's CPU comes from: from a minute before its
 * start, since metrics land about once a minute, to its end. Samples after the
 * end are cut before bucketing by minute, or one landing later in the
 * measurement's last minute would change its finding.
 */
export const samplesInMeasurement = (
  samples: ProcessMetricsSample[],
  { startMs, endMs }: Pick<Measurement, "startMs" | "endMs">,
) =>
  samples.filter(({ occurredAt }) => {
    const atMs = occurredAt.getTime();
    return atMs >= startMs - millisecondsInMinute && atMs <= endMs;
  });

/**
 * Past this many times the other workers' average, they get 2% of the busiest
 * worker's time on the object, next to nothing, as with a cross join. A hot
 * key still leaves the other workers the rows of every other key.
 */
export const ONE_WORKER_RATIO = 50;

/**
 * How an object's time spreads across the workers. `allOn` is from
 * `ONE_WORKER_RATIO` up, with an infinite ratio when the others spent none.
 */
export type ObjectSpread =
  | { kind: "even" }
  | { kind: "leans" | "allOn"; workerId: number; ratio: number };

export const objectSpread = ({
  ratio,
  hottestWorkerId: workerId,
}: ObjectCpuRow): ObjectSpread => {
  if (ratio >= ONE_WORKER_RATIO) return { kind: "allOn", workerId, ratio };
  return ratio >= SKEW_RATIO
    ? { kind: "leans", workerId, ratio }
    : { kind: "even" };
};

/**
 * The row with the most dataflow time, with its share of all of it. The
 * queries row counts like an object, as in `topObjectOnWorker`, so every
 * finding blames the same row.
 */
export function largestShare(rows: ObjectCpuRow[], workerTotalsNs: number[]) {
  const totalNs = workerTotalsNs.reduce((sum, ns) => sum + ns, 0);
  const top = rows
    .filter((row) => row.objectId !== SYSTEM_ROW_ID)
    .reduce<ObjectCpuRow | undefined>(
      (best, row) => (!best || row.totalNs > best.totalNs ? row : best),
      undefined,
    );
  if (!top || totalNs === 0) return undefined;
  return { objectId: top.objectId, share: top.totalNs / totalNs };
}

/**
 * The objects that took most of a worker's time, most first: each one's time
 * on the worker, its share of the worker's dataflow time, the other workers'
 * average time on it, and the object's own hottest worker and ratio.
 */
export function topObjectsOnWorker(
  rows: ObjectCpuRow[],
  workerTotalsNs: number[],
  workerId: number,
  count: number,
) {
  const totalNs = workerTotalsNs[workerId] ?? 0;
  if (totalNs === 0) return [];
  const workerCount = workerTotalsNs.length;
  return rows
    .filter((row) => row.perWorkerNs[workerId] > 0)
    .sort((a, b) => b.perWorkerNs[workerId] - a.perWorkerNs[workerId])
    .slice(0, count)
    .map((row) => {
      const ns = row.perWorkerNs[workerId];
      return {
        objectId: row.objectId,
        ns,
        share: ns / totalNs,
        othersAverageNs:
          workerCount > 1 ? (row.totalNs - ns) / (workerCount - 1) : 0,
        hottestWorkerId: row.hottestWorkerId,
        ratio: row.ratio,
      };
    });
}

/**
 * The object that took most of a worker's time, with its share of it. A
 * finding is never about Materialize's own dataflows, so they're left out.
 */
export const topObjectOnWorker = (
  rows: ObjectCpuRow[],
  workerTotalsNs: number[],
  workerId: number,
) =>
  topObjectsOnWorker(
    rows.filter(({ objectId }) => objectId !== SYSTEM_ROW_ID),
    workerTotalsNs,
    workerId,
    1,
  ).at(0);

/**
 * The object a verdict is about: the one with the most of the busiest
 * worker's time when the finding is about that worker, else the one with the
 * most time overall.
 */
export function verdictObjectId(
  verdict: CpuVerdict,
  { rows, workerTotalsNs }: Measurement,
) {
  if (verdict.kind === "idle") return undefined;
  if (
    isSkewVerdict(verdict) ||
    verdict.kind === "pegged" ||
    verdict.kind === "light"
  ) {
    return topObjectOnWorker(rows, workerTotalsNs, verdict.busiest.workerId)
      ?.objectId;
  }
  return largestShare(rows, workerTotalsNs)?.objectId;
}

/**
 * The worker the bars call out, named as the verdict names it: overloaded
 * only when it's at its limit with skewed work.
 */
export function markedWorker(verdict: CpuVerdict | undefined) {
  switch (verdict?.kind) {
    case "saturated":
      return { workerId: verdict.busiest.workerId, label: "Overloaded worker" };
    case "pegged":
      return {
        workerId: verdict.busiest.workerId,
        label: "Worker at its core's limit",
      };
    case "uneven":
      return { workerId: verdict.busiest.workerId, label: "Busiest worker" };
    default:
      return undefined;
  }
}

/**
 * The process a worker runs in. Timely numbers workers process by process,
 * `my_process * threads + index` in its zero-copy allocator.
 */
export const processOfWorker = (workerId: number, workersPerProcess: number) =>
  Math.floor(workerId / Math.max(1, workersPerProcess));

/** A replica the Workers tab reads. */
export interface WorkerReplica {
  id: string;
  name: string;
  size: string;
  /** The replica's color in the resource usage charts. */
  color: string;
  /** One process's allocation. */
  allocation: SizeAllocation;
}

/** A worker's name, with its process when the replica runs more than one. */
export const workerLabel = (workerId: number, { allocation }: WorkerReplica) =>
  allocation.processes > 1
    ? `w${workerId} (process ${processOfWorker(workerId, allocation.workers)})`
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
  samples: ProcessMetricsSample[],
  allocation: SizeAllocation,
): CpuPoint[] {
  return bucketProcessReadings(
    readProcessSamples({ samples, allocation, startMs: 0 }),
    millisecondsInMinute,
  ).flatMap(({ bucketStart, readings }) => {
    const percent = mean([...readings.values()], ({ cpu }) => cpu);
    return percent === undefined ? [] : [{ atMs: bucketStart, percent }];
  });
}
