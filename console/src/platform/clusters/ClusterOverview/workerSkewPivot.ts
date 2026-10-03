// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Generic row contract consumed by `WorkerSkewHeatmap`. Concrete pivots
 * (cluster-scoped, object-scoped) extend this with their own metadata.
 */
export type HeatmapRow = {
  /** Stable React key for this row. */
  id: string;
  /** Primary label shown on the row. */
  label: string;
  /** Optional secondary label rendered below the primary (e.g. schema). */
  subLabel?: string;
  /** Whether the row should respond to clicks (cursor style + onRowClick). */
  clickable?: boolean;
  /** Elapsed ns per worker, indexed by worker id. */
  workers: number[];
  total: number;
  min: number;
  max: number;
  /** total / numWorkers; per-cell `ratioToAvg = workers[w] / avg` matches the
   *  metric the dataflow-troubleshooting docs use for skew (alert at >2). */
  avg: number;
  /** max / min; 1 means perfectly balanced, >1 means skewed. */
  skew: number;
};

export type DataflowRow = HeatmapRow & {
  objectId: string;
  /** Null when the object has been dropped but its dataflow is still running. */
  objectName: string | null;
  schemaName: string | null;
  databaseName: string | null;
};

export type PivotResult = {
  rows: DataflowRow[];
  numWorkers: number;
  globalWorkerTotals: number[];
};

/**
 * Naming for one object, supplied by the caller from the app-wide objects
 * subscribe. The CPU query deliberately does not join `mz_objects`, because
 * with the session pinned to a replica that join would be planned on the
 * customer's cluster instead of against the `mz_catalog_server` indexes.
 */
export type ObjectNaming = {
  name: string;
  schemaName: string | null;
  databaseName: string | null;
};

export type ResolveObjectNaming = (
  objectId: string,
) => ObjectNaming | undefined;

/**
 * One CPU reading for an (object, worker) pair. Widened from
 * `DataflowCpuPerWorkerRow` so the pivot accepts both a raw sample, whose
 * counter arrives as a bigint, and a differenced one, whose delta is a number.
 */
export type CpuSampleRow = {
  objectId: string;
  workerId: number;
  elapsedNs: number | bigint;
};

const toNum = (v: unknown): number => {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  if (typeof v === "bigint") return Number(v);
  return Number(v);
};

const rowKey = (objectId: string, workerId: number) =>
  `${objectId} ${workerId}`;

/**
 * Differences two samples of the cumulative CPU counters into per-window
 * elapsed times.
 *
 * `elapsed_ns` accumulates from the moment an operator is created and is never
 * windowed, so a single sample is a lifetime average: on a replica that has
 * been up for days, a skew that started minutes ago barely moves it. The
 * difference between two samples is CPU spent in between, which is what the
 * panel is actually claiming to show.
 *
 * Rows only in `next` are new dataflows and pass through whole, since their
 * counter started inside the window. Rows only in `previous` have gone away and
 * are dropped. A counter that moved backwards means the replica restarted and
 * reset it, so the delta is clamped to zero rather than rendered as negative
 * work.
 */
export function diffDataflowCpuSamples(
  previous: CpuSampleRow[],
  next: CpuSampleRow[],
): CpuSampleRow[] {
  const before = new Map<string, number>();
  for (const r of previous) {
    before.set(rowKey(r.objectId, toNum(r.workerId)), toNum(r.elapsedNs));
  }

  return next.map((r) => {
    const prior = before.get(rowKey(r.objectId, toNum(r.workerId)));
    const delta =
      prior === undefined ? toNum(r.elapsedNs) : toNum(r.elapsedNs) - prior;
    return { ...r, elapsedNs: delta < 0 ? 0 : delta };
  });
}

/**
 * Collapses one (object, worker) row per element into one DataflowRow per
 * object, with `workers` indexed by `worker_id`. Also returns the cluster-wide
 * per-worker totals used by the footer row in the heatmap.
 *
 * `resolveNaming` supplies labels. An object it cannot resolve is an orphaned
 * dataflow, still running after its catalog entry was dropped, so the row is
 * labelled by id and left unclickable: navigating to it would 404.
 */
export function pivotDataflowCpuPerWorker(
  raw: CpuSampleRow[],
  resolveNaming?: ResolveObjectNaming,
): PivotResult {
  if (raw.length === 0) {
    return { rows: [], numWorkers: 0, globalWorkerTotals: [] };
  }

  const byObject = new Map<string, DataflowRow>();
  let maxWorkerId = 0;

  for (const r of raw) {
    const workerId = toNum(r.workerId);
    const ns = toNum(r.elapsedNs);
    maxWorkerId = Math.max(maxWorkerId, workerId);

    let row = byObject.get(r.objectId);
    if (!row) {
      const naming = resolveNaming?.(r.objectId);
      const subLabel =
        naming?.databaseName && naming?.schemaName
          ? `${naming.databaseName}.${naming.schemaName}`
          : undefined;
      row = {
        id: r.objectId,
        label: naming?.name ?? r.objectId,
        subLabel,
        clickable: Boolean(naming),
        objectId: r.objectId,
        objectName: naming?.name ?? null,
        schemaName: naming?.schemaName ?? null,
        databaseName: naming?.databaseName ?? null,
        workers: [],
        total: 0,
        min: Number.POSITIVE_INFINITY,
        max: 0,
        avg: 0,
        skew: 1,
      };
      byObject.set(r.objectId, row);
    }
    row.workers[workerId] = ns;
  }

  const numWorkers = maxWorkerId + 1;
  const globalWorkerTotals = new Array(numWorkers).fill(0);

  for (const row of byObject.values()) {
    // Backfill any worker slots that had zero work for this dataflow.
    for (let w = 0; w < numWorkers; w++) {
      if (row.workers[w] === undefined) row.workers[w] = 0;
    }
    let total = 0;
    let min = Number.POSITIVE_INFINITY;
    let max = 0;
    for (let w = 0; w < numWorkers; w++) {
      const v = row.workers[w];
      total += v;
      if (v < min) min = v;
      if (v > max) max = v;
      globalWorkerTotals[w] += v;
    }
    row.total = total;
    row.min = min === Number.POSITIVE_INFINITY ? 0 : min;
    row.max = max;
    row.avg = numWorkers > 0 ? total / numWorkers : 0;
    row.skew = row.min > 0 ? row.max / row.min : 1;
  }

  return {
    rows: Array.from(byObject.values()),
    numWorkers,
    globalWorkerTotals,
  };
}
