// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { DataflowCpuPerWorkerRow } from "~/api/materialize/cluster/dataflowCpuPerWorker";

const STRIP_DATAFLOW_PREFIX = /^Dataflow: /;

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
  /** Non-null: comes from the INNER JOIN on `mz_compute_exports.export_id`. */
  objectId: string;
  /** LEFT-JOIN nullable: null when the object was dropped but the dataflow is
   *  still running (an "orphaned" dataflow). */
  objectName: string | null;
  schemaName: string | null;
  databaseName: string | null;
  objectType: DataflowCpuPerWorkerRow["objectType"];
  dataflowName: string;
};

export type PivotResult = {
  rows: DataflowRow[];
  numWorkers: number;
  globalWorkerTotals: number[];
};

const toNum = (v: unknown): number => {
  if (v == null) return 0;
  if (typeof v === "number") return v;
  if (typeof v === "bigint") return Number(v);
  return Number(v);
};

/**
 * Collapses one (dataflow, worker) row per element into one DataflowRow per
 * dataflow, with `workers` indexed by `worker_id`. Also returns the cluster-
 * wide per-worker totals used by the footer row in the heatmap.
 */
export function pivotDataflowCpuPerWorker(
  raw: DataflowCpuPerWorkerRow[],
): PivotResult {
  if (raw.length === 0) {
    return { rows: [], numWorkers: 0, globalWorkerTotals: [] };
  }

  const byDataflow = new Map<string, DataflowRow>();
  let maxWorkerId = 0;

  for (const r of raw) {
    const key = r.dataflowName;
    const workerId = toNum(r.workerId);
    const ns = toNum(r.elapsedNs);
    maxWorkerId = Math.max(maxWorkerId, workerId);

    let row = byDataflow.get(key);
    if (!row) {
      const label =
        r.objectName ??
        r.dataflowName.replace(STRIP_DATAFLOW_PREFIX, "").split(".").pop() ??
        r.dataflowName;
      const subLabel =
        r.databaseName && r.schemaName
          ? `${r.databaseName}.${r.schemaName}`
          : undefined;
      row = {
        id: r.objectId,
        label,
        subLabel,
        // Orphaned dataflows still have an `objectId`, but the GlobalId points
        // at a deleted catalog entry — navigating to it would 404. Only mark
        // rows clickable when the object is still resolvable via mz_objects.
        clickable: Boolean(r.objectName),
        objectId: r.objectId,
        objectName: r.objectName,
        schemaName: r.schemaName,
        databaseName: r.databaseName,
        objectType: r.objectType,
        dataflowName: r.dataflowName,
        workers: [],
        total: 0,
        min: Number.POSITIVE_INFINITY,
        max: 0,
        avg: 0,
        skew: 1,
      };
      byDataflow.set(key, row);
    }
    row.workers[workerId] = ns;
  }

  const numWorkers = maxWorkerId + 1;
  const globalWorkerTotals = new Array(numWorkers).fill(0);

  for (const row of byDataflow.values()) {
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
    rows: Array.from(byDataflow.values()),
    numWorkers,
    globalWorkerTotals,
  };
}
