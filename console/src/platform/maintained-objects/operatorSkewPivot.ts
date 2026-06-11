// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { OperatorCpuPerWorkerRow } from "~/api/materialize/maintained-objects/operatorCpuPerWorker";
import { HeatmapRow } from "~/platform/clusters/ClusterOverview/workerSkewPivot";

export type OperatorRow = HeatmapRow & {
  operatorId: string;
  operatorName: string;
};

export type OperatorPivotResult = {
  rows: OperatorRow[];
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
 * Collapses per-(operator, worker) rows into one OperatorRow per operator,
 * with `workers` indexed by worker id. Mirrors `pivotDataflowCpuPerWorker`
 * but produces operator-scoped rows for the object detail Performance tab.
 *
 * Operators within a single dataflow have nothing further to drill into, so
 * `clickable` is always `false`.
 */
export function pivotOperatorCpuPerWorker(
  raw: OperatorCpuPerWorkerRow[],
): OperatorPivotResult {
  if (raw.length === 0) {
    return { rows: [], numWorkers: 0, globalWorkerTotals: [] };
  }

  const byOperator = new Map<string, OperatorRow>();
  let maxWorkerId = 0;

  for (const r of raw) {
    const workerId = toNum(r.workerId);
    const ns = toNum(r.elapsedNs);
    maxWorkerId = Math.max(maxWorkerId, workerId);

    let row = byOperator.get(r.operatorId);
    if (!row) {
      row = {
        id: r.operatorId,
        label: r.operatorName,
        clickable: false,
        operatorId: r.operatorId,
        operatorName: r.operatorName,
        workers: [],
        total: 0,
        min: Number.POSITIVE_INFINITY,
        max: 0,
        avg: 0,
        skew: 1,
      };
      byOperator.set(r.operatorId, row);
    }
    row.workers[workerId] = ns;
  }

  const numWorkers = maxWorkerId + 1;
  const globalWorkerTotals = new Array(numWorkers).fill(0);

  for (const row of byOperator.values()) {
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
    rows: Array.from(byOperator.values()),
    numWorkers,
    globalWorkerTotals,
  };
}
