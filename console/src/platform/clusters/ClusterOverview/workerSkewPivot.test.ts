// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { DataflowCpuPerWorkerRow } from "~/api/materialize/cluster/dataflowCpuPerWorker";

import { pivotDataflowCpuPerWorker } from "./workerSkewPivot";

const mkRow = (
  overrides: Partial<DataflowCpuPerWorkerRow> &
    Pick<DataflowCpuPerWorkerRow, "dataflowName" | "workerId" | "elapsedNs">,
): DataflowCpuPerWorkerRow => ({
  objectId: "u100",
  objectName: "my_mv",
  schemaName: "public",
  databaseName: "materialize",
  objectType: "materialized-view",
  ...overrides,
});

describe("pivotDataflowCpuPerWorker", () => {
  it("returns an empty result when there are no rows", () => {
    const result = pivotDataflowCpuPerWorker([]);
    expect(result.rows).toHaveLength(0);
    expect(result.numWorkers).toBe(0);
    expect(result.globalWorkerTotals).toEqual([]);
  });

  it("collapses per-worker rows into a single DataflowRow with workers indexed by id", () => {
    const result = pivotDataflowCpuPerWorker([
      mkRow({
        dataflowName: "Dataflow: materialize.public.my_mv",
        workerId: 0,
        elapsedNs: 100n,
      }),
      mkRow({
        dataflowName: "Dataflow: materialize.public.my_mv",
        workerId: 1,
        elapsedNs: 200n,
      }),
      mkRow({
        dataflowName: "Dataflow: materialize.public.my_mv",
        workerId: 2,
        elapsedNs: 300n,
      }),
    ]);

    expect(result.numWorkers).toBe(3);
    expect(result.rows).toHaveLength(1);
    const [row] = result.rows;
    expect(row.workers).toEqual([100, 200, 300]);
    expect(row.total).toBe(600);
    expect(row.min).toBe(100);
    expect(row.max).toBe(300);
    expect(row.skew).toBe(3);
    expect(result.globalWorkerTotals).toEqual([100, 200, 300]);
  });

  it("backfills missing worker slots with zero when a dataflow skips workers", () => {
    const result = pivotDataflowCpuPerWorker([
      mkRow({ dataflowName: "Dataflow: a", workerId: 0, elapsedNs: 100n }),
      mkRow({ dataflowName: "Dataflow: a", workerId: 2, elapsedNs: 300n }),
      // numWorkers is derived from the max worker_id across all rows (here, 2 from above)
    ]);

    expect(result.numWorkers).toBe(3);
    const [row] = result.rows;
    expect(row.workers).toEqual([100, 0, 300]);
    expect(row.min).toBe(0);
    expect(row.max).toBe(300);
    // min=0 forces skew to fall back to 1 rather than Infinity, which would break the UI.
    expect(row.skew).toBe(1);
  });

  it("computes per-worker totals across multiple dataflows", () => {
    const result = pivotDataflowCpuPerWorker([
      mkRow({ dataflowName: "Dataflow: a", workerId: 0, elapsedNs: 10n }),
      mkRow({ dataflowName: "Dataflow: a", workerId: 1, elapsedNs: 20n }),
      mkRow({ dataflowName: "Dataflow: b", workerId: 0, elapsedNs: 100n }),
      mkRow({ dataflowName: "Dataflow: b", workerId: 1, elapsedNs: 200n }),
    ]);

    expect(result.rows).toHaveLength(2);
    expect(result.globalWorkerTotals).toEqual([110, 220]);
  });

  it("uses the object name as the label when present", () => {
    const result = pivotDataflowCpuPerWorker([
      mkRow({
        objectName: "my_mv",
        dataflowName: "Dataflow: materialize.public.my_mv",
        workerId: 0,
        elapsedNs: 1n,
      }),
    ]);
    expect(result.rows[0].label).toBe("my_mv");
    expect(result.rows[0].clickable).toBe(true);
  });

  it("falls back to the dataflow name suffix when the object is orphaned", () => {
    const result = pivotDataflowCpuPerWorker([
      mkRow({
        // Orphaned: objectId still references the dropped GlobalId, but the
        // LEFT JOIN to mz_objects/mz_schemas/mz_databases returns null.
        objectName: null,
        schemaName: null,
        databaseName: null,
        dataflowName: "Dataflow: materialize.public.dropped_idx",
        workerId: 0,
        elapsedNs: 1n,
      }),
    ]);
    expect(result.rows[0].label).toBe("dropped_idx");
    // Not clickable — would navigate to a deleted object.
    expect(result.rows[0].clickable).toBe(false);
  });
});
