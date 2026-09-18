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

import {
  diffDataflowCpuSamples,
  pivotDataflowCpuPerWorker,
  ResolveObjectNaming,
} from "./workerSkewPivot";

const mkRow = (
  overrides: Pick<DataflowCpuPerWorkerRow, "workerId" | "elapsedNs"> &
    Partial<DataflowCpuPerWorkerRow>,
): DataflowCpuPerWorkerRow => ({
  objectId: "u100",
  ...overrides,
});

const naming: ResolveObjectNaming = (objectId) =>
  objectId === "u100"
    ? { name: "my_mv", schemaName: "public", databaseName: "materialize" }
    : undefined;

describe("pivotDataflowCpuPerWorker", () => {
  it("returns an empty result when there are no rows", () => {
    const result = pivotDataflowCpuPerWorker([]);
    expect(result.rows).toHaveLength(0);
    expect(result.numWorkers).toBe(0);
    expect(result.globalWorkerTotals).toEqual([]);
  });

  it("collapses per-worker rows into a single DataflowRow with workers indexed by id", () => {
    const result = pivotDataflowCpuPerWorker([
      mkRow({ workerId: 0, elapsedNs: 100n }),
      mkRow({ workerId: 1, elapsedNs: 200n }),
      mkRow({ workerId: 2, elapsedNs: 300n }),
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
      mkRow({ workerId: 0, elapsedNs: 100n }),
      mkRow({ workerId: 2, elapsedNs: 300n }),
    ]);

    expect(result.numWorkers).toBe(3);
    const [row] = result.rows;
    expect(row.workers).toEqual([100, 0, 300]);
    expect(row.min).toBe(0);
    expect(row.max).toBe(300);
    // min=0 forces skew to fall back to 1 rather than Infinity, which would break the UI.
    expect(row.skew).toBe(1);
  });

  it("computes per-worker totals across multiple objects", () => {
    const result = pivotDataflowCpuPerWorker([
      mkRow({ objectId: "u1", workerId: 0, elapsedNs: 10n }),
      mkRow({ objectId: "u1", workerId: 1, elapsedNs: 20n }),
      mkRow({ objectId: "u2", workerId: 0, elapsedNs: 100n }),
      mkRow({ objectId: "u2", workerId: 1, elapsedNs: 200n }),
    ]);

    expect(result.rows).toHaveLength(2);
    expect(result.globalWorkerTotals).toEqual([110, 220]);
  });

  it("labels rows from the resolver rather than from the query", () => {
    const result = pivotDataflowCpuPerWorker(
      [mkRow({ workerId: 0, elapsedNs: 1n })],
      naming,
    );
    expect(result.rows[0].label).toBe("my_mv");
    expect(result.rows[0].subLabel).toBe("materialize.public");
    expect(result.rows[0].clickable).toBe(true);
  });

  it("falls back to the object id and blocks navigation for an orphaned dataflow", () => {
    // The dataflow is still running on the replica after its catalog entry was
    // dropped, so the objects subscribe has nothing to resolve.
    const result = pivotDataflowCpuPerWorker(
      [mkRow({ objectId: "u999", workerId: 0, elapsedNs: 1n })],
      naming,
    );
    expect(result.rows[0].label).toBe("u999");
    expect(result.rows[0].clickable).toBe(false);
  });
});

describe("diffDataflowCpuSamples", () => {
  it("reports the CPU spent between the two samples, not the lifetime total", () => {
    const result = diffDataflowCpuSamples(
      [mkRow({ workerId: 0, elapsedNs: 1_000n })],
      [mkRow({ workerId: 0, elapsedNs: 1_250n })],
    );
    expect(result).toHaveLength(1);
    expect(result[0].elapsedNs).toBe(250);
  });

  it("keeps workers separate when differencing", () => {
    const result = diffDataflowCpuSamples(
      [
        mkRow({ workerId: 0, elapsedNs: 100n }),
        mkRow({ workerId: 1, elapsedNs: 100n }),
      ],
      [
        mkRow({ workerId: 0, elapsedNs: 400n }),
        mkRow({ workerId: 1, elapsedNs: 150n }),
      ],
    );
    expect(result.map((r) => r.elapsedNs)).toEqual([300, 50]);
  });

  it("passes through a dataflow that first appeared inside the window", () => {
    const result = diffDataflowCpuSamples(
      [],
      [mkRow({ objectId: "u7", workerId: 0, elapsedNs: 42n })],
    );
    expect(result[0].elapsedNs).toBe(42);
  });

  it("drops a dataflow that went away during the window", () => {
    const result = diffDataflowCpuSamples(
      [mkRow({ objectId: "u7", workerId: 0, elapsedNs: 42n })],
      [],
    );
    expect(result).toHaveLength(0);
  });

  it("clamps to zero when the replica restarted and reset the counter", () => {
    const result = diffDataflowCpuSamples(
      [mkRow({ workerId: 0, elapsedNs: 5_000n })],
      [mkRow({ workerId: 0, elapsedNs: 12n })],
    );
    expect(result[0].elapsedNs).toBe(0);
  });

  it("surfaces skew that a single cumulative sample would hide", () => {
    // Both workers have burned the same CPU over the replica's lifetime, but in
    // this window worker 1 did nine times the work of worker 0.
    const previous = [
      mkRow({ workerId: 0, elapsedNs: 1_000_000n }),
      mkRow({ workerId: 1, elapsedNs: 999_000n }),
    ];
    const next = [
      mkRow({ workerId: 0, elapsedNs: 1_000_100n }),
      mkRow({ workerId: 1, elapsedNs: 999_900n }),
    ];

    expect(pivotDataflowCpuPerWorker(next).rows[0].skew).toBeCloseTo(1, 2);
    expect(
      pivotDataflowCpuPerWorker(diffDataflowCpuSamples(previous, next)).rows[0]
        .skew,
    ).toBe(9);
  });
});
