// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SEARCH_PATH } from "~/api/materialize";
import {
  executeSqlHttp,
  getMaterializeClient,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";
import { readSubscribeSnapshot } from "~/test/sql/subscribeSnapshot";

import {
  buildReplicaProcessMetricsSubscribe,
  buildReplicaSizeAllocationsQuery,
} from "./replicaProcessMetrics";

const MOCKED_SEARCH_PATH = "internal_test, " + SEARCH_PATH;

describe("buildReplicaProcessMetricsSubscribe", () => {
  it("reads each process's samples for the given replicas from the literal start", async () => {
    await testdrive(`
      > CREATE SCHEMA internal_test;
      > SET schema = internal_test;
      > CREATE TABLE mz_cluster_replica_metrics_history (
          replica_id text NOT NULL,
          process_id uint8 NOT NULL,
          cpu_nano_cores uint8,
          memory_bytes uint8,
          disk_bytes uint8,
          occurred_at timestamptz NOT NULL,
          heap_bytes uint8,
          heap_limit uint8,
          swap_bytes uint8
        );
      > INSERT INTO mz_cluster_replica_metrics_history VALUES
          ('u1', 0, 100, 1000, 0, '2026-10-20T09:59:00Z', 100, 1000, 0),
          ('u1', 0, 300, 1000, 0, '2026-10-20T10:00:00Z', 500, 1000, 0),
          ('u1', 1, NULL, NULL, NULL, '2026-10-20T10:00:00Z', NULL, NULL, NULL),
          ('u1', 1, 900, 2000, 0, '2026-10-20T10:01:00Z', 700, 1000, 0),
          ('u2', 0, 300, 1000, 0, '2026-10-20T10:00:00Z', 500, 1000, 0),
          ('u3', 0, 400, 1000, 0, '2026-10-20T10:02:00Z', NULL, NULL, 0);
    `);
    const client = await getMaterializeClient();
    await client.query(`SET search_path TO ${MOCKED_SEARCH_PATH}`);

    const rows = await readSubscribeSnapshot(
      client,
      buildReplicaProcessMetricsSubscribe({
        replicaIds: ["u1", "u3"],
        since: new Date("2026-10-20T10:00:00Z"),
      }),
    );

    expect(rows.every((row) => row.mz_state === "upsert")).toBe(true);
    const samples = rows
      .map((row) => ({
        replicaId: row.replicaId,
        processId: row.processId,
        occurredAt: (row.occurredAt as Date).toISOString(),
        cpuNanoCores: row.cpuNanoCores,
        memoryBytes: row.memoryBytes,
        heapBytes: row.heapBytes,
        heapLimit: row.heapLimit,
      }))
      .sort((a, b) =>
        `${a.replicaId}${a.processId}${a.occurredAt}`.localeCompare(
          `${b.replicaId}${b.processId}${b.occurredAt}`,
        ),
      );
    // Replica u2's sample and u1's sample before the start are left out. A
    // process whose metrics couldn't be read keeps its row, with nulls.
    expect(samples).toEqual([
      {
        replicaId: "u1",
        processId: "0",
        occurredAt: "2026-10-20T10:00:00.000Z",
        cpuNanoCores: 300,
        memoryBytes: 1000,
        heapBytes: 500,
        heapLimit: 1000,
      },
      {
        replicaId: "u1",
        processId: "1",
        occurredAt: "2026-10-20T10:00:00.000Z",
        cpuNanoCores: null,
        memoryBytes: null,
        heapBytes: null,
        heapLimit: null,
      },
      {
        replicaId: "u1",
        processId: "1",
        occurredAt: "2026-10-20T10:01:00.000Z",
        cpuNanoCores: 900,
        memoryBytes: 2000,
        heapBytes: 700,
        heapLimit: 1000,
      },
      {
        replicaId: "u3",
        processId: "0",
        occurredAt: "2026-10-20T10:02:00.000Z",
        cpuNanoCores: 400,
        memoryBytes: 1000,
        heapBytes: null,
        heapLimit: null,
      },
    ]);
  });

  it("runs against the catalog's metrics history", async () => {
    // Reset Materialize state.
    await testdrive(`> SET cluster to quickstart;`);
    const client = await getMaterializeClient();
    const {
      rows: [replica],
    } = await client.query(
      "SELECT r.id FROM mz_cluster_replicas r JOIN mz_clusters c ON c.id = r.cluster_id WHERE c.name = 'quickstart'",
    );

    const rows = await readSubscribeSnapshot(
      client,
      buildReplicaProcessMetricsSubscribe({
        replicaIds: [replica.id],
        since: new Date(Date.now() - 60 * 60 * 1000),
      }),
    );

    for (const row of rows) {
      expect(row.replicaId).toEqual(replica.id);
      expect(row.processId).toMatch(/^\d+$/);
    }
  });
});

describe("buildReplicaSizeAllocationsQuery", () => {
  it("reads each listed size's process and worker counts and per-process allocation", async () => {
    // Reset Materialize state.
    await testdrive(`> SET cluster to quickstart;`);
    const client = await getMaterializeClient();
    const sizes = ["scale=2,workers=4", "scale=1,workers=1"];
    const { rows: expected } = await client.query(
      `SELECT size, processes, workers, cpu_nano_cores, memory_bytes, disk_bytes
       FROM mz_cluster_replica_sizes WHERE size IN ('${sizes.join("', '")}')`,
    );

    const { rows } = await executeSqlHttp(
      buildReplicaSizeAllocationsQuery({ sizes }).compile(),
    );

    const bySize = new Map(rows.map((row) => [row.size, row]));
    expect([...bySize.keys()].sort()).toEqual([...sizes].sort());
    expect(bySize.get("scale=2,workers=4")).toMatchObject({
      processes: 2,
      workers: 4,
    });
    for (const size of expected) {
      expect(bySize.get(size.size)).toEqual({
        size: size.size,
        processes: Number(size.processes),
        workers: Number(size.workers),
        cpuNanoCores: Number(size.cpu_nano_cores),
        memoryBytes: Number(size.memory_bytes),
        diskBytes: size.disk_bytes === null ? null : Number(size.disk_bytes),
      });
    }
  });
});
