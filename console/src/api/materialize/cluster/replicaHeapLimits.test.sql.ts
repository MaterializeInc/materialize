// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CATALOG_SERVER_CLUSTER, SEARCH_PATH } from "~/api/materialize";
import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildReplicaHeapLimitsQuery } from "./replicaHeapLimits";

const MOCKED_SEARCH_PATH = "internal_test, " + SEARCH_PATH;

describe("buildReplicaHeapLimitsQuery", () => {
  it("reads each process's heap limit and use for the given replicas", async () => {
    await testdrive(`
      > CREATE SCHEMA internal_test;
      > SET schema = internal_test;
      > CREATE TABLE mz_cluster_replica_metrics (
          replica_id text NOT NULL,
          process_id uint8 NOT NULL,
          cpu_nano_cores uint8,
          memory_bytes uint8,
          disk_bytes uint8,
          heap_bytes uint8,
          heap_limit uint8
        );
      > INSERT INTO mz_cluster_replica_metrics VALUES
          ('u1', 0, 100, 1000, 0, 400, 2000),
          ('u1', 1, NULL, NULL, NULL, NULL, NULL),
          ('u2', 0, 100, 1000, 0, 500, 2000);
    `);

    const { rows } = await executeSqlHttp(
      buildReplicaHeapLimitsQuery({ replicaIds: ["u1"] }).compile(),
      {
        sessionVariables: {
          search_path: MOCKED_SEARCH_PATH,
          cluster: QUICKSTART_CLUSTER,
        },
      },
    );

    // A process whose metrics couldn't be read keeps its row, with nulls.
    expect(
      [...rows].sort((a, b) => Number(a.processId) - Number(b.processId)),
    ).toEqual([
      {
        replicaId: "u1",
        processId: BigInt(0),
        heapLimit: BigInt(2000),
        memoryBytes: BigInt(1000),
        heapBytes: BigInt(400),
      },
      {
        replicaId: "u1",
        processId: BigInt(1),
        heapLimit: null,
        memoryBytes: null,
        heapBytes: null,
      },
    ]);
  });

  it("is a fast-path index lookup on the catalog server", async () => {
    const client = await getMaterializeClient();
    await client.query(`SET cluster = ${CATALOG_SERVER_CLUSTER}`);
    const { sql, parameters } = buildReplicaHeapLimitsQuery({
      replicaIds: ["u1", "u2"],
    }).compile();

    const {
      rows: [plan],
    } = await client.query(`EXPLAIN ${sql}`, [...parameters]);

    const explained = String(Object.values(plan)[0]);
    expect(explained).toContain("Explained Query (fast path)");
    expect(explained).toContain(
      "Index Lookup on mz_internal.mz_cluster_replica_metrics",
    );
  });
});
