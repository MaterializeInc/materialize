// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SEARCH_PATH } from "~/api/materialize";
import { NUMBER_OR_NULL } from "~/test/sql/matchers";
import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildReplicaUtilizationQuery } from "./replicaUtilization";

describe("buildReplicaUtilizationQuery", () => {
  it("runs against the maintained view", async () => {
    const query = buildReplicaUtilizationQuery({ includeSwap: true }).compile();
    const result = await executeSqlHttp(query);

    // The environment may have no metric samples inside the window yet, so the
    // assertion is on the shape rather than on a reading. What this proves is
    // that the query plans and executes: in particular that `mz_now()` is
    // accepted in the `WHERE` of a one-shot SELECT, and that every column named
    // here exists on `mz_console_cluster_utilization_overview_3h`.
    for (const row of result.rows) {
      expect(row).toEqual({
        replicaId: expect.any(String),
        cpuPercent: NUMBER_OR_NULL,
        memoryPercent: NUMBER_OR_NULL,
        diskPercent: NUMBER_OR_NULL,
        heapPercent: NUMBER_OR_NULL,
        swapPercent: NUMBER_OR_NULL,
      });
    }
  });

  it("reads swap as a share of the heap limit, only for sizes that can swap", async () => {
    const mockedSearchPath = "internal_test, " + SEARCH_PATH;
    await testdrive(`
        > CREATE SCHEMA IF NOT EXISTS internal_test;
        > SET schema = internal_test;
        > DROP TABLE IF EXISTS mz_console_cluster_utilization_overview_3h;
        > CREATE TABLE mz_console_cluster_utilization_overview_3h (
            replica_id TEXT,
            occurred_at TIMESTAMPTZ,
            cpu_percent DOUBLE,
            memory_percent DOUBLE,
            disk_percent DOUBLE,
            heap_percent DOUBLE,
            swap_of_ram_percent DOUBLE,
            heap_limit_percent DOUBLE
          );
        > INSERT INTO internal_test.mz_console_cluster_utilization_overview_3h VALUES
            ('u1', '2030-01-01T00:00:00Z', 0.1, 0.9, NULL, 0.7, 0.5, 2),
            ('u2', '2030-01-01T00:00:00Z', 0.1, 0.9, NULL, 0.9, 0, 1),
            ('u3', '2030-01-01T00:00:00Z', 0.1, 0.9, NULL, 0.9, 0, NULL);
    `);
    const client = await getMaterializeClient();
    await client.query(`SET search_path TO ${mockedSearchPath};`);

    const { rows } = await executeSqlHttp(
      buildReplicaUtilizationQuery({ includeSwap: true }).compile(),
      {
        sessionVariables: {
          search_path: mockedSearchPath,
          cluster: QUICKSTART_CLUSTER,
        },
      },
    );

    // u2's heap limit is its RAM and u3 reports none, so neither can swap.
    expect(
      rows.map(({ replicaId, swapPercent }) => [replicaId, swapPercent]),
    ).toEqual([
      ["u1", 0.25],
      ["u2", null],
      ["u3", null],
    ]);
  });
});
