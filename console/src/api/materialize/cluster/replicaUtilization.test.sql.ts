// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { NUMBER_OR_NULL } from "~/test/sql/matchers";
import { executeSqlHttp } from "~/test/sql/materializeSqlClient";

import { buildReplicaUtilizationQuery } from "./replicaUtilization";

describe("buildReplicaUtilizationQuery", () => {
  it("runs against the maintained view", async () => {
    const query = buildReplicaUtilizationQuery().compile();
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
      });
    }
  });
});
