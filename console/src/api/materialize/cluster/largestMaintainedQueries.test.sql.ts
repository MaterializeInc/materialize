// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { executeSqlHttp } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildLargestMaintainedQueriesQuery } from "./largestMaintainedQueries";

describe("buildLargestMaintainedQueriesQuery", () => {
  it(
    "fetches the largest maintained queries",
    { timeout: 20_000 },
    async () => {
      await testdrive(`> SET database TO materialize;`);
      const query = buildLargestMaintainedQueriesQuery({
        replicaHeapLimit: 1024 ** 4,
        limit: 100,
      }).compile();
      const result = await executeSqlHttp(query, {
        sessionVariables: {
          cluster: "mz_catalog_server",
          cluster_replica: "r1",
        },
      });
      expect(result.rows).toContainEqual({
        databaseName: null,
        dataflowId: expect.any(BigInt),
        dataflowName: "Dataflow: mz_catalog.mz_views_ind",
        id: expect.any(String),
        memoryPercentage: expect.any(Number),
        name: "mz_views_ind",
        schemaName: "mz_catalog",
        size: expect.any(BigInt),
        type: "index",
      });
    },
  );
});
