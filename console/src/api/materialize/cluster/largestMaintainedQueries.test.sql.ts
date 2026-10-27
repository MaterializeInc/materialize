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

import {
  buildLargestMaintainedObjectSizesQuery,
  buildLargestMaintainedQueriesQuery,
  buildMaintainedObjectNamesQuery,
} from "./largestMaintainedQueries";

describe("buildLargestMaintainedQueriesQuery", () => {
  it(
    "fetches the largest maintained queries",
    { timeout: 45_000 },
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

describe("buildLargestMaintainedObjectSizesQuery", () => {
  it(
    "fetches sizes from mz_object_arrangement_sizes and resolves names",
    { timeout: 60_000 },
    async () => {
      const fetchSizes = () =>
        executeSqlHttp(
          buildLargestMaintainedObjectSizesQuery({
            // mz_catalog_server has the stable builtin cluster id s2.
            clusterId: "s2",
            replicaName: "r1",
            replicaHeapLimit: 1024 ** 4,
            limit: 100,
          }).compile(),
          { sessionVariables: { cluster: "mz_catalog_server" } },
        );
      // The collection is fed by an introspection subscribe, so it can be
      // briefly empty right after the stack boots.
      let sizes = await fetchSizes();
      for (
        let attempt = 0;
        attempt < 15 && sizes.rows.length === 0;
        attempt++
      ) {
        await new Promise((resolve) => setTimeout(resolve, 2000));
        sizes = await fetchSizes();
      }
      expect(sizes.rows.length).toBeGreaterThan(0);
      expect(sizes.rows[0]).toEqual({
        object_id: expect.any(String),
        size: expect.any(BigInt),
        memoryPercentage: expect.any(Number),
      });

      const namesQuery = buildMaintainedObjectNamesQuery(
        sizes.rows.map((row) => row.object_id),
      ).compile();
      const names = await executeSqlHttp(namesQuery, {
        sessionVariables: { cluster: "mz_catalog_server" },
      });
      expect(names.rows).toContainEqual({
        databaseName: null,
        id: expect.any(String),
        name: "mz_views_ind",
        schemaName: "mz_catalog",
        type: "index",
      });
    },
  );
});
