// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import PostgresInterval from "postgres-interval";

import {
  executeSqlHttp,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { buildLagHistoryQuery } from "./lagHistory";

describe("lagHistory", () => {
  it("buckets lag metrics by cluster", async () => {
    // Simple system test for historical lookback of one hour
    {
      // Retry until the table has data in it
      await testdrive(
        `> SET cluster to quickstart;
        > select (select count(*) from mz_internal.mz_wallclock_global_lag_recent_history) > 0
        true`,
        {
          noReset: true,
          timeoutSeconds: 20,
        },
      );

      const query = buildLagHistoryQuery({
        lookback: {
          type: "historical",
          lookbackMs: 60_000 * 60,
        },
        groupByCluster: true,
        clusterId: "s2", // mz_catalog_server
        includeSystemObjects: true,
      }).compile();

      const res = (
        await executeSqlHttp(query, {
          sessionVariables: {
            cluster: QUICKSTART_CLUSTER,
          },
        })
      ).rows;

      expect(res[0]).toMatchObject({
        bucketStart: expect.any(Date),
        clusterId: expect.any(String),
        objectId: expect.any(String),
        clusterName: expect.any(String),
        schemaName: expect.any(String),
        objectName: expect.any(String),
      });

      // Lag can be null when an index isn't hydrated or when there is no readable time
      expect(res[0].lag).toSatisfy(
        (lag: any) => lag === null || lag instanceof PostgresInterval,
      );
    }
  });
});
