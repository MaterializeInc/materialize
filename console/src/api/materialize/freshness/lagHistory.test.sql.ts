// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import PostgresInterval from "postgres-interval";

import { CATALOG_SERVER_CLUSTER, SEARCH_PATH } from "~/api/materialize";
import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import {
  buildLagHistoryQuery,
  buildLatestLagQuery,
  buildObjectLagHistoryQuery,
} from "./lagHistory";

const MANY_IDS = Array.from({ length: 400 }, (_, i) => `u${i + 1}`);

describe("lag reads for a known set of objects", () => {
  it("returns each object's newest reading for 400 ids", async () => {
    await testdrive(`
      > CREATE SCHEMA lag_test;
      > SET schema = lag_test;
      > CREATE TABLE mz_wallclock_global_lag_recent_history (
          object_id text NOT NULL,
          lag interval,
          occurred_at timestamptz NOT NULL
        );
      > INSERT INTO mz_wallclock_global_lag_recent_history VALUES
          ('u1', INTERVAL '5 SECONDS', now() - INTERVAL '1 MINUTE'),
          ('u1', INTERVAL '2 SECONDS', now()),
          ('u2', NULL, now());
    `);

    const { rows } = await executeSqlHttp(
      buildLatestLagQuery(MANY_IDS).compile(),
      {
        sessionVariables: {
          search_path: `lag_test, ${SEARCH_PATH}`,
          cluster: QUICKSTART_CLUSTER,
        },
      },
    );

    expect(
      rows
        .map((row) => ({
          objectId: row.objectId,
          lag: row.lag?.toPostgres() ?? null,
        }))
        .sort((a, b) => a.objectId.localeCompare(b.objectId)),
    ).toEqual([
      { objectId: "u1", lag: "2 seconds" },
      { objectId: "u2", lag: null },
    ]);
  });

  it.each([
    [
      "the binned series",
      buildObjectLagHistoryQuery({
        objectIds: MANY_IDS,
        lookbackMs: 60 * 60 * 1000,
      }),
    ],
    ["the newest reading", buildLatestLagQuery(MANY_IDS)],
  ])("reads %s by key, not a full scan, for 400 ids", async (_, query) => {
    const client = await getMaterializeClient();
    await client.query(`SET cluster = ${CATALOG_SERVER_CLUSTER}`);
    const { sql, parameters } = query.compile();

    const {
      rows: [plan],
    } = await client.query(`EXPLAIN ${sql}`, [...parameters]);

    const explained = String(Object.values(plan)[0]);
    expect(explained).toContain(
      "mz_internal.mz_wallclock_global_lag_recent_history_ind",
    );
    expect(explained).not.toContain(
      "mz_wallclock_global_lag_recent_history_ind (*** full scan ***)",
    );
  });
});

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
