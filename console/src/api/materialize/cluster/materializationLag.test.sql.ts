// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { SEARCH_PATH } from "../executeSql";
import {
  buildBlockedDependenciesQuery,
  buildMaterializationLagQuery,
  OUTDATED_THRESHOLD_SECONDS,
} from "./materializationLag";

async function createBasicLagGraph() {
  /*
   * ┌───────────────────┐              ┌───────────────────┐
   * │                   │              │                   │
   * │ source 1 (u1)     │              │ source 2 (u2)     │
   * │                   │              │                   │
   * │ freshness: 1s     │              │ freshness: 1s     │
   * └─────────┬─────────┘              └──────────┬────────┘
   *           │                                   │
   * ┌─────────▼─────────┐              ┌──────────▼────────┐
   * │                   │              │                   │
   * │  index 1 (u3)     │              │      mv 1(u4)     │
   * │                   │◄─────────────┤                   │
   * │ freshness:   3s   │              │ freshness:  2s    │
   * └─────────┬─────────┘              └───────────────────┘
   *           │
   * ┌─────────▼─────────┐
   * │                   │
   * │   index 2(u5)     │
   * │                   │
   * │ freshness:  10s   │
   * └───────────────────┘
   */
  const objectIds = ["u1", "u2", "u3", "u4", "u5"];

  const testSchema = "test_schema";
  await testdrive(`
      > CREATE SCHEMA ${testSchema};
      > SET schema to ${testSchema};
      > CREATE TABLE ${testSchema}.mz_objects (
          id TEXT NOT NULL,
          name TEXT NOT NULL,
          type TEXT NOT NULL
        );
      > INSERT INTO ${testSchema}.mz_objects VALUES
          ('u1', 'source 1', 'source'),
          ('u2', 'source 2', 'source'),
          ('u3', 'index 1', 'index'),
          ('u4', 'materialized view 1', 'materialized-view'),
          ('u5', 'index 2', 'index');
      > CREATE TABLE ${testSchema}.mz_hydration_statuses (
          object_id TEXT NOT NULL,
          hydrated BOOLEAN
        );
      > INSERT INTO ${testSchema}.mz_hydration_statuses VALUES
          ('u1', true),
          ('u2', true),
          ('u3', true),
          ('u4', true),
          ('u5', true);
      > CREATE TABLE ${testSchema}.mz_wallclock_global_lag_recent_history (
          object_id TEXT NOT NULL,
          lag INTERVAL,
          occurred_at TIMESTAMP
        );
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u1', INTERVAL '1 SECOND', now()),
          ('u2', INTERVAL '1 SECOND', now()),
          ('u3', INTERVAL '3 SECONDS', now()),
          ('u4', INTERVAL '2 SECONDS', now()),
          ('u5', INTERVAL '${OUTDATED_THRESHOLD_SECONDS} SECONDS', now());`);

  return { objectIds, testSchema };
}

describe("buildMaterializationLagQuery", () => {
  it("up to date and hydrated workflow graph", async () => {
    const { objectIds, testSchema } = await createBasicLagGraph();

    const materializationLagQuery = buildMaterializationLagQuery({
      objectIds,
    }).compile();

    const result = await executeSqlHttp(materializationLagQuery, {
      sessionVariables: {
        cluster: QUICKSTART_CLUSTER,
        search_path: `${testSchema}, ${SEARCH_PATH}`,
      },
    });

    const [source1, source2, index1, mv1, index2] = result.rows.sort((a, b) =>
      a.targetObjectId.localeCompare(b.targetObjectId),
    );

    expect(source1).toMatchObject({
      targetObjectId: "u1",
      type: "source",
      hydrated: true,
      isOutdated: false,
    });

    expect(source1.lag?.toPostgres()).toBe("1 seconds");

    expect(source2).toMatchObject({
      targetObjectId: "u2",
      type: "source",
      hydrated: true,
      isOutdated: false,
    });

    expect(source2.lag?.toPostgres()).toBe("1 seconds");

    expect(index1).toMatchObject({
      targetObjectId: "u3",
      type: "index",
      hydrated: true,
      isOutdated: false,
    });

    expect(index1.lag?.toPostgres()).toBe("3 seconds");

    expect(mv1).toMatchObject({
      targetObjectId: "u4",
      type: "materialized-view",
      hydrated: true,
      isOutdated: false,
    });

    expect(mv1.lag?.toPostgres()).toBe("2 seconds");

    expect(index2).toMatchObject({
      targetObjectId: "u5",
      type: "index",
      hydrated: true,
      // Should be outdated because the freshness value is greater than 10 seconds
      isOutdated: true,
    });

    expect(index2.lag?.toPostgres()).toBe("10 seconds");
  });
});

describe("buildBlockedDependenciesQuery", () => {
  it("should return associated cluster info for indexes and materialized views", async () => {
    await testdrive(`
      > CREATE CLUSTER mv_cluster (SIZE = 'scale=1,workers=1');
      > SET CLUSTER TO mv_cluster;
      > CREATE MATERIALIZED VIEW mv AS SELECT id FROM mz_catalog.mz_objects;
      > CREATE CLUSTER index_cluster (SIZE = 'scale=1,workers=1');
      > SET CLUSTER TO index_cluster;
      > CREATE DEFAULT INDEX idx ON mv;
      > SET CLUSTER TO quickstart;
      `);
    const client = await getMaterializeClient();

    const {
      rows: [idx, mv],
    } = await client.query(
      "select id, name from mz_objects where name = 'mv' OR name='idx' ORDER BY name",
    );

    // Sometimes each object's frontiers are not created yet, so we need to wait for them
    await client.query(
      `select object_id from mz_internal.mz_wallclock_global_lag_recent_history where object_id = '${idx.id}' OR object_id='${mv.id}'`,
    );

    const blockedDependenciesQuery = buildBlockedDependenciesQuery({
      objectIds: [idx.id, mv.id],
    }).compile();

    const result = await executeSqlHttp(blockedDependenciesQuery);

    const [idxResult, mvResult] = result.rows;

    expect(idxResult).toMatchObject({
      targetObjectId: idx.id,
      clusterId: expect.stringMatching("^u"),
      clusterName: "index_cluster",
    });

    expect(mvResult).toMatchObject({
      targetObjectId: mv.id,
      clusterId: expect.stringMatching("^u"),
      clusterName: "mv_cluster",
    });
  });
});
