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

import { buildDroppedObjectLifetimesQuery } from "./objectCreationTimes";

describe("buildDroppedObjectLifetimesQuery", () => {
  it("returns the cluster's indexes and materialized views created since the start and dropped since", async () => {
    // Reset Materialize state.
    await testdrive(`
      > CREATE TABLE materialize.public.t (a int);
      > CREATE INDEX before_start_idx IN CLUSTER quickstart ON materialize.public.t (a);
    `);
    const client = await getMaterializeClient();
    const {
      rows: [{ start_date: startDate, cluster_id: clusterId }],
    } = await client.query(
      "SELECT now() AS start_date, id AS cluster_id FROM mz_clusters WHERE name = 'quickstart'",
    );
    for (const statement of [
      "CREATE INDEX dropped_idx IN CLUSTER quickstart ON materialize.public.t (a)",
      "CREATE MATERIALIZED VIEW materialize.public.dropped_mv IN CLUSTER quickstart AS SELECT a FROM materialize.public.t",
      "CREATE INDEX kept_idx IN CLUSTER quickstart ON materialize.public.t (a)",
      "CREATE TABLE materialize.public.dropped_t (a int)",
    ]) {
      await client.query(statement);
    }
    const { rows: objects } = await client.query(
      "SELECT id, name FROM mz_objects WHERE name IN ('dropped_idx', 'dropped_mv')",
    );
    for (const statement of [
      "DROP INDEX materialize.public.before_start_idx",
      "DROP INDEX materialize.public.dropped_idx",
      "DROP MATERIALIZED VIEW materialize.public.dropped_mv",
      "DROP TABLE materialize.public.dropped_t",
    ]) {
      await client.query(statement);
    }

    const { rows } = await executeSqlHttp(
      buildDroppedObjectLifetimesQuery({
        clusterId,
        objectTypes: ["index", "materialized-view"],
        startDate: (startDate as Date).toISOString(),
      }).compile(),
      { sessionVariables: { cluster: QUICKSTART_CLUSTER } },
    );

    expect(rows.map(({ id, objectType }) => [id, objectType]).sort()).toEqual(
      objects
        .map(({ id, name }) => [
          id,
          name === "dropped_idx" ? "index" : "materialized-view",
        ])
        .sort(),
    );
    for (const { createdAt, droppedAt } of rows) {
      expect(createdAt).toBeInstanceOf(Date);
      expect(droppedAt.getTime()).toBeGreaterThan(createdAt.getTime());
    }
  });
});
