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
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";

import { SEARCH_PATH } from "../executeSql";
import { buildHydrationAggregateQuery } from "./hydrationAggregate";

describe("buildHydrationAggregateQuery", () => {
  it("aggregates hydrated and total replicas per object", async () => {
    const testSchema = "test_hydration_aggregate";

    // u1: single replica, hydrated → 1/1
    // u2: 3 replicas, all hydrated → 3/3
    // u3: 3 replicas, 1 hydrated → 1/3
    // u4: 2 replicas, none hydrated → 0/2
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${testSchema} CASCADE;
      > CREATE SCHEMA ${testSchema};
      > CREATE TABLE ${testSchema}.mz_hydration_statuses (
          object_id TEXT NOT NULL,
          replica_id TEXT,
          hydrated BOOLEAN
        );
      > INSERT INTO ${testSchema}.mz_hydration_statuses VALUES
          ('u1', 'r1', true),
          ('u2', 'r1', true),
          ('u2', 'r2', true),
          ('u2', 'r3', true),
          ('u3', 'r1', true),
          ('u3', 'r2', false),
          ('u3', 'r3', false),
          ('u4', 'r1', false),
          ('u4', 'r2', false);
    `);

    const compiled = buildHydrationAggregateQuery().compile();
    const result = await executeSqlHttp(compiled, {
      sessionVariables: {
        cluster: QUICKSTART_CLUSTER,
        search_path: `${testSchema}, ${SEARCH_PATH}`,
      },
    });

    const rows = result.rows.sort((a, b) =>
      a.object_id.localeCompare(b.object_id),
    );
    expect(rows).toHaveLength(4);

    expect(rows[0]).toMatchObject({ object_id: "u1" });
    expect(Number(rows[0].hydratedReplicas)).toBe(1);
    expect(Number(rows[0].totalReplicas)).toBe(1);

    expect(rows[1]).toMatchObject({ object_id: "u2" });
    expect(Number(rows[1].hydratedReplicas)).toBe(3);
    expect(Number(rows[1].totalReplicas)).toBe(3);

    expect(rows[2]).toMatchObject({ object_id: "u3" });
    expect(Number(rows[2].hydratedReplicas)).toBe(1);
    expect(Number(rows[2].totalReplicas)).toBe(3);

    expect(rows[3]).toMatchObject({ object_id: "u4" });
    expect(Number(rows[3].hydratedReplicas)).toBe(0);
    expect(Number(rows[3].totalReplicas)).toBe(2);
  });

  it("treats null hydrated values as not-hydrated for the FILTER clause", async () => {
    const testSchema = "test_hydration_aggregate";

    // A replica that hasn't reported a status yet shows up as a null row;
    // the FILTER (WHERE hydrated) should not count it as hydrated, but the
    // total count still includes it.
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${testSchema} CASCADE;
      > CREATE SCHEMA ${testSchema};
      > CREATE TABLE ${testSchema}.mz_hydration_statuses (
          object_id TEXT NOT NULL,
          replica_id TEXT,
          hydrated BOOLEAN
        );
      > INSERT INTO ${testSchema}.mz_hydration_statuses VALUES
          ('u1', 'r1', true),
          ('u1', 'r2', NULL);
    `);

    const compiled = buildHydrationAggregateQuery().compile();
    const result = await executeSqlHttp(compiled, {
      sessionVariables: {
        cluster: QUICKSTART_CLUSTER,
        search_path: `${testSchema}, ${SEARCH_PATH}`,
      },
    });

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ object_id: "u1" });
    expect(Number(result.rows[0].hydratedReplicas)).toBe(1);
    expect(Number(result.rows[0].totalReplicas)).toBe(2);
  });
});
