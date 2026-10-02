// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SEARCH_PATH } from "~/api/materialize";
import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";
import { waitFor } from "~/test/sql/waitFor";

import { buildObjectCreationTimesQuery } from "./objectCreationTimes";
import {
  buildEnvironmentStartedAtQuery,
  buildObjectHydrationTimesQuery,
} from "./replicaStatusTimeline";

const TEST_SCHEMA = "test_object_hydration_times";

const run = (
  params: Parameters<typeof buildObjectHydrationTimesQuery>[0],
  searchPath = SEARCH_PATH,
) =>
  executeSqlHttp(buildObjectHydrationTimesQuery(params).compile(), {
    sessionVariables: { cluster: QUICKSTART_CLUSTER, search_path: searchPath },
  });

describe("buildObjectHydrationTimesQuery", () => {
  it("reads the given replicas' hydration times of the given objects in milliseconds", async () => {
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
      > CREATE SCHEMA ${TEST_SCHEMA};
      > CREATE TABLE ${TEST_SCHEMA}.mz_compute_hydration_times (
          replica_id TEXT NOT NULL,
          object_id TEXT NOT NULL,
          time_ns uint8
        );
      > INSERT INTO ${TEST_SCHEMA}.mz_compute_hydration_times VALUES
          ('u1', 'u11', 1500000000),
          ('u1', 'u12', NULL),
          ('u2', 'u11', 2500000),
          ('u3', 'u11', 1000000),
          ('u1', 'u99', 1000000);
    `);

    const { rows } = await run(
      { replicaIds: ["u1", "u2"], objectIds: ["u11", "u12"] },
      `${TEST_SCHEMA}, ${SEARCH_PATH}`,
    );

    expect(
      rows.sort(
        (a, b) =>
          a.replicaId.localeCompare(b.replicaId) ||
          a.objectId.localeCompare(b.objectId),
      ),
    ).toEqual([
      { replicaId: "u1", objectId: "u11", hydrationMs: 1500 },
      { replicaId: "u1", objectId: "u12", hydrationMs: null },
      { replicaId: "u2", objectId: "u11", hydrationMs: 2.5 },
    ]);
  });

  it("reads a new index's hydration time from the catalog", async () => {
    await testdrive(`
      > DROP TABLE IF EXISTS hydration_times_t CASCADE;
      > CREATE TABLE hydration_times_t (a int);
      > CREATE INDEX hydration_times_t_idx IN CLUSTER ${QUICKSTART_CLUSTER} ON hydration_times_t (a);
    `);
    const client = await getMaterializeClient();
    const {
      rows: [{ indexId, replicaId }],
    } = await client.query(`
      SELECT i.id AS "indexId", r.id AS "replicaId"
      FROM mz_indexes i
      JOIN mz_cluster_replicas r USING (cluster_id)
      WHERE i.name = 'hydration_times_t_idx'`);

    await waitFor(async () => {
      const { rows } = await run({
        replicaIds: [replicaId],
        objectIds: [indexId],
      });
      expect(rows).toEqual([
        { replicaId, objectId: indexId, hydrationMs: expect.any(Number) },
      ]);
    }, 20_000);
  });
});

describe("buildEnvironmentStartedAtQuery", () => {
  it("places environmentd's start before an object created since", async () => {
    await testdrive(`
      > CREATE TABLE started_after_t (a int);
      > CREATE INDEX started_after_t_idx IN CLUSTER ${QUICKSTART_CLUSTER} ON started_after_t (a);
    `);
    const client = await getMaterializeClient();
    const {
      rows: [{ id }],
    } = await client.query(
      "SELECT id FROM mz_indexes WHERE name = 'started_after_t_idx'",
    );
    const {
      rows: [created],
    } = await executeSqlHttp(
      buildObjectCreationTimesQuery({
        objectIds: [id],
        startDate: new Date(0).toISOString(),
      }).compile(),
    );
    const {
      rows: [started],
    } = await executeSqlHttp(buildEnvironmentStartedAtQuery().compile());

    expect(started.startedAt).toBeInstanceOf(Date);
    expect(started.startedAt.getTime()).toBeLessThanOrEqual(
      created.occurredAt.getTime(),
    );
  });
});
