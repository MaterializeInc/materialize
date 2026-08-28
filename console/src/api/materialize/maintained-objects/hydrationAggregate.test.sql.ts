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

const TEST_SCHEMA = "test_hydration_aggregate";

/** Shadows the four relations the query reads. Empty value lists are elided.
 *  `replicas` defaults to the ids the fixtures use, so statistics rows count
 *  as coming from live replicas unless a test overrides it. */
const seed = ({
  hydration = "",
  statuses = "",
  statistics = "",
  replicas = "('r1'), ('r2'), ('r3')",
  sources = "",
}: {
  hydration?: string;
  statuses?: string;
  statistics?: string;
  replicas?: string;
  sources?: string;
}) => `
  > DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
  > CREATE SCHEMA ${TEST_SCHEMA};
  > CREATE TABLE ${TEST_SCHEMA}.mz_hydration_statuses (
      object_id TEXT NOT NULL,
      replica_id TEXT,
      hydrated BOOLEAN
    );
  > CREATE TABLE ${TEST_SCHEMA}.mz_sources (
      id TEXT NOT NULL,
      type TEXT NOT NULL
    );
  > CREATE TABLE ${TEST_SCHEMA}.mz_source_statuses (
      id TEXT NOT NULL,
      status TEXT NOT NULL,
      error TEXT,
      type TEXT NOT NULL
    );
  > CREATE TABLE ${TEST_SCHEMA}.mz_source_statistics (
      id TEXT NOT NULL,
      replica_id TEXT,
      snapshot_committed BOOLEAN
    );
  > CREATE TABLE ${TEST_SCHEMA}.mz_cluster_replicas (
      id TEXT NOT NULL
    );
  ${hydration ? `> INSERT INTO ${TEST_SCHEMA}.mz_hydration_statuses VALUES ${hydration};` : ""}
  ${statuses ? `> INSERT INTO ${TEST_SCHEMA}.mz_source_statuses VALUES ${statuses};` : ""}
  ${statistics ? `> INSERT INTO ${TEST_SCHEMA}.mz_source_statistics VALUES ${statistics};` : ""}
  ${replicas ? `> INSERT INTO ${TEST_SCHEMA}.mz_cluster_replicas VALUES ${replicas};` : ""}
  ${sources ? `> INSERT INTO ${TEST_SCHEMA}.mz_sources VALUES ${sources};` : ""}
`;

const run = async () => {
  const compiled = buildHydrationAggregateQuery().compile();
  const result = await executeSqlHttp(compiled, {
    sessionVariables: {
      cluster: QUICKSTART_CLUSTER,
      search_path: `${TEST_SCHEMA}, ${SEARCH_PATH}`,
    },
  });
  return result.rows.sort((a, b) => a.object_id.localeCompare(b.object_id));
};

describe("buildHydrationAggregateQuery", () => {
  it("aggregates hydrated and total replicas per object", async () => {
    // u1: single replica, hydrated → 1/1
    // u2: 3 replicas, all hydrated → 3/3
    // u3: 3 replicas, 1 hydrated → 1/3
    // u4: 2 replicas, none hydrated → 0/2
    await testdrive(
      seed({
        hydration: `
          ('u1', 'r1', true),
          ('u2', 'r1', true),
          ('u2', 'r2', true),
          ('u2', 'r3', true),
          ('u3', 'r1', true),
          ('u3', 'r2', false),
          ('u3', 'r3', false),
          ('u4', 'r1', false),
          ('u4', 'r2', false)`,
      }),
    );

    const rows = await run();
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

    // No source rows seeded, so the source columns are null throughout.
    expect(rows[0].sourceStatus).toBeNull();
    expect(rows[0].snapshotCommitted).toBeNull();
  });

  it("treats null hydrated values as not-hydrated for the FILTER clause", async () => {
    // A replica that hasn't reported a status yet shows up as a null row;
    // the FILTER (WHERE hydrated) should not count it as hydrated, but the
    // total count still includes it.
    await testdrive(
      seed({ hydration: `('u1', 'r1', true), ('u1', 'r2', NULL)` }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ object_id: "u1" });
    expect(Number(rows[0].hydratedReplicas)).toBe(1);
    expect(Number(rows[0].totalReplicas)).toBe(2);
  });

  it("joins source status, error and the snapshot flag onto source rows", async () => {
    await testdrive(
      seed({
        hydration: `('u1', 'r1', true), ('u2', 'r1', false)`,
        statuses: `
          ('u1', 'running', NULL, 'kafka'),
          ('u2', 'stalled', 'broker unreachable', 'kafka')`,
        statistics: `('u1', 'r1', true), ('u2', 'r1', false)`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      object_id: "u1",
      sourceStatus: "running",
      sourceError: null,
      snapshotCommitted: true,
    });
    expect(rows[1]).toMatchObject({
      object_id: "u2",
      sourceStatus: "stalled",
      sourceError: "broker unreachable",
      snapshotCommitted: false,
    });
  });

  it("only reports the snapshot as committed once every replica agrees", async () => {
    // A replica that is still snapshotting holds the whole source at false, so
    // the UI keeps showing Snapshotting rather than flipping to Running early.
    await testdrive(
      seed({
        hydration: `('u1', 'r1', true)`,
        statuses: `('u1', 'running', NULL, 'kafka')`,
        statistics: `('u1', 'r1', true), ('u1', 'r2', false)`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      object_id: "u1",
      snapshotCommitted: false,
    });
  });

  it("keeps sources that have a status but no hydration rows", async () => {
    // Webhook sources are omitted from mz_hydration_statuses, so the FULL
    // JOIN is what keeps their status visible.
    await testdrive(
      seed({
        hydration: `('u1', 'r1', true)`,
        statuses: `('u1', 'running', NULL, 'kafka'), ('u7', 'running', NULL, 'webhook')`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({
      object_id: "u7",
      sourceStatus: "running",
      snapshotCommitted: null,
    });
    expect(rows[1].hydratedReplicas).toBeNull();
    expect(rows[1].totalReplicas).toBeNull();
  });

  it("excludes subsources and progress collections from the feed", async () => {
    // The UI hides them, and each source carries one progress collection and
    // often several subsources, so keeping them out shrinks the subscribe.
    // Subsources also have hydration rows of their own (they run on the
    // parent's cluster), so the exclusion must hold on both join sides.
    await testdrive(
      seed({
        hydration: `('u1', 'r1', true), ('u2', 'r1', true)`,
        statuses: `
          ('u1', 'running', NULL, 'postgres'),
          ('u2', 'running', NULL, 'subsource'),
          ('u3', 'running', NULL, 'progress')`,
        sources: `('u1', 'postgres'), ('u2', 'subsource'), ('u3', 'progress')`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ object_id: "u1", sourceStatus: "running" });
  });

  it("ignores statistics rows from dropped replicas", async () => {
    // Statistics rows can outlive their replica; a dropped replica's stale
    // false must not pin a committed source back to snapshotting.
    await testdrive(
      seed({
        statuses: `('u1', 'running', NULL, 'kafka')`,
        statistics: `('u1', 'r1', true), ('u1', 'r_dropped', false)`,
        replicas: `('r1')`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      object_id: "u1",
      snapshotCommitted: true,
    });
  });

  it("keeps statistics rows that report no replica", async () => {
    // Webhook sources report statistics with a null replica_id.
    await testdrive(
      seed({
        statuses: `('u1', 'running', NULL, 'webhook')`,
        statistics: `('u1', NULL, true)`,
        replicas: `('r1')`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      object_id: "u1",
      snapshotCommitted: true,
    });
  });
});
