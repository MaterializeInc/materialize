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
import { buildReplicaHydrationQuery } from "./replicaHydration";

const TEST_SCHEMA = "test_replica_hydration";

/** Shadows the two relations the query reads. Empty value lists are elided. */
const seed = ({
  hydration = "",
  sources = "",
}: {
  hydration?: string;
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
  ${hydration ? `> INSERT INTO ${TEST_SCHEMA}.mz_hydration_statuses VALUES ${hydration};` : ""}
  ${sources ? `> INSERT INTO ${TEST_SCHEMA}.mz_sources VALUES ${sources};` : ""}
`;

const run = async () => {
  const compiled = buildReplicaHydrationQuery().compile();
  const result = await executeSqlHttp(compiled, {
    sessionVariables: {
      cluster: QUICKSTART_CLUSTER,
      search_path: `${TEST_SCHEMA}, ${SEARCH_PATH}`,
    },
  });
  // `replica_id` is nullable in the catalog and the query filters the nulls
  // out, so the fallbacks only satisfy the type.
  return result.rows.sort((a, b) =>
    (a.replicaId ?? "").localeCompare(b.replicaId ?? ""),
  );
};

describe("buildReplicaHydrationQuery", () => {
  it("counts hydrated objects out of the total per replica", async () => {
    // u10: 3 objects, all hydrated → 3/3
    // u11: 3 objects, 1 hydrated   → 1/3
    // u12: 2 objects, none hydrated → 0/2
    await testdrive(
      seed({
        hydration: `
          ('u1', 'u10', true),
          ('u2', 'u10', true),
          ('u3', 'u10', true),
          ('u1', 'u11', true),
          ('u2', 'u11', false),
          ('u3', 'u11', false),
          ('u1', 'u12', false),
          ('u2', 'u12', false)`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(3);

    expect(rows[0]).toMatchObject({ replicaId: "u10" });
    expect(Number(rows[0].hydratedObjects)).toBe(3);
    expect(Number(rows[0].totalObjects)).toBe(3);

    expect(rows[1]).toMatchObject({ replicaId: "u11" });
    expect(Number(rows[1].hydratedObjects)).toBe(1);
    expect(Number(rows[1].totalObjects)).toBe(3);

    expect(rows[2]).toMatchObject({ replicaId: "u12" });
    expect(Number(rows[2].hydratedObjects)).toBe(0);
    expect(Number(rows[2].totalObjects)).toBe(2);
  });

  it("counts system introspection objects", async () => {
    // Every replica carries dozens of `s`-prefixed introspection indexes. They
    // are counted, matching the Maintained Objects feed, which never meets them
    // because its rows are user objects. Narrowing this is deferred.
    await testdrive(
      seed({
        hydration: `
          ('u1', 'u10', true),
          ('s100', 'u10', true),
          ('s101', 'u10', false)`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(Number(rows[0].hydratedObjects)).toBe(2);
    expect(Number(rows[0].totalObjects)).toBe(3);
  });

  it("counts a null hydrated value toward the total but not as hydrated", async () => {
    // Sinks are the only branch of the real view that reports a null, and they
    // are counted here, so an unreported sink holds a replica below its total
    // and the cell reads Hydrating until it reports. Narrowing this is
    // deferred.
    await testdrive(
      seed({
        hydration: `('u1', 'u10', true), ('u2', 'u10', NULL)`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(Number(rows[0].hydratedObjects)).toBe(1);
    expect(Number(rows[0].totalObjects)).toBe(2);
  });

  it("excludes rows that name no replica", async () => {
    // The view emits these for objects with no hydration report at all, so they
    // describe no replica and would group under a key matching none.
    await testdrive(
      seed({
        hydration: `('u1', 'u10', true), ('u2', NULL, false)`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ replicaId: "u10" });
    expect(Number(rows[0].totalObjects)).toBe(1);
  });

  it("reports replicas of system clusters", async () => {
    // The clusters list shows system clusters when "Show system clusters" is
    // on, and their rows carry hydration like any other. The `s` prefix here is
    // the replica's, so the cluster's, not the object's.
    await testdrive(
      seed({
        hydration: `
          ('u1', 'u10', true),
          ('u1', 's1', true),
          ('u2', 's1', false)`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(2);

    expect(rows[0]).toMatchObject({ replicaId: "s1" });
    expect(Number(rows[0].hydratedObjects)).toBe(1);
    expect(Number(rows[0].totalObjects)).toBe(2);

    expect(rows[1]).toMatchObject({ replicaId: "u10" });
    expect(Number(rows[1].totalObjects)).toBe(1);
  });

  it("excludes subsources and progress collections", async () => {
    // The one exclusion the Maintained Objects feed also makes, so the two
    // tables classify a replica's objects the same way. Subsources have
    // hydration rows of their own because they run on their parent's cluster.
    await testdrive(
      seed({
        hydration: `
          ('u1', 'u10', true),
          ('u2', 'u10', false),
          ('u3', 'u10', false)`,
        sources: `('u1', 'postgres'), ('u2', 'subsource'), ('u3', 'progress')`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(1);
    expect(Number(rows[0].hydratedObjects)).toBe(1);
    expect(Number(rows[0].totalObjects)).toBe(1);
  });

  it("omits a replica whose every object is excluded", async () => {
    // Nothing counted means no row at all, which the column renders as a dash
    // rather than as an unhydrated replica.
    await testdrive(
      seed({
        hydration: `('u1', 'u10', true)`,
        sources: `('u1', 'subsource')`,
      }),
    );

    const rows = await run();
    expect(rows).toHaveLength(0);
  });
});
