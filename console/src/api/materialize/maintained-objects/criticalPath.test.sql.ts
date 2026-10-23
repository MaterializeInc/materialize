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
import { buildCriticalPathLiveQuery, CriticalPathRow } from "./criticalPath";

/** Creates the test catalog tables that the query reads from. */
const seedCatalogSql = (testSchema: string) => `
      > DROP SCHEMA IF EXISTS ${testSchema} CASCADE;
      > CREATE SCHEMA ${testSchema};
      > CREATE TABLE ${testSchema}.mz_materialization_dependencies (
          object_id TEXT NOT NULL,
          dependency_id TEXT NOT NULL
        );
      > CREATE TABLE ${testSchema}.mz_wallclock_global_lag_recent_history (
          object_id TEXT NOT NULL,
          lag INTERVAL,
          occurred_at TIMESTAMP
        );
      > CREATE TABLE ${testSchema}.mz_object_dependencies (
          object_id TEXT NOT NULL,
          referenced_object_id TEXT NOT NULL
        );
      > CREATE TABLE ${testSchema}.mz_objects (
          id TEXT NOT NULL,
          type TEXT,
          cluster_id TEXT
        );
      > CREATE TABLE ${testSchema}.mz_object_fully_qualified_names (
          id TEXT NOT NULL,
          name TEXT,
          schema_name TEXT,
          database_name TEXT
        );
      > CREATE TABLE ${testSchema}.mz_clusters (
          id TEXT NOT NULL,
          name TEXT
        );
      > CREATE TABLE ${testSchema}.mz_sources (
          id TEXT NOT NULL,
          type TEXT,
          cluster_id TEXT
        );
      > CREATE TABLE ${testSchema}.mz_tables (
          id TEXT NOT NULL,
          source_id TEXT
        );`;

/**
 * Source exports. The probe reads a source-fed table, a subsource, and a
 * table written with INSERT. The table and subsource have no cluster.
 *
 *                 ┌────────────────┐
 *                 │ u1  src · 2s   │
 *                 └───────┬────────┘
 *           ┌─────────────┴────┐
 *           ▼                  ▼
 *   ┌────────────────┐ ┌────────────────┐ ┌──────────────────┐
 *   │ t1 table · 30s │ │ s1 subsrc · 4s │ │ t2 written · 50s │
 *   └───────┬────────┘ └───────┬────────┘ └────────┬─────────┘
 *           │                  │                   │ (dropped)
 *           └─────────┬────────┘
 *                     ▼
 *           ┌──────────────────┐
 *           │ u9  probe · 31s  │
 *           └──────────────────┘
 *
 * Expectation: the chain runs u1 → t1 → u9, and s1 is off-path. The exports
 * report the cluster u1 ingests on. t2 is not on the critical path even
 * though it lags the most.
 */
const seedSourceExports = (testSchema: string) => `${seedCatalogSql(testSchema)}
      > INSERT INTO ${testSchema}.mz_materialization_dependencies VALUES
          ('u9', 't1'),
          ('u9', 's1'),
          ('u9', 't2');
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('t1', INTERVAL '30 SECONDS', now()),
          ('s1', INTERVAL '4 SECONDS', now()),
          ('t2', INTERVAL '50 SECONDS', now()),
          ('u1', INTERVAL '2 SECONDS', now()),
          ('u9', INTERVAL '31 SECONDS', now());
      > INSERT INTO ${testSchema}.mz_objects VALUES
          ('u1', 'source', 'c1'),
          ('t1', 'table', NULL),
          ('s1', 'source', NULL),
          ('t2', 'table', NULL),
          ('u9', 'materialized-view', 'c2');
      > INSERT INTO ${testSchema}.mz_object_fully_qualified_names VALUES
          ('u1', 'pg', 'public', 'mz'),
          ('t1', 'orders', 'public', 'mz'),
          ('s1', 'customers', 'public', 'mz'),
          ('t2', 'written', 'public', 'mz'),
          ('u9', 'probe', 'public', 'mz');
      > INSERT INTO ${testSchema}.mz_sources VALUES
          ('u1', 'postgres', 'c1'),
          ('s1', 'subsource', NULL);
      > INSERT INTO ${testSchema}.mz_tables VALUES
          ('t1', 'u1'),
          ('t2', NULL);
      > INSERT INTO ${testSchema}.mz_object_dependencies VALUES
          ('s1', 'u1'),
          ('t1', 'u1');
      > INSERT INTO ${testSchema}.mz_clusters VALUES
          ('c1', 'ingest'),
          ('c2', 'compute');
  `;

// Bucket size large enough that all seed rows in the same `now()` second land
// in one bucket — the probe-peak snapshot returns every object's lag from
// that bucket.
const TEST_BUCKET_MS = 60 * 60 * 1000;

const runQuery = async (
  testSchema: string,
  probeId: string,
  lookbackMinutes = 60,
) => {
  const compiled = buildCriticalPathLiveQuery(
    probeId,
    lookbackMinutes,
    TEST_BUCKET_MS,
  );
  const result = await executeSqlHttp(compiled, {
    sessionVariables: {
      cluster: QUICKSTART_CLUSTER,
      search_path: `${testSchema}, ${SEARCH_PATH}`,
    },
  });
  return result.rows as CriticalPathRow[];
};

describe("buildCriticalPathLiveQuery", () => {
  /**
   * Linear chain — probe at u3, two upstream hops back to source u1.
   *
   *                ┌──────────────────┐
   *                │   u1  src · 2s   │  source
   *                └────────┬─────────┘
   *                         │
   *                         ▼
   *                ┌──────────────────┐
   *                │   u2  mid · 5s   │
   *                └────────┬─────────┘
   *                         │
   *                         ▼
   *                ┌──────────────────┐
   *                │  u3  probe · 8s  │  ← queried object
   *                └──────────────────┘
   *
   * Expectation: two chain edges (u1→u2 and u2→u3), both isBottleneck=true,
   * each row carrying its own lag and full metadata.
   */
  it("walks a linear chain back to the source", async () => {
    const testSchema = "test_critical_path";

    await testdrive(`${seedCatalogSql(testSchema)}
      > INSERT INTO ${testSchema}.mz_materialization_dependencies VALUES
          ('u3', 'u2'),
          ('u2', 'u1');
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u1', INTERVAL '2 SECONDS', now()),
          ('u2', INTERVAL '5 SECONDS', now()),
          ('u3', INTERVAL '8 SECONDS', now());
      > INSERT INTO ${testSchema}.mz_objects VALUES
          ('u1', 'source', 'c1'),
          ('u2', 'materialized-view', 'c1'),
          ('u3', 'materialized-view', 'c1');
      > INSERT INTO ${testSchema}.mz_object_fully_qualified_names VALUES
          ('u1', 'src', 'public', 'mz'),
          ('u2', 'mid', 'public', 'mz'),
          ('u3', 'probe', 'public', 'mz');
      > INSERT INTO ${testSchema}.mz_clusters VALUES ('c1', 'compute');
    `);

    const rows = (await runQuery(testSchema, "u3")).sort((a, b) =>
      a.id.localeCompare(b.id),
    );

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      id: "u1",
      childId: "u2",
      name: "src",
      objectType: "source",
      clusterName: "compute",
      isBottleneck: true,
    });
    expect(rows[0].lag?.toPostgres()).toBe("2 seconds");
    expect(rows[1]).toMatchObject({
      id: "u2",
      childId: "u3",
      name: "mid",
      objectType: "materialized-view",
      isBottleneck: true,
    });
    expect(rows[1].lag?.toPostgres()).toBe("5 seconds");
  });

  /**
   * Tied bottlenecks — three sources all at 4s feeding the probe.
   *
   *      ┌────────┐  ┌────────┐  ┌────────┐
   *      │ u1·4s  │  │ u2·4s  │  │ u3·4s  │  all tied
   *      └────┬───┘  └────┬───┘  └────┬───┘
   *           │           │           │
   *           └───────────┼───────────┘
   *                       ▼
   *             ┌──────────────────┐
   *             │  u9  probe · 5s  │
   *             └──────────────────┘
   *
   * Expectation: all three rows have isBottleneck=true. No arbitrary
   * tie-breaking — every input that ties for max lag stays on the chain.
   */
  it("flags every input tied for max lag as a chain bottleneck", async () => {
    const testSchema = "test_critical_path";

    await testdrive(`${seedCatalogSql(testSchema)}
      > INSERT INTO ${testSchema}.mz_materialization_dependencies VALUES
          ('u9', 'u1'),
          ('u9', 'u2'),
          ('u9', 'u3');
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u1', INTERVAL '4 SECONDS', now()),
          ('u2', INTERVAL '4 SECONDS', now()),
          ('u3', INTERVAL '4 SECONDS', now()),
          ('u9', INTERVAL '5 SECONDS', now());
      > INSERT INTO ${testSchema}.mz_objects VALUES
          ('u1', 'source', NULL),
          ('u2', 'source', NULL),
          ('u3', 'source', NULL),
          ('u9', 'materialized-view', NULL);
      > INSERT INTO ${testSchema}.mz_object_fully_qualified_names VALUES
          ('u1', 'a', 'public', 'mz'),
          ('u2', 'b', 'public', 'mz'),
          ('u3', 'c', 'public', 'mz'),
          ('u9', 'probe', 'public', 'mz');
    `);

    const rows = (await runQuery(testSchema, "u9")).sort((a, b) =>
      a.id.localeCompare(b.id),
    );
    expect(rows.map((r) => r.id)).toEqual(["u1", "u2", "u3"]);
    expect(rows.every((r) => r.isBottleneck)).toBe(true);
  });

  /**
   * Off-path sibling — probe has one chain input and one slower-but-not-
   * bottleneck input. Both are returned, distinguished by isBottleneck.
   *
   *    ┌─────────────────┐         ┌─────────────────┐
   *    │ u_fast src · 5s │         │ u_slow src · 1s │
   *    │  (bottleneck)   │         │   (off-path)    │
   *    └────────┬────────┘         └────────┬────────┘
   *             │                           │
   *             └─────────────┬─────────────┘
   *                           ▼
   *                ┌──────────────────┐
   *                │  u9  probe · 6s  │
   *                └──────────────────┘
   *
   * Expectation: u_fast has isBottleneck=true (it's pinning the probe's
   * frontier). u_slow has isBottleneck=false but is still in the result so
   * the DAG can render it as a grey off-path sibling.
   */
  it("returns slower siblings as off-path inputs of chain nodes", async () => {
    const testSchema = "test_critical_path";

    await testdrive(`${seedCatalogSql(testSchema)}
      > INSERT INTO ${testSchema}.mz_materialization_dependencies VALUES
          ('u9', 'u_fast'),
          ('u9', 'u_slow');
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u_fast', INTERVAL '5 SECONDS', now()),
          ('u_slow', INTERVAL '1 SECOND', now()),
          ('u9', INTERVAL '6 SECONDS', now());
      > INSERT INTO ${testSchema}.mz_objects VALUES
          ('u_fast', 'source', NULL),
          ('u_slow', 'source', NULL),
          ('u9', 'materialized-view', NULL);
      > INSERT INTO ${testSchema}.mz_object_fully_qualified_names VALUES
          ('u_fast', 'fast', 'public', 'mz'),
          ('u_slow', 'slow', 'public', 'mz'),
          ('u9', 'probe', 'public', 'mz');
    `);

    const rows = await runQuery(testSchema, "u9");
    const fast = rows.find((r) => r.id === "u_fast");
    const slow = rows.find((r) => r.id === "u_slow");
    expect(fast?.isBottleneck).toBe(true);
    expect(slow?.isBottleneck).toBe(false);
  });

  it("follows source-fed tables and subsources back to their source", async () => {
    const testSchema = "test_critical_path";
    await testdrive(seedSourceExports(testSchema));

    const rows = (await runQuery(testSchema, "u9")).sort((a, b) =>
      a.id.localeCompare(b.id),
    );
    expect(rows.map((r) => [r.id, r.childId])).toEqual([
      ["s1", "u9"],
      ["t1", "u9"],
      ["u1", "t1"],
    ]);
    expect(rows[0]).toMatchObject({
      objectType: "subsource",
      clusterId: "c1",
      clusterName: "ingest",
      isBottleneck: false,
      isIngestion: false,
    });
    expect(rows[1]).toMatchObject({
      objectType: "table",
      clusterId: "c1",
      clusterName: "ingest",
      isBottleneck: true,
      isIngestion: false,
    });
    expect(rows[2]).toMatchObject({
      objectType: "source",
      clusterName: "ingest",
      isBottleneck: true,
      isIngestion: true,
    });
  });

  it("walks a subsource back to its source", async () => {
    const testSchema = "test_critical_path";
    await testdrive(seedSourceExports(testSchema));

    const rows = await runQuery(testSchema, "s1");
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: "u1",
      childId: "s1",
      name: "pg",
      isBottleneck: true,
      isIngestion: true,
    });
    expect(rows[0].lag?.toPostgres()).toBe("2 seconds");
    expect(rows[0].targetLag?.toPostgres()).toBe("4 seconds");
  });

  /**
   * Legacy source. u2 keeps its progress in the subsource p2 and is itself
   * just another export, so its subsource s2 gets no edge back to it.
   *
   *   u2 legacy src · 40s     p2 progress · 1s
   *          ╎ (no edge)
   *   ┌────────────────┐
   *   │ s2 subsrc · 40s│
   *   └───────┬────────┘
   *           ▼
   *   ┌────────────────┐
   *   │ u8 probe · 41s │
   *   └────────────────┘
   */
  it("gives a legacy source's subsources no edge back to the source", async () => {
    const testSchema = "test_critical_path";
    await testdrive(`${seedCatalogSql(testSchema)}
      > INSERT INTO ${testSchema}.mz_materialization_dependencies VALUES
          ('u8', 's2');
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u2', INTERVAL '40 SECONDS', now()),
          ('p2', INTERVAL '1 SECOND', now()),
          ('s2', INTERVAL '40 SECONDS', now()),
          ('u8', INTERVAL '41 SECONDS', now());
      > INSERT INTO ${testSchema}.mz_objects VALUES
          ('u2', 'source', 'c1'),
          ('p2', 'source', NULL),
          ('s2', 'source', NULL),
          ('u8', 'materialized-view', 'c2');
      > INSERT INTO ${testSchema}.mz_object_fully_qualified_names VALUES
          ('u2', 'legacy', 'public', 'mz'),
          ('p2', 'legacy_progress', 'public', 'mz'),
          ('s2', 'customers', 'public', 'mz'),
          ('u8', 'probe', 'public', 'mz');
      > INSERT INTO ${testSchema}.mz_sources VALUES
          ('u2', 'postgres', 'c1'),
          ('p2', 'progress', NULL),
          ('s2', 'subsource', NULL);
      > INSERT INTO ${testSchema}.mz_object_dependencies VALUES
          ('s2', 'u2'),
          ('u2', 'p2');
      > INSERT INTO ${testSchema}.mz_clusters VALUES
          ('c1', 'ingest'),
          ('c2', 'compute');
    `);

    expect(await runQuery(testSchema, "s2")).toEqual([]);
    const rows = await runQuery(testSchema, "u8");
    expect(rows.map((r) => [r.id, r.childId])).toEqual([["s2", "u8"]]);
    expect(rows[0]).toMatchObject({
      clusterName: "ingest",
      isIngestion: false,
    });
  });
});
