// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CompiledQuery } from "kysely";

import {
  executeSqlHttp,
  getMaterializeClient,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";
import { readSubscribeSnapshot } from "~/test/sql/subscribeSnapshot";
import { waitFor } from "~/test/sql/waitFor";

import { SEARCH_PATH } from "../executeSql";
import {
  buildClusterHydrationEpisodesQuery,
  buildClusterReplicaHistoryQuery,
  buildEpisodeObjectHydrationsQuery,
  buildLiveObjectHydrationsSubscribe,
  buildReplicaCpuPeakQuery,
  buildReplicaHydrationStatesSubscribe,
} from "./hydrationHistory";

const TEST_SCHEMA = "test_hydration_history";

/** Shadows one catalog relation with a table holding `rows`. */
const seed = (table: string, columns: string, rows: string) => `
  > DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
  > CREATE SCHEMA ${TEST_SCHEMA};
  > CREATE TABLE ${TEST_SCHEMA}.${table} (${columns});
  > INSERT INTO ${TEST_SCHEMA}.${table} VALUES ${rows};
`;

const runShadowed = <Q extends CompiledQuery>(query: Q) =>
  executeSqlHttp(query, {
    sessionVariables: {
      cluster: QUICKSTART_CLUSTER,
      search_path: `${TEST_SCHEMA}, ${SEARCH_PATH}`,
    },
  });

const runOnCatalog = <Q extends CompiledQuery>(query: Q) =>
  executeSqlHttp(query, { sessionVariables: { cluster: QUICKSTART_CLUSTER } });

/** The quickstart cluster's id and its one replica's id and name. */
async function quickstartIds() {
  // Reset Materialize state.
  await testdrive(`> SET cluster to quickstart;`);
  const client = await getMaterializeClient();
  const {
    rows: [row],
  } = await client.query(
    "SELECT c.id AS cluster_id, r.id AS replica_id, r.name AS replica_name FROM mz_clusters c JOIN mz_cluster_replicas r ON r.cluster_id = c.id WHERE c.name = 'quickstart'",
  );
  return {
    clusterId: row.cluster_id,
    replicaId: row.replica_id,
    replicaName: row.replica_name,
  };
}

const iso = (value: unknown) => (value as Date).toISOString();

describe("buildClusterHydrationEpisodesQuery", () => {
  it("returns one row per episode with the highest peaks across processes", async () => {
    await testdrive(
      seed(
        "mz_replica_hydration_history",
        `replica_id TEXT NOT NULL,
         cluster_id TEXT NOT NULL,
         started_at TIMESTAMPTZ NOT NULL,
         finished_at TIMESTAMPTZ,
         object_count UINT8 NOT NULL,
         peak_memory_bytes UINT8,
         peak_disk_bytes UINT8,
         status TEXT NOT NULL,
         process_id UINT8`,
        `('u1', 'u9', '2026-10-20T10:00:00Z', '2026-10-20T10:04:00Z', 8, 100, 10, 'hydrated', 0),
         ('u1', 'u9', '2026-10-20T10:00:00Z', '2026-10-20T10:04:00Z', 8, 300, NULL, 'hydrated', 1),
         ('u1', 'u9', '2026-10-20T11:00:00Z', '2026-10-20T11:00:30Z', 1, 120, 40, 'hydrated', 0),
         ('u1', 'u9', '2026-10-19T10:00:00Z', '2026-10-19T10:04:00Z', 9, 100, 10, 'hydrated', NULL),
         ('u1', 'u9', '2026-10-20T12:00:00Z', '2026-10-20T12:00:05Z', 0, 50, 0, 'hydrated', 0),
         ('u1', 'u9', '2026-10-18T10:00:00Z', '2026-10-18T10:04:00Z', 8, 100, 10, 'hydrated', 0),
         ('u2', 'u8', '2026-10-20T10:00:00Z', '2026-10-20T10:04:00Z', 8, 100, 10, 'hydrated', 0)`,
      ),
    );

    const { rows } = await runShadowed(
      buildClusterHydrationEpisodesQuery({
        clusterId: "u9",
        startDate: "2026-10-19T00:00:00Z",
      }).compile(),
    );

    // Left out: the row from before 26.43 (no process id), the episode before
    // the start, and the other cluster's replica.
    expect(
      rows
        .map((row) => ({
          ...row,
          startedAt: iso(row.startedAt),
          finishedAt: iso(row.finishedAt),
        }))
        .sort((a, b) => a.startedAt.localeCompare(b.startedAt)),
    ).toEqual([
      {
        replicaId: "u1",
        startedAt: "2026-10-20T10:00:00.000Z",
        finishedAt: "2026-10-20T10:04:00.000Z",
        objectCount: 8,
        peakMemoryBytes: 300,
        peakDiskBytes: 10,
        peakHeapBytes: 300,
      },
      {
        replicaId: "u1",
        startedAt: "2026-10-20T11:00:00.000Z",
        finishedAt: "2026-10-20T11:00:30.000Z",
        objectCount: 1,
        peakMemoryBytes: 120,
        peakDiskBytes: 40,
        peakHeapBytes: 160,
      },
      {
        replicaId: "u1",
        startedAt: "2026-10-20T12:00:00.000Z",
        finishedAt: "2026-10-20T12:00:05.000Z",
        objectCount: 0,
        peakMemoryBytes: 50,
        peakDiskBytes: 0,
        peakHeapBytes: 50,
      },
    ]);
  });

  it("runs against the catalog's hydration history", async () => {
    const { clusterId } = await quickstartIds();

    const { rows } = await runOnCatalog(
      buildClusterHydrationEpisodesQuery({
        clusterId,
        startDate: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      }).compile(),
    );

    for (const row of rows) {
      expect(typeof row.replicaId).toBe("string");
      expect(typeof row.objectCount).toBe("number");
    }
  });
});

describe("buildClusterReplicaHistoryQuery", () => {
  it("returns the cluster's current replicas and those dropped since the start", async () => {
    await testdrive(
      seed(
        "mz_cluster_replica_history",
        `replica_id TEXT,
         size TEXT,
         cluster_id TEXT,
         cluster_name TEXT,
         replica_name TEXT,
         created_at TIMESTAMPTZ NOT NULL,
         dropped_at TIMESTAMPTZ,
         credits_per_hour NUMERIC`,
        `('u1', '50cc', 'u9', 'analytics', 'r1', '2026-10-01T00:00:00Z', '2026-10-16T10:40:00Z', 0.5),
         ('u3', '100cc', 'u9', 'analytics', 'r1', '2026-10-16T10:40:00Z', NULL, 1),
         ('u4', '50cc', 'u9', 'analytics', 'r2', '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z', 0.5),
         ('u5', '100cc', 'u8', 'other', 'r1', '2026-10-16T10:40:00Z', NULL, 1)`,
      ),
    );

    const { rows } = await runShadowed(
      buildClusterReplicaHistoryQuery({
        clusterId: "u9",
        startDate: "2026-10-13T00:00:00Z",
      }).compile(),
    );

    expect(
      rows
        .map((row) => ({
          ...row,
          createdAt: iso(row.createdAt),
          droppedAt: row.droppedAt === null ? null : iso(row.droppedAt),
        }))
        .sort((a, b) => (a.replicaId ?? "").localeCompare(b.replicaId ?? "")),
    ).toEqual([
      {
        replicaId: "u1",
        name: "r1",
        size: "50cc",
        createdAt: "2026-10-01T00:00:00.000Z",
        droppedAt: "2026-10-16T10:40:00.000Z",
      },
      {
        replicaId: "u3",
        name: "r1",
        size: "100cc",
        createdAt: "2026-10-16T10:40:00.000Z",
        droppedAt: null,
      },
    ]);
  });

  it("finds the quickstart replica in the catalog's replica history", async () => {
    const { clusterId, replicaId } = await quickstartIds();

    const { rows } = await runOnCatalog(
      buildClusterReplicaHistoryQuery({
        clusterId,
        startDate: new Date().toISOString(),
      }).compile(),
    );

    expect(rows.map((row) => row.replicaId)).toContain(replicaId);
  });
});

describe("buildReplicaHydrationStatesSubscribe", () => {
  it("streams whether each of the replicas' objects is hydrated, leaving out system and introspection dataflows", async () => {
    await testdrive(
      seed(
        "mz_compute_hydration_times",
        `replica_id TEXT NOT NULL, object_id TEXT NOT NULL, time_ns UINT8`,
        `('u1', 'u5', 1000),
         ('u1', 'u6', NULL),
         ('u1', 'si1', 10),
         ('u1', 's2', NULL),
         ('u2', 'u5', 1000)`,
      ),
    );
    const client = await getMaterializeClient();
    await client.query(`SET search_path TO ${TEST_SCHEMA}, ${SEARCH_PATH}`);

    const rows = await readSubscribeSnapshot(
      client,
      buildReplicaHydrationStatesSubscribe({ replicaIds: ["u1"] }),
    );

    expect(
      rows
        .map(({ replicaId, objectId, isHydrated }) => ({
          replicaId,
          objectId,
          isHydrated,
        }))
        .sort((a, b) => String(a.objectId).localeCompare(String(b.objectId))),
    ).toEqual([
      { replicaId: "u1", objectId: "u5", isHydrated: true },
      { replicaId: "u1", objectId: "u6", isHydrated: false },
    ]);
  });

  it("runs against the catalog's hydration times", async () => {
    const { replicaId } = await quickstartIds();
    const client = await getMaterializeClient();

    const rows = await readSubscribeSnapshot(
      client,
      buildReplicaHydrationStatesSubscribe({ replicaIds: [replicaId] }),
    );

    for (const row of rows) {
      expect(row.replicaId).toEqual(replicaId);
      expect(typeof row.isHydrated).toBe("boolean");
    }
  });
});

describe("buildLiveObjectHydrationsSubscribe", () => {
  it("combines each object's workers: hydrated once all are, working once any started", async () => {
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
      > CREATE SCHEMA ${TEST_SCHEMA};
      > CREATE TABLE ${TEST_SCHEMA}.mz_compute_hydration_times_per_worker (
          export_id TEXT NOT NULL,
          worker_id UINT8 NOT NULL,
          time_ns UINT8,
          installed_at TIMESTAMPTZ NOT NULL,
          started_at TIMESTAMPTZ,
          hydrated_at TIMESTAMPTZ
        );
      > CREATE TABLE ${TEST_SCHEMA}.mz_object_global_ids (
          id TEXT NOT NULL,
          global_id TEXT NOT NULL
        );
      > INSERT INTO ${TEST_SCHEMA}.mz_compute_hydration_times_per_worker VALUES
          ('u20', 0, 1, '2026-10-20T10:00:00Z', '2026-10-20T10:00:01Z', '2026-10-20T10:00:30Z'),
          ('u20', 1, 1, '2026-10-20T10:00:00Z', '2026-10-20T10:00:02Z', '2026-10-20T10:00:40Z'),
          ('u21', 0, NULL, '2026-10-20T10:00:00Z', '2026-10-20T10:00:05Z', '2026-10-20T10:00:50Z'),
          ('u21', 1, NULL, '2026-10-20T10:00:00Z', '2026-10-20T10:00:06Z', NULL),
          ('u22', 0, NULL, '2026-10-20T10:00:00Z', NULL, NULL),
          ('u22', 1, NULL, '2026-10-20T10:00:00Z', NULL, NULL),
          ('si5', 0, 1, '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z', '2026-10-20T10:00:01Z'),
          ('t7', 0, NULL, '2026-10-20T10:00:00Z', NULL, NULL);
      > INSERT INTO ${TEST_SCHEMA}.mz_object_global_ids VALUES ('u10', 'u20');
    `);
    const client = await getMaterializeClient();
    await client.query(`SET search_path TO ${TEST_SCHEMA}, ${SEARCH_PATH}`);

    const rows = await readSubscribeSnapshot(
      client,
      buildLiveObjectHydrationsSubscribe(),
    );

    // u21 has a worker still working, u22 is queued, and u21 and u22 no
    // longer resolve to catalog ids, as for dropped objects' dataflows.
    expect(
      rows
        .map((row) => ({
          globalId: row.globalId,
          objectId: row.objectId,
          startedAt: row.startedAt === null ? null : iso(row.startedAt),
          hydratedAt: row.hydratedAt === null ? null : iso(row.hydratedAt),
        }))
        .sort((a, b) => String(a.globalId).localeCompare(String(b.globalId))),
    ).toEqual([
      {
        globalId: "u20",
        objectId: "u10",
        startedAt: "2026-10-20T10:00:01.000Z",
        hydratedAt: "2026-10-20T10:00:40.000Z",
      },
      {
        globalId: "u21",
        objectId: null,
        startedAt: "2026-10-20T10:00:05.000Z",
        hydratedAt: null,
      },
      {
        globalId: "u22",
        objectId: null,
        startedAt: null,
        hydratedAt: null,
      },
    ]);
  });

  it("streams a new index's hydration from a session pinned to the replica", async () => {
    const { replicaName } = await quickstartIds();
    await testdrive(`
      > SET cluster to quickstart;
      > CREATE TABLE hydration_t (a int);
      > CREATE INDEX hydration_t_idx IN CLUSTER quickstart ON hydration_t (a);
    `);
    const client = await getMaterializeClient();
    const {
      rows: [index],
    } = await client.query(
      "SELECT id FROM mz_indexes WHERE name = 'hydration_t_idx'",
    );
    await client.query("SET cluster = quickstart");
    await client.query(`SET cluster_replica = ${replicaName}`);

    await waitFor(async () => {
      const rows = await readSubscribeSnapshot(
        client,
        buildLiveObjectHydrationsSubscribe(),
      );
      const row = rows.find(({ objectId }) => objectId === index.id);
      expect(row?.hydratedAt).toBeInstanceOf(Date);
    }, 30_000);
  });
});

describe("buildEpisodeObjectHydrationsQuery", () => {
  it("returns the replica's object hydrations installed in the window, with catalog ids", async () => {
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
      > CREATE SCHEMA ${TEST_SCHEMA};
      > CREATE TABLE ${TEST_SCHEMA}.mz_object_hydration_history (
          object_id TEXT NOT NULL,
          cluster_id TEXT NOT NULL,
          replica_id TEXT NOT NULL,
          installed_at TIMESTAMPTZ NOT NULL,
          started_at TIMESTAMPTZ,
          hydrated_at TIMESTAMPTZ,
          status TEXT NOT NULL
        );
      > CREATE TABLE ${TEST_SCHEMA}.mz_object_global_ids (
          id TEXT NOT NULL,
          global_id TEXT NOT NULL
        );
      > INSERT INTO ${TEST_SCHEMA}.mz_object_hydration_history VALUES
          ('u20', 'u9', 'u1', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z', '2026-10-20T10:00:38Z', 'hydrated'),
          ('u21', 'u9', 'u1', '2026-10-20T10:00:01Z', '2026-10-20T10:00:38Z', '2026-10-20T10:01:20Z', 'hydrated'),
          ('u22', 'u9', 'u1', '2026-10-20T09:00:00Z', '2026-10-20T09:00:00Z', '2026-10-20T09:00:10Z', 'hydrated'),
          ('u20', 'u9', 'u2', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z', '2026-10-20T10:00:40Z', 'hydrated');
      > INSERT INTO ${TEST_SCHEMA}.mz_object_global_ids VALUES ('u10', 'u20');
    `);

    const { rows } = await runShadowed(
      buildEpisodeObjectHydrationsQuery({
        replicaId: "u1",
        startDate: "2026-10-20T10:00:00Z",
        endDate: "2026-10-20T10:04:00Z",
      }).compile(),
    );

    // u21 no longer resolves to a catalog id, as for a dropped object.
    expect(
      rows
        .map((row) => ({
          globalId: row.globalId,
          objectId: row.objectId,
          installedAt: iso(row.installedAt),
          startedAt: row.startedAt === null ? null : iso(row.startedAt),
          hydratedAt: row.hydratedAt === null ? null : iso(row.hydratedAt),
        }))
        .sort((a, b) => a.globalId.localeCompare(b.globalId)),
    ).toEqual([
      {
        globalId: "u20",
        objectId: "u10",
        installedAt: "2026-10-20T10:00:00.000Z",
        startedAt: "2026-10-20T10:00:00.000Z",
        hydratedAt: "2026-10-20T10:00:38.000Z",
      },
      {
        globalId: "u21",
        objectId: null,
        installedAt: "2026-10-20T10:00:01.000Z",
        startedAt: "2026-10-20T10:00:38.000Z",
        hydratedAt: "2026-10-20T10:01:20.000Z",
      },
    ]);
  });

  it("runs against the catalog's object hydration history", async () => {
    const { replicaId } = await quickstartIds();

    const { rows } = await runOnCatalog(
      buildEpisodeObjectHydrationsQuery({
        replicaId,
        startDate: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        endDate: new Date().toISOString(),
      }).compile(),
    );

    for (const row of rows) {
      expect(typeof row.globalId).toBe("string");
      expect(row.installedAt).toBeInstanceOf(Date);
    }
  });
});

describe("buildReplicaCpuPeakQuery", () => {
  it("returns the highest CPU sample of any process in the window", async () => {
    await testdrive(
      seed(
        "mz_cluster_replica_metrics_history",
        `replica_id TEXT NOT NULL,
         process_id UINT8 NOT NULL,
         cpu_nano_cores UINT8,
         memory_bytes UINT8,
         disk_bytes UINT8,
         occurred_at TIMESTAMPTZ NOT NULL,
         heap_bytes UINT8,
         heap_limit UINT8,
         swap_bytes UINT8`,
        `('u1', 0, 400, 1, 0, '2026-10-20T10:01:00Z', 1, 1, 0),
         ('u1', 1, 900, 1, 0, '2026-10-20T10:02:00Z', 1, 1, 0),
         ('u1', 0, NULL, NULL, NULL, '2026-10-20T10:03:00Z', NULL, NULL, NULL),
         ('u1', 0, 1000, 1, 0, '2026-10-20T10:05:00Z', 1, 1, 0),
         ('u2', 0, 1000, 1, 0, '2026-10-20T10:02:00Z', 1, 1, 0)`,
      ),
    );

    const run = (startDate: string, endDate: string) =>
      runShadowed(
        buildReplicaCpuPeakQuery({
          replicaId: "u1",
          startDate,
          endDate,
        }).compile(),
      );

    expect(
      (await run("2026-10-20T10:00:00Z", "2026-10-20T10:04:00Z")).rows,
    ).toEqual([{ peakCpuNanoCores: 900 }]);
    expect(
      (await run("2026-10-20T11:00:00Z", "2026-10-20T11:04:00Z")).rows,
    ).toEqual([{ peakCpuNanoCores: null }]);
  });

  it("runs against the catalog's metrics history", async () => {
    const { replicaId } = await quickstartIds();

    const { rows } = await runOnCatalog(
      buildReplicaCpuPeakQuery({
        replicaId,
        startDate: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
        endDate: new Date().toISOString(),
      }).compile(),
    );

    expect(rows).toHaveLength(1);
  });
});
