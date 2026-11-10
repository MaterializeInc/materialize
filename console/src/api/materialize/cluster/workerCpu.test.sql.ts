// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { RawBuilder, sql } from "kysely";
import { Client } from "pg";

import { queryBuilder } from "~/api/materialize";
import {
  ADHOC_ROW_ID,
  objectRowId,
  SYSTEM_ROW_ID,
} from "~/platform/clusters/ClusterPage/WorkerCpu/workerCpuModel";
import { getMaterializeClient } from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";
import { waitFor } from "~/test/sql/waitFor";

import {
  buildDataflowElapsedQuery,
  buildWorkerCpuSubscribe,
  DataflowExportRow,
  ELAPSED_UNIT_NS,
  splitWorkerCpuRows,
  WorkerCpuRow,
  workerCpuRowKey,
} from "./workerCpu";

// Below the suite's `testTimeout`, so a snapshot that never completes fails on
// the error below rather than on the timeout.
const SUBSCRIBE_SNAPSHOT_TIMEOUT_MS = 30_000;
// Past any test here, so only the bound test's subscribe ends on its own.
const UP_TO_LATER_MS = 10 * 60_000;

// The per-dataflow rows as the per-operator relation gives them: a count
// kept for every operator on every worker, joined to worker 0's addresses.
const PER_OPERATOR_ROWS = `
  SELECT
    a.address[1]::text AS "dataflowId",
    e.worker_id::int4 AS "workerId",
    (floor(e.elapsed_ns / ${ELAPSED_UNIT_NS}) * ${ELAPSED_UNIT_NS})::float8 AS "elapsedNs"
  FROM mz_scheduling_elapsed_per_worker e
  JOIN mz_dataflow_addresses a ON a.id = e.id
  WHERE list_length(a.address) = 1`;

// Each worker's time over the operators no other operator contains, found
// through the operators' parents rather than their addresses' length.
const PARENTLESS_TOTALS = `
  SELECT
    e.worker_id::int4 AS "workerId",
    sum((floor(e.elapsed_ns / ${ELAPSED_UNIT_NS}) * ${ELAPSED_UNIT_NS})::float8) AS ns
  FROM mz_scheduling_elapsed_per_worker e
  WHERE NOT EXISTS (
    SELECT 1 FROM mz_dataflow_operator_parents_per_worker p
    WHERE p.id = e.id AND p.worker_id = e.worker_id
  )
  GROUP BY 1`;

/** A two-worker replica maintaining one index, hydrated. */
async function createIndexedReplica() {
  await testdrive(`
    > CREATE CLUSTER skew SIZE 'scale=1,workers=2';
    > CREATE TABLE t (a int);
    > INSERT INTO t SELECT generate_series(1, 1000);
    > CREATE INDEX t_idx IN CLUSTER skew ON t (a);
    > SET cluster = skew;
    > SELECT count(*) FROM t;
    1000
  `);
  const client = await getMaterializeClient();
  const {
    rows: [index],
  } = await client.query("SELECT id FROM mz_indexes WHERE name = 't_idx'");
  await client.end();
  return { indexId: index.id as string };
}

const pinnedSession = async () => {
  const client = await getMaterializeClient();
  await client.query("SET cluster = skew");
  await client.query("SET cluster_replica = r1");
  return client;
};

type Row = Record<string, unknown>;

const nullableNumber = (value: unknown) =>
  value === null ? null : Number(value);

const toWorkerCpuRow = (row: Row) =>
  ({
    kind: row.kind,
    dataflowId: String(row.dataflowId),
    workerId: nullableNumber(row.workerId),
    elapsedNs: nullableNumber(row.elapsedNs),
    objectId: row.objectId,
    connectionId: nullableNumber(row.connectionId),
    dataflowName: row.dataflowName,
  }) as WorkerCpuRow;

/**
 * A subscribe held open through a cursor on its own session pinned to the
 * replica, as the Workers tab holds its own. `rows` is the upsert-reduced
 * state as of the last `fetch`.
 */
async function openSubscribe<T>(
  subscribe: RawBuilder<unknown>,
  key: (row: Row) => string,
  toRow: (row: Row) => T,
) {
  const client: Client = await pinnedSession();
  const {
    rows: [{ pid }],
  } = await client.query("SELECT pg_backend_pid()::text AS pid");
  const compiled = subscribe.compile(queryBuilder);
  await client.query("BEGIN");
  await client.query(`DECLARE c CURSOR FOR ${compiled.sql}`, [
    ...compiled.parameters,
  ]);
  const state = new Map<string, Row>();
  let firstTimestamp: bigint | undefined;
  let isSnapshotComplete = false;
  /** Applies what arrived, and returns the updates among it. */
  const fetch = async () => {
    const { rows } = await client.query(`FETCH ALL c WITH (timeout='1s')`);
    const updates: Row[] = [];
    for (const row of rows) {
      const timestamp = BigInt(row.mz_timestamp);
      firstTimestamp ??= timestamp;
      if (row.mz_progressed) {
        if (timestamp > firstTimestamp) isSnapshotComplete = true;
        continue;
      }
      if (row.mz_state === "delete") state.delete(key(row));
      else state.set(key(row), row);
      if (isSnapshotComplete) updates.push(row);
    }
    return updates;
  };
  const deadline = Date.now() + SUBSCRIBE_SNAPSHOT_TIMEOUT_MS;
  while (!isSnapshotComplete) {
    if (Date.now() > deadline) {
      throw new Error("The subscribe's snapshot did not complete");
    }
    await fetch();
  }
  return {
    client,
    connectionId: Number(pid),
    fetch,
    rows: () => [...state.values()].map(toRow),
    close: async () => {
      await client.query("ROLLBACK");
      await client.end();
    },
  };
}

const openWorkerCpu = (upToMs = Date.now() + UP_TO_LATER_MS) =>
  openSubscribe(
    buildWorkerCpuSubscribe(upToMs),
    (row) => workerCpuRowKey(toWorkerCpuRow(row)),
    toWorkerCpuRow,
  );

/** One read of `sql` on a session pinned to the replica. */
const readPinned = async (query: string) => {
  const client = await pinnedSession();
  try {
    return (await client.query(query)).rows;
  } finally {
    await client.end();
  }
};

const isRounded = (ns: number) => ns % ELAPSED_UNIT_NS === 0;

/** The objects-table row an export's dataflow counts toward. */
const rowIdOf = ({ objectId, dataflowId, dataflowName }: DataflowExportRow) =>
  objectRowId({
    objectId,
    dataflowId,
    dataflowName: dataflowName ?? undefined,
  });

describe("worker CPU subscribe", () => {
  it("reads each dataflow's time on each worker in 10 ms units, as the per-operator relation's root rows give it", async () => {
    await createIndexedReplica();
    const { sql: elapsed } = buildDataflowElapsedQuery().compile();
    const rows = `SELECT "dataflowId", "workerId", "elapsedNs" FROM (${elapsed}) q`;

    // A new replica's counters take a moment to appear and pass 10 ms.
    await waitFor(async () => {
      // One read, so both sides see the counters at the same time.
      const [counts] = await readPinned(`
        SELECT
          (SELECT count(*) FROM (${rows} EXCEPT ALL ${PER_OPERATOR_ROWS}) x)::int AS "onlyNew",
          (SELECT count(*) FROM (${PER_OPERATOR_ROWS} EXCEPT ALL ${rows}) x)::int AS "onlyPerOperator",
          (SELECT count(*) FROM (${rows}) x)::int AS "rows",
          (SELECT count(*) FROM (${rows}) x WHERE "elapsedNs"::int8 % ${ELAPSED_UNIT_NS} <> 0)::int AS "unrounded",
          (SELECT count(*) FROM (${rows}) x WHERE "elapsedNs" > 0)::int AS "counted"`);

      expect(counts).toEqual({
        onlyNew: 0,
        onlyPerOperator: 0,
        rows: expect.any(Number),
        unrounded: 0,
        counted: expect.any(Number),
      });
      expect(counts.rows).toBeGreaterThan(2);
      // Below 10 ms every value rounds to 0, which any query would match.
      expect(counts.counted).toBeGreaterThan(2);
    }, 30_000);
  });

  it("sums to each worker's time over the operators no other contains", async () => {
    await createIndexedReplica();
    const { sql: elapsed } = buildDataflowElapsedQuery().compile();

    await waitFor(async () => {
      const totals = await readPinned(`
        SELECT n."workerId", n.ns::float8 AS "ns", p.ns::float8 AS "parentlessNs"
        FROM (SELECT "workerId", sum("elapsedNs") AS ns FROM (${elapsed}) q GROUP BY 1) n
        FULL JOIN (${PARENTLESS_TOTALS}) p USING ("workerId")
        ORDER BY 1`);

      expect(totals.map(({ workerId }) => workerId)).toEqual([0, 1]);
      for (const { ns, parentlessNs } of totals) {
        expect(ns).toBeGreaterThan(0);
        expect(ns).toEqual(parentlessNs);
      }
    }, 30_000);
  });

  it("tags its own subscribe with its session, and names transient dataflows so the coordinator's own pool in their own row", async () => {
    const { indexId } = await createIndexedReplica();
    const page = await openWorkerCpu();
    try {
      await waitFor(async () => {
        await page.fetch();
        const { exports } = splitWorkerCpuRows(page.rows());
        expect(
          exports.some(
            ({ connectionId }) => connectionId === page.connectionId,
          ),
        ).toBe(true);
      }, 30_000);

      const { dataflows, exports } = splitWorkerCpuRows(page.rows());
      const own = exports.find(
        ({ connectionId }) => connectionId === page.connectionId,
      );
      expect(own?.dataflowName).toMatch(/^Dataflow: subscribe-t\d+$/);
      expect(
        dataflows.some(({ dataflowId }) => dataflowId === own?.dataflowId),
      ).toBe(true);
      expect(dataflows.every(({ elapsedNs }) => isRounded(elapsedNs))).toBe(
        true,
      );

      const indexExport = exports.find(({ objectId }) => objectId === indexId);
      expect(indexExport).toMatchObject({
        dataflowName: null,
        connectionId: null,
      });

      // Every replica runs these, with transient ids and no session.
      const coordinators = exports.filter(({ dataflowName }) =>
        /^Dataflow: (introspection-subscribe|metric-sink)-/.test(
          dataflowName ?? "",
        ),
      );
      expect(coordinators.length).toBeGreaterThan(0);
      expect(coordinators.map(rowIdOf)).toEqual(
        coordinators.map(() => SYSTEM_ROW_ID),
      );
      expect(own && rowIdOf(own)).toBe(ADHOC_ROW_ID);
      expect(indexExport && rowIdOf(indexExport)).toBe(indexId);
    } finally {
      await page.close();
    }
  });

  it.each([
    ["a SELECT", "SELECT count(*)", /^Dataflow: oneshot-select-/],
    [
      "an INSERT ... SELECT",
      "INSERT INTO materialize.public.t SELECT a.a",
      /^Dataflow: frontend-read-then-write-subscribe-/,
    ],
  ])(
    "names %s's dataflow so it pools in the queries row while it runs",
    async (_statement, head, dataflowName) => {
      await createIndexedReplica();
      const page = await openWorkerCpu();
      const session = await pinnedSession();
      const {
        rows: [{ pid }],
      } = await session.query("SELECT pg_backend_pid()::text AS pid");
      // A cross join of t with itself three times over runs long enough to
      // read its dataflow, and is cancelled once read.
      const running = session
        .query(
          `${head}
           FROM materialize.public.t a, materialize.public.t b, materialize.public.t c
           WHERE a.a + b.a + c.a < 0`,
        )
        .catch(() => undefined);
      try {
        await waitFor(async () => {
          await page.fetch();
          const { exports } = splitWorkerCpuRows(page.rows());
          const statementExport = exports.find((row) =>
            dataflowName.test(row.dataflowName ?? ""),
          );
          expect(statementExport && rowIdOf(statementExport)).toBe(
            ADHOC_ROW_ID,
          );
        }, 30_000);
      } finally {
        const admin = await getMaterializeClient();
        await admin.query(`SELECT pg_cancel_backend(${pid})`);
        await admin.end();
        await running;
        await session.end();
        await page.close();
      }
    },
  );

  it("ends on its own at its UP TO bound", async () => {
    await createIndexedReplica();
    const page = await openWorkerCpu(Date.now() + 5_000);
    try {
      // A fetch without a timeout waits for rows, and the replica reports
      // progress each second, so only an ended subscribe returns none.
      const deadline = Date.now() + SUBSCRIBE_SNAPSHOT_TIMEOUT_MS;
      while ((await page.client.query("FETCH ALL c")).rows.length > 0) {
        if (Date.now() > deadline) throw new Error("The subscribe didn't end");
      }
      await waitFor(async () => {
        const [{ running }] = await readPinned(`
          SELECT count(*)::int AS running
          FROM mz_subscriptions s JOIN mz_sessions ss ON ss.id = s.session_id
          WHERE ss.connection_id = ${page.connectionId}`);
        expect(running).toBe(0);
      }, 10_000);
    } finally {
      await page.close();
    }
  });

  it("stops sending idle dataflows' time once rounded", async () => {
    await createIndexedReplica();
    const rounded = await openWorkerCpu();
    const unrounded = await openSubscribe(
      // The per-dataflow rows without their rounding.
      sql.raw(`SUBSCRIBE (
        SELECT
          a.address[1]::text AS "dataflowId",
          r.worker_id::int4 AS "workerId",
          count(*)::float8 AS "elapsedNs"
        FROM mz_scheduling_elapsed_raw r
        JOIN mz_dataflow_addresses_per_worker a
          ON a.id = r.id AND a.worker_id = r.worker_id
        WHERE list_length(a.address) = 1
        GROUP BY 1, 2
      ) WITH (PROGRESS) ENVELOPE UPSERT (KEY ("dataflowId", "workerId"))`),
      (row) => `${row.dataflowId}|${row.workerId}`,
      (row) => row,
    );
    try {
      let roundedUpdates = 0;
      let unroundedUpdates = 0;
      const until = Date.now() + 5_000;
      while (Date.now() < until) {
        const [fromRounded, fromUnrounded] = await Promise.all([
          rounded.fetch(),
          unrounded.fetch(),
        ]);
        roundedUpdates += fromRounded.filter(
          ({ kind }) => kind === "elapsed",
        ).length;
        unroundedUpdates += fromUnrounded.length;
      }

      expect(unroundedUpdates).toBeGreaterThan(0);
      expect(roundedUpdates).toBeLessThan(unroundedUpdates / 4);
    } finally {
      await Promise.all([rounded.close(), unrounded.close()]);
    }
  });
});
