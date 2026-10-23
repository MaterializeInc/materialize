// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import PostgresInterval from "postgres-interval";

import { IPostgresInterval, queryBuilder, SEARCH_PATH } from "~/api/materialize";
import {
  executeSqlHttp,
  explainOnCatalogServer,
  QUICKSTART_CLUSTER,
} from "~/test/sql/materializeSqlClient";
import { testdrive } from "~/test/sql/mzcompose";
import { sumPostgresIntervalMs } from "~/util";

import {
  buildLagHistoryQuery,
  buildLatestLagQuery,
  buildObjectLagHistoryQuery,
  buildObjectLagPercentilesQuery,
  buildScopeLagPercentilesQuery,
  LAG_PERCENTILES,
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
  ])("reads %s by key for 400 ids", async (_, query) => {
    expect(await explainOnCatalogServer(query.compile())).toContain(
      "mz_internal.mz_wallclock_global_lag_recent_history_ind (differential join)",
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

const TEST_SCHEMA = "test_lag_percentiles";
const HOUR_MS = 3_600_000;
const DAY_MS = 24 * HOUR_MS;

/**
 * Shadows `mz_wallclock_global_lag_recent_history`. Readings are a minute
 * apart, skipping minutes 55 to 64 and stopping at 23 hours, so none is within
 * five minutes of either range's edge and none ages out between a query and
 * the reference read.
 *
 * - u1: 1,370 readings, far more than the 145 kept per object.
 * - u2: young, 30 readings.
 * - u3: every 20th reading unreadable, 5%: inside p90, past p99.
 * - u4: 20 of 30 readings unreadable.
 * - u5: one reading, two days old, outside every range.
 * - u6: no readings.
 */
const SEED = `
  > DROP SCHEMA IF EXISTS ${TEST_SCHEMA} CASCADE;
  > CREATE SCHEMA ${TEST_SCHEMA};
  > CREATE TABLE ${TEST_SCHEMA}.mz_wallclock_global_lag_recent_history (
      object_id TEXT NOT NULL,
      lag INTERVAL,
      occurred_at TIMESTAMPTZ NOT NULL
    );
  > INSERT INTO ${TEST_SCHEMA}.mz_wallclock_global_lag_recent_history
    SELECT
      o.id,
      CASE
        WHEN o.id = 'u3' AND m % 20 = 0 THEN NULL
        WHEN o.id = 'u4' AND m % 3 <> 0 THEN NULL
        ELSE ((m * 7919 + length(o.id) * 13) % 5000) * INTERVAL '1 millisecond'
      END,
      now() - m * INTERVAL '1 minute' - INTERVAL '30 seconds'
    FROM (VALUES ('u1', 1380), ('u2', 30), ('u3', 1380), ('u4', 30)) AS o (id, readings),
      generate_series(0, 1379) AS m
    WHERE m < o.readings AND m NOT BETWEEN 55 AND 64;
  > INSERT INTO ${TEST_SCHEMA}.mz_wallclock_global_lag_recent_history
    VALUES ('u5', INTERVAL '9 seconds', now() - INTERVAL '2 days');
`;

const OBJECT_IDS = ["u1", "u2", "u3", "u4", "u5", "u6"];

const run = <Q extends Parameters<typeof executeSqlHttp>[0]>(query: Q) =>
  executeSqlHttp(query, {
    sessionVariables: {
      cluster: QUICKSTART_CLUSTER,
      search_path: `${TEST_SCHEMA}, ${SEARCH_PATH}`,
    },
  });

type Percentiles = Record<
  "peak" | "p90" | "p99" | "p99_9",
  IPostgresInterval | null
>;

/** Milliseconds, or null for an unreadable reading. */
const toMs = (row: Percentiles) => ({
  peak: row.peak && sumPostgresIntervalMs(row.peak),
  p90: row.p90 && sumPostgresIntervalMs(row.p90),
  p99: row.p99 && sumPostgresIntervalMs(row.p99),
  p99_9: row.p99_9 && sumPostgresIntervalMs(row.p99_9),
});

/**
 * Nearest-rank percentiles by the textbook definition: sort ascending and take
 * the reading at position ceil(p * n). Unreadable sorts above every lag.
 */
const reference = (lags: (number | null)[]) => {
  const sorted = lags.map((lag) => lag ?? Infinity).sort((a, b) => a - b);
  const at = (p: number) => {
    const value = sorted[Math.ceil(p * sorted.length) - 1];
    return value === Infinity ? null : value;
  };
  return {
    peak: at(1),
    p90: at(LAG_PERCENTILES.p90),
    p99: at(LAG_PERCENTILES.p99),
    p99_9: at(LAG_PERCENTILES.p99_9),
  };
};

/** The fixture's raw readings in the range, by object. */
const readingsIn = async (lookbackMs: number) => {
  const { rows } = await run(
    queryBuilder
      .selectFrom("mz_wallclock_global_lag_recent_history")
      .select(["object_id", "lag"])
      .where("object_id", "in", OBJECT_IDS)
      .where(
        (eb) =>
          sql`${eb.ref("occurred_at")} + INTERVAL '${sql.raw(`${lookbackMs}`)} MILLISECONDS'`,
        ">=",
        sql<Date>`mz_now()`,
      )
      .compile(),
  );
  const byObject = new Map<string, (number | null)[]>();
  for (const row of rows) {
    const lags = byObject.get(row.object_id) ?? [];
    lags.push(row.lag && sumPostgresIntervalMs(row.lag));
    byObject.set(row.object_id, lags);
  }
  return byObject;
};

describe("lag percentiles", () => {
  beforeAll(async () => {
    await testdrive(SEED);
  });

  describe.each([
    ["1 hour", HOUR_MS],
    ["24 hours", DAY_MS],
  ])("over %s", (_, lookbackMs) => {
    it("matches the reference for every object", async () => {
      const raw = await readingsIn(lookbackMs);
      const { rows } = await run(
        buildObjectLagPercentilesQuery({
          objectIds: OBJECT_IDS,
          lookbackMs,
        }).compile(),
      );

      const actual = Object.fromEntries(
        rows.map((row) => [row.objectId, toMs(row)]),
      );
      const expected = Object.fromEntries(
        [...raw].map(([id, lags]) => [id, reference(lags)]),
      );
      expect(actual).toEqual(expected);
    });

    it("matches the reference for the scope", async () => {
      const raw = await readingsIn(lookbackMs);
      const { rows } = await run(
        buildScopeLagPercentilesQuery({
          objectIds: OBJECT_IDS,
          lookbackMs,
        }).compile(),
      );

      expect(rows).toHaveLength(1);
      expect(toMs(rows[0])).toEqual(reference([...raw.values()].flat()));
    });
  });

  it("ranks each object over its own readings", async () => {
    const { rows } = await run(
      buildObjectLagPercentilesQuery({
        objectIds: OBJECT_IDS,
        lookbackMs: DAY_MS,
      }).compile(),
    );
    const byId = new Map(rows.map((row) => [row.objectId, toMs(row)]));

    // Neither the reading outside the range nor the missing object is a row.
    expect([...byId.keys()].sort()).toEqual(["u1", "u2", "u3", "u4"]);
    // 5% unreadable sits above p90 and reaches p99.
    expect(byId.get("u3")?.p90).toEqual(expect.any(Number));
    expect(byId.get("u3")?.p99).toBeNull();
    expect(byId.get("u4")).toEqual({
      peak: null,
      p90: null,
      p99: null,
      p99_9: null,
    });
  });

  it("returns no row for a scope with no readings", async () => {
    const { rows } = await run(
      buildScopeLagPercentilesQuery({
        objectIds: ["u5", "u6"],
        lookbackMs: DAY_MS,
      }).compile(),
    );
    expect(rows).toEqual([]);
  });
});
