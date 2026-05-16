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
import { buildLagAggregateQuery } from "./lagAggregate";

describe("buildLagAggregateQuery", () => {
  it("returns max(lag) per object_id across multiple samples", async () => {
    const testSchema = "test_lag_aggregate";

    // u1 has three recent samples — max should win.
    // u2 has a single recent sample.
    // All samples are at `now()` so the temporal filter doesn't exclude any.
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${testSchema} CASCADE;
      > CREATE SCHEMA ${testSchema};
      > CREATE TABLE ${testSchema}.mz_wallclock_global_lag_recent_history (
          object_id TEXT NOT NULL,
          lag INTERVAL,
          occurred_at TIMESTAMP
        );
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u1', INTERVAL '500 MILLISECONDS', now()),
          ('u1', INTERVAL '2 SECONDS', now()),
          ('u1', INTERVAL '1 SECOND', now()),
          ('u2', INTERVAL '100 MILLISECONDS', now());
    `);

    const compiled = buildLagAggregateQuery({ lookbackMinutes: 60 }).compile();
    const result = await executeSqlHttp(compiled, {
      sessionVariables: {
        cluster: QUICKSTART_CLUSTER,
        search_path: `${testSchema}, ${SEARCH_PATH}`,
      },
    });

    const rows = result.rows.sort((a, b) =>
      a.object_id.localeCompare(b.object_id),
    );
    expect(rows).toHaveLength(2);
    expect(rows[0].object_id).toBe("u1");
    expect(rows[0].lag?.toPostgres()).toBe("2 seconds");
    expect(rows[1].object_id).toBe("u2");
    expect(rows[1].lag?.toPostgres()).toBe("0.1 seconds");
  });

  it("respects a 5-minute lookback at the temporal-filter boundary", async () => {
    const testSchema = "test_lag_aggregate";

    // u1: a 2-minute-old sample (inside the 5m window).
    // u2: a 7-minute-old sample (outside the 5m window).
    // u3: one 1-minute-old sample (in window) and one 7-minute-old sample
    //     (out of window) — only the in-window sample should drive max(lag).
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${testSchema} CASCADE;
      > CREATE SCHEMA ${testSchema};
      > CREATE TABLE ${testSchema}.mz_wallclock_global_lag_recent_history (
          object_id TEXT NOT NULL,
          lag INTERVAL,
          occurred_at TIMESTAMP
        );
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u1', INTERVAL '300 MILLISECONDS', now() - INTERVAL '2 MINUTES'),
          ('u2', INTERVAL '9 SECONDS', now() - INTERVAL '7 MINUTES'),
          ('u3', INTERVAL '500 MILLISECONDS', now() - INTERVAL '1 MINUTE'),
          ('u3', INTERVAL '12 SECONDS', now() - INTERVAL '7 MINUTES');
    `);

    const compiled = buildLagAggregateQuery({ lookbackMinutes: 5 }).compile();
    const result = await executeSqlHttp(compiled, {
      sessionVariables: {
        cluster: QUICKSTART_CLUSTER,
        search_path: `${testSchema}, ${SEARCH_PATH}`,
      },
    });

    const rows = result.rows.sort((a, b) =>
      a.object_id.localeCompare(b.object_id),
    );
    expect(rows).toHaveLength(2);
    expect(rows[0].object_id).toBe("u1");
    expect(rows[0].lag?.toPostgres()).toBe("0.3 seconds");
    // u3's 12-second sample is older than 5 minutes and must be filtered out;
    // only the 0.5-second sample remains.
    expect(rows[1].object_id).toBe("u3");
    expect(rows[1].lag?.toPostgres()).toBe("0.5 seconds");
  });

  it("excludes samples older than the lookback window", async () => {
    const testSchema = "test_lag_aggregate";

    // u1: one recent sample (in window for 1m), one 10-minute-old sample
    //     (in window for 60m, out of window for 1m)
    // u2: only an old (10-minute-old) sample
    await testdrive(`
      > DROP SCHEMA IF EXISTS ${testSchema} CASCADE;
      > CREATE SCHEMA ${testSchema};
      > CREATE TABLE ${testSchema}.mz_wallclock_global_lag_recent_history (
          object_id TEXT NOT NULL,
          lag INTERVAL,
          occurred_at TIMESTAMP
        );
      > INSERT INTO ${testSchema}.mz_wallclock_global_lag_recent_history VALUES
          ('u1', INTERVAL '200 MILLISECONDS', now()),
          ('u1', INTERVAL '5 SECONDS', now() - INTERVAL '10 MINUTES'),
          ('u2', INTERVAL '8 SECONDS', now() - INTERVAL '10 MINUTES');
    `);

    // Lookback 1 minute → only u1's recent sample passes the temporal filter.
    {
      const compiled = buildLagAggregateQuery({ lookbackMinutes: 1 }).compile();
      const result = await executeSqlHttp(compiled, {
        sessionVariables: {
          cluster: QUICKSTART_CLUSTER,
          search_path: `${testSchema}, ${SEARCH_PATH}`,
        },
      });
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].object_id).toBe("u1");
      expect(result.rows[0].lag?.toPostgres()).toBe("0.2 seconds");
    }

    // Lookback 60 minutes → both objects in scope; max(lag) for u1 should
    // include the 5-second 10-minute-old sample.
    {
      const compiled = buildLagAggregateQuery({
        lookbackMinutes: 60,
      }).compile();
      const result = await executeSqlHttp(compiled, {
        sessionVariables: {
          cluster: QUICKSTART_CLUSTER,
          search_path: `${testSchema}, ${SEARCH_PATH}`,
        },
      });
      const rows = result.rows.sort((a, b) =>
        a.object_id.localeCompare(b.object_id),
      );
      expect(rows).toHaveLength(2);
      expect(rows[0].object_id).toBe("u1");
      expect(rows[0].lag?.toPostgres()).toBe("5 seconds");
      expect(rows[1].object_id).toBe("u2");
      expect(rows[1].lag?.toPostgres()).toBe("8 seconds");
    }
  });
});
