# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import time
from datetime import datetime
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Cockroach(setup_materialize=True),
    Materialized(propagate_crashes=True, external_cockroach=True),
    Testdrive(no_reset=True, default_timeout="5s"),
]


def workflow_default(c: Composition) -> None:
    """Test the retain history feature."""
    setup(c)
    run_test_with_mv_on_table(c)
    run_test_with_mv_on_counter_source(c)


def setup(c: Composition) -> None:
    c.up("materialized")
    c.up("testdrive", persistent=True)

    c.testdrive(
        dedent(
            """
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_logical_compaction_window = true;
            ALTER SYSTEM SET enable_refresh_every_mvs = true;
            """,
        )
    )


def run_test_with_mv_on_table(c: Composition) -> None:
    mv_on_mv1_retention_in_sec = 1
    mv_on_mv_on_mv1_retention_in_sec = 60

    mz_time0 = fetch_now_from_mz(c)

    c.testdrive(
        dedent(
            f"""
            > CREATE TABLE retain_history_table (key INT, value INT);

            > INSERT INTO retain_history_table VALUES (1, 100), (2, 200);

            > CREATE MATERIALIZED VIEW retain_history_mv1 WITH (RETAIN HISTORY FOR '10s') AS
                    SELECT * FROM retain_history_table;

            > CREATE MATERIALIZED VIEW retain_history_mv_on_mv1 WITH (RETAIN HISTORY FOR '{mv_on_mv1_retention_in_sec}s') AS
                    SELECT * FROM retain_history_mv1;

            > CREATE MATERIALIZED VIEW retain_history_mv_on_mv_on_mv1 WITH (RETAIN HISTORY FOR '{mv_on_mv_on_mv1_retention_in_sec}s') AS
                    SELECT * FROM retain_history_mv_on_mv1;

            > SELECT count(*) FROM retain_history_mv1;
            2
            """,
        )
    )

    mz_time1 = fetch_now_from_mz(c)
    test_time1 = datetime.now()

    c.testdrive(
        dedent(
            f"""
            > UPDATE retain_history_table SET value = value + 1;
            > INSERT INTO retain_history_table VALUES (3, 300);

            > SELECT * FROM retain_history_mv1;
            1 101
            2 201
            3 300

            ! SELECT count(*) FROM retain_history_mv1 AS OF '{mz_time0}'::TIMESTAMP;
            contains: is not valid for all inputs

            > SELECT count(*) FROM retain_history_mv1 AS OF AT LEAST'{mz_time0}'::TIMESTAMP;
            0

            > SELECT * FROM retain_history_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            1 100
            2 200

            > INSERT INTO retain_history_table VALUES (4, 400);
            """,
        )
    )

    mz_time2 = fetch_now_from_mz(c)

    c.testdrive(
        dedent(
            """
            > DELETE FROM retain_history_table WHERE key IN (3, 4);
            """,
        )
    )

    mz_time3 = fetch_now_from_mz(c)

    c.testdrive(
        dedent(
            f"""
            > SELECT count(*) FROM retain_history_mv1;
            2

            > SELECT * FROM retain_history_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            1 100
            2 200

            > SELECT * FROM retain_history_mv_on_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            1 100
            2 200

            > SELECT * FROM retain_history_mv1 AS OF '{mz_time2}'::TIMESTAMP;
            1 101
            2 201
            3 300
            4 400

            > SELECT * FROM retain_history_mv1 AS OF AT LEAST '{mz_time2}'::TIMESTAMP;
            1 101
            2 201
            3 300
            4 400

            > SELECT sum(value), max(value) FROM retain_history_mv1 AS OF '{mz_time2}'::TIMESTAMP;
            1002 400

            > SELECT count(*) FROM retain_history_mv1 AS OF '{mz_time3}'::TIMESTAMP;
            2

            ? EXPLAIN SELECT * FROM retain_history_mv1 AS OF '{mz_time2}'::TIMESTAMP;
            Explained Query:
              ReadStorage materialize.public.retain_history_mv1

            > SELECT mv1a.key, mv1b.key
              FROM retain_history_mv1 mv1a
              LEFT OUTER JOIN retain_history_mv1 mv1b
              ON mv1a.key = mv1b.key
              AS OF '{mz_time2}'::TIMESTAMP;
            1 1
            2 2
            3 3
            4 4

            ! SELECT t.key, mv.key
              FROM retain_history_table t
              LEFT OUTER JOIN retain_history_mv1 mv
              ON t.key = mv.key
              AS OF '{mz_time2}'::TIMESTAMP;
            contains: is not valid for all inputs

            > UPDATE retain_history_table SET key = 9 WHERE key = 1;
            """,
        )
    )

    mz_time4 = fetch_now_from_mz(c)

    c.testdrive(
        dedent(
            f"""
            > SELECT count(*) FROM retain_history_mv1 WHERE key = 1 AS OF '{mz_time3}'::TIMESTAMP;
            1

            > SELECT count(*) FROM retain_history_mv1 WHERE key = 1 AS OF '{mz_time4}'::TIMESTAMP;
            0

            > SELECT 1 WHERE 1 = (SELECT count(*) FROM retain_history_mv1 WHERE key = 1) AS OF '{mz_time3}'::TIMESTAMP;
            1
            """,
        )
    )

    test_time5 = datetime.now()

    if (test_time5 - test_time1).total_seconds() <= mv_on_mv1_retention_in_sec:
        time.sleep(1)

    assert (
        test_time5 - test_time1
    ).total_seconds() < mv_on_mv_on_mv1_retention_in_sec, "test precondition not satisfied, consider increasing 'mv_on_mv_on_mv1_retention_in_sec'"

    mz_time_in_far_future = "2044-01-11 09:24:10.459000+00:00"

    c.testdrive(
        dedent(
            f"""
            # retain period exceeded
            ! SELECT * FROM retain_history_mv_on_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            contains: is not valid for all inputs

            # retain period on wrapping mv still valid
            > SELECT * FROM retain_history_mv_on_mv_on_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            1 100
            2 200

            # retain period in future
            ! SELECT * FROM retain_history_mv_on_mv1 AS OF '{mz_time_in_far_future}'::TIMESTAMP;
            timeout
            """,
        )
    )


def run_test_with_mv_on_counter_source(c: Composition) -> None:
    sleep_duration_between_mz_time1_and_mz_time2 = 1.5

    c.testdrive(
        dedent(
            """
            > CREATE SOURCE retain_history_source1
              FROM LOAD GENERATOR COUNTER
              (TICK INTERVAL '100ms');

            > CREATE MATERIALIZED VIEW retain_history_mv2 WITH (RETAIN HISTORY FOR '10s') AS
                    SELECT * FROM retain_history_source1;

            > SELECT count(*) > 0 FROM retain_history_mv2;
            true
            """,
        )
    )

    mz_time1 = fetch_now_from_mz(c)
    count_at_mz_time1 = c.sql_query(
        f"SELECT count(*) FROM retain_history_mv2 AS OF '{mz_time1}'::TIMESTAMP"
    )[0][0]

    time.sleep(sleep_duration_between_mz_time1_and_mz_time2)

    mz_time2 = fetch_now_from_mz(c)
    count_at_mz_time2 = c.sql_query(
        f"SELECT count(*) FROM retain_history_mv2 AS OF '{mz_time2}'::TIMESTAMP"
    )[0][0]
    count_at_mz_time1_queried_at_mz_time2 = c.sql_query(
        f"SELECT count(*) FROM retain_history_mv2 AS OF '{mz_time1}'::TIMESTAMP"
    )[0][0]

    assert count_at_mz_time1 == count_at_mz_time1_queried_at_mz_time2
    assert (
        count_at_mz_time2 > count_at_mz_time1
    ), f"value at time2 did not progress ({count_at_mz_time2} vs. {count_at_mz_time1}), consider increasing 'sleep_duration_between_mz_time1_and_mz_time2'"


def fetch_now_from_mz(c: Composition) -> str:
    return c.sql_query("SELECT now()")[0][0]
