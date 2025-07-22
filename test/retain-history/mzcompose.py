# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Test the retain history feature."""

import time
from datetime import datetime
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import CockroachOrPostgresMetadata
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    CockroachOrPostgresMetadata(),
    Materialized(propagate_crashes=True, external_metadata_store=True),
    Testdrive(no_reset=True, default_timeout="5s"),
]


def workflow_default(c: Composition) -> None:
    setup(c)
    run_test_with_mv_on_table(c)
    run_test_with_mv_on_table_with_altered_retention(c)
    run_test_with_mv_on_counter_source(c)
    run_test_with_counter_source(c)
    # TODO: database-issues#7310 needs to be fixed
    # run_test_gh_24479(c)
    run_test_with_index(c)
    run_test_consistency(c)


def setup(c: Composition) -> None:
    c.up("materialized", {"name": "testdrive", "persistent": True})


# Test that the catalog is consistent for the three types of retain histories (disabled, default,
# specified).
def run_test_consistency(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > CREATE TABLE testdrive_consistency_table (i INT);

            > CREATE INDEX testdrive_consistency_table_idx ON testdrive_consistency_table(i);

            > ALTER INDEX testdrive_consistency_table_idx SET (RETAIN HISTORY = FOR '1m')

            > ALTER INDEX testdrive_consistency_table_idx SET (RETAIN HISTORY = FOR '1000 hours')

            > ALTER INDEX testdrive_consistency_table_idx RESET (RETAIN HISTORY)
            """,
        ),
        args=["--consistency-checks=statement"],
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
            > UPDATE retain_history_table SET value = value + 10;
            > INSERT INTO retain_history_table VALUES (3, 300);
            > INSERT INTO retain_history_table VALUES (4, 400);
            > INSERT INTO retain_history_table VALUES (5, 500);
            > DELETE FROM retain_history_table WHERE key = 4;
            > UPDATE retain_history_table SET key = 4 WHERE key = 5;
            > UPDATE retain_history_table SET value = value + 1;

            > SELECT * FROM retain_history_mv1;
            1 111
            2 211
            3 301
            4 501

            ! SELECT count(*) FROM retain_history_mv1 AS OF '{mz_time0}'::TIMESTAMP;
            contains: is not valid for all inputs

            > SELECT count(*) >= 2 FROM retain_history_mv1 AS OF AT LEAST '{mz_time1}'::TIMESTAMP;
            true

            > SELECT * FROM retain_history_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            1 100
            2 200

            > INSERT INTO retain_history_table VALUES (6, 600);
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
            3

            > SELECT * FROM retain_history_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            1 100
            2 200

            > SELECT * FROM retain_history_mv_on_mv1 AS OF '{mz_time1}'::TIMESTAMP;
            1 100
            2 200

            > SELECT * FROM retain_history_mv1 AS OF '{mz_time2}'::TIMESTAMP;
            1 111
            2 211
            3 301
            4 501
            6 600

            > SELECT count(*) IN (2, 5) FROM retain_history_mv1 AS OF AT LEAST '{mz_time2}'::TIMESTAMP;
            true

            > SELECT sum(value), max(value) FROM retain_history_mv1 AS OF '{mz_time2}'::TIMESTAMP;
            1724 600

            > SELECT count(*) FROM retain_history_mv1 AS OF '{mz_time3}'::TIMESTAMP;
            3

            ? EXPLAIN SELECT * FROM retain_history_mv1 AS OF '{mz_time2}'::TIMESTAMP;
            Explained Query:
              ReadStorage materialize.public.retain_history_mv1

            Target cluster: quickstart

            > SELECT mv1a.key, mv1b.key
              FROM retain_history_mv1 mv1a
              LEFT OUTER JOIN retain_history_mv1 mv1b
              ON mv1a.key = mv1b.key
              AS OF '{mz_time2}'::TIMESTAMP;
            1 1
            2 2
            3 3
            4 4
            6 6

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


def run_test_with_mv_on_table_with_altered_retention(c: Composition) -> None:
    """
    Verify we can still read the most recent timestamp, then reduce the retain history and verify we can't read anymore.
    """

    c.testdrive(
        dedent(
            """
            > DROP MATERIALIZED VIEW IF EXISTS retain_history_mv;
            > DROP TABLE IF EXISTS retain_history_table;

            > CREATE TABLE retain_history_table (key INT, value INT);
            > INSERT INTO retain_history_table VALUES (1, 100), (2, 200);

            > CREATE MATERIALIZED VIEW retain_history_mv WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table;
            """,
        )
    )

    mz_time1 = fetch_now_from_mz(c)

    c.testdrive(
        dedent(
            """
            > INSERT INTO retain_history_table VALUES (3, 300);
            """,
        )
    )

    mz_time2 = fetch_now_from_mz(c)

    c.testdrive(
        dedent(
            f"""
            > SELECT count(*) FROM retain_history_mv AS OF '{mz_time1}'::TIMESTAMP; -- mz_time1
            2

            > SELECT count(*) FROM retain_history_mv AS OF '{mz_time2}'::TIMESTAMP; -- mz_time2
            3

            > INSERT INTO retain_history_table VALUES (4, 400);

            # reduce retention period
            > ALTER MATERIALIZED VIEW retain_history_mv SET (RETAIN HISTORY FOR '2s');
            """,
        ),
    )

    mz_time3 = fetch_now_from_mz(c)

    # wait for the retention period to expire
    time.sleep(2 + 1)

    c.testdrive(
        dedent(
            f"""
            ! SELECT count(*) FROM retain_history_mv AS OF '{mz_time2}'::TIMESTAMP; -- mz_time2
            contains: is not valid for all inputs

            ! SELECT count(*) FROM retain_history_mv AS OF '{mz_time3}'::TIMESTAMP; -- mz_time3
            contains: is not valid for all inputs

            > SELECT count(*) FROM retain_history_mv;
            4
            """,
        ),
        # use a timeout that is significantly lower than the original retention period
        args=["--default-timeout=1s"],
    )

    mz_time4 = fetch_now_from_mz(c)

    c.testdrive(
        dedent(
            """
            # increase the retention period again
            > ALTER MATERIALIZED VIEW retain_history_mv SET (RETAIN HISTORY FOR '30s');

            > INSERT INTO retain_history_table VALUES (5, 500);
            """,
        ),
    )

    mz_time5 = fetch_now_from_mz(c)

    # let the duration of the old retention period pass
    time.sleep(2 + 1)

    c.testdrive(
        dedent(
            f"""
            # do not expect to regain old states
            ! SELECT count(*) FROM retain_history_mv AS OF '{mz_time3}'::TIMESTAMP; -- mz_time3
            contains: is not valid for all inputs

            # expect the new retention period to apply
            > SELECT count(*) FROM retain_history_mv AS OF '{mz_time4}'::TIMESTAMP; -- mz_time4
            4

            > SELECT count(*) FROM retain_history_mv AS OF '{mz_time5}'::TIMESTAMP; -- mz_time5
            5
            """,
        ),
    )


def run_test_with_mv_on_counter_source(c: Composition) -> None:
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

    _validate_count_of_counter_source(c, "retain_history_mv2")


def run_test_with_counter_source(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > CREATE SOURCE retain_history_source2
              FROM LOAD GENERATOR COUNTER
              (TICK INTERVAL '100ms')
              WITH (RETAIN HISTORY FOR '10s');
            """,
        )
    )

    _validate_count_of_counter_source(c, "retain_history_source2")


def _validate_count_of_counter_source(c: Composition, object_name: str) -> None:
    sleep_duration_between_mz_time1_and_mz_time2 = 1.5

    mz_time1 = fetch_now_from_mz(c)
    count_at_mz_time1 = c.sql_query(
        f"SELECT count(*) FROM {object_name} AS OF '{mz_time1}'::TIMESTAMP"
    )[0][0]

    time.sleep(sleep_duration_between_mz_time1_and_mz_time2)

    mz_time2 = fetch_now_from_mz(c)
    count_at_mz_time2 = c.sql_query(
        f"SELECT count(*) FROM {object_name} AS OF '{mz_time2}'::TIMESTAMP"
    )[0][0]
    count_at_mz_time1_queried_at_mz_time2 = c.sql_query(
        f"SELECT count(*) FROM {object_name} AS OF '{mz_time1}'::TIMESTAMP"
    )[0][0]

    assert count_at_mz_time1 == count_at_mz_time1_queried_at_mz_time2
    assert (
        count_at_mz_time2 > count_at_mz_time1
    ), f"value at time2 did not progress ({count_at_mz_time2} vs. {count_at_mz_time1}), consider increasing 'sleep_duration_between_mz_time1_and_mz_time2'"


def run_test_with_index(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > CREATE SOURCE retain_history_source3
              FROM LOAD GENERATOR COUNTER
              (TICK INTERVAL '100ms');
            > CREATE DEFAULT INDEX retain_history_idx
              ON retain_history_source2
              WITH (RETAIN HISTORY FOR '10s');
            """
        )
    )
    _validate_count_of_counter_source(c, "retain_history_source3")


def run_test_with_table(c: Composition) -> None:
    c.testdrive(
        dedent(
            """
            > CREATE TABLE time (time_index int, t timestamp);
            > CREATE TABLE table_with_retain_history (x int) WITH (RETAIN HISTORY FOR = '10s');
            > INSERT INTO time VALUES (0, now());
            # sleep justification: force time to advance
            $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s";
            > INSERT INTO table_with_retain_history VALUES (0);
            > INSERT INTO time VALUES (1, now());
            # sleep justification: force time to advance
            $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s";
            > INSERT INTO table_with_retain_history VALUES (1);
            > SELECT count(*) FROM table_with_retain_history;
            2
            $ set-from-sql var=time0
            SELECT t::string FROM time WHERE time_index = 0
            > SELECT count(*) FROM table_with_retain_history AS OF '${{time0}}'::timestamp;
            0
            $ set-from-sql var=time0
            SELECT t::string FROM time WHERE time_index = 1
            > SELECT count(*) FROM table_with_retain_history AS OF '${{time1}}'::timestamp;
            1
            """
        )
    )


def run_test_gh_24479(c: Composition) -> None:
    for seed, sleep_enabled in [(0, False), (1, True)]:
        c.testdrive(
            dedent(
                f"""
                > CREATE TABLE time_{seed} (time_index INT, t TIMESTAMP);

                > CREATE TABLE retain_history_table_{seed} (key INT, value INT);
                > INSERT INTO time_{seed} VALUES (1, now());

                {'$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"' if sleep_enabled else ''}

                > CREATE MATERIALIZED VIEW retain_history_mv2_{seed} WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table_{seed};

                $ set-from-sql var=time1_{seed}
                SELECT t::STRING FROM time_{seed} WHERE time_index = 1

                > SELECT count(*) FROM retain_history_mv2_{seed} AS OF '${{time1_{seed}}}'::TIMESTAMP; -- time1_{seed} with sleep_enabled={sleep_enabled}
                0
                """
            )
        )


def fetch_now_from_mz(c: Composition) -> str:
    return c.sql_query("SELECT now()")[0][0]
