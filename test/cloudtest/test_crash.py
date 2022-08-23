# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from pg8000.exceptions import InterfaceError  # type: ignore

from materialize.cloudtest.application import MaterializeApplication
from materialize.cloudtest.wait import wait


def populate(mz: MaterializeApplication, seed: int) -> None:
    mz.testdrive.run_string(
        dedent(
            """
            > CREATE TABLE t1 (f1 INTEGER);

            > INSERT INTO t1 VALUES (123);

            > CREATE DEFAULT INDEX ON t1;

            > INSERT INTO t1 VALUES (234);

            > CREATE SOURCE s1
              FROM KAFKA BROKER '${testdrive.kafka-addr}'
              TOPIC 'testdrive-crash-${testdrive.seed}'
              FORMAT BYTES
              ENVELOPE NONE;

            $ kafka-create-topic topic=crash

            $ kafka-ingest format=bytes topic=crash
            CDE

            > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1 UNION ALL SELECT COUNT(*) FROM s1;

            $ kafka-ingest format=bytes topic=crash
            DEF

            > CREATE DEFAULT INDEX ON v1;

            > SELECT COUNT(*) > 0 FROM s1;
            true
            """
        ),
        seed=seed,
    )


def validate(mz: MaterializeApplication, seed: int) -> None:
    mz.testdrive.run_string(
        dedent(
            """
            > INSERT INTO t1 VALUES (345);

            $ kafka-ingest format=bytes topic=crash
            EFG

            > SELECT COUNT(*) FROM t1;
            3

            > SELECT COUNT(*) FROM s1;
            3

            > SELECT * FROM v1;
            3
            3
            """
        ),
        no_reset=True,
        seed=seed,
    )


def test_crash_storaged(mz: MaterializeApplication) -> None:
    populate(mz, 1)

    source_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_sources WHERE name = 's1';"
    )[0][0]
    assert source_id is not None
    pod_name = f"pod/storage-{source_id}-0"

    wait(condition="jsonpath={.status.phase}=Running", resource=pod_name)
    mz.kubectl("exec", pod_name, "--", "bash", "-c", "kill -9 `pidof storaged` || true")
    wait(condition="jsonpath={.status.phase}=Running", resource=pod_name)

    validate(mz, 1)


def test_crash_environmentd(mz: MaterializeApplication) -> None:
    populate(mz, 2)
    try:
        mz.environmentd.sql("SELECT mz_internal.mz_panic('forced panic')")
    except InterfaceError:
        pass
    validate(mz, 2)


def test_crash_computed(mz: MaterializeApplication) -> None:
    populate(mz, 3)
    mz.environmentd.sql("CREATE TABLE crash_table (f1 TEXT)")
    mz.environmentd.sql(
        "CREATE MATERIALIZED VIEW crash_view AS SELECT mz_internal.mz_panic(f1) FROM crash_table"
    )
    mz.environmentd.sql("INSERT INTO crash_table VALUES ('forced panic')")

    mz.testdrive.run_string(
        dedent(
            """
            > DROP MATERIALIZED VIEW crash_view
            """
        ),
        no_reset=True,
        seed=3,
    )

    validate(mz, 3)
