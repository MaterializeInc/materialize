# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Computed, Materialized, Testdrive

SERVICES = [
    Computed(
        name="computed_1_1",
        options="--workers 1 --processes 2 --process 0 computed_1_1:2102 computed_1_2:2102 --storage-addr materialized:2101",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_1_2",
        options="--workers 1 --processes 2 --process 1 computed_1_1:2102 computed_1_2:2102 --storage-addr materialized:2101",
        ports=[2100, 2102],
    ),
    Materialized(extra_ports=[2101]),
    Testdrive(
        volumes=[
            "mzdata:/share/mzdata",
            "tmp:/share/tmp",
        ],
        no_reset=True,
    ),
]


def workflow_default(c: Composition) -> None:
    c.up("testdrive", persistent=True)
    c.up("materialized")
    c.wait_for_materialized(service="materialized")

    for id in ["1_1", "1_2"]:
        c.up(f"computed_{id}")

    c.sql("DROP CLUSTER IF EXISTS cluster1 cascade;")
    c.sql(
        "CREATE CLUSTER cluster1 replica replica1 (remote ('computed_1_1:2100', 'computed_1_2:2100'));"
    )

    while True:
        c.testdrive(
            input="""
> SET cluster=cluster1

> DROP VIEW IF EXISTS panic_view;

# > DROP TABLE IF EXISTS panic_table;

> DROP TABLE IF EXISTS other_table;

# > CREATE TABLE panic_table (f1 TEXT)

> CREATE TABLE other_table (f1 TEXT)

# > CREATE MATERIALIZED VIEW panic_view AS SELECT mz_internal.mz_panic(f1) FROM panic_table;

# > INSERT INTO panic_table VALUES ('panic!');

> INSERT INTO other_table VALUES ('other1');

> SET cluster=default

> SELECT * FROM other_table
other1

# > DROP VIEW panic_view

> INSERT INTO other_table VALUES ('other2');
"""
        )

        time.sleep(1)
        for id in ["1_1", "1_2"]:
            c.kill(f"computed_{id}")

        # time.sleep(10)
        for id in ["1_1", "1_2"]:
            c.up(f"computed_{id}")

        c.testdrive(
            input="""
> SET cluster = cluster1

> INSERT INTO other_table VALUES ('other3');


> SELECT * FROM other_table
other1
other2
other3
"""
        )
