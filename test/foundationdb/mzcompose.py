# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test FoundationDB as a backend for the timestamp oracle.
"""

from textwrap import dedent

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.foundationdb import FoundationDB
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Cockroach(setup_materialize=True),
    FoundationDB(),
    Materialized(
        external_metadata_store=True,
        metadata_store="foundationdb",
    ),
    Testdrive(
        external_metadata_store=True, metadata_store="foundationdb", no_reset=True
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test that Materialize works with FoundationDB as the timestamp oracle backend."""

    c.down(destroy_volumes=True, sanity_restart_mz=False)
    c.up("foundationdb")

    c.up("materialized")

    # Basic smoke test - create objects and verify they work
    c.testdrive(
        input=dedent(
            """
            > CREATE TABLE t1 (a INT);
            > INSERT INTO t1 VALUES (1), (2), (3);
            > SELECT * FROM t1 ORDER BY a;
            1
            2
            3

            > CREATE VIEW v1 AS SELECT a * 2 AS doubled FROM t1;
            > SELECT * FROM v1 ORDER BY doubled;
            2
            4
            6

            > CREATE MATERIALIZED VIEW mv1 AS SELECT SUM(a) AS total FROM t1;
            > SELECT * FROM mv1;
            6

            > INSERT INTO t1 VALUES (4);
            > SELECT * FROM mv1;
            10

            > DROP MATERIALIZED VIEW mv1;
            > DROP VIEW v1;
            > DROP TABLE t1;
            """
        )
    )

    # Test restart - verify timestamp oracle state is preserved
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        input=dedent(
            """
            > CREATE TABLE t2 (b TEXT);
            > INSERT INTO t2 VALUES ('hello'), ('world');
            > SELECT * FROM t2 ORDER BY b;
            hello
            world
            """
        )
    )
