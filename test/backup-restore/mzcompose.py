# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    Mc(),
    Materialized(external_minio=True, external_cockroach=True, sanity_restart=False),
    Testdrive(no_reset=True),
    Persistcli(),
]


def workflow_default(c: Composition) -> None:
    # TODO: extract common substrings

    # Enable versioning for the Persist bucket
    c.enable_minio_versioning()

    # Start Materialize, and set up some basic state in it
    c.up("materialized")
    c.up("testdrive", persistent=True)
    c.testdrive(
        dedent(
            """
                > DROP TABLE IF EXISTS numbers;
                > CREATE TABLE IF NOT EXISTS numbers (id BIGINT);
                > INSERT INTO numbers SELECT * from generate_series(1, 1);
                > INSERT INTO numbers SELECT * from generate_series(1, 10);
                > INSERT INTO numbers SELECT * from generate_series(1, 100);
                """
        )
    )

    c.backup_crdb()

    # Make further updates to Materialize's state
    for i in range(0, 100):
        # TODO: This seems to be enough to produce interesting shard state;
        # ie. if we remove the restore-blob step we can see the restore fail.
        # Is there any cheaper or more obvious way to do that?
        c.testdrive(
            dedent(
                """
                    > INSERT INTO numbers SELECT * from generate_series(1, 1);
                    > INSERT INTO numbers SELECT * from generate_series(1, 10);
                    > INSERT INTO numbers SELECT * from generate_series(1, 100);
                    """
            )
        )

    # Restore CRDB from backup, run persistcli restore-blob and restart Mz
    c.restore_mz()

    # Confirm that the database is readable / has shard data
    c.exec(
        "cockroach",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "SELECT shard, min(sequence_number), max(sequence_number) "
        "FROM consensus.consensus GROUP BY 1 ORDER BY 2 DESC, 3 DESC, 1 ASC LIMIT 32;",
    )

    # Check that the cluster is up and that it answers queries as of the old state
    c.testdrive(
        dedent(
            """
                > SELECT count(*) FROM numbers;
                111
                """
        )
    )
