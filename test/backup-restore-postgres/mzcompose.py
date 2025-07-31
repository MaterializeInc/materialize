# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Basic Backup & Restore test with a table
"""

from textwrap import dedent

from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.postgres import PostgresMetadata
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    PostgresMetadata(),
    Minio(setup_materialize=True),
    Mc(),
    Materialized(
        external_blob_store=True,
        external_metadata_store=True,
        sanity_restart=False,
        metadata_store="postgres-metadata",
    ),
    Testdrive(no_reset=True, metadata_store="postgres-metadata"),
    Persistcli(),
]


def workflow_default(c: Composition) -> None:
    # TODO: extract common substrings

    # Enable versioning for the Persist bucket
    c.enable_minio_versioning()

    # Start Materialize, and set up some basic state in it
    c.up("materialized", Service("testdrive", idle=True))
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

    c.backup_postgres()

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
    c.restore_postgres()

    # Confirm that the database is readable / has shard data
    c.exec(
        "postgres-metadata",
        "psql",
        "--command",
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
