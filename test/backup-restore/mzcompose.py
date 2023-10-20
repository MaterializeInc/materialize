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
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    Mc(),
    Materialized(external_minio=True, external_cockroach=True, sanity_restart=False),
    Testdrive(no_reset=True),
    Service(
        name="persistcli",
        config={
            # TODO: depends!
            "mzbuild": "jobs"
        },
    ),
]


def workflow_default(c: Composition) -> None:
    # TODO: extract common substrings

    # Enable versioning for the Persist bucket
    c.up("minio")
    c.up("mc", persistent=True)
    c.exec(
        "mc",
        "mc",
        "alias",
        "set",
        "persist",
        "http://minio:9000/",
        "minioadmin",
        "minioadmin",
    )
    c.exec("mc", "mc", "version", "enable", "persist/persist")
    blob_uri = "s3://minioadmin:minioadmin@persist/persist?endpoint=http://minio:9000/&region=minio"

    # Set up a backupable connection for CRDB
    c.up("cockroach")
    c.exec("mc", "mc", "mb", "persist/crdb-backup")
    c.exec(
        "cockroach",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "CREATE EXTERNAL CONNECTION backup_bucket as "
        "'s3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&"
        "AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';"
        "SHOW CREATE ALL EXTERNAL CONNECTIONS;",
    )

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

    # Run a manual CRDB backup
    c.exec(
        "cockroach",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "BACKUP INTO 'external://backup_bucket';",
    )
    c.exec(
        "cockroach",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "SHOW BACKUPS IN 'external://backup_bucket';",
    )

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

    # Stop the running cluster and execute a restore
    c.kill(
        "materialized"
    )  # Very important that MZ is not running during the restore process!
    c.exec(
        "cockroach", "cockroach", "sql", "--insecure", "-e", "DROP DATABASE defaultdb;"
    )
    c.exec(
        "cockroach",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "RESTORE DATABASE defaultdb FROM LATEST IN 'external://backup_bucket';",
    )

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

    # Bring the persist state back into sync
    c.run(
        "persistcli",
        "admin",
        "--commit",
        "restore-blob",
        f"--blob-uri={blob_uri}",
        "--consensus-uri=postgres://root@cockroach:26257?options=--search_path=consensus",
    )

    # Restart Materialize
    c.up("materialized")

    # Check that the cluster is up and that it answers queries as of the old state
    c.testdrive(
        dedent(
            """
                > SELECT count(*) FROM numbers;
                111
                """
        )
    )
