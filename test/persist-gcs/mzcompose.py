# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test that Materialize can use GCS as a blob store via fake-gcs-server.
"""

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.gcs import GcsEmulator
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    GcsEmulator(setup_materialize=True),
    Materialized(
        external_blob_store=True,
        blob_store_is_gcs=True,
        sanity_restart=False,
        #environment_extra=["MZ_LOG_FILTER=mz_persist=debug,mz_persist_client=debug"],
        environment_extra=["RUST_LOG=debug"],
    ),
    Testdrive(
        external_blob_store=True,
        blob_store_is_gcs=True,
        volumes_extra=["mzdata:/mzdata"],
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test basic GCS blob store functionality."""

    parser.add_argument(
        "--default-size",
        type=int,
        default=Materialized.Size.DEFAULT_SIZE,
        help="Use SIZE 'scale=N,workers=N' for replicas",
    )
    args = parser.parse_args()

    with c.override(
        Materialized(
            default_size=args.default_size,
            external_blob_store=True,
            blob_store_is_gcs=True,
            sanity_restart=False,
            environment_extra=["RUST_LOG=debug"],
        ),
    ):
        c.up("gcs-emulator", "materialized")

        # Basic connectivity test - ensure Materialize can start with GCS blob store
        c.sql("SELECT 1")

        # Create a table and insert data to exercise blob storage
        c.sql(
            """
            CREATE TABLE gcs_test (a INT, b TEXT);
            INSERT INTO gcs_test VALUES (1, 'hello'), (2, 'world');
            """
        )

        # Verify data was persisted
        result = c.sql_query("SELECT * FROM gcs_test ORDER BY a")
        assert result == [["1", "hello"], ["2", "world"]], f"Unexpected result: {result}"

        # Create a materialized view to exercise more blob operations
        c.sql(
            """
            CREATE MATERIALIZED VIEW gcs_mv AS
            SELECT a, upper(b) as b_upper FROM gcs_test;
            """
        )

        # Verify materialized view
        result = c.sql_query("SELECT * FROM gcs_mv ORDER BY a")
        assert result == [
            ["1", "HELLO"],
            ["2", "WORLD"],
        ], f"Unexpected result: {result}"

        # Clean up
        c.sql("DROP MATERIALIZED VIEW gcs_mv")
        c.sql("DROP TABLE gcs_test")

        print("GCS blob store test passed!")


def workflow_testdrive(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive tests with GCS blob store."""

    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified testdrive files",
    )
    args = parser.parse_args()

    c.up("gcs-emulator", "materialized")

    c.run_testdrive_files(*args.files)
