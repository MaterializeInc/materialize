# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shared workflow functions for pg-cdc and pg-cdc-old-syntax tests."""

from collections.abc import Callable
from typing import Any

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.postgres_util import (
    await_postgres_replication_slot_state,
    claim_postgres_replication_slot,
    create_postgres,
    get_targeted_pg_version,
    get_testdrive_ssl_args,
    verify_exactly_n_replication_slots_exist,
)


def workflow_wal_level(c: Composition, parser: WorkflowArgumentParser) -> None:
    pg_version = get_targeted_pg_version(parser)
    for wal_level in ["replica", "minimal"]:
        with c.override(
            create_postgres(
                pg_version=pg_version,
                extra_command=[
                    "-c",
                    "max_wal_senders=0",
                    "-c",
                    f"wal_level={wal_level}",
                ],
            )
        ):
            c.up("materialized", "postgres")
            c.run_testdrive_files("override/insufficient-wal-level.td")


def workflow_replication_disabled(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version, extra_command=["-c", "max_wal_senders=0"]
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-disabled.td")


def kill_pg_and_mz(c: Composition) -> None:
    c.kill("postgres")
    c.rm("postgres")
    c.kill("materialized")
    c.rm("materialized")


def workflow_cdc(
    c: Composition,
    parser: WorkflowArgumentParser,
    exclude: list[str] | None = None,
) -> None:
    pg_version = get_targeted_pg_version(parser)
    sharded_files = c.glob_test_files(parser, exclude=exclude)
    ssl_args_dict = get_testdrive_ssl_args(c)
    testdrive_ssl_args = ssl_args_dict["testdrive_args"]

    testdrive_args = testdrive_ssl_args + Materialized.default_testdrive_size_args()
    with c.override(create_postgres(pg_version=pg_version)):
        c.up("materialized", "test-certs", "postgres")
        c.test_parts(
            sharded_files,
            lambda file: c.run_testdrive_files(
                *testdrive_args,
                file,
            ),
        )


def workflow_replication_slots(
    c: Composition,
    parser: WorkflowArgumentParser,
    max_replication_slots: int = 2,
) -> None:
    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version,
            extra_command=["-c", f"max_replication_slots={max_replication_slots}"],
        )
    ):
        c.up("materialized", "postgres")
        c.run_testdrive_files("override/replication-slots.td")


def workflow_silent_connection_drop(
    c: Composition,
    parser: WorkflowArgumentParser,
    connect_fn: Callable[..., Any] | None = None,
) -> None:
    """Test that mz can regain a replication slot that is used by another service."""
    if connect_fn is None:
        import psycopg

        connect_fn = psycopg.connect

    pg_version = get_targeted_pg_version(parser)
    with c.override(
        create_postgres(
            pg_version=pg_version,
            extra_command=[
                "-c",
                "wal_sender_timeout=0",
            ],
        ),
    ):
        c.up("postgres")

        pg_conn = connect_fn(
            host="localhost",
            user="postgres",
            password="postgres",
            port=c.default_port("postgres"),
        )

        verify_exactly_n_replication_slots_exist(pg_conn, n=0)

        c.up("materialized")

        c.run_testdrive_files(
            "--no-reset",
            f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
            "override/silent-connection-drop-part-1.td",
        )

        verify_exactly_n_replication_slots_exist(pg_conn, n=1)

        await_postgres_replication_slot_state(
            pg_conn,
            await_active=False,
            error_message="Replication slot is still active",
        )

        claim_postgres_replication_slot(c, pg_conn)

        await_postgres_replication_slot_state(
            pg_conn,
            await_active=True,
            error_message="Replication slot has not been claimed",
        )

        c.run_testdrive_files("--no-reset", "override/silent-connection-drop-part-2.td")

        verify_exactly_n_replication_slots_exist(pg_conn, n=1)
