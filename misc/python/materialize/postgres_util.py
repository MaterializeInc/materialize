# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from typing import Any

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.service import Service as MzComposeService
from materialize.mzcompose.service import ServiceConfig
from materialize.mzcompose.services.postgres import Postgres

# Set the max slot WAL keep size to 10MB
DEFAULT_PG_EXTRA_COMMAND = ["-c", "max_slot_wal_keep_size=10"]


class PostgresRecvlogical(MzComposeService):
    """
    Command to start a replication.
    """

    def __init__(self, replication_slot_name: str, publication_name: str) -> None:
        command: list[str] = [
            "pg_recvlogical",
            "--start",
            "--slot",
            f"{replication_slot_name}",
            "--file",
            "-",
            # We pass the maximum allowed fsync-interval (~24 days) to prevent
            # this process from advancing the slot. The purpose of this reader
            # is to just mark the slot as busy, not to move its reserved WAL
            # forward.
            "--fsync-interval",
            "2147483",
            "--dbname",
            "postgres",
            "--host",
            "postgres",
            "--port",
            "5432",
            "--username",
            "postgres",
            "--no-password",
            "-o",
            "proto_version=1",
            "-o",
            f"publication_names={publication_name}",
        ]
        config: ServiceConfig = {"mzbuild": "postgres"}

        config.update(
            {
                "command": command,
                "allow_host_ports": True,
                "ports": ["5432"],
                "environment": ["PGPASSWORD=postgres"],
            }
        )

        super().__init__(name="pg_recvlogical", config=config)


def create_postgres(
    pg_version: str | None, extra_command: list[str] = DEFAULT_PG_EXTRA_COMMAND
) -> Postgres:
    if pg_version is None:
        image = None
    else:
        image = f"postgres:{pg_version}"

    return Postgres(image=image, extra_command=extra_command)


def get_targeted_pg_version(parser: WorkflowArgumentParser) -> str | None:
    parser.add_argument(
        "--pg-version",
        type=str,
    )

    args, _ = parser.parse_known_args()
    pg_version = args.pg_version

    if pg_version is not None:
        print(f"Running with Postgres version {pg_version}")

    return pg_version


def await_postgres_replication_slot_state(
    pg_conn: Any, await_active: bool, error_message: str
) -> None:
    for i in range(1, 5):
        is_active = is_postgres_replication_slot_active(pg_conn)

        if is_active == await_active:
            return
        else:
            time.sleep(1)

    raise RuntimeError(error_message)


def get_postgres_replication_slot_name(pg_conn: Any) -> str:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT slot_name FROM pg_replication_slots;")
    return cursor.fetchall()[0][0]


def claim_postgres_replication_slot(c: Composition, pg_conn: Any) -> None:
    replicator = PostgresRecvlogical(
        replication_slot_name=get_postgres_replication_slot_name(pg_conn),
        publication_name="mz_source",
    )

    with c.override(replicator):
        c.up(replicator.name)


def is_postgres_replication_slot_active(pg_conn: Any) -> bool:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT active FROM pg_replication_slots;")
    is_active = cursor.fetchall()[0][0]
    return is_active


def verify_exactly_n_replication_slots_exist(pg_conn: Any, n: int) -> None:
    cursor = pg_conn.cursor()
    cursor.execute("SELECT count(*) FROM pg_replication_slots;")
    count_slots = cursor.fetchall()[0][0]
    assert (
        count_slots == n
    ), f"Expected {n} replication slot(s) but found {count_slots} slot(s)"
