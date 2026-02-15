# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shared workflow functions for pg-cdc and pg-cdc-old-syntax tests."""

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.postgres_util import create_postgres, get_targeted_pg_version


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
