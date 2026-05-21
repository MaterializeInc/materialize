#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis first_ command: configure the upstream SQL Server and create
the Materialize SQL Server CDC source.

Runs once per Antithesis timeline before any parallel/singleton drivers.

The data-loss workload owns a dedicated `antithesis_test` database so
the singleton testdrive-runner's setup.td (which DROP/CREATEs `test`)
never collides with it. Steps:

  1. Wait for the upstream SQL Server to accept connections.
  2. (Idempotently) create the `antithesis_test` database with a
     UTF-8 collation, enable database-level CDC, and set
     ALLOW_SNAPSHOT_ISOLATION ON.
  3. Create dbo.cdc_test, enable table-level CDC on it.
  4. Create the Materialize-side secret / connection / source / table
     via helper_sql_server_source.ensure_sql_server_cdc_source.

No DummyTicker bootstrap here — the data-loss workload's parallel
driver drives enough upstream DML to keep CDC log activity flowing.
The DummyTicker job is a testdrive-runner concern and is set up by
the repo's own `setup.td`, prepended to each .td run by the singleton
testdrive driver.
"""

from __future__ import annotations

import sys

import helper_logging
import helper_sql_server_upstream as sql_server
from helper_sql_server_source import (
    SQL_SERVER_DATABASE,
    UPSTREAM_SCHEMA,
    UPSTREAM_TABLE,
    ensure_sql_server_cdc_source,
)

from antithesis.assertions import reachable

LOG = helper_logging.setup_logging("first.sql_server_cdc_setup")


def setup_database() -> None:
    """Idempotently create the data-loss workload's database with CDC enabled.

    Mirrors the shape of test/sql-server-cdc/setup/setup.td but targets
    `antithesis_test` instead of `test`:
      * UTF-8 collation so non-ASCII payloads in `value` columns work.
      * `sys.sp_cdc_enable_db` — required before any per-table CDC
        enablement.
      * `ALLOW_SNAPSHOT_ISOLATION ON` — Materialize's SQL Server source
        uses snapshot isolation for consistent initial snapshots.
    """
    LOG.info("ensuring `%s` database exists with CDC enabled", SQL_SERVER_DATABASE)
    # CREATE DATABASE / USE must run against `master`.
    sql_server.execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{SQL_SERVER_DATABASE}')
        BEGIN
            CREATE DATABASE {SQL_SERVER_DATABASE}
                COLLATE Latin1_General_100_CI_AI_SC_UTF8;
        END
        """,
        database="master",
    )
    sql_server.execute(
        f"""
        IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = N'{SQL_SERVER_DATABASE}') = 0
        BEGIN
            EXEC sys.sp_cdc_enable_db;
        END
        """,
        database=SQL_SERVER_DATABASE,
    )
    sql_server.execute(
        f"ALTER DATABASE {SQL_SERVER_DATABASE} SET ALLOW_SNAPSHOT_ISOLATION ON",
        database="master",
    )


def setup_cdc_table() -> None:
    """Create dbo.cdc_test if missing and enable table-level CDC on it.

    `sp_cdc_enable_table` rejects re-enablement on an already-CDC'd
    table with error 22832; we probe via `cdc.change_tables` first
    rather than relying on that error path.
    """
    LOG.info("creating %s.%s with CDC enabled", UPSTREAM_SCHEMA, UPSTREAM_TABLE)
    sql_server.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables
            WHERE name = N'{UPSTREAM_TABLE}'
              AND schema_id = SCHEMA_ID(N'{UPSTREAM_SCHEMA}')
        )
        BEGIN
            CREATE TABLE {UPSTREAM_SCHEMA}.{UPSTREAM_TABLE} (
                id          VARCHAR(64) NOT NULL PRIMARY KEY,
                batch_id    VARCHAR(64) NOT NULL,
                value       VARCHAR(MAX) NOT NULL,
                updated_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            );
        END
        """,
        database=SQL_SERVER_DATABASE,
    )
    sql_server.execute(
        f"""
        IF NOT EXISTS (
            SELECT 1 FROM cdc.change_tables ct
            JOIN sys.tables t ON ct.source_object_id = t.object_id
            WHERE t.name = N'{UPSTREAM_TABLE}'
              AND t.schema_id = SCHEMA_ID(N'{UPSTREAM_SCHEMA}')
        )
        BEGIN
            EXEC sys.sp_cdc_enable_table
                @source_schema = N'{UPSTREAM_SCHEMA}',
                @source_name = N'{UPSTREAM_TABLE}',
                @role_name = N'SA',
                @supports_net_changes = 0;
        END
        """,
        database=SQL_SERVER_DATABASE,
    )


def main() -> int:
    LOG.info("waiting for upstream SQL Server (%s)...", sql_server.SQL_SERVER_HOST)
    sql_server.wait_until_ready()

    setup_database()
    setup_cdc_table()
    ensure_sql_server_cdc_source()

    reachable(
        "sql-server: first-run setup complete — upstream seeded, Materialize source created",
        {
            "upstream": sql_server.SQL_SERVER_HOST,
            "database": sql_server.SQL_SERVER_DATABASE,
        },
    )
    LOG.info("SQL Server CDC setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
