# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Idempotent setup for the Antithesis SQL Server CDC source in Materialize.

The pipeline:
  sql-server (CDC enabled at the DB level)
    --change-table polling--> Materialize

SQL Server CDC is single-instance: Materialize polls the engine's
`cdc.<schema>_<table>_CT` change tables via the SQL Server source code
path (mz_storage::source::sql_server). Faults on the upstream exercise
the source recovery path; faults on materialized exercise the LSN-resume
path.

Objects created in Materialize:
  - SECRET     antithesis_sql_server_password
  - CONNECTION antithesis_sql_server_conn  -> sql-server
  - SOURCE     sql_server_cdc_source       (IN CLUSTER antithesis_cluster)
  - TABLE      antithesis_sql_server_cdc   (REFERENCE dbo.cdc_test)

Objects created on the upstream SQL Server (see first_sql_server_cdc_setup):
  - DATABASE   test            (CDC + ALLOW_SNAPSHOT_ISOLATION ON)
  - TABLE      dbo.cdc_test    (CDC-enabled via sys.sp_cdc_enable_table)
  - JOB        DummyTicker     (keeps the CDC capture instance hot)

The data-loss workload deliberately owns the `dbo.cdc_test` name rather
than the names used by the repo's `test/sql-server-cdc/*.td` files — the
testdrive-runner driver under this group claims exclusive ownership of
those names (`t1_pk`, `t2_no_cdc`, `t3_text`, …), so the two layers don't
collide. The shared dependency is the `test` database itself, which both
need to exist and which the setup driver creates if missing.
"""

from __future__ import annotations

import logging
import os

import psycopg
from helper_pg import create_source_idempotent, execute_retry, query_retry

LOG = logging.getLogger("antithesis.helper_sql_server_source")

CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")
SQL_SERVER_HOST = os.environ.get("SQL_SERVER_HOST", "sql-server")
SQL_SERVER_PORT = int(os.environ.get("SQL_SERVER_PORT", "1433"))
SQL_SERVER_USER = os.environ.get("SQL_SERVER_USER", "SA")
SQL_SERVER_PASSWORD = os.environ.get("SQL_SERVER_PASSWORD", "RPSsql12345")
SQL_SERVER_DATABASE = os.environ.get("SQL_SERVER_DATABASE", "antithesis_test")

# Upstream-side names. `dbo` is SQL Server's default schema; the
# data-loss table sits there alongside the testdrive-runner's tables
# (`t1_pk`, `t3_text`, …). Distinct names ensure no collision.
UPSTREAM_SCHEMA = "dbo"
UPSTREAM_TABLE = "cdc_test"

# Materialize-side names. The source + reference table live in a
# dedicated `antithesis_sql_server` schema so the singleton testdrive-
# runner's `SHOW SOURCES` / `mz_sources` assertions (which run in the
# default `public` schema) don't see our data-loss source and trip on
# unexpected extra rows. The secret + connection are namespace-shared
# but `CREATE SECRET / CONNECTION` don't show up in `SHOW SOURCES`,
# so they stay in the default schema.
MZ_SCHEMA = "antithesis_sql_server"
SECRET_NAME = "antithesis_sql_server_password"
CONNECTION_NAME = "antithesis_sql_server_conn"
SOURCE_NAME = f"{MZ_SCHEMA}.sql_server_cdc_source"
TABLE_NAME = f"{MZ_SCHEMA}.antithesis_sql_server_cdc"
# Bare (unqualified) names — used when looking them up in mz_sources
# / mz_tables, which store `name` un-qualified and expose the schema
# via a separate column.
SOURCE_BASENAME = "sql_server_cdc_source"
TABLE_BASENAME = "antithesis_sql_server_cdc"


def ensure_sql_server_connection() -> None:
    """Create the SQL Server secret and connection in Materialize (idempotent)."""
    execute_retry(
        f"CREATE SECRET IF NOT EXISTS {SECRET_NAME} AS '{SQL_SERVER_PASSWORD}'"
    )
    execute_retry(
        f"CREATE CONNECTION IF NOT EXISTS {CONNECTION_NAME} TO SQL SERVER ("
        f"HOST '{SQL_SERVER_HOST}', "
        f"PORT {SQL_SERVER_PORT}, "
        f"DATABASE {SQL_SERVER_DATABASE}, "
        f"USER '{SQL_SERVER_USER}', "
        f"PASSWORD SECRET {SECRET_NAME}"
        f")"
    )
    LOG.info(
        "sql-server connection %s ready (upstream=%s)",
        CONNECTION_NAME,
        SQL_SERVER_HOST,
    )


def ensure_sql_server_cdc_table() -> None:
    """Create the Materialize-side reference table from the source (idempotent)."""
    try:
        execute_retry(
            f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} "
            f"FROM SOURCE {SOURCE_NAME} "
            f"(REFERENCE {UPSTREAM_SCHEMA}.{UPSTREAM_TABLE})"
        )
    except psycopg.errors.InternalError as exc:
        if "already exists" not in str(exc):
            raise
        rows = query_retry("SELECT 1 FROM mz_tables WHERE name = %s", (TABLE_BASENAME,))
        if rows:
            LOG.info("table %s landed concurrently; tolerating collision", TABLE_NAME)
            return
        raise
    LOG.info(
        "sql-server cdc table %s ready (upstream=%s.%s)",
        TABLE_NAME,
        UPSTREAM_SCHEMA,
        UPSTREAM_TABLE,
    )


def ensure_sql_server_cdc_source() -> None:
    """Create the full SQL Server CDC pipeline in Materialize (idempotent).

    Requires `test` DB + `dbo.cdc_test` (with CDC enabled) to already
    exist on the upstream. Call first_sql_server_cdc_setup.py before
    this in any standalone use.
    """
    # Dedicated schema so testdrive-runner `SHOW SOURCES` queries in
    # the default `public` schema don't see our source.
    execute_retry(f"CREATE SCHEMA IF NOT EXISTS {MZ_SCHEMA}")
    ensure_sql_server_connection()
    # `create_source_idempotent` looks up `mz_sources.name` — un-qualified —
    # so pass `SOURCE_BASENAME` rather than the schema-qualified ref.
    create_source_idempotent(
        f"CREATE SOURCE IF NOT EXISTS {SOURCE_NAME} "
        f"IN CLUSTER {CLUSTER} "
        f"FROM SQL SERVER CONNECTION {CONNECTION_NAME}",
        SOURCE_BASENAME,
    )
    LOG.info("sql-server cdc source %s ready", SOURCE_NAME)
    ensure_sql_server_cdc_table()
