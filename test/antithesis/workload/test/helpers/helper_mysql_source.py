# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Idempotent setup for the Antithesis MySQL CDC source in Materialize.

The MySQL CDC pipeline:
  mysql (primary) --binlog--> mysql-replica --CDC--> Materialize

Materialize reads from the replica so that faults to the replica exercise
the Materialize source recovery path independently of faults to the primary.

Objects created in Materialize:
  - SECRET  antithesis_mysql_password
  - CONNECTION antithesis_mysql_conn  -> mysql-replica
  - SOURCE  mysql_cdc_source          (IN CLUSTER antithesis_cluster)
  - TABLE   antithesis_cdc            (REFERENCE antithesis.cdc_test)
  - TABLE   antithesis_cdc_myisam     (REFERENCE antithesis.cdc_test_myisam)

The MyISAM-backed reference exercises CDC for non-transactional DML: in
MySQL, MyISAM statements commit immediately (BEGIN/COMMIT is silently
ignored), so the binlog sees them as standalone events with their own
GTIDs rather than bundled inside a transaction. Materialize's source
code path doesn't distinguish engines, so this is a property check that
the engine-agnostic behavior actually holds.
"""

from __future__ import annotations

import logging
import os

import psycopg
from helper_pg import create_source_idempotent, execute_retry, query_retry

LOG = logging.getLogger("antithesis.helper_mysql_source")

CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")
MYSQL_REPLICA_HOST = os.environ.get("MYSQL_REPLICA_HOST", "mysql-replica")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "p@ssw0rd")

MYSQL_DATABASE = "antithesis"
MYSQL_TABLE = "cdc_test"
MYSQL_TABLE_MYISAM = "cdc_test_myisam"

# Materialize-side names. Source + reference tables live in a
# dedicated `antithesis_mysql` schema so the singleton testdrive-
# runner's `DROP SCHEMA public CASCADE` (used to clear prior test
# state) doesn't take down the data-loss source.
MZ_SCHEMA = "antithesis_mysql"
SECRET_NAME = "antithesis_mysql_password"
CONNECTION_NAME = "antithesis_mysql_conn"
SOURCE_NAME = f"{MZ_SCHEMA}.mysql_cdc_source"
TABLE_NAME = f"{MZ_SCHEMA}.antithesis_cdc"
TABLE_NAME_MYISAM = f"{MZ_SCHEMA}.antithesis_cdc_myisam"
# Bare (unqualified) names — used when looking them up in mz_sources
# / mz_tables, which store `name` un-qualified and expose the schema
# via a separate column.
SOURCE_BASENAME = "mysql_cdc_source"
TABLE_BASENAME = "antithesis_cdc"
TABLE_BASENAME_MYISAM = "antithesis_cdc_myisam"


def ensure_mysql_connection() -> None:
    """Create the MySQL secret and connection in Materialize (idempotent)."""
    execute_retry(f"CREATE SECRET IF NOT EXISTS {SECRET_NAME} AS '{MYSQL_PASSWORD}'")
    execute_retry(
        f"CREATE CONNECTION IF NOT EXISTS {CONNECTION_NAME} TO MYSQL ("
        f"HOST '{MYSQL_REPLICA_HOST}', "
        f"USER 'root', "
        f"PASSWORD SECRET {SECRET_NAME}"
        f")"
    )
    LOG.info(
        "mysql connection %s ready (replica=%s)", CONNECTION_NAME, MYSQL_REPLICA_HOST
    )


def _ensure_mysql_cdc_subtable(
    mz_table: str, mz_table_basename: str, upstream_table: str
) -> None:
    """Create one Materialize table that references `upstream_table` in the
    MySQL CDC source (idempotent). Shared between the InnoDB and MyISAM
    references; both come from the same source.
    """
    try:
        execute_retry(
            f"CREATE TABLE IF NOT EXISTS {mz_table} "
            f"FROM SOURCE {SOURCE_NAME} "
            f"(REFERENCE {MYSQL_DATABASE}.{upstream_table})"
        )
    except psycopg.errors.InternalError as exc:
        if "already exists" not in str(exc):
            raise
        rows = query_retry(
            "SELECT 1 FROM mz_tables WHERE name = %s", (mz_table_basename,)
        )
        if rows:
            LOG.info("table %s landed concurrently; tolerating collision", mz_table)
            return
        raise
    LOG.info("mysql cdc table %s ready (upstream=%s)", mz_table, upstream_table)


def ensure_mysql_cdc_table() -> None:
    """Create the InnoDB-backed Materialize table from the source."""
    _ensure_mysql_cdc_subtable(TABLE_NAME, TABLE_BASENAME, MYSQL_TABLE)


def ensure_mysql_cdc_myisam_table() -> None:
    """Create the MyISAM-backed Materialize table from the source."""
    _ensure_mysql_cdc_subtable(
        TABLE_NAME_MYISAM, TABLE_BASENAME_MYISAM, MYSQL_TABLE_MYISAM
    )


def ensure_mysql_cdc_source() -> None:
    """Create the full MySQL CDC pipeline in Materialize (idempotent).

    Requires antithesis.cdc_test AND antithesis.cdc_test_myisam to already
    exist on the MySQL replica. Call first_mysql_replica_setup.py before
    this in any standalone use.
    """
    # Dedicated schema so testdrive-runner `DROP SCHEMA public` doesn't
    # take down our data-loss source.
    execute_retry(f"CREATE SCHEMA IF NOT EXISTS {MZ_SCHEMA}")
    ensure_mysql_connection()
    # `create_source_idempotent` looks up `mz_sources.name` un-qualified.
    create_source_idempotent(
        f"CREATE SOURCE IF NOT EXISTS {SOURCE_NAME} "
        f"IN CLUSTER {CLUSTER} "
        f"FROM MYSQL CONNECTION {CONNECTION_NAME}",
        SOURCE_BASENAME,
    )
    LOG.info("mysql cdc source %s ready", SOURCE_NAME)
    ensure_mysql_cdc_table()
    ensure_mysql_cdc_myisam_table()
