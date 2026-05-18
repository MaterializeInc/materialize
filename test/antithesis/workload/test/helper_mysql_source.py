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

SECRET_NAME = "antithesis_mysql_password"
CONNECTION_NAME = "antithesis_mysql_conn"
SOURCE_NAME = "mysql_cdc_source"
TABLE_NAME = "antithesis_cdc"


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


def ensure_mysql_cdc_table() -> None:
    """Create the Materialize table from the MySQL CDC source (idempotent)."""
    try:
        execute_retry(
            f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} "
            f"FROM SOURCE {SOURCE_NAME} "
            f"(REFERENCE {MYSQL_DATABASE}.{MYSQL_TABLE})"
        )
    except psycopg.errors.InternalError as exc:
        if "already exists" not in str(exc):
            raise
        rows = query_retry("SELECT 1 FROM mz_tables WHERE name = %s", (TABLE_NAME,))
        if rows:
            LOG.info("table %s landed concurrently; tolerating collision", TABLE_NAME)
            return
        raise
    LOG.info("mysql cdc table %s ready", TABLE_NAME)


def ensure_mysql_cdc_source() -> None:
    """Create the full MySQL CDC pipeline in Materialize (idempotent).

    Requires antithesis.cdc_test to already exist on the MySQL replica.
    Call first_mysql_replica_setup.py before this in any standalone use.
    """
    ensure_mysql_connection()
    create_source_idempotent(
        f"CREATE SOURCE IF NOT EXISTS {SOURCE_NAME} "
        f"IN CLUSTER {CLUSTER} "
        f"FROM MYSQL CONNECTION {CONNECTION_NAME}",
        SOURCE_NAME,
    )
    LOG.info("mysql cdc source %s ready", SOURCE_NAME)
    ensure_mysql_cdc_table()
