# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Idempotent setup for the Antithesis Postgres CDC source in Materialize.

The pipeline:
  postgres-source --logical replication slot--> Materialize

Production Postgres CDC is single-instance — there's no equivalent of
the MySQL replica intermediate. Materialize opens a replication slot on
the upstream PG and consumes WAL directly. Faults on the upstream
exercise the source recovery path; faults on materialized exercise the
replication-slot-resume path.

Objects created in Materialize:
  - SECRET     antithesis_pg_password
  - CONNECTION antithesis_pg_conn  -> postgres-source
  - SOURCE     pg_cdc_source        (IN CLUSTER antithesis_cluster)
  - TABLE      antithesis_pg_cdc    (REFERENCE antithesis_pg_cdc.cdc_test)

Objects created on the upstream PG:
  - SCHEMA      antithesis_pg_cdc
  - TABLE       antithesis_pg_cdc.cdc_test (REPLICA IDENTITY FULL so
                 deletes carry the old row even with no PK column added)
  - PUBLICATION antithesis_pub FOR TABLE antithesis_pg_cdc.cdc_test

Note: the data-loss workload deliberately owns its own schema rather
than `public`. The repository's `test/pg-cdc/*.td` testdrive files
assume exclusive ownership of `public` (they DROP/CREATE it), so
running them under Antithesis alongside this driver requires that we
stay out of their way.
"""

from __future__ import annotations

import logging
import os

import psycopg
from helper_pg import create_source_idempotent, execute_retry, query_retry

LOG = logging.getLogger("antithesis.helper_pg_source")

CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")
PG_SOURCE_HOST = os.environ.get("PG_SOURCE_HOST", "postgres-source")
PG_SOURCE_PORT = int(os.environ.get("PG_SOURCE_PORT", "5432"))
PG_SOURCE_USER = os.environ.get("PG_SOURCE_USER", "postgres")
PG_SOURCE_PASSWORD = os.environ.get("PG_SOURCE_PASSWORD", "postgres")
PG_SOURCE_DATABASE = os.environ.get("PG_SOURCE_DATABASE", "postgres")

# Upstream-side names. The schema deliberately is NOT `public` so the
# testdrive-runner drivers (which assume exclusive ownership of `public`)
# can run concurrently with the data-loss workload without trampling.
UPSTREAM_SCHEMA = "antithesis_pg_cdc"
UPSTREAM_TABLE = "cdc_test"
UPSTREAM_PUBLICATION = "antithesis_pub"

# Materialize-side names.
SECRET_NAME = "antithesis_pg_password"
CONNECTION_NAME = "antithesis_pg_conn"
SOURCE_NAME = "pg_cdc_source"
TABLE_NAME = "antithesis_pg_cdc"


def ensure_pg_connection() -> None:
    """Create the upstream-PG secret and connection in Materialize (idempotent)."""
    execute_retry(
        f"CREATE SECRET IF NOT EXISTS {SECRET_NAME} AS '{PG_SOURCE_PASSWORD}'"
    )
    execute_retry(
        f"CREATE CONNECTION IF NOT EXISTS {CONNECTION_NAME} TO POSTGRES ("
        f"HOST '{PG_SOURCE_HOST}', "
        f"PORT {PG_SOURCE_PORT}, "
        f"USER '{PG_SOURCE_USER}', "
        f"PASSWORD SECRET {SECRET_NAME}, "
        f"DATABASE '{PG_SOURCE_DATABASE}'"
        f")"
    )
    LOG.info("pg connection %s ready (upstream=%s)", CONNECTION_NAME, PG_SOURCE_HOST)


def ensure_pg_cdc_table() -> None:
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
        rows = query_retry("SELECT 1 FROM mz_tables WHERE name = %s", (TABLE_NAME,))
        if rows:
            LOG.info("table %s landed concurrently; tolerating collision", TABLE_NAME)
            return
        raise
    LOG.info(
        "pg cdc table %s ready (upstream=%s.%s)",
        TABLE_NAME,
        UPSTREAM_SCHEMA,
        UPSTREAM_TABLE,
    )


def ensure_pg_cdc_source() -> None:
    """Create the full PG CDC pipeline in Materialize (idempotent).

    Requires {UPSTREAM_SCHEMA}.cdc_test and PUBLICATION antithesis_pub to
    already exist on the upstream PG. Call first_pg_cdc_setup.py before
    this.
    """
    ensure_pg_connection()
    create_source_idempotent(
        f"CREATE SOURCE IF NOT EXISTS {SOURCE_NAME} "
        f"IN CLUSTER {CLUSTER} "
        f"FROM POSTGRES CONNECTION {CONNECTION_NAME} "
        f"(PUBLICATION '{UPSTREAM_PUBLICATION}')",
        SOURCE_NAME,
    )
    LOG.info("pg cdc source %s ready", SOURCE_NAME)
    ensure_pg_cdc_table()
