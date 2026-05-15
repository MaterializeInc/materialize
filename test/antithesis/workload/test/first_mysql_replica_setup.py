#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis first_ command: configure MySQL multithreaded replica replication
and create the Materialize MySQL CDC source.

Runs once per Antithesis timeline before any parallel/singleton drivers start.
Steps:
  1. Wait for both MySQL containers to accept connections.
  2. Create the `antithesis` database and `cdc_test` table on the primary.
  3. Configure the replica to replicate from the primary via GTID with 4
     parallel worker threads (multithreaded replication).
  4. Start the replica.
  5. Wait for `antithesis.cdc_test` to appear on the replica (confirms
     replication is flowing).
  6. Create the Materialize connection, source, and table from the replica.
"""

from __future__ import annotations

import sys
import time

import helper_logging
import helper_mysql
from helper_mysql_source import ensure_mysql_cdc_source

from antithesis.assertions import reachable, sometimes

LOG = helper_logging.setup_logging("first.mysql_replica_setup")


def setup_primary() -> None:
    """Create the antithesis schema and both cdc_test tables on the MySQL
    primary.

    Two tables on different engines so we exercise both the transactional
    (InnoDB) and non-transactional (MyISAM) DML paths through the binlog
    and the Materialize MySQL source. MyISAM differences worth noting for
    triage:
      * BEGIN/COMMIT around MyISAM statements is silently ignored — each
        statement commits immediately.
      * Each MyISAM statement is its own GTID-tagged binlog event (no
        bundling into a multi-statement transaction).
      * No rollback semantics: a MyISAM statement that fails partway
        through leaves whatever rows it managed to write committed.
      * No ON UPDATE TIMESTAMP support before MySQL 5.6 — we use a
        simpler schema (no updated_at) on MyISAM to avoid version churn.
    """
    LOG.info("creating antithesis database and cdc_test tables on primary")
    helper_mysql.execute_primary("CREATE DATABASE IF NOT EXISTS antithesis")
    helper_mysql.execute_primary(
        """
        CREATE TABLE IF NOT EXISTS antithesis.cdc_test (
            id VARCHAR(64) NOT NULL PRIMARY KEY,
            batch_id VARCHAR(64) NOT NULL,
            value TEXT NOT NULL,
            updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6)
        ) ENGINE=InnoDB
        """,
        database="antithesis",
    )
    helper_mysql.execute_primary(
        """
        CREATE TABLE IF NOT EXISTS antithesis.cdc_test_myisam (
            id VARCHAR(64) NOT NULL PRIMARY KEY,
            batch_id VARCHAR(64) NOT NULL,
            value TEXT NOT NULL
        ) ENGINE=MyISAM
        """,
        database="antithesis",
    )
    LOG.info(
        "antithesis.cdc_test (InnoDB) and cdc_test_myisam (MyISAM) ready on primary"
    )


def configure_replica() -> None:
    """Configure the MySQL replica to replicate from the primary.

    Uses GTID auto-positioning with 4 parallel workers. The replica starts
    with --skip-replica-start so we configure the channel before starting.
    Idempotent: stops and resets any existing channel first.
    """
    LOG.info(
        "configuring replica to replicate from %s with 4 parallel workers",
        helper_mysql.MYSQL_HOST,
    )
    # Stop and reset any existing channel (no-op on a fresh container).
    try:
        helper_mysql.execute_replica("STOP REPLICA")
    except Exception:  # noqa: BLE001
        pass
    try:
        helper_mysql.execute_replica("RESET REPLICA ALL")
    except Exception:  # noqa: BLE001
        pass

    helper_mysql.execute_replica(
        f"CHANGE REPLICATION SOURCE TO "
        f"SOURCE_HOST='{helper_mysql.MYSQL_HOST}', "
        f"SOURCE_USER='root', "
        f"SOURCE_PASSWORD='{helper_mysql.MYSQL_PASSWORD}', "
        f"SOURCE_AUTO_POSITION=1, "
        f"GET_SOURCE_PUBLIC_KEY=1"
    )
    # Set parallel replication parameters before starting.
    helper_mysql.execute_replica("SET GLOBAL replica_parallel_workers = 4")
    helper_mysql.execute_replica("SET GLOBAL replica_preserve_commit_order = ON")
    helper_mysql.execute_replica("START REPLICA")
    LOG.info("MySQL replica started")


def wait_for_replica_tables(timeout_s: float = 90.0) -> bool:
    """Wait until both antithesis.cdc_test (InnoDB) and cdc_test_myisam
    (MyISAM) are visible on the replica.

    Returns True when both tables appear (replication is flowing across
    both engines), False on timeout.
    """
    deadline = time.monotonic() + timeout_s
    needed = {"cdc_test", "cdc_test_myisam"}
    while time.monotonic() < deadline:
        try:
            rows = helper_mysql.query_replica(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'antithesis' "
                "AND table_name IN ('cdc_test', 'cdc_test_myisam')",
            )
            seen = {r[0] for r in rows}
            if needed.issubset(seen):
                LOG.info(
                    "antithesis cdc tables visible on replica — replication flowing (%s)",
                    sorted(seen),
                )
                return True
        except Exception as exc:  # noqa: BLE001
            LOG.info("waiting for replica tables: %s", exc)
        time.sleep(2)
    LOG.warning("timed out waiting for antithesis.cdc_test{,_myisam} on replica")
    return False


def main() -> int:
    LOG.info("waiting for MySQL primary (%s)...", helper_mysql.MYSQL_HOST)
    helper_mysql.wait_for_primary()

    LOG.info("waiting for MySQL replica (%s)...", helper_mysql.MYSQL_REPLICA_HOST)
    helper_mysql.wait_for_replica()

    setup_primary()
    configure_replica()

    replica_ready = wait_for_replica_tables()
    sometimes(
        replica_ready,
        "mysql replica: both cdc_test tables replicated from primary within 90s",
        {
            "primary": helper_mysql.MYSQL_HOST,
            "replica": helper_mysql.MYSQL_REPLICA_HOST,
        },
    )
    if not replica_ready:
        # Proceed anyway — replication may catch up before Materialize tries to
        # validate the source, but log a warning so triage can correlate.
        LOG.warning("replica tables not yet visible; proceeding with source creation")

    ensure_mysql_cdc_source()

    reachable(
        "mysql: first-run setup complete — replica configured, Materialize source created",
        {
            "primary": helper_mysql.MYSQL_HOST,
            "replica": helper_mysql.MYSQL_REPLICA_HOST,
        },
    )
    LOG.info("MySQL CDC setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
