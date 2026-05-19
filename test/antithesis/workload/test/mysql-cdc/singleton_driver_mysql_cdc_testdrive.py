#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: pick a random `test/mysql-cdc/*.td` file and run it.

Mirrors `singleton_driver_pg_cdc_testdrive` for the MySQL CDC source
code path. The Antithesis-side coverage grows automatically as new
.td files land in `test/mysql-cdc/`.

Each invocation:
  1. Picks one `.td` file at random from the bundled set, excluding
     files known to be incompatible with our topology (SSL fixtures,
     toxiproxy harness expectations).
  2. Runs the file via `helper_testdrive.run` — which strips
     `$ skip-if / SELECT true` headers and invokes the bundled
     testdrive binary with `--var=mysql-root-password=…` already in
     place.
  3. Asserts on the result.

Singleton (rather than parallel) because most files under
`test/mysql-cdc/` create connections / sources at well-known names
(`mysq`, `mysqlpass`, `mz_source`, …) and would collide with each
other if two ran on the same timeline. The parallel data-loss driver
lives in the `antithesis.cdc_test` schema with the `antithesis_mysql_*`
Materialize-side namespace, so it doesn't overlap with the names td
scripts use.

Property name: `mysql-cdc-testdrive-suite-no-spurious-failure`.
"""

from __future__ import annotations

import os
import sys

import helper_logging
import helper_random
import helper_testdrive
from helper_pg import execute_internal_retry, query_retry

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.mysql_cdc_testdrive")

TD_DIR = os.path.join(helper_testdrive.TESTDRIVE_FILES_ROOT, "test/mysql-cdc")

# Files known to fail deterministically in our topology — filtered out
# so the random picker doesn't fire `always(False)` on guaranteed-bad
# picks. Each entry should explain *why* the test is incompatible;
# revisit when the underlying assumption changes.
#
# Setup-incompatible (would need topology changes to support):
#
#   15-create-connection-tls.td, mysql-cdc-ssl.td — require a TLS-
#     configured upstream MySQL with test-certs CA bundle. Our `mysql`
#     primary service runs plain TCP — the Antithesis sandbox has no
#     host filesystem to source certs from.
#
# Stale / non-Antithesis testing concerns (the test files exercise
# things Antithesis schedules don't help with):
#
#   correctness-property-2.td — extremely long-running (~100k row
#     INSERT) deterministic regression for database-issues#4231;
#     timing out under fault injection adds noise without signal.
#
# Disabled in CI pending a product fix (skip-if-true headers) — the
# helper_testdrive layer strips those headers and runs anyway, so
# these entries cover cases where the underlying product behavior
# hasn't changed since the test was disabled:
#
#   alter-column-irrelevant.td, 25-schema-changes.td,
#   alter-table-after-source.td, alter-source.td, types-enum.td,
#   move-table.td — all flagged with `# TODO: Reenable when
#   database-issues#{8127,7900,7776} is fixed`. Until the product
#   fix lands, un-skipping under Antithesis just reproduces the same
#   product-level failure deterministically; not race-sensitivity
#   Antithesis can help with.
_EXCLUDE_FILES: frozenset[str] = frozenset(
    {
        "15-create-connection-tls.td",
        "mysql-cdc-ssl.td",
        "correctness-property-2.td",
        "alter-column-irrelevant.td",
        "25-schema-changes.td",
        "alter-table-after-source.td",
        "alter-source.td",
        "types-enum.td",
        "move-table.td",
    }
)


def _reset_public_schema() -> None:
    """Drop and recreate `materialize.public` so a prior testdrive run's
    leftover sources / connections / secrets don't collide with the next
    test's `CREATE` (none of the checked-in .td files use
    `IF NOT EXISTS`).  Mirrors the sql-server-cdc cleanup.  The data-
    loss workload's source / connection / table live in the dedicated
    `antithesis_mysql` schema, so they survive `DROP SCHEMA public`.

    NOTE: in real Antithesis runs this driver is a *singleton* (runs
    at most once per timeline per Test Composer's primitive), so
    cross-run leftovers can't actually accumulate.  The reset is
    primarily insurance for local-dev validation loops (`make up-local`
    + manual `docker exec workload …/singleton_driver_*`) that re-run
    the driver many times against the same long-lived materialized
    container.  Cheap enough at runtime that we always do it.

    Runs via mz_system on the internal SQL port; the materialize-user
    role doesn't own the public schema and would get OWNER errors.
    """
    LOG.info("resetting materialize.public schema before testdrive run")
    execute_internal_retry("DROP SCHEMA IF EXISTS public CASCADE")
    execute_internal_retry("CREATE SCHEMA public")
    execute_internal_retry("GRANT ALL ON SCHEMA public TO materialize")

    # Drop user clusters left behind by previous tests. Built-in clusters
    # (id starts with `s`) and the persistent ones the workload owns
    # (`quickstart`, `antithesis_cluster`) must stay.
    rows = query_retry(
        "SELECT name FROM mz_clusters "
        "WHERE name NOT IN ('quickstart', 'antithesis_cluster') "
        "AND id NOT LIKE %s",
        ("s%",),
    )
    for (name,) in rows:
        try:
            execute_internal_retry(f'DROP CLUSTER IF EXISTS "{name}" CASCADE')
            LOG.info("dropped leftover cluster %s", name)
        except Exception as exc:  # noqa: BLE001
            LOG.info("DROP CLUSTER %s failed: %s; continuing", name, exc)


def _list_td_files() -> list[str]:
    """Return repo-relative td paths to the bundled mysql-cdc tests."""
    if not os.path.isdir(TD_DIR):
        LOG.warning("td dir %s missing; image may not be rebuilt", TD_DIR)
        return []
    entries = []
    for name in sorted(os.listdir(TD_DIR)):
        if not name.endswith(".td"):
            continue
        if name in _EXCLUDE_FILES:
            continue
        entries.append(f"test/mysql-cdc/{name}")
    return entries


def main() -> int:
    files = _list_td_files()
    if not files:
        LOG.warning("no mysql-cdc td files bundled; exiting cleanly")
        return 0

    td_file = helper_random.random_choice(files)
    LOG.info(
        "picked %s from %d candidates (excluded %d)",
        td_file,
        len(files),
        len(_EXCLUDE_FILES),
    )

    result = helper_testdrive.run(td_file)

    clean_or_transient = result.succeeded or result.looks_transient
    always(
        clean_or_transient,
        "mysql-cdc: testdrive script doesn't fail with non-transient error "
        "under Antithesis fault injection",
        {
            "td_file": td_file,
            "exit_code": result.exit_code,
            "looks_transient": result.looks_transient,
            "stdout_tail": result.stdout[-1500:],
            "stderr_tail": result.stderr[-1500:],
        },
    )

    sometimes(
        result.succeeded,
        "mysql-cdc: testdrive script runs cleanly under Antithesis",
        {
            "td_file": td_file,
            "exit_code": result.exit_code,
        },
    )

    LOG.info(
        "mysql-cdc testdrive %s: exit=%d transient=%s clean_or_transient=%s",
        td_file,
        result.exit_code,
        result.looks_transient,
        clean_or_transient,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
