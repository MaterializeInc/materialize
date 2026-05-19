#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: pick a random `test/sql-server-cdc/*.td` file and run it.

Mirrors `singleton_driver_pg_cdc_testdrive`: as new .td files land in
`test/sql-server-cdc/` the Antithesis-side coverage grows automatically,
no per-file driver edit needed.

Each invocation:
  1. Picks one `.td` file at random from the bundled set, excluding
     files known to be incompatible with our topology (SSL fixtures).
  2. Inlines `setup/setup.td` as a prelude — the repo's `workflow_cdc`
     runs setup.td before every per-file run because each test
     assumes a freshly-reset `test` database + DummyTicker job. The
     prelude DROP/CREATEs the `test` DB only; the data-loss workload
     lives in `antithesis_test`, so its source isn't touched.
  3. Runs both via `helper_testdrive.run` — which strips
     `$ skip-if / SELECT true` headers and invokes the bundled
     testdrive binary with the sql-server credentials baked into the
     workload service's environment.
  4. Asserts on the result.

Singleton (rather than parallel) because the .td files share the same
`test` database namespace and would collide if two ran concurrently —
the singleton harness primitive runs at most once per timeline so
there's no second testdrive run on the same timeline to race against
the parallel data-loss driver (which lives in `antithesis_test`).

Property name: `sql-server-cdc-testdrive-suite-no-spurious-failure`.
"""

from __future__ import annotations

import os
import sys

import helper_logging
import helper_random
import helper_testdrive
from helper_pg import execute_internal_retry, query_retry

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.sql_server_cdc_testdrive")

TD_DIR = os.path.join(helper_testdrive.TESTDRIVE_FILES_ROOT, "test/sql-server-cdc")

# `setup.td` resets the `test` DB and the DummyTicker job to fresh
# state. Every checked-in .td file assumes that prelude has run.
SETUP_TD = "test/sql-server-cdc/setup/setup.td"

# Files known to fail deterministically in our topology — filtered out
# so the random picker doesn't fire `always(False)` on guaranteed-bad
# picks. Each entry should explain *why* the test is incompatible;
# revisit when the underlying assumption changes.
#
# Setup-incompatible (would need topology changes to support):
#
#   11-sql-server-cdc-ssl.td — requires a TLS-configured upstream SQL
#     Server with test-certs CA chain. Our `sql-server` service runs
#     plain TCP (no `secrets:/var/opt/mssql/certs` volume) because the
#     Antithesis sandbox has no host filesystem to source certs from.
_EXCLUDE_FILES: frozenset[str] = frozenset(
    {
        "11-sql-server-cdc-ssl.td",
    }
)


def _reset_public_schema() -> None:
    """Drop and recreate `materialize.public` so a prior testdrive run's
    leftover sources / connections / secrets don't collide with the next
    test's `CREATE` (none of the checked-in .td files use
    `IF NOT EXISTS`).  Mirrors what test/sql-server-cdc/mzcompose.py's
    workflow_cdc gets for free by killing+removing the materialized
    container between runs — we can't do that under Antithesis, so we
    do a schema-level reset via the internal SQL port instead.

    NOTE: in real Antithesis runs this driver is a *singleton* (runs
    at most once per timeline per Test Composer's primitive), so
    cross-run leftovers can't actually accumulate.  The reset is
    primarily insurance for local-dev validation loops (`make up-local`
    + manual `docker exec workload …/singleton_driver_*`) that re-run
    the driver many times against the same long-lived materialized
    container.  Cheap enough at runtime that we always do it.

    Data-loss workload state survives this because it lives in
    `materialize.antithesis_sql_server.*`, a separate schema.  User
    clusters created by individual tests (`stats_cluster`, `cdc_cluster`,
    …) are also dropped — they're named in the tests so a name collision
    would otherwise fail the second run.

    Runs via mz_system on the internal SQL port; the materialize-user
    role doesn't own the public schema and would get OWNER errors.
    """
    LOG.info("resetting materialize.public schema before testdrive run")
    execute_internal_retry("DROP SCHEMA IF EXISTS public CASCADE")
    execute_internal_retry("CREATE SCHEMA public")
    execute_internal_retry("GRANT ALL ON SCHEMA public TO materialize")

    # Drop user clusters left behind by previous tests. Built-in clusters
    # (mz_system, mz_catalog_server, mz_probe, mz_support, mz_analytics)
    # and the persistent ones the workload owns (`quickstart`,
    # `antithesis_cluster`) must stay.  Identifying user clusters via
    # `owner_id <> 's1'` (s1 is mz_system) keeps the predicate
    # forward-compatible with new built-ins.
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
    """Return repo-relative td paths to the bundled sql-server-cdc tests."""
    if not os.path.isdir(TD_DIR):
        LOG.warning("td dir %s missing; image may not be rebuilt", TD_DIR)
        return []
    entries = []
    for name in sorted(os.listdir(TD_DIR)):
        if not name.endswith(".td"):
            continue
        if name in _EXCLUDE_FILES:
            continue
        entries.append(f"test/sql-server-cdc/{name}")
    return entries


def main() -> int:
    files = _list_td_files()
    if not files:
        LOG.warning("no sql-server-cdc td files bundled; exiting cleanly")
        return 0

    td_file = helper_random.random_choice(files)
    LOG.info(
        "picked %s from %d candidates (excluded %d)",
        td_file,
        len(files),
        len(_EXCLUDE_FILES),
    )

    _reset_public_schema()

    result = helper_testdrive.run(td_file, prelude_files=[SETUP_TD])

    # Safety: under Antithesis fault injection, a testdrive run on any
    # sql-server-cdc test file must either succeed or fail with a
    # recognized transient marker. A non-transient failure means a `>`
    # or `!` checkpoint inside the test disagreed with the SUT — i.e. a
    # real property violation surfaced by the schedule Antithesis
    # explored.
    clean_or_transient = result.succeeded or result.looks_transient
    always(
        clean_or_transient,
        "sql-server-cdc: testdrive script doesn't fail with non-transient error "
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
        "sql-server-cdc: testdrive script runs cleanly under Antithesis",
        {
            "td_file": td_file,
            "exit_code": result.exit_code,
        },
    )

    LOG.info(
        "sql-server-cdc testdrive %s: exit=%d transient=%s clean_or_transient=%s",
        td_file,
        result.exit_code,
        result.looks_transient,
        clean_or_transient,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
