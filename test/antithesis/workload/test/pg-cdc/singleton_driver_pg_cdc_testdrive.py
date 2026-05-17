#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: pick a random `test/pg-cdc/*.td` file and run it.

Replaces the per-file singleton drivers. The Antithesis-side coverage
of the PG CDC source code path grows automatically as new `.td` files
land in `test/pg-cdc/` — no driver-level edit needed.

Each invocation:
  1. Resets Materialize user state (drops all user-visible objects not
     owned by the data-loss workload) and the upstream PG's `public`
     schema. Defends against state leftover from a previous run that
     crashed mid-script or from a still-running `parallel_driver_pg_cdc`.
  2. Picks one `.td` file at random from the bundled set, excluding
     files known to be incompatible with our topology (SSL fixtures).
  3. Runs the file via `helper_testdrive.run` — which strips the
     `$ skip-if / SELECT true` disable header so the test actually
     executes, then invokes the bundled testdrive binary.
  4. Asserts on the result.
  5. Cleans up Materialize state again and re-creates the data-loss
     workload's source/connection/secret/table so `parallel_driver_pg_cdc`
     finds them on its next invocation.

This is a `singleton_driver_` because almost every `.td` file under
`test/pg-cdc/` assumes exclusive ownership of `public` schema, the
`mz_source` publication on the upstream, and the `pgpass`/`pg`/`mz_source`
names on the materialize side. Two concurrent runs would trample each
other; the singleton harness primitive enforces serial execution.

Property name: `pg-cdc-testdrive-suite-no-spurious-failure`. The
assertion message is constant; the `td_file` lives in the assertion
details so Antithesis triage reports break the result down per-file.
"""

from __future__ import annotations

import os
import sys

import helper_logging
import helper_pg_source
import helper_random
import helper_testdrive

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.pg_cdc_testdrive")

TD_DIR = os.path.join(helper_testdrive.TESTDRIVE_FILES_ROOT, "test/pg-cdc")

# Files known to fail deterministically in our topology — filtered out so
# the random picker doesn't fire `always(False)` on guaranteed-bad picks.
# Each entry should explain *why* the test is incompatible; revisit when
# the underlying assumption changes.
#
# Setup-incompatible (would need topology changes to support):
#
#   pg-cdc-ssl.td, pg-cdc-ssl-ca-bundle.td — require a TLS-configured
#     upstream PG (custom certs). `postgres-source` is plain-TCP.
#
#   pg-cdc.td — exercises `! CREATE CONNECTION` with bad credentials and
#     asserts the error message contains `password authentication failed
#     for user "no_such_user"`. Our upstream PG runs with
#     `POSTGRES_HOST_AUTH_METHOD=trust` (set by export-compose.py's
#     inline_postgres_setup for the Antithesis sandbox network), so the
#     auth path returns a different error class. Setup-specific —
#     enabling MD5 auth would mean managing real passwords in the compose,
#     which the sandbox doesn't need.
#
#   subsource-resolution-duplicates.td — needs a custom `pg_hba.conf`
#     entry to test multi-user authentication paths. Our `postgres-source`
#     uses the stock `pg_hba.conf` (trust for all internal traffic) so
#     the test's auth-specific assertions don't apply.
#
# Stale relative to current product behavior (test pre-dates a tightening):
#
#   replica-identity-default-nothing.td — `> CREATE TABLE … FROM SOURCE`
#     against a table with `REPLICA IDENTITY DEFAULT` is expected to
#     succeed, but Materialize now eagerly rejects it at source-purify
#     time with `referenced items not tables with REPLICA IDENTITY FULL`
#     (src/sql/src/pure/error.rs). Test is skip-if-disabled in CI
#     pending database-issues#4231; un-skipping under Antithesis still
#     hits the same product-level rejection regardless of schedule.
#
#   alter-source.td — the 412-line database-issues#9571 flake suite.
#     Asserts very specific error-message text for `! ALTER SOURCE` and
#     `! CREATE TABLE FROM SOURCE` paths that has drifted with the
#     product. Failures here are real test/product divergence, not
#     race-sensitivity Antithesis can help with — fixes need test
#     rewrites alongside the product changes that broke them.
_EXCLUDE_FILES: frozenset[str] = frozenset(
    {
        "pg-cdc-ssl.td",
        "pg-cdc-ssl-ca-bundle.td",
        "pg-cdc.td",
        "replica-identity-default-nothing.td",
        "subsource-resolution-duplicates.td",
        "alter-source.td",
    }
)


def _list_td_files() -> list[str]:
    """Return repo-relative td paths to the bundled pg-cdc tests."""
    if not os.path.isdir(TD_DIR):
        LOG.warning("td dir %s missing; image may not be rebuilt", TD_DIR)
        return []
    entries = []
    for name in sorted(os.listdir(TD_DIR)):
        if not name.endswith(".td"):
            continue
        if name in _EXCLUDE_FILES:
            continue
        entries.append(f"test/pg-cdc/{name}")
    return entries


def main() -> int:
    files = _list_td_files()
    if not files:
        LOG.warning("no pg-cdc td files bundled; exiting cleanly")
        return 0

    td_file = helper_random.random_choice(files)
    LOG.info(
        "picked %s from %d candidates (excluded %d)",
        td_file,
        len(files),
        len(_EXCLUDE_FILES),
    )

    # Pre-run reset. Defends against residue from a prior crash. Also
    # frees up `public` on the upstream and standard names like
    # `pgpass`/`mz_source` on materialize so the td file's unprotected
    # CREATEs land cleanly.
    helper_testdrive.reset_materialize_user_state()
    helper_testdrive.reset_upstream_state()

    try:
        result = helper_testdrive.run(td_file)
    finally:
        # Post-run reset regardless of outcome — even on failure we want
        # the SUT in a known-clean state for the next driver.
        helper_testdrive.reset_materialize_user_state()
        helper_testdrive.reset_upstream_state()
        # Restore the data-loss workload's PG CDC pipeline. The reset
        # above didn't touch our antithesis_pg_* objects, but the
        # upstream schema reset wiped `antithesis_pg_cdc` if the td file
        # somehow touched it (none should — they all live in `public`).
        # Re-running ensure_pg_cdc_source is idempotent; this is
        # defense in depth.
        try:
            helper_pg_source.ensure_pg_cdc_source()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("post-run ensure_pg_cdc_source failed: %s", exc)

    # Safety: under Antithesis fault injection, a testdrive run on any
    # pg-cdc test file must either succeed or fail with a recognized
    # transient marker. A non-transient failure means a `>` or `!`
    # checkpoint inside the test disagreed with the SUT — i.e. a real
    # property violation surfaced by the schedule Antithesis explored.
    clean_or_transient = result.succeeded or result.looks_transient
    always(
        clean_or_transient,
        "pg-cdc: testdrive script doesn't fail with non-transient error "
        "under Antithesis fault injection",
        {
            "td_file": td_file,
            "exit_code": result.exit_code,
            "looks_transient": result.looks_transient,
            "stdout_tail": result.stdout[-1500:],
            "stderr_tail": result.stderr[-1500:],
        },
    )

    # Liveness: at least sometimes, on at least some file, the suite
    # runs cleanly. If this never fires the safety assertion is
    # vacuously satisfied by transient demotion.
    sometimes(
        result.succeeded,
        "pg-cdc: testdrive script runs cleanly under Antithesis",
        {
            "td_file": td_file,
            "exit_code": result.exit_code,
        },
    )

    LOG.info(
        "pg-cdc testdrive %s: exit=%d transient=%s clean_or_transient=%s",
        td_file,
        result.exit_code,
        result.looks_transient,
        clean_or_transient,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
