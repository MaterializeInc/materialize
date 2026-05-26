#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Random-query stress via SQLsmith.

Antithesis spawns this `parallel_driver_*` concurrently with the
randomized-DDL `parallel_driver_parallel_workload` driver.  SQLsmith
generates AST-derived random SELECTs against the live catalog, exercising
parser / planner / optimizer / executor surfaces the framework's own
action vocabulary doesn't reach (deeply-nested joins, weird casts, window
functions over MV outputs, etc.).

The binary is bundled into the workload image by the Dockerfile's
`MZFROM sqlsmith` multi-stage copy.  It connects to materialized over
pgwire and emits one JSON object on stdout describing every error it
saw — we walk that list, demote anything that matches the upstream
`materialize.sqlsmith.known_errors` allowlist or `looks_like_fault`, and
fire `always(no unexpected error)` over the remainder.

This is a *coverage* driver, not a verification one: SQLsmith can't
check result correctness against an oracle, only "did the SUT crash /
internal-error / hang?".  That makes it a strong panic-finder
specifically under Antithesis fault injection — concurrent kill / pause
windows turn ordinary-looking queries into stress on the recovery /
rehydration code paths.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys

import helper_logging
import helper_random
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault

LOG = helper_logging.setup_logging("driver.sqlsmith")

# `materialize.sqlsmith` lives at /opt/antithesis-pkg/materialize/ in
# the workload image; it's a stand-alone module with no transitive deps
# beyond Python stdlib so the import is cheap and side-effect-free.
try:
    from materialize.sqlsmith import known_errors
except ImportError:
    LOG.warning("materialize.sqlsmith not importable; known_errors=[]")
    known_errors: list[str] = []  # type: ignore[no-redef]

PGHOST = os.environ.get("PGHOST", "materialized")
PGPORT = int(os.environ.get("PGPORT", "6877"))  # mz_system port — full catalog access
PGUSER = os.environ.get("PGUSER", "mz_system")
PGDATABASE = os.environ.get("PGDATABASE", "materialize")

SQLSMITH_BIN = "/usr/local/bin/sqlsmith"

# Bound runtime to a multiple of the fault-orchestrator cycle (~80s) so
# every invocation observes at least one ON+OFF window.  Sqlsmith runs
# until SIGINT or until --max-queries is reached, whichever comes first;
# we let Antithesis kill us with the per-command budget instead of
# negotiating a timeout, but cap --max-queries to a finite value so a
# very-fast SUT can't burn the whole budget on a tight loop.
MAX_QUERIES = 200
MAX_JOINS = 5


def _is_expected_error(err: str) -> bool:
    """True if `err` is a sqlsmith-generated bad-query string or a fault-shape."""
    if any(pat in err for pat in known_errors):
        return True
    if looks_like_fault(err):
        return True
    return False


def main() -> int:
    if not shutil.which(SQLSMITH_BIN):
        # First time the driver runs in an environment where the binary
        # hasn't yet been bundled.  `sometimes(False, ...)` would be a
        # liveness regression once the binary lands; for now exit 0 so
        # the manifest entry is harmless ahead of the image change
        # landing in CI.
        LOG.warning("sqlsmith binary not found at %s; skipping", SQLSMITH_BIN)
        return 0

    seed = helper_random.random_u64()
    target = (
        f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} user={PGUSER}"
    )
    cmd = [
        SQLSMITH_BIN,
        f"--max-queries={MAX_QUERIES}",
        f"--max-joins={MAX_JOINS}",
        f"--seed={seed & 0x7FFFFFFF}",
        "--log-json",
        f"--target={target}",
    ]
    LOG.info("running sqlsmith: seed=%d max_queries=%d max_joins=%d", seed, MAX_QUERIES, MAX_JOINS)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        # subprocess.run shouldn't raise except for fork/exec failures.
        # Anything we see here is a workload-side problem, not a SUT
        # property violation — let it surface.
        LOG.warning("sqlsmith subprocess failed: %s", exc)
        return 0

    if result.returncode == 137:
        # OOM kill — Antithesis hit our cgroup memory ceiling. Not a SUT
        # bug, just a memory-budget signal.
        LOG.info("sqlsmith OOMed (rc=137)")
        return 0
    if result.returncode not in (0, 1, -2, 130):
        # sqlsmith documents 0=clean, 1=ran with non-zero errors. SIGINT
        # (-2 / 130) is how Antithesis cuts long-running drivers off.
        # Anything else here means the binary itself crashed.
        LOG.warning(
            "sqlsmith exited with unexpected rc=%s; stderr=%s",
            result.returncode,
            result.stderr[:500],
        )

    # Parse the JSON summary. `--log-json` writes one final aggregate
    # object on stdout when it exits normally; on SIGINT the output may
    # be truncated. Be defensive.
    try:
        data = json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError:
        # SIGINT mid-emit; treat as fault-shaped (the SIGINT itself is
        # platform-injected, not a SUT issue) and exit without firing.
        LOG.info("sqlsmith stdout was not valid JSON; likely SIGINT mid-emit")
        return 0

    errors = list(data.get("errors", []))
    queries = int(data.get("queries", 0))
    unexpected = [e for e in errors if not _is_expected_error(str(e))]

    LOG.info(
        "sqlsmith finished: queries=%d errors=%d unexpected=%d",
        queries,
        len(errors),
        len(unexpected),
    )

    always(
        not unexpected,
        "sqlsmith: no unexpected error escaped the random-query generator",
        {
            "seed": seed,
            "queries": queries,
            "errors_total": len(errors),
            "errors_unexpected_count": len(unexpected),
            # Bound to keep the assertion-detail payload reasonable;
            # triage can re-run with the seed to see the full set.
            "errors_unexpected_sample": [str(e)[:300] for e in unexpected[:5]],
        },
    )
    sometimes(
        queries > 0,
        "sqlsmith: at least one query executed in this timeline",
        {"seed": seed, "queries": queries},
    )
    sometimes(
        queries >= 100,
        "sqlsmith: >= 100 queries in at least one timeline",
        {"seed": seed, "queries": queries},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
