#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: pick a random `test/testdrive/*.td` file and run it.

Mirrors `singleton_driver_pg_cdc_testdrive.py` but against the broader
`test/testdrive/` suite. Each invocation picks one `.td` at random from
the bundled set (minus a hand-curated incompatibility list) and runs it
via `helper_testdrive.run`.

Why singleton, not parallel: most `test/testdrive/*.td` files take
exclusive ownership of identifier names in the `public` schema (`t1`,
`t2`, `src1`, `cluster1`, …) and have no DROP cleanup. Two concurrent
files in the same execution history would collide on those names; the
singleton harness primitive runs at most once per timeline, so each
file gets the schema to itself.

`testdrive` is run via the same `helper_testdrive` plumbing as
`pg-cdc` / `mysql-cdc` / `sql-server-cdc`, which:
  - reads the bundled file from /opt/testdrive-files/test/testdrive/,
  - strips a `$ skip-if / SELECT true` disable header if present (the
    Antithesis run *is* the fix proving ground for files gated that
    way),
  - spawns testdrive with the antithesis-topology endpoints and the
    standard `--var=…` set the helper assembles.

Property name: `testdrive-suite-no-spurious-failure`. The `td_file`
lives in the assertion details so triage reports break the result down
per-file.
"""

from __future__ import annotations

import os
import sys

import helper_logging
import helper_random
import helper_testdrive

from antithesis.assertions import always, sometimes

LOG = helper_logging.setup_logging("driver.testdrive")

TD_DIR = os.path.join(helper_testdrive.TESTDRIVE_FILES_ROOT, "test/testdrive")

# Files known to be incompatible with the `testdrive` group's kafka-stack
# topology (kafka + zookeeper + schema-registry; no postgres-source,
# mysql, sql-server, ssh-bastion, aws/s3, fivetran). Each entry explains
# *why*; revisit when the assumption changes or the topology grows.
#
# Topology-incompatible — file references a service the testdrive
# group's compose doesn't include:
#
#   - postgres-source (group has none):
#       connection-create-drop.td, coord-read-holds.td, empty-array.td,
#       force-source-tables.td, mz-depends.td, source-tables.td,
#       source-timestamps.td, status-history.td, structured-errors.td
#   - mysql / mysql-replica (group has none):
#       force-source-tables.td, source-tables.td, source-timestamps.td,
#       status-history.td  (overlap with postgres-source set)
#   - ssh-bastion (group has none):
#       connection-alter.td, connection-create-drop.td,
#       connection-validation.td, mz-depends.td
#   - kafka SSL/SASL endpoints (the group's kafka runs plaintext only):
#       connection-create-drop.td, privilege-checks.td
#   - minio configured for S3 testdrive builtins (the group's minio is
#     the persist blob store, not pre-provisioned with COPY-INTO
#     buckets / AWS-style credentials):
#       copy-from-parquet-unsorted-map-keys.td,
#       copy-from-s3-minio-parquet.td, copy-from-s3-minio.td,
#       copy-s3-roundtrip-minio.td
#   - fivetran-destination sidecar (the group has none):
#       fivetran-destination.td
#
# Test-shape-incompatible:
#
#   github-1744.td references `${arg.single-replica-cluster}` without
#     a `$ set-arg-default single-replica-cluster=…` fallback, and
#     `helper_testdrive` doesn't pass it (test/testdrive/mzcompose.py
#     only passes it when replicas > 1). The file would fail at
#     testdrive parse time with "unknown variable: arg.single-
#     replica-cluster". Fixable upstream by adding a set-arg-default
#     to the file; until then exclude.
_EXCLUDE_FILES: frozenset[str] = frozenset(
    {
        # postgres-source dependents
        "coord-read-holds.td",
        "empty-array.td",
        "structured-errors.td",
        # postgres-source + mysql dependents
        "force-source-tables.td",
        "source-tables.td",
        "source-timestamps.td",
        "status-history.td",
        # ssh-bastion + postgres-source + ssl-kafka
        "connection-create-drop.td",
        # ssh-bastion + postgres-source
        "mz-depends.td",
        # ssh-bastion
        "connection-alter.td",
        "connection-validation.td",
        # kafka SSL/SASL
        "privilege-checks.td",
        # AWS / minio-as-S3
        "copy-from-parquet-unsorted-map-keys.td",
        "copy-from-s3-minio-parquet.td",
        "copy-from-s3-minio.td",
        "copy-s3-roundtrip-minio.td",
        # fivetran sidecar
        "fivetran-destination.td",
        # missing set-arg-default
        "github-1744.td",
    }
)


def _list_td_files() -> list[str]:
    """Return repo-relative td paths to the bundled test/testdrive tests."""
    if not os.path.isdir(TD_DIR):
        LOG.warning("td dir %s missing; image may not be rebuilt", TD_DIR)
        return []
    entries = []
    for name in sorted(os.listdir(TD_DIR)):
        if not name.endswith(".td"):
            continue
        if name in _EXCLUDE_FILES:
            continue
        entries.append(f"test/testdrive/{name}")
    return entries


def _count_directives(td_file_path: str) -> int | None:
    """Count `>` / `!` / `$` directives in the bundled .td source.

    Testdrive doesn't print a summary line we can grep for an
    "executable statements" count, so we use the *source* directive
    count as a deterministic upper bound on how much work a clean run
    would have done.  Progress is then "bytes of stdout produced" /
    "directives in source" as a rough proxy for fraction-completed.

    Returns None if the file isn't readable (image-staging glitch).
    """
    full = os.path.join(helper_testdrive.TESTDRIVE_FILES_ROOT, td_file_path)
    try:
        with open(full) as f:
            text = f.read()
    except OSError:
        return None
    # Conservative: only count leading-of-line directives, ignoring
    # comments / multi-line continuations.  Same shape testdrive
    # itself uses in its parser.
    return sum(
        1
        for line in text.splitlines()
        if line.startswith((">", "!", "$"))
    )


def main() -> int:
    files = _list_td_files()
    if not files:
        LOG.warning("no test/testdrive td files bundled; exiting cleanly")
        return 0

    td_file = helper_random.random_choice(files)
    LOG.info(
        "[PROGRESS] testdrive START file=%s (of %d candidates, excluded %d)",
        td_file,
        len(files),
        len(_EXCLUDE_FILES),
    )

    source_directives = _count_directives(td_file)
    result = helper_testdrive.run(td_file)
    stdout_bytes = len(result.stdout)

    # Safety: under Antithesis fault injection, a testdrive run on any
    # test/testdrive file must either succeed or fail with a recognized
    # transient marker. A non-transient failure means a `>` or `!`
    # checkpoint inside the test disagreed with the SUT — i.e. a real
    # property violation surfaced by the schedule Antithesis explored.
    clean_or_transient = result.succeeded or result.looks_transient
    always(
        clean_or_transient,
        "testdrive: script doesn't fail with non-transient error "
        "under Antithesis fault injection",
        {
            "td_file": td_file,
            "exit_code": result.exit_code,
            "looks_transient": result.looks_transient,
            "source_directives": source_directives,
            "stdout_bytes": stdout_bytes,
            "stdout_tail": result.stdout[-1500:],
            "stderr_tail": result.stderr[-1500:],
        },
    )

    # Liveness: at least sometimes, on at least some file, the suite
    # runs cleanly. If this never fires the safety assertion is
    # vacuously satisfied by transient demotion — i.e. faults are
    # masking every run and we'd have no signal.
    sometimes(
        result.succeeded,
        "testdrive: script runs cleanly under Antithesis",
        {
            "td_file": td_file,
            "exit_code": result.exit_code,
            "source_directives": source_directives,
            "stdout_bytes": stdout_bytes,
        },
    )

    # Tiered progress milestones.  Each `sometimes(True, ...)` fires when
    # ANY timeline observes the run produce at least that much stdout —
    # a proxy for "how deep into the .td body did testdrive get before
    # a fault stopped execution".  Mirrors the workload-replay driver's
    # tiered anchors; the triage report ends up with one green entry per
    # tier.  Names parameterised by tier (not by .td file) so the report
    # breadth stays bounded; the file path lives in the assertion-detail
    # blob above.
    for threshold in (1024, 10_240, 102_400):
        sometimes(
            stdout_bytes >= threshold,
            f"testdrive: stdout_bytes >= {threshold} in at least one timeline",
            {
                "threshold": threshold,
                "stdout_bytes": stdout_bytes,
                "td_file": td_file,
                "source_directives": source_directives,
            },
        )

    LOG.info(
        "[SUMMARY] testdrive END file=%s exit=%d transient=%s clean_or_transient=%s "
        "stdout_bytes=%d source_directives=%s",
        td_file,
        result.exit_code,
        result.looks_transient,
        clean_or_transient,
        stdout_bytes,
        source_directives if source_directives is not None else "?",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
