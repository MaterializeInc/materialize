# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Iceberg sink names, Materialize-side bootstrap, and a duckdb
verifier for the table contents on the Polaris/minio side.

The iceberg group drives one MZ table -> one iceberg sink -> one
iceberg table at `default_catalog.default_namespace.<TABLE>`.  Per-
invocation drivers scope their writes by `batch_id`, so multiple
concurrent driver invocations can share the same sink without
interfering with each other's expected-state model.

Iceberg-side verification rides on testdrive's `duckdb-query` builtin
(libduckdb.so is already in the workload image because testdrive
bundles it).  `verify_iceberg_count` writes a small inline `.td` that
ATTACHes the Polaris REST catalog from duckdb (plus an S3 secret for
minio data-file fetches) and asserts the count for the caller's
batch_id matches the expected value.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Callable

import helper_testdrive
import psycopg
from helper_pg import execute_retry, query_one_retry, query_retry

LOG = logging.getLogger("antithesis.helper_iceberg")

# Polaris realm / catalog / namespace.  These match `helper_polaris_setup`
# (the shared bootstrap that provisions the same names) — keep in sync if
# either side changes.
CATALOG_NAME = "default_catalog"
NAMESPACE_NAME = "default_namespace"
BUCKET_NAME = "test-bucket"

# The MZ-side cluster every driver in this workload provisions onto.
# Wired up by workload-entrypoint.sh before any first_/parallel_driver_
# command runs.
CLUSTER = os.environ.get("MZ_ANTITHESIS_CLUSTER", "antithesis_cluster")

# MZ-side names.  The sink is created once by first_iceberg_setup and
# parallel drivers reuse it across invocations — Antithesis exercises
# the per-batch failure cases by driving INSERTs through this single
# long-lived sink rather than creating a fresh sink per invocation.
SECRET_S3_KEY = "antithesis_minio_secret"
CONNECTION_AWS = "antithesis_aws_conn"
CONNECTION_POLARIS = "antithesis_polaris_conn"
SRC_TABLE = "iceberg_src"
SINK_NAME = "iceberg_sink"

# Iceberg-side table name (the actual iceberg table in
# `default_namespace`).  Distinct from the MZ-side `SRC_TABLE` name —
# the sink's `TABLE 'X'` parameter is the iceberg-side identifier, not
# the MZ-side identifier.
ICEBERG_TABLE = "antithesis_sink_target"

# Sink commit cadence.  Short enough that per-invocation drivers finish
# their catchup wait inside the catchup budget; long enough that
# multiple INSERTs from the same invocation typically land in one
# snapshot (so the sink batches data files like it would in
# production — single-row commits would mask the SS-148-shaped bug we
# hope to surface, since each retry would only ever apply one row).
COMMIT_INTERVAL = "2s"

# minio credentials.  Static root creds — see helper_polaris_setup for
# the reasoning (single-tenant Antithesis sandbox).
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT_HOST = "minio"
MINIO_ENDPOINT_PORT = 9000


def ensure_iceberg_objects() -> None:
    """Create the MZ-side secret/connection/source-table/sink (idempotent).

    Requires `helper_polaris_setup.main()` to have already provisioned
    Polaris and the backing minio bucket.
    """
    execute_retry(
        f"CREATE SECRET IF NOT EXISTS {SECRET_S3_KEY} AS '{MINIO_SECRET_KEY}'"
    )
    execute_retry(
        f"CREATE CONNECTION IF NOT EXISTS {CONNECTION_AWS} TO AWS ("
        f"ENDPOINT 'http://{MINIO_ENDPOINT_HOST}:{MINIO_ENDPOINT_PORT}/', "
        f"REGION 'minio', "
        f"ACCESS KEY ID '{MINIO_ACCESS_KEY}', "
        f"SECRET ACCESS KEY SECRET {SECRET_S3_KEY})"
    )
    execute_retry(
        f"CREATE CONNECTION IF NOT EXISTS {CONNECTION_POLARIS} TO ICEBERG CATALOG ("
        f"CATALOG TYPE = 'REST', "
        f"URL = 'http://polaris:8181/api/catalog', "
        f"CREDENTIAL = 'root:root', "
        f"WAREHOUSE = '{CATALOG_NAME}', "
        f"SCOPE = 'PRINCIPAL_ROLE:ALL')"
    )
    # The upstream MZ table the sink reads from.  Plain table (no source)
    # — drivers INSERT into it directly via SQL.  Schema mirrors the
    # source-side property tests: id (key) + batch_id (per-invocation
    # scoping) + value (the cell the driver checks for correctness).
    execute_retry(
        f"CREATE TABLE IF NOT EXISTS {SRC_TABLE} ("
        f"id TEXT NOT NULL, "
        f"batch_id TEXT NOT NULL, "
        f"value TEXT NOT NULL)"
    )
    # MODE UPSERT with KEY (id): each id maps to at most one iceberg row.
    # Picked over MODE NONE (append) because the SS-148 bug duplicates
    # snapshots; an upsert sink's per-key uniqueness is a sharper safety
    # contract — a duplicate snapshot of the same data files violates
    # per-key uniqueness directly, where as an append sink could be
    # plausibly defended as "well, it's append, every insert produces a
    # new row".  We want the property to be unambiguously violated.
    try:
        execute_retry(
            f"CREATE SINK IF NOT EXISTS {SINK_NAME} "
            f"IN CLUSTER {CLUSTER} "
            f"FROM {SRC_TABLE} "
            f"INTO ICEBERG CATALOG CONNECTION {CONNECTION_POLARIS} ("
            f"NAMESPACE '{NAMESPACE_NAME}', "
            f"TABLE '{ICEBERG_TABLE}') "
            f"USING AWS CONNECTION {CONNECTION_AWS} "
            f"KEY (id) NOT ENFORCED "
            f"MODE UPSERT "
            f"WITH (COMMIT INTERVAL '{COMMIT_INTERVAL}')"
        )
    except psycopg.errors.InternalError as exc:
        if "already exists" not in str(exc):
            raise
        rows = query_retry("SELECT 1 FROM mz_sinks WHERE name = %s", (SINK_NAME,))
        if rows:
            LOG.info("sink %s landed concurrently; tolerating collision", SINK_NAME)
            return
        raise
    LOG.info(
        "iceberg sink %s ready (table=%s.%s in %s)",
        SINK_NAME,
        NAMESPACE_NAME,
        ICEBERG_TABLE,
        CATALOG_NAME,
    )


def messages_committed() -> int | None:
    """Return rolled-up `messages_committed` for the iceberg sink, or None.

    NULL when the statistics row hasn't been populated yet (early in
    sink lifetime, or right after a clusterd restart before stats first
    reported).
    """
    row = query_one_retry(
        """
        SELECT MAX(ss.messages_committed)::bigint
        FROM mz_internal.mz_sink_statistics ss
        JOIN mz_sinks s ON s.id = ss.id
        WHERE s.name = %s
        """,
        (SINK_NAME,),
    )
    if row is None or row[0] is None:
        return None
    return int(row[0])


def sink_status() -> tuple[str, str] | None:
    """Return (status, error) for the iceberg sink, or None if not found."""
    row = query_one_retry(
        """
        SELECT ss.status, COALESCE(ss.error, '')
        FROM mz_internal.mz_sink_statuses ss
        JOIN mz_sinks s ON s.id = ss.id
        WHERE s.name = %s
        """,
        (SINK_NAME,),
    )
    if row is None:
        return None
    return (str(row[0]), str(row[1]))


def polaris_qualified_table(table: str = ICEBERG_TABLE) -> str:
    """Catalog-attached SQL identifier for the iceberg table.  Matches the
    `ATTACH 'default_catalog' AS polaris` alias set up by
    `_duckdb_secret_block`.
    """
    return f"polaris.{NAMESPACE_NAME}.{table}"


def _duckdb_secret_block() -> str:
    """Inline duckdb-execute block that attaches the iceberg catalog and
    sets up the S3 secret minio needs for data-file fetches.

    The polaris ATTACH replaces the previous `iceberg_scan` +
    `unsafe_enable_version_guessing = true` path.  Going through the
    catalog has two upsides for triage:

    1.  When polaris is down (Antithesis node-fault) duckdb fails with a
        connection error containing the polaris endpoint, which
        `helper_fault_tolerance.FAULT_PATTERNS` classifies as transient —
        the driver demotes to a `sometimes` instead of asserting on stale
        data.
    2.  duckdb reads the metadata version polaris reports as current,
        which is the same view `messages_committed` increments against.
        Direct `iceberg_scan` with version guessing reads the highest
        metadata file it finds in S3, which can race with the sink's
        commit-rename and surface as a phantom partial count.

    Wrapped in `CREATE OR REPLACE` so re-invocations under the same
    `name=ice` duckdb instance don't trip on a leftover secret from a
    previous testdrive run.  ATTACH runs exactly once per testdrive
    subprocess (verify_iceberg_count / verify_iceberg_rows_match each
    emit one inline script), so no IF NOT EXISTS guard is needed.
    """
    return (
        "$ duckdb-execute name=ice\n"
        f"CREATE OR REPLACE SECRET ice_s3 (TYPE S3, "
        f"KEY_ID '{MINIO_ACCESS_KEY}', SECRET '{MINIO_SECRET_KEY}', "
        f"ENDPOINT '{MINIO_ENDPOINT_HOST}:{MINIO_ENDPOINT_PORT}', "
        f"URL_STYLE 'path', USE_SSL false, REGION 'minio');\n"
        # OAUTH2_SCOPE is required for Polaris: the bootstrap-creds
        # principal (POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,root) only
        # issues a token when the OAuth request asks for `PRINCIPAL_ROLE:ALL`.
        # Without it, Polaris responds with an OAuth error body that has no
        # `access_token` field, and duckdb's iceberg extension surfaces that
        # as `OAuthTokenResponse is missing required property 'access_token'`
        # at SECRET-creation time.  The Python-side helper_polaris_setup
        # passes the same scope when bootstrapping the catalog — see
        # `get_polaris_token`.
        f"CREATE OR REPLACE SECRET polaris_secret "
        f"(TYPE iceberg, CLIENT_ID 'root', CLIENT_SECRET 'root', "
        f"OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL', "
        f"ENDPOINT 'http://polaris:8181/api/catalog');\n"
        f"ATTACH '{CATALOG_NAME}' AS polaris "
        f"(TYPE iceberg, SECRET polaris_secret, "
        f"ACCESS_DELEGATION_MODE 'none');\n"
    )


class IcebergCheckResult:
    """Outcome of a duckdb-side count check.

    `matched` is True only if testdrive returned the expected count.
    `looks_transient` is True if the failure shape matches the canonical
    fault-injection patterns (network errors talking to minio/polaris,
    SQL-state 08006 from polaris's JDBC pool, etc.) — callers demote
    transient failures to `sometimes(False)` rather than asserting.
    """

    __slots__ = ("matched", "looks_transient", "stdout", "stderr", "exit_code")

    def __init__(
        self,
        matched: bool,
        looks_transient: bool,
        stdout: str,
        stderr: str,
        exit_code: int,
    ) -> None:
        self.matched = matched
        self.looks_transient = looks_transient
        self.stdout = stdout
        self.stderr = stderr
        self.exit_code = exit_code


# Between retries of a duckdb verify, give the sink at least one more
# COMMIT INTERVAL (2s for our setup) to flush.  Anything shorter just
# re-fires the same one-shot duckdb query without giving the sink a
# chance to make new progress.
_VERIFY_RETRY_INTERVAL_S = 3.0

# Per-attempt subprocess cap.  Each duckdb verify runs CREATE SECRET +
# ATTACH + one SELECT — empirically <10s in steady state and ~6s during
# the observed failure-mode runs.  60s gives ample headroom while
# ensuring a hung subprocess can't burn the entire retry budget.
_VERIFY_PER_ATTEMPT_TIMEOUT_S = 60.0


# Parses the integer immediately following the `actual (N rows):` line
# in testdrive's count-mismatch output.  Used to short-circuit the
# count retry loop when `actual > expected` — see
# `_count_actual_overshoots`.
_COUNT_ACTUAL_RE = re.compile(r"actual \(1 rows\):\s*\n\s*(\d+)")


def _count_actual_overshoots(expected: int) -> Callable[[str], bool]:
    """Build a stdout-parser that returns True when testdrive reported
    `actual > expected` for the count check.

    The count-check `.td` runs `SELECT COUNT(*) … = expected`, so the
    only possible mismatch shapes are `actual < expected` (catchup
    race, recoverable by waiting) and `actual > expected` (duplicate
    rows — the SS-148 target — never recoverable by waiting).  We pass
    this parser as the retry loop's `overshoot_check` so the duplicate
    case fails fast with the first observed counts intact, instead of
    burning the full catchup budget re-observing the same overshoot.
    """

    def check(stdout: str) -> bool:
        m = _COUNT_ACTUAL_RE.search(stdout)
        if m is None:
            return False
        try:
            return int(m.group(1)) > expected
        except ValueError:
            return False

    return check


def _run_verify_once(
    td: str,
    *,
    label: str,
    timeout_s: float,
) -> IcebergCheckResult:
    """Run a duckdb verify inline `.td` exactly once.  Used by the
    per-row check, which only runs after the count check has already
    passed and therefore can't be a catchup race — a mismatch at that
    point is a real correctness issue (extra/missing/wrong rows), so
    there's nothing to gain from retrying."""
    per_attempt = min(_VERIFY_PER_ATTEMPT_TIMEOUT_S, max(15.0, timeout_s))
    result = helper_testdrive.run_inline(td, label=label, timeout_s=per_attempt)
    return IcebergCheckResult(
        matched=result.succeeded,
        looks_transient=result.looks_transient,
        stdout=result.stdout,
        stderr=result.stderr,
        exit_code=result.exit_code,
    )


def _run_verify_with_retry(
    td_template: str,
    *,
    label: str,
    timeout_s: float,
    overshoot_check: Callable[[str], bool] | None = None,
) -> IcebergCheckResult:
    """Run a duckdb verify inline `.td` repeatedly until it succeeds, the
    budget is exhausted, a transient (fault-injection-shaped) failure
    is observed, or the optional `overshoot_check` parser signals that
    the mismatch shape won't recover with more waiting.

    `testdrive`'s `duckdb-query` directive is one-shot — it runs the
    SELECT exactly once and fails the script if the row set doesn't
    match.  Per-batch verification therefore needs the retry loop here,
    on top: the sink's `messages_committed` gate is necessarily-but-not-
    sufficient (see `_wait_for_sink_progress` in
    `parallel_driver_iceberg_no_data_loss_or_dup.py`), so a count
    mismatch on the first iceberg-side query is more often "not flushed
    yet" than a real correctness gap.  Retrying on a real *undercount*
    is safe — if the sink genuinely lost data the retry keeps
    undercounting and we still fail at the deadline.  But for
    mismatches that *can't* recover (duplicate rows, where actual >
    expected), `overshoot_check` lets the caller bail immediately so
    the SS-148 signal isn't delayed by a 120s wait that won't change
    the answer.

    Transient (fault-injected) failures short-circuit immediately so
    the caller can demote to a `sometimes(False, …)` rather than
    burning the budget on a polaris/minio outage that's outside the
    property's scope.
    """
    deadline = time.monotonic() + timeout_s
    attempt = 0
    result = None
    while True:
        attempt += 1
        per_attempt = min(_VERIFY_PER_ATTEMPT_TIMEOUT_S, max(15.0, deadline - time.monotonic()))
        result = helper_testdrive.run_inline(
            td_template,
            label=f"{label} attempt={attempt}",
            timeout_s=per_attempt,
        )
        if result.succeeded:
            return IcebergCheckResult(
                matched=True,
                looks_transient=False,
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.exit_code,
            )
        if result.looks_transient:
            return IcebergCheckResult(
                matched=False,
                looks_transient=True,
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.exit_code,
            )
        if overshoot_check is not None and overshoot_check(result.stdout):
            LOG.info(
                "%s attempt %d: actual exceeds expected, short-circuiting retry "
                "(duplicate rows won't disappear)",
                label,
                attempt,
            )
            return IcebergCheckResult(
                matched=False,
                looks_transient=False,
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.exit_code,
            )
        if time.monotonic() + _VERIFY_RETRY_INTERVAL_S >= deadline:
            LOG.info(
                "%s: persistent mismatch after %d attempt(s) in %.0fs budget",
                label,
                attempt,
                timeout_s,
            )
            return IcebergCheckResult(
                matched=False,
                looks_transient=False,
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.exit_code,
            )
        time.sleep(_VERIFY_RETRY_INTERVAL_S)


def verify_iceberg_count(
    batch_id: str,
    expected: int,
    *,
    timeout_s: float = 120.0,
) -> IcebergCheckResult:
    """Assert `SELECT COUNT(*) FROM <iceberg table> WHERE batch_id = bid`
    equals `expected`.

    Runs a testdrive script that opens the S3 secret and feeds
    `duckdb-query` an inline expected-rows block, retrying through
    transient under-count mismatches until `timeout_s` elapses.  See
    `_run_verify_with_retry` for why retry is required even though the
    caller already waited for `messages_committed` to advance.

    `overshoot_check` short-circuits the retry when `actual > expected`
    (duplicate rows — the SS-148 target), so a real correctness signal
    isn't delayed by the catchup budget.
    """
    td = (
        _duckdb_secret_block()
        + "\n"
        + "$ duckdb-query name=ice\n"
        + f"SELECT COUNT(*) FROM {polaris_qualified_table()} "
        + f"WHERE batch_id = '{batch_id}'\n"
        + f"{expected}\n"
    )
    return _run_verify_with_retry(
        td,
        label=f"iceberg/verify-count batch_id={batch_id}",
        timeout_s=timeout_s,
        overshoot_check=_count_actual_overshoots(expected),
    )


def verify_iceberg_rows_match(
    batch_id: str,
    expected: dict[str, str],
    *,
    timeout_s: float = 120.0,
) -> IcebergCheckResult:
    """Assert every (id, value) in `expected` is present in the iceberg
    table under `batch_id`.

    Mirrors the per-row safety check in parallel_driver_pg_cdc.  The
    `.td` we emit lists every expected (id, value) tuple in deterministic
    order and feeds it to `duckdb-query` as the expected-rows block;
    duckdb prints rows sorted by ORDER BY id, so we sort `expected` the
    same way.

    Runs exactly once — by the time this is called, the count check
    has already confirmed catchup, so a per-row mismatch (extra rows,
    missing rows, wrong values) cannot be a catchup race and retrying
    only delays surfacing a real bug.  Transient (fault-injected)
    failures still short-circuit via `looks_transient` so the caller
    can demote to `sometimes(False, …)`.
    """
    rows_sorted = sorted(expected.items())
    expected_block = "\n".join(f"{rid} {val}" for rid, val in rows_sorted)
    td = (
        _duckdb_secret_block()
        + "\n"
        + "$ duckdb-query name=ice\n"
        + f"SELECT id, value FROM {polaris_qualified_table()} "
        + f"WHERE batch_id = '{batch_id}' ORDER BY id\n"
        + expected_block
        + "\n"
    )
    return _run_verify_once(
        td,
        label=f"iceberg/verify-rows batch_id={batch_id} rows={len(expected)}",
        timeout_s=timeout_s,
    )
