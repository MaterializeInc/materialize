# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Iceberg sink against a real Databricks Unity Catalog.

Everything else that exercises Iceberg vended credentials does so against
Polaris and MinIO on a private Docker network, where credentials never expire
during the run. The point of this test is the two things that setup cannot
reach: that Unity Catalog accepts our commits at all, and that the sink keeps
committing after its credentials age out.

Two clocks have to be crossed, and they are far apart:

  * The vended storage credentials, refreshed either 900s before a reported
    expiry or every 300s when the catalog reports none. See
    VENDED_CREDENTIAL_REFRESH_BUFFER / VENDED_CREDENTIAL_DEFAULT_TTL in
    src/storage-types/src/connections/iceberg_credentials.rs.
  * The catalog OAuth token, which Databricks issues with a 3600s lifetime and
    which iceberg-rust re-mints 600s early (REFRESH_MARGIN in
    crates/catalog/rest/src/token.rs). The clock starts when the *sink dataflow
    renders*, not when the connection is created, so the first re-mint lands
    around 50 minutes in.

So the run is long by construction. Rather than sleeping through it, the
workflow inserts a batch and checks the catalog every PHASE_INTERVAL_S, which
means a failure reports which phase broke instead of just that the end state
was wrong.

Prerequisites on the Databricks side, none of which this test creates:

  * A service principal with an OAuth secret.
  * External data access enabled on the metastore.
  * A catalog (ICEBERG_DATABRICKS_CATALOG) and a schema
    (ICEBERG_DATABRICKS_NAMESPACE, default `nightly`) that already exist.
    Unity Catalog governs schemas itself and is not documented to create them
    over Iceberg REST, so the workflow checks for the schema and fails with
    instructions rather than trying to make one.
  * Grants to the service principal on that schema: EXTERNAL USE SCHEMA,
    CREATE TABLE, MODIFY, SELECT.

To run this locally:

  $ cd test/databricks
  $ ICEBERG_DATABRICKS_WORKSPACE_URL=https://dbc-xxxx.cloud.databricks.com \
    ICEBERG_DATABRICKS_CLIENT_ID=... \
    ICEBERG_DATABRICKS_CLIENT_SECRET=... \
    ICEBERG_DATABRICKS_CATALOG=iceberg-ci \
    ./mzcompose --dev run default

With no credentials set it skips, so the composition stays runnable on a
machine with no Databricks account. Under Buildkite it fails instead: a nightly
that silently skips is how a broken credential rotation goes unnoticed.
"""

import json
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from materialize import buildkite
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError
from materialize.unity_catalog import (
    TokenCache,
    credential_expiry,
    drop_table,
    drop_table_via_tables_api,
    iceberg_rest_base,
    list_tables,
    load_credentials,
    load_table,
    namespace_exists,
    resolve_warehouse_prefix,
)

SERVICES = [
    Materialized(
        # The sink's own debug output is the only in-product record that commits
        # kept landing across a credential boundary, and that the vended
        # credentials were re-fetched. _assert_refresh_evidence reads it.
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::sink::iceberg=debug,"
            "mz_storage_types::connections=debug,info",
        },
        # The workflow drops the sink and then deletes the Unity Catalog table.
        # A sanity restart would re-render the sink against a table that is
        # already gone.
        sanity_restart=False,
    ),
    Testdrive(default_timeout="60s"),
]

# Per-run table name: mz_e2e_<UTC date>_<random hex>. The date lets the pre-test
# sweep age out tables left behind by killed runs without a metadata call each.
# Lowercase, digits and underscore only, so it needs no quoting in either the
# Iceberg REST path or the Unity Catalog tables API.
TABLE_PREFIX = "mz_e2e"
TABLE_DATE_FORMAT = "%Y%m%d"
TABLE_RE = re.compile(rf"^{TABLE_PREFIX}_(\d{{8}})_[0-9a-f]{{8}}$")
# One day is long enough that a concurrent run, which finishes in minutes, is
# never targeted.
STALE_TABLE_AGE = timedelta(days=1)

COMMIT_INTERVAL_S = 30
ROWS_PER_PHASE = 1000
PHASE_INTERVAL_S = 300

# These mirror product constants. If the product's margins grow past these, the
# run stops crossing the boundary it means to cross while still passing, so they
# are asserted against the sizing logic rather than trusted silently.
#   REFRESH_MARGIN, iceberg-rust crates/catalog/rest/src/token.rs
OAUTH_REFRESH_MARGIN_S = 600
#   VENDED_CREDENTIAL_REFRESH_BUFFER, iceberg_credentials.rs
VENDED_REFRESH_BUFFER_S = 900
#   VENDED_CREDENTIAL_DEFAULT_TTL, iceberg_credentials.rs
VENDED_DEFAULT_TTL_S = 300

# Commits must keep landing *after* the boundary, not merely up to it.
PHASES_AFTER_REFRESH = 2
# Keeps the workflow inside its Buildkite timeout with room for teardown. If the
# step timeout fires the container is killed and the `finally` never runs, so
# this budget is the primary defense against leaking a table.
MAX_RUN_S = 75 * 60

ENV_VARS = [
    "ICEBERG_DATABRICKS_WORKSPACE_URL",
    "ICEBERG_DATABRICKS_CLIENT_ID",
    "ICEBERG_DATABRICKS_CLIENT_SECRET",
    "ICEBERG_DATABRICKS_CATALOG",
]

SINK_NAME = "databricks_sink"


@dataclass
class Creds:
    workspace_url: str
    client_id: str
    client_secret: str
    catalog: str
    namespace: str

    @property
    def credential(self) -> str:
        """The CREDENTIAL an ICEBERG CATALOG connection wants.

        Materialize splits this on the first colon into client id and secret;
        see the OAuth branch of `connect_rest` in
        src/storage-types/src/connections.rs.
        """
        return f"{self.client_id}:{self.client_secret}"


@dataclass
class Progress:
    """What one loadTable call says about the sink's progress."""

    rows: int
    snapshot_id: Any
    last_updated_ms: int
    frontier: int | None
    snapshot_count: int

    def __str__(self) -> str:
        return (
            f"rows={self.rows} snapshot={self.snapshot_id} "
            f"last_updated_ms={self.last_updated_ms} frontier={self.frontier} "
            f"snapshots={self.snapshot_count}"
        )


def _credentials() -> Creds | None:
    """Read the Databricks credentials, or return None to skip.

    Absent credentials are a skip locally and a failure under Buildkite.
    """
    import os

    missing = [name for name in ENV_VARS if not os.environ.get(name)]
    if missing:
        detail = ", ".join(missing)
        if buildkite.is_in_buildkite():
            raise UIError(
                f"{detail} not set. This test requires real Databricks "
                "credentials; see the docstring at the top of mzcompose.py."
            )
        print(
            f"+++ Skipping: {detail} not set, so there is no Databricks "
            "workspace to test against."
        )
        return None

    return Creds(
        workspace_url=os.environ["ICEBERG_DATABRICKS_WORKSPACE_URL"],
        client_id=os.environ["ICEBERG_DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["ICEBERG_DATABRICKS_CLIENT_SECRET"],
        catalog=os.environ["ICEBERG_DATABRICKS_CATALOG"],
        namespace=os.environ.get("ICEBERG_DATABRICKS_NAMESPACE", "nightly"),
    )


def _sink_snapshots(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """The snapshots this sink wrote, oldest first, along the current ancestry.

    Filtering matters for two reasons. Unity Catalog runs its own maintenance,
    and a compaction snapshot would inflate any row total. And a snapshot off
    the current ancestry was orphaned by a conflict resolution, so its rows are
    not in the table. The sink stamps `mz-sink-id` into every summary it writes
    (see src/storage/src/sink/iceberg.rs), which is what makes ours identifiable.
    """
    snapshots = metadata.get("snapshots", [])
    by_id = {s["snapshot-id"]: s for s in snapshots if "snapshot-id" in s}

    ancestry = []
    current = metadata.get("current-snapshot-id")
    while current is not None and current in by_id:
        snapshot = by_id[current]
        ancestry.append(snapshot)
        current = snapshot.get("parent-snapshot-id")
    ancestry.reverse()

    return [
        s
        for s in ancestry
        if s.get("summary", {}).get("operation") == "append"
        and "mz-sink-id" in s.get("summary", {})
    ]


def _appended_rows(metadata: dict[str, Any]) -> int:
    """Rows this sink appended, from snapshot summaries alone.

    Sums `added-records`, which a zero-file commit omits entirely, so the many
    empty snapshots a 30s commit interval produces contribute nothing. This is
    deliberately not `total-records`: that field only carries forward when the
    previous summary had it, which is what made an earlier version of the GCP
    test flaky.

    Counts rows *appended*, not rows *live*, so it is exact only for MODE APPEND
    with no deletes or rewrites. `_assert_no_rewrites` enforces that premise.
    """
    total = 0
    for snapshot in _sink_snapshots(metadata):
        raw = snapshot.get("summary", {}).get("added-records")
        if raw is not None:
            total += int(raw)
    return total


def _committed_frontier(metadata: dict[str, Any]) -> int | None:
    """The sink's committed frontier, from the newest snapshot carrying one.

    Advances on every commit including empty ones, which makes it a sharper
    liveness signal than a row count that only moves when data arrives.
    """
    for snapshot in reversed(_sink_snapshots(metadata)):
        raw = snapshot.get("summary", {}).get("mz-frontier")
        if raw is None:
            continue
        try:
            elements = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if elements:
            return int(elements[0])
    return None


def _assert_no_rewrites(metadata: dict[str, Any]) -> None:
    """Fail loudly if anything removed data underneath us.

    `_appended_rows` is only a valid row count while nothing deletes or rewrites
    files. If Unity Catalog maintenance ever does, this says so rather than
    letting the count drift.
    """
    for snapshot in _sink_snapshots(metadata):
        summary = snapshot.get("summary", {})
        for key in ("deleted-records", "deleted-data-files", "removed-delete-files"):
            value = summary.get(key)
            if value is not None and int(value) > 0:
                raise AssertionError(
                    f"snapshot {snapshot.get('snapshot-id')} reports {key}={value}; "
                    "appended-row accounting assumes an append-only table"
                )


def _progress(
    tokens: TokenCache, base: str, prefix: str, creds: Creds, table: str
) -> Progress:
    metadata = load_table(tokens.token(), base, prefix, creds.namespace, table)[
        "metadata"
    ]
    _assert_no_rewrites(metadata)
    sink_snapshots = _sink_snapshots(metadata)
    return Progress(
        rows=_appended_rows(metadata),
        snapshot_id=metadata.get("current-snapshot-id"),
        last_updated_ms=int(metadata.get("last-updated-ms", 0)),
        frontier=_committed_frontier(metadata),
        snapshot_count=len(sink_snapshots),
    )


def _await_rows(
    tokens: TokenCache,
    base: str,
    prefix: str,
    creds: Creds,
    table: str,
    expected: int,
    timeout_s: int = 180,
) -> Progress:
    """Poll the catalog until the sink has appended `expected` rows."""
    deadline = time.time() + timeout_s
    last = "no successful loadTable"
    while time.time() < deadline:
        try:
            progress = _progress(tokens, base, prefix, creds, table)
        except RuntimeError as e:
            # A table the sink has not created yet 404s; keep polling.
            last = str(e).splitlines()[0]
            time.sleep(5)
            continue
        last = str(progress)
        if progress.rows >= expected:
            return progress
        time.sleep(5)

    raise AssertionError(
        f"Unity Catalog table {creds.namespace}.{table} did not reach "
        f"{expected} appended rows within {timeout_s}s; last state: {last}"
    )


def _assert_sink_healthy(c: Composition, expected_rows: int) -> None:
    """Check the sink's whole history, not just its current state.

    A point-in-time check misses a stall that recovered, which is exactly the
    shape a failed credential refresh would take.
    """
    errors = c.sql_query(f"""
        SELECT count(*)
        FROM mz_internal.mz_sink_status_history h
        JOIN mz_sinks s ON s.id = h.sink_id
        WHERE s.name = '{SINK_NAME}'
          AND (h.error IS NOT NULL OR h.status = 'stalled')
        """)[0][0]
    assert errors == 0, f"sink reported {errors} error/stalled status transitions"

    # A restart would rebuild the connection's OAuth2TokenProvider and reset the
    # token clock, so a sink that flapped could survive the whole run without
    # ever refreshing anything. That would make the test pass while proving
    # nothing, so it is a failure rather than a warning.
    starts = c.sql_query(f"""
        SELECT count(*)
        FROM mz_internal.mz_sink_status_history h
        JOIN mz_sinks s ON s.id = h.sink_id
        WHERE s.name = '{SINK_NAME}' AND h.status = 'starting'
        """)[0][0]
    assert starts == 1, (
        f"sink started {starts} times; a restart resets the credential clock, so "
        "this run did not necessarily cross a refresh boundary"
    )

    committed = c.sql_query(f"""
        SELECT messages_committed
        FROM mz_internal.mz_sink_statistics st
        JOIN mz_sinks s ON s.id = st.id
        WHERE s.name = '{SINK_NAME}'
        """)[0][0]
    assert (
        committed >= expected_rows
    ), f"sink committed {committed} messages, expected at least {expected_rows}"


def _assert_refresh_evidence(c: Composition, min_commits: int) -> None:
    """Look for positive evidence that credentials were re-fetched.

    Commit continuity past the boundary is the load-bearing part: every commit
    goes through the catalog client's token(), so a commit after the refresh
    margin means the re-mint branch ran or the request would have been rejected.
    The vended-credential line is the direct signal for the storage half.
    """
    logs = c.invoke("logs", "materialized", capture=True).stdout

    for fatal in [
        "ExpiredToken",
        "InvalidAccessKeyId",
        "failed to obtain a catalog token",
        "vended no storage credentials",
    ]:
        assert fatal not in logs, f"materialized logs contain {fatal!r}"

    commits = logs.count("iceberg commit applied batch")
    assert (
        commits >= min_commits
    ), f"only {commits} iceberg commits in the logs, expected at least {min_commits}"

    vended = logs.count("fetched vended Iceberg storage credentials")
    assert vended >= 2, (
        f"only {vended} vended-credential fetches in the logs; the sink should "
        "have re-fetched at least once after the initial fetch"
    )
    print(f"log evidence: {commits} commits, {vended} vended-credential fetches")


def _sweep_stale_tables(
    tokens: TokenCache, base: str, prefix: str, creds: Creds
) -> None:
    """Drop mz_e2e_* tables left behind by runs that were killed.

    The `finally` below cleans up a normal run, but SIGKILL (oomkill, agent
    reboot, step timeout) skips it. The date embedded in the name ages out
    leftovers without a metadata call per table.
    """
    today = datetime.now(timezone.utc).date()
    try:
        tables = list_tables(tokens.token(), base, prefix, creds.namespace)
    except RuntimeError as e:
        print(f"warning: could not list tables to sweep: {e}")
        return

    for table in tables:
        match = TABLE_RE.match(table)
        if not match:
            continue
        table_date = datetime.strptime(match.group(1), TABLE_DATE_FORMAT).date()
        age = today - table_date
        if age <= STALE_TABLE_AGE:
            continue
        print(f"sweeping stale Unity Catalog table: {table} (age {age})")
        try:
            _drop_table(tokens, base, prefix, creds, table)
        except Exception as e:
            # Keep sweeping; one undroppable table should not block the test.
            print(f"warning: could not sweep {table}: {e}")


def _drop_table(
    tokens: TokenCache, base: str, prefix: str, creds: Creds, table: str
) -> None:
    """Drop a table, falling back to the Unity Catalog tables API.

    Unity Catalog is not documented to implement the Iceberg REST dropTable. If
    it does not, the fallback is what keeps a nightly from leaking one table a
    night.
    """
    if not drop_table(tokens.token(), base, prefix, creds.namespace, table):
        drop_table_via_tables_api(
            tokens.token(), creds.workspace_url, creds.catalog, creds.namespace, table
        )
    print(f"dropped Unity Catalog table {creds.namespace}.{table}")


def _required_run_seconds(oauth_expires_in: int, vended_ttl: int | None) -> int:
    """How long the sink must run to cross both refresh boundaries.

    Derived from the token lifetime the workspace actually reported rather than
    hardcoded, so a Databricks-side change to token lifetimes surfaces as a
    failure here instead of quietly shortening the run below the boundary.
    """
    oauth_at = oauth_expires_in - OAUTH_REFRESH_MARGIN_S
    if vended_ttl is None:
        # No reported expiry, so the loader falls back to a fixed interval.
        vended_at = VENDED_DEFAULT_TTL_S
    else:
        vended_at = max(0, vended_ttl - VENDED_REFRESH_BUFFER_S)

    required = max(oauth_at, vended_at) + PHASES_AFTER_REFRESH * PHASE_INTERVAL_S
    if required > MAX_RUN_S:
        raise AssertionError(
            f"crossing the refresh boundaries needs {required}s "
            f"(oauth at {oauth_at}s, vended at {vended_at}s) but the run is "
            f"capped at {MAX_RUN_S}s. Either the token lifetime or a product "
            "refresh margin changed; re-check OAUTH_REFRESH_MARGIN_S and "
            "MAX_RUN_S rather than letting the run pass without refreshing."
        )
    return required


def workflow_default(c: Composition) -> None:
    creds = _credentials()
    if creds is None:
        return

    tokens = TokenCache(creds.workspace_url, creds.client_id, creds.client_secret)
    base = iceberg_rest_base(creds.workspace_url)

    # Fail fast on credentials and grants, so a bad secret costs ten seconds
    # rather than surfacing an hour into the run.
    prefix = resolve_warehouse_prefix(tokens.token(), base, creds.catalog)
    if not namespace_exists(tokens.token(), base, prefix, creds.namespace):
        raise UIError(
            f"Unity Catalog schema '{creds.catalog}.{creds.namespace}' does not "
            "exist, or the service principal cannot see it. Unity Catalog "
            "governs schemas itself, so this test does not create one; see the "
            "prerequisites in the docstring at the top of mzcompose.py."
        )

    _sweep_stale_tables(tokens, base, prefix, creds)

    table = (
        f"{TABLE_PREFIX}"
        f"_{datetime.now(timezone.utc).strftime(TABLE_DATE_FORMAT)}"
        f"_{random.getrandbits(32):08x}"
    )
    print(f"--- Sinking into Unity Catalog table {creds.namespace}.{table}")

    c.down(destroy_volumes=True)
    c.up("materialized")

    try:
        c.run_testdrive_files(
            "--no-reset",
            f"--var=databricks-credential={creds.credential}",
            f"--var=workspace-url={creds.workspace_url}",
            f"--var=catalog={creds.catalog}",
            f"--var=namespace={creds.namespace}",
            f"--var=table={table}",
            f"--var=rows={ROWS_PER_PHASE}",
            f"--var=commit-interval={COMMIT_INTERVAL_S}s",
            "databricks-iceberg-setup.td",
        )
        sink_started = time.monotonic()

        progress = _await_rows(
            tokens, base, prefix, creds, table, ROWS_PER_PHASE, timeout_s=300
        )
        print(f"phase 0 landed: {progress}")

        # Probe what the catalog actually reports, which decides how long the
        # run has to be. Only key names are printed; the values are live
        # credentials.
        expiry_prop, vended_ttl = credential_expiry(
            load_credentials(tokens.token(), base, prefix, creds.namespace, table)
        )
        if expiry_prop is None:
            print(
                "Unity Catalog reported no vended-credential expiry, so the "
                f"loader refreshes every {VENDED_DEFAULT_TTL_S}s"
            )
        else:
            print(
                f"Unity Catalog reports vended-credential expiry via "
                f"{expiry_prop}, {vended_ttl}s from now"
            )

        assert tokens.initial_expires_in is not None
        run_for = _required_run_seconds(tokens.initial_expires_in, vended_ttl)
        print(
            f"--- Running the sink for {run_for}s to cross both refresh "
            f"boundaries (OAuth token lifetime {tokens.initial_expires_in}s, "
            f"refreshed {OAUTH_REFRESH_MARGIN_S}s early)"
        )

        phase = 1
        while time.monotonic() - sink_started < run_for:
            next_phase_at = sink_started + phase * PHASE_INTERVAL_S
            delay = next_phase_at - time.monotonic()
            if delay > 0:
                time.sleep(delay)

            lo = phase * ROWS_PER_PHASE + 1
            hi = (phase + 1) * ROWS_PER_PHASE
            c.run_testdrive_files(
                "--no-reset",
                f"--var=lo={lo}",
                f"--var=hi={hi}",
                "databricks-iceberg-append.td",
            )

            previous, progress = progress, _await_rows(
                tokens, base, prefix, creds, table, hi
            )

            assert (
                progress.snapshot_id != previous.snapshot_id
            ), f"no new snapshot in phase {phase}: {progress}"
            assert progress.last_updated_ms > previous.last_updated_ms, (
                f"table metadata did not advance in phase {phase}: "
                f"{progress.last_updated_ms} <= {previous.last_updated_ms}"
            )
            if previous.frontier is not None and progress.frontier is not None:
                assert progress.frontier > previous.frontier, (
                    f"sink frontier did not advance in phase {phase}: "
                    f"{progress.frontier} <= {previous.frontier}"
                )

            elapsed = int(time.monotonic() - sink_started)
            print(
                f"phase {phase} at {elapsed}s of {run_for}s: {progress} "
                f"(OAuth refresh due at "
                f"{tokens.initial_expires_in - OAUTH_REFRESH_MARGIN_S}s)"
            )
            phase += 1

        total_rows = phase * ROWS_PER_PHASE
        _assert_sink_healthy(c, total_rows)
        # Half of the theoretical commit count, since commits coalesce and the
        # first interval is partly setup.
        _assert_refresh_evidence(c, min_commits=int(run_for / COMMIT_INTERVAL_S / 2))

        # Drop the sink before the table, so it cannot observe a table that has
        # been deleted from under it.
        c.sql(f"DROP SINK {SINK_NAME};")
    except Exception:
        # The sink's own logs are where a credential failure shows up.
        logs = c.invoke("logs", "materialized", capture=True)
        print("--- materialized logs (tail)")
        print("\n".join(logs.stdout.splitlines()[-200:]))
        raise
    finally:
        try:
            _drop_table(tokens, base, prefix, creds, table)
        except Exception as cleanup_error:
            # Don't mask the real failure if there was one.
            print(f"warning: Unity Catalog cleanup failed: {cleanup_error}")
