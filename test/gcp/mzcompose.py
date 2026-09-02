# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests of GCP functionality that run against GCP.

To run these tests locally:

  $ cd test/gcp
  $ ICEBERG_GCS_BUCKET=... \
    ICEBERG_GCP_PROJECT=... \
    ICEBERG_GCP_SA_JSON_B64=... \
    ./mzcompose --dev run default
"""

import base64
import io
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

import fastavro

from materialize.biglake import (
    biglake_request,
    catalog_url,
    create_namespace,
    ensure_catalog,
    mint_gcp_access_token,
    namespace_url,
    resolve_warehouse_prefix,
    table_url,
)
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

# Per-run namespace name format: e2e_<UTC date>_<random hex>. The date lets the
# pre-test sweep age out namespaces left behind by killed runs without extra API
# calls.
NAMESPACE_PREFIX = "e2e"
NAMESPACE_DATE_FORMAT = "%Y%m%d"
NAMESPACE_RE = re.compile(rf"^{NAMESPACE_PREFIX}_(\d{{8}})_[0-9a-f]+$")
# Sweep anything strictly older than this. One day is long enough that any
# in-flight concurrent run (which finishes in minutes) is never targeted.
STALE_NAMESPACE_AGE = timedelta(days=1)

SERVICES = [
    Materialized(),
    Testdrive(),
]


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. This test requires real GCP credentials; "
            "see the docstring at the top of mzcompose.py."
        )
    return value


def _read_gcs_avro(gs_url: str, token: str, project: str) -> list[Any]:
    """Download a ``gs://`` Avro file (Iceberg manifest list or manifest) and parse it.

    We hit the GCS JSON API's media endpoint directly with the same bearer token and
    `x-goog-user-project` header the catalog uses, so no extra GCS client is needed.
    """
    if not gs_url.startswith("gs://"):
        raise ValueError(f"expected a gs:// URL, got {gs_url}")
    bucket, _, obj = gs_url[len("gs://") :].partition("/")
    media_url = (
        f"https://storage.googleapis.com/storage/v1/b/{urllib.parse.quote(bucket, safe='')}"
        f"/o/{urllib.parse.quote(obj, safe='')}?alt=media"
    )
    req = urllib.request.Request(
        media_url,
        headers={
            "Authorization": f"Bearer {token}",
            "x-goog-user-project": project,
        },
    )
    with urllib.request.urlopen(req) as resp:
        return list(fastavro.reader(io.BytesIO(resp.read())))


def _live_row_count(snapshot: dict, token: str, project: str) -> int:
    """Count live rows in a snapshot by reading its manifest list.

    The snapshot `summary.total-records` is unreliable here: the sink's 1s commit
    interval emits a fresh (empty) snapshot every interval, and those empty commits
    report `total-records: 0` even though the data files from the original commit are
    still live. The manifest list always references the live data manifests with their
    true per-file row counts, so summing those is what a reader actually sees in the
    table, regardless of which snapshot first added the data.

    NOTE: this counts live data-file rows (added + existing - deleted). It does not
    subtract equality/position deletes, so it is only exact for insert-only tables.
    The e2e test inserts three rows and never updates them.
    """
    rows = 0
    for manifest in _read_gcs_avro(snapshot["manifest-list"], token, project):
        # content 0 = data manifest, 1 = delete manifest. We only count data rows.
        if manifest.get("content", 0) != 0:
            continue
        rows += manifest.get("added_rows_count") or 0
        rows += manifest.get("existing_rows_count") or 0
        rows -= manifest.get("deleted_rows_count") or 0
    return rows


def _verify_sink_committed(
    token: str,
    project: str,
    prefix: str,
    namespace: str,
    table: str,
    expected_rows: int,
) -> None:
    """Poll BigLake until the table holds expected_rows of live data.

    Iceberg sinks commit asynchronously. Iceberg uses -1 to mean "no snapshot yet";
    we poll until data lands or we time out. Row count comes from the current
    snapshot's manifest list (see `_live_row_count`), not the snapshot summary.

    Unlike the AWS test, we verify only the row count, not the row values. DuckDB
    is how the AWS test reads the iceberg table back, but DuckDB's iceberg
    extension didn't gain custom-HTTP-header support (needed for BigLake's
    `x-goog-user-project`) until duckdb-iceberg PR #687, which only landed on
    `main` / `v1.5-variegata`. Our workspace duckdb is pinned to 1.4.x, whose
    iceberg extension branch (`v1.4-andium`) does not have the backport.
    """
    url = table_url(prefix, namespace, table)
    print(f"BigLake GET table URL: {url}")

    deadline = time.time() + 60
    last_state = "no response yet"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                biglake_request("GET", url, token, project)
            ) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            last_state = f"HTTP {e.code}"
            if e.code != 404:
                raise
            time.sleep(2)
            continue

        metadata = body["metadata"]
        snapshot_id = metadata.get("current-snapshot-id")
        if snapshot_id is None or snapshot_id == -1:
            last_state = "current-snapshot-id is null/-1 (table created, no commit yet)"
            time.sleep(2)
            continue

        snapshot = next(
            (
                s
                for s in metadata.get("snapshots", [])
                if s["snapshot-id"] == snapshot_id
            ),
            None,
        )
        if snapshot is None:
            last_state = f"current-snapshot-id={snapshot_id} but not in snapshots list"
            time.sleep(2)
            continue

        live_rows = _live_row_count(snapshot, token, project)
        if live_rows != expected_rows:
            last_state = f"table has {live_rows} live rows, want {expected_rows}"
            time.sleep(2)
            continue

        return

    raise AssertionError(
        f"BigLake table {namespace}.{table} did not converge to {expected_rows} "
        f"rows within timeout; last state: {last_state}"
    )


def _delete_biglake(method_url: str, token: str, project: str) -> None:
    """DELETE a BigLake resource, tolerating 404 so cleanup is idempotent."""
    try:
        urllib.request.urlopen(biglake_request("DELETE", method_url, token, project))
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def _paginated_get(url: str, token: str, project: str, items_key: str):
    """Yield items from a paginated Iceberg REST list endpoint."""
    page_token = None
    while True:
        page_url = url
        if page_token:
            sep = "&" if "?" in page_url else "?"
            page_url = f"{page_url}{sep}pageToken={urllib.parse.quote(page_token)}"
        with urllib.request.urlopen(
            biglake_request("GET", page_url, token, project)
        ) as resp:
            body = json.loads(resp.read())
        yield from body.get(items_key, [])
        page_token = body.get("next-page-token")
        if not page_token:
            return


def _list_biglake_namespaces(token: str, project: str, prefix: str) -> list[str]:
    """Return single-segment namespace names under this catalog prefix."""
    return [
        ns[0]
        for ns in _paginated_get(
            catalog_url(prefix, "namespaces"), token, project, "namespaces"
        )
        if len(ns) == 1
    ]


def _list_biglake_tables(
    token: str, project: str, prefix: str, namespace: str
) -> list[str]:
    return [
        ident["name"]
        for ident in _paginated_get(
            catalog_url(
                prefix, f"namespaces/{urllib.parse.quote(namespace, safe='')}/tables"
            ),
            token,
            project,
            "identifiers",
        )
    ]


def _sweep_stale_biglake_namespaces(token: str, project: str, prefix: str) -> None:
    """Delete e2e_* namespaces left behind by killed/timed-out previous runs.

    Iceberg sinks normally clean themselves up in mzcompose.py's `finally`, but
    SIGKILL (oomkill, agent reboot) skips that path. We parse the date embedded
    in the namespace name to age out anything older than STALE_NAMESPACE_AGE
    without an extra metadata round-trip per namespace.
    """
    today = datetime.now(timezone.utc).date()
    for ns in _list_biglake_namespaces(token, project, prefix):
        match = NAMESPACE_RE.match(ns)
        if not match:
            continue
        ns_date = datetime.strptime(match.group(1), NAMESPACE_DATE_FORMAT).date()
        age = today - ns_date
        if age <= STALE_NAMESPACE_AGE:
            continue
        print(f"sweeping stale BigLake namespace: {ns} (age {age})")
        try:
            for tbl in _list_biglake_tables(token, project, prefix, ns):
                _delete_biglake(table_url(prefix, ns, tbl), token, project)
            _delete_biglake(namespace_url(prefix, ns), token, project)
        except Exception as e:
            # Keep sweeping; a single bad namespace shouldn't block the test.
            print(f"warning: failed to sweep namespace {ns}: {e}")


def workflow_default(c: Composition) -> None:
    bucket = _require_env("ICEBERG_GCS_BUCKET")
    project = _require_env("ICEBERG_GCP_PROJECT")
    sa_json_b64 = _require_env("ICEBERG_GCP_SA_JSON_B64")

    service_account = json.loads(base64.b64decode(sa_json_b64))

    # Per-run namespace so concurrent / repeated runs against the shared
    # bucket don't collide on table state. The embedded date lets the pre-test
    # sweep age out namespaces left behind by killed runs.
    seed = random.getrandbits(32)
    today = datetime.now(timezone.utc).strftime(NAMESPACE_DATE_FORMAT)
    namespace = f"{NAMESPACE_PREFIX}_{today}_{seed:08x}"
    table = "demo_table"
    # The .td inserts these three rows; verification asserts the table's live
    # row count matches.
    expected_rows = 3

    materialized = Materialized()
    with c.override(materialized):
        c.down()
        c.up("materialized")
        c.sql(
            port=6877,
            user="mz_system",
            sql="""
            ALTER SYSTEM SET enable_connection_validation_syntax = true;
            """,
        )

        # Mint once and reuse for verification + cleanup. Tokens last an hour;
        # minting up front also fails fast if the service-account key is broken.
        token = mint_gcp_access_token(service_account)
        # Bootstrap the catalog if absent; /v1/config 403s otherwise.
        ensure_catalog(token, project, bucket)
        # Discover the per-warehouse catalog prefix once; it's identical for
        # the verify and cleanup paths.
        prefix = resolve_warehouse_prefix(token, project, bucket)
        # Garbage-collect namespaces from previous runs that were killed before
        # their `finally` could run. Best-effort; logged failures don't block.
        _sweep_stale_biglake_namespaces(token, project, prefix)
        # BigLake doesn't auto-create namespaces on first commit, so we have to
        # pre-create (matching how the AWS test pre-creates the S3 Tables namespace).
        create_namespace(token, project, prefix, namespace)

        try:
            c.run_testdrive_files(
                "--no-reset",
                f"--var=gcp-sa-json-b64={sa_json_b64}",
                f"--var=gcs-bucket={bucket}",
                f"--var=namespace={namespace}",
                f"--var=table={table}",
                "gcp-iceberg-e2e.td",
            )

            try:
                _verify_sink_committed(
                    token, project, prefix, namespace, table, expected_rows
                )
            except Exception:
                # Dump Materialize logs to make sink errors visible.
                logs = c.invoke("logs", "materialized", capture=True)
                print("--- materialized logs (tail) ---")
                print("\n".join(logs.stdout.splitlines()[-200:]))
                print("--- end materialized logs ---")
                raise

            c.sql("DROP SINK demo;")
        finally:
            try:
                _delete_biglake(table_url(prefix, namespace, table), token, project)
                _delete_biglake(namespace_url(prefix, namespace), token, project)
            except Exception as cleanup_error:
                # Don't mask the real failure if there was one.
                print(f"warning: BigLake cleanup failed: {cleanup_error}")
