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
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import jwt

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

# Blanket scope used to mint tokens for both GCS and BigLake. Matches GCP_SCOPE
# in src/storage-types/src/connections/gcp.rs.
GCP_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

BIGLAKE_REST_BASE = "https://biglake.googleapis.com/iceberg/v1/restcatalog"

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


def _mint_gcp_access_token(service_account: dict) -> str:
    """Mint an OAuth2 access token from a GCP service-account key (JWT bearer flow)."""
    now = int(time.time())
    assertion = jwt.encode(
        {
            "iss": service_account["client_email"],
            "scope": GCP_SCOPE,
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now,
            "exp": now + 3600,
        },
        service_account["private_key"],
        algorithm="RS256",
    )
    body = urllib.parse.urlencode(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        }
    ).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["access_token"]


def _biglake_request(
    method: str, url: str, token: str, project: str
) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "x-goog-user-project": project,
        },
    )


def _ensure_biglake_catalog(token: str, project: str, bucket: str) -> None:
    """Create the BigLake Iceberg REST catalog for this bucket if it's missing.

    The Iceberg REST `/v1/config?warehouse=gs://<bucket>` lookup resolves the catalog
    whose id equals the bucket name; it does not provision one on demand. Without it,
    every REST call returns a sanitized 403. Creating it here lets the test bootstrap
    its own catalog instead of depending on out-of-band (Pulumi/console) setup.

    Credential mode is END_USER: the Materialize sink writes to GCS with the service
    account's own credentials (see `connect_rest` for the GCP branch in
    mz_storage_types::connections), not credentials vended by the catalog.
    """
    base = f"{BIGLAKE_REST_BASE}/extensions/projects/{project}/catalogs"

    # GET returns the catalog if it exists, 404 if not.
    try:
        urllib.request.urlopen(
            _biglake_request("GET", f"{base}/{bucket}", token, project)
        )
        return
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise

    create_url = f"{base}?iceberg-catalog-id={urllib.parse.quote(bucket, safe='')}"
    req = _biglake_request("POST", create_url, token, project)
    req.add_header("Content-Type", "application/json")
    req.data = json.dumps(
        {
            "catalog-type": "CATALOG_TYPE_GCS_BUCKET",
            "credential-mode": "CREDENTIAL_MODE_END_USER",
        }
    ).encode()
    try:
        urllib.request.urlopen(req)
        print(f"created BigLake catalog for gs://{bucket}")
    except urllib.error.HTTPError as e:
        # A concurrent run can create the catalog between our GET and POST.
        if e.code == 409:
            return
        body = e.read().decode("utf-8", errors="replace")
        print(f"BigLake catalog create failed: HTTP {e.code}\n{body}")
        raise


def _resolve_warehouse_prefix(token: str, project: str, bucket: str) -> str:
    """Return the catalog prefix BigLake assigns to this warehouse.

    Iceberg REST clients call GET /v1/config?warehouse=... before any other
    operation. The catalog's response includes an `overrides.prefix` that the
    client splices in between `/v1/` and resource paths for every later call:

        {uri}/v1/{prefix}/namespaces/{ns}/tables/{tbl}

    See `RestCatalogConfig::url_prefixed` in iceberg-catalog-rest.
    """
    warehouse = f"gs://{bucket}"
    url = (
        f"{BIGLAKE_REST_BASE}/v1/config"
        f"?warehouse={urllib.parse.quote(warehouse, safe='')}"
    )
    with urllib.request.urlopen(_biglake_request("GET", url, token, project)) as resp:
        config = json.loads(resp.read())
    print(f"BigLake /v1/config response: {json.dumps(config)}")
    return config.get("overrides", {}).get("prefix", "")


def _catalog_url(prefix: str, suffix: str) -> str:
    middle = f"{prefix}/" if prefix else ""
    return f"{BIGLAKE_REST_BASE}/v1/{middle}{suffix}"


def _table_url(prefix: str, namespace: str, table: str) -> str:
    ns = urllib.parse.quote(namespace, safe="")
    tbl = urllib.parse.quote(table, safe="")
    return _catalog_url(prefix, f"namespaces/{ns}/tables/{tbl}")


def _namespace_url(prefix: str, namespace: str) -> str:
    ns = urllib.parse.quote(namespace, safe="")
    return _catalog_url(prefix, f"namespaces/{ns}")


def _verify_sink_committed(
    token: str,
    project: str,
    prefix: str,
    namespace: str,
    table: str,
    expected_rows: int,
) -> None:
    """Poll BigLake until the table's current snapshot has expected_rows.

    Iceberg sinks commit asynchronously. Iceberg uses -1 to mean "no snapshot yet";
    we poll until the snapshot lands or we time out.

    Unlike the AWS test, we verify only the row count, not the row values. DuckDB
    is how the AWS test reads the iceberg table back, but DuckDB's iceberg
    extension didn't gain custom-HTTP-header support (needed for BigLake's
    `x-goog-user-project`) until duckdb-iceberg PR #687, which only landed on
    `main` / `v1.5-variegata`. Our workspace duckdb is pinned to 1.4.x, whose
    iceberg extension branch (`v1.4-andium`) does not have the backport.
    """
    url = _table_url(prefix, namespace, table)
    print(f"BigLake GET table URL: {url}")

    deadline = time.time() + 60
    last_state = "no response yet"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                _biglake_request("GET", url, token, project)
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

        # Iceberg summary values are strings per the REST spec.
        total_records = int(snapshot["summary"]["total-records"])
        if total_records != expected_rows:
            last_state = f"snapshot has {total_records} rows, want {expected_rows}"
            time.sleep(2)
            continue

        return

    raise AssertionError(
        f"BigLake table {namespace}.{table} did not converge to {expected_rows} "
        f"rows within timeout; last state: {last_state}"
    )


def _create_biglake_namespace(
    token: str, project: str, prefix: str, namespace: str
) -> None:
    """Create the namespace. BigLake doesn't auto-create on first commit."""
    req = _biglake_request("POST", _catalog_url(prefix, "namespaces"), token, project)
    req.add_header("Content-Type", "application/json")
    req.data = json.dumps({"namespace": [namespace]}).encode()
    urllib.request.urlopen(req)


def _delete_biglake(method_url: str, token: str, project: str) -> None:
    """DELETE a BigLake resource, tolerating 404 so cleanup is idempotent."""
    try:
        urllib.request.urlopen(_biglake_request("DELETE", method_url, token, project))
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
            _biglake_request("GET", page_url, token, project)
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
            _catalog_url(prefix, "namespaces"), token, project, "namespaces"
        )
        if len(ns) == 1
    ]


def _list_biglake_tables(
    token: str, project: str, prefix: str, namespace: str
) -> list[str]:
    return [
        ident["name"]
        for ident in _paginated_get(
            _catalog_url(
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
                _delete_biglake(_table_url(prefix, ns, tbl), token, project)
            _delete_biglake(_namespace_url(prefix, ns), token, project)
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
    # The .td inserts these three rows; verification asserts the snapshot
    # summary matches.
    expected_rows = 3

    materialized = Materialized(
        system_parameter_defaults={"enable_iceberg_sink": "true"},
    )
    with c.override(materialized):
        c.down()
        c.up("materialized")
        c.sql(
            port=6877,
            user="mz_system",
            sql="""
            ALTER SYSTEM SET enable_connection_validation_syntax = true;
            ALTER SYSTEM SET enable_iceberg_sink = true;
            """,
        )

        # Mint once and reuse for verification + cleanup. Tokens last an hour;
        # minting up front also fails fast if the service-account key is broken.
        token = _mint_gcp_access_token(service_account)
        # Bootstrap the catalog if absent; /v1/config 403s otherwise.
        _ensure_biglake_catalog(token, project, bucket)
        # Discover the per-warehouse catalog prefix once; it's identical for
        # the verify and cleanup paths.
        prefix = _resolve_warehouse_prefix(token, project, bucket)
        # Garbage-collect namespaces from previous runs that were killed before
        # their `finally` could run. Best-effort; logged failures don't block.
        _sweep_stale_biglake_namespaces(token, project, prefix)
        # BigLake doesn't auto-create namespaces on first commit, so we have to
        # pre-create (matching how the AWS test pre-creates the S3 Tables namespace).
        _create_biglake_namespace(token, project, prefix, namespace)

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
                _delete_biglake(_table_url(prefix, namespace, table), token, project)
                _delete_biglake(_namespace_url(prefix, namespace), token, project)
            except Exception as cleanup_error:
                # Don't mask the real failure if there was one.
                print(f"warning: BigLake cleanup failed: {cleanup_error}")
