# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Helpers for bootstrapping Google Cloud BigLake Iceberg REST catalogs.

BigLake (the API is still called BigLake, though it now lives under the
"Lakehouse" umbrella) exposes an Iceberg REST catalog at
``https://biglake.googleapis.com/iceberg/v1/restcatalog``.

Unlike some Iceberg catalogs, BigLake does **not** auto-create catalogs or
namespaces on first write. A Materialize Iceberg sink only creates the *table*
(see ``load_or_create_table`` in ``src/storage/src/sink/iceberg.rs``), not the
namespace, so a sink targeting a missing namespace fails at runtime with::

    Failed to create Iceberg table '<t>' in namespace '<ns>':
    Tried to create a table under a namespace that does not exist

These helpers create the catalog and namespace out of band so the sink can then
create its tables. Shared by ``test/gcp`` (per-run, throwaway ``e2e_*``
namespaces) and ``test/canary-environment`` (a fixed, long-lived namespace).
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request

import jwt

# Blanket scope used to mint tokens for both GCS and BigLake. Matches GCP_SCOPE
# in src/storage-types/src/connections/gcp.rs.
GCP_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

BIGLAKE_REST_BASE = "https://biglake.googleapis.com/iceberg/v1/restcatalog"


def mint_gcp_access_token(service_account: dict) -> str:
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


def biglake_request(
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


def ensure_catalog(token: str, project: str, bucket: str) -> None:
    """Create the BigLake Iceberg REST catalog for this bucket if it's missing.

    The Iceberg REST `/v1/config?warehouse=gs://<bucket>` lookup resolves the catalog
    whose id equals the bucket name; it does not provision one on demand. Without it,
    every REST call returns a sanitized 403. Creating it here lets callers bootstrap
    their own catalog instead of depending on out-of-band (Pulumi/console) setup.

    Credential mode is END_USER: the Materialize sink writes to GCS with the service
    account's own credentials (see `connect_rest` for the GCP branch in
    mz_storage_types::connections), not credentials vended by the catalog.
    """
    base = f"{BIGLAKE_REST_BASE}/extensions/projects/{project}/catalogs"

    # GET returns the catalog if it exists, 404 if not.
    try:
        urllib.request.urlopen(
            biglake_request("GET", f"{base}/{bucket}", token, project)
        )
        return
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise

    create_url = f"{base}?iceberg-catalog-id={urllib.parse.quote(bucket, safe='')}"
    req = biglake_request("POST", create_url, token, project)
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


def resolve_warehouse_prefix(token: str, project: str, bucket: str) -> str:
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
    with urllib.request.urlopen(biglake_request("GET", url, token, project)) as resp:
        config = json.loads(resp.read())
    print(f"BigLake /v1/config response: {json.dumps(config)}")
    return config.get("overrides", {}).get("prefix", "")


def catalog_url(prefix: str, suffix: str) -> str:
    middle = f"{prefix}/" if prefix else ""
    return f"{BIGLAKE_REST_BASE}/v1/{middle}{suffix}"


def table_url(prefix: str, namespace: str, table: str) -> str:
    ns = urllib.parse.quote(namespace, safe="")
    tbl = urllib.parse.quote(table, safe="")
    return catalog_url(prefix, f"namespaces/{ns}/tables/{tbl}")


def namespace_url(prefix: str, namespace: str) -> str:
    ns = urllib.parse.quote(namespace, safe="")
    return catalog_url(prefix, f"namespaces/{ns}")


def create_namespace(
    token: str, project: str, prefix: str, namespace: str, *, exist_ok: bool = True
) -> None:
    """Create the namespace. BigLake doesn't auto-create on first commit.

    With ``exist_ok`` (the default), a 409 from a namespace that already exists
    is treated as success, so this is safe to call repeatedly against a fixed
    long-lived namespace.
    """
    req = biglake_request("POST", catalog_url(prefix, "namespaces"), token, project)
    req.add_header("Content-Type", "application/json")
    req.data = json.dumps({"namespace": [namespace]}).encode()
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if exist_ok and e.code == 409:
            return
        raise


def bootstrap_namespace(service_account: dict, bucket: str, namespace: str) -> str:
    """Ensure the catalog and namespace for ``gs://<bucket>`` exist.

    Mints a token from ``service_account``, ensures the BigLake catalog for the
    bucket exists, resolves the warehouse prefix, and creates ``namespace`` if it
    is missing. Idempotent: a no-op once the catalog and namespace exist. Returns
    the resolved catalog prefix.
    """
    project = service_account["project_id"]
    token = mint_gcp_access_token(service_account)
    ensure_catalog(token, project, bucket)
    prefix = resolve_warehouse_prefix(token, project, bucket)
    create_namespace(token, project, prefix, namespace)
    print(f"BigLake namespace ready: {namespace} (gs://{bucket})")
    return prefix
