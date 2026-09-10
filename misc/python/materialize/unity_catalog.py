# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Helpers for talking to a Databricks Unity Catalog Iceberg REST catalog.

Unity Catalog exposes an Iceberg REST catalog at
``{workspace}/api/2.1/unity-catalog/iceberg-rest`` and authenticates it with an
OAuth2 machine-to-machine token minted from
``{workspace}/oidc/v1/token``. Only *managed* Iceberg tables are writable
through it; foreign Iceberg and Delta tables are read-only.

Unlike BigLake (see ``biglake.py``), Unity Catalog does not let an Iceberg REST
client provision its own namespace: schemas are Unity Catalog objects governed
by Unity Catalog grants, and the client needs ``EXTERNAL USE SCHEMA`` on one
that already exists. So these helpers create and drop *tables* only, and callers
are expected to point at a schema an administrator set up out of band.

Tokens are short-lived (an hour), which matters because the tests using this
module deliberately outlive one. Use `TokenCache` rather than minting once.
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

ICEBERG_REST_PATH = "/api/2.1/unity-catalog/iceberg-rest"
OIDC_TOKEN_PATH = "/oidc/v1/token"

# Databricks' blanket machine-to-machine scope. Matches the SCOPE that an
# ICEBERG CATALOG connection sends; the Iceberg REST default of "catalog" does
# not work against Unity Catalog.
SCOPE = "all-apis"

# Mirrors ICEBERG_ACCESS_DELEGATION_HEADER in
# src/storage-types/src/connections/iceberg_credentials.rs.
DELEGATION_HEADER = "X-Iceberg-Access-Delegation"
DELEGATION_VENDED_CREDENTIALS = "vended-credentials"

# The property the Iceberg Java client uses to report vended-credential expiry,
# and the only one Materialize reads. Kept in sync with
# S3_SESSION_TOKEN_EXPIRES_AT_MS in
# src/storage-types/src/connections/iceberg_credentials.rs.
S3_SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms"
# Spellings seen in Databricks' own documentation. Checked only so a test can
# report which one the server actually used; Materialize itself reads the one
# above and falls back to a fixed interval otherwise.
ALTERNATE_EXPIRY_PROPERTIES = ["expires-at-ms", "expiration-time"]


def iceberg_rest_base(workspace_url: str) -> str:
    return f"{workspace_url.rstrip('/')}{ICEBERG_REST_PATH}"


def oauth2_token_url(workspace_url: str) -> str:
    return f"{workspace_url.rstrip('/')}{OIDC_TOKEN_PATH}"


def mint_token(
    workspace_url: str, client_id: str, client_secret: str, scope: str = SCOPE
) -> tuple[str, int]:
    """Mint an OAuth2 token for a service principal. Returns (token, lifetime_seconds).

    A missing `expires_in` is an error rather than a defaulted value: callers
    size their run against the token lifetime, and guessing it would silently
    produce a test that never reaches the refresh it means to exercise.
    """
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        }
    ).encode()
    req = urllib.request.Request(
        oauth2_token_url(workspace_url),
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            payload = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        # Deliberately not echoing the response body: a failed token exchange
        # can reflect the request back, and the request carries the secret.
        raise RuntimeError(
            f"Databricks OAuth2 token exchange failed: HTTP {e.code}. "
            "Check the client id and secret, and that the service principal "
            "still exists."
        ) from e

    token = payload.get("access_token")
    if not token:
        raise RuntimeError("Databricks OAuth2 response carried no access_token")
    expires_in = payload.get("expires_in")
    if not expires_in:
        raise RuntimeError(
            "Databricks OAuth2 response carried no expires_in; cannot size a "
            "run against an unknown token lifetime"
        )
    return token, int(expires_in)


class TokenCache:
    """Re-mints the catalog token before it expires.

    Materialize refreshes its own token; this is the test harness's copy. It
    exists because a run long enough to exercise Materialize's refresh is by
    construction longer than one Databricks token, so the mint-once pattern in
    `biglake.py` would start returning 401s partway through verification.
    """

    def __init__(
        self,
        workspace_url: str,
        client_id: str,
        client_secret: str,
        margin_s: int = 300,
    ) -> None:
        self.workspace_url = workspace_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.margin_s = margin_s
        self._token: str | None = None
        self._expires_at = 0.0
        self.initial_expires_in: int | None = None

    def token(self) -> str:
        if self._token is None or time.monotonic() + self.margin_s >= self._expires_at:
            token, expires_in = mint_token(
                self.workspace_url, self.client_id, self.client_secret
            )
            if self.initial_expires_in is None:
                self.initial_expires_in = expires_in
            self._token = token
            self._expires_at = time.monotonic() + expires_in
        return self._token


def request(
    method: str,
    url: str,
    token: str,
    *,
    body: dict[str, Any] | None = None,
    delegation: bool = False,
) -> urllib.request.Request:
    req = urllib.request.Request(
        url, method=method, headers={"Authorization": f"Bearer {token}"}
    )
    if delegation:
        req.add_header(DELEGATION_HEADER, DELEGATION_VENDED_CREDENTIALS)
    if body is not None:
        req.add_header("Content-Type", "application/json")
        req.data = json.dumps(body).encode()
    return req


def _get_json(req: urllib.request.Request, what: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{what} failed: HTTP {e.code}\n{detail}") from e


def resolve_warehouse_prefix(token: str, base: str, warehouse: str) -> str:
    """Return the request prefix Unity Catalog assigns to this warehouse.

    Iceberg REST clients call GET /v1/config before anything else, and splice
    the returned prefix in between `/v1/` and every resource path. Unity Catalog
    answers `catalogs/<name>`; catalogs that use no prefix answer nothing. See
    `RestCatalogConfig::url_prefixed` in iceberg-catalog-rest, and
    `table_credentials_url` in
    src/storage-types/src/connections/iceberg_credentials.rs, which reproduces
    this same lookup on the Materialize side.
    """
    url = f"{base}/v1/config?warehouse={urllib.parse.quote(warehouse, safe='')}"
    config = _get_json(
        request("GET", url, token), f"Unity Catalog /v1/config for {warehouse}"
    )
    print(f"Unity Catalog /v1/config response: {json.dumps(config)}")
    # Overrides win over defaults, matching how the catalog client merges them.
    overrides = config.get("overrides", {})
    defaults = config.get("defaults", {})
    return overrides.get("prefix", defaults.get("prefix", ""))


def catalog_url(base: str, prefix: str, suffix: str) -> str:
    middle = f"{prefix}/" if prefix else ""
    return f"{base}/v1/{middle}{suffix}"


def namespace_url(base: str, prefix: str, namespace: str) -> str:
    return catalog_url(
        base, prefix, f"namespaces/{urllib.parse.quote(namespace, safe='')}"
    )


def table_url(base: str, prefix: str, namespace: str, table: str) -> str:
    return f"{namespace_url(base, prefix, namespace)}/tables/{urllib.parse.quote(table, safe='')}"


def namespace_exists(token: str, base: str, prefix: str, namespace: str) -> bool:
    try:
        urllib.request.urlopen(
            request("GET", namespace_url(base, prefix, namespace), token)
        )
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def load_table(
    token: str, base: str, prefix: str, namespace: str, table: str
) -> dict[str, Any]:
    """Fetch a table's metadata.

    `snapshots=all` is explicit because the specification lets a server return
    only the snapshots referenced by refs, and every assertion built on this
    reads whole snapshot summaries.
    """
    url = f"{table_url(base, prefix, namespace, table)}?snapshots=all"
    return _get_json(request("GET", url, token), f"Unity Catalog loadTable for {table}")


def list_tables(token: str, base: str, prefix: str, namespace: str) -> list[str]:
    """Names of every table in the namespace, following pagination."""
    names: list[str] = []
    url = f"{namespace_url(base, prefix, namespace)}/tables"
    while True:
        page = _get_json(
            request("GET", url, token), f"Unity Catalog listTables for {namespace}"
        )
        names.extend(
            identifier["name"]
            for identifier in page.get("identifiers", [])
            if "name" in identifier
        )
        next_token = page.get("next-page-token")
        if not next_token:
            return names
        base_url = f"{namespace_url(base, prefix, namespace)}/tables"
        url = f"{base_url}?pageToken={urllib.parse.quote(next_token, safe='')}"


def drop_table(token: str, base: str, prefix: str, namespace: str, table: str) -> bool:
    """Drop a table through the Iceberg REST catalog.

    Returns True when the table is gone (including when it was already absent),
    and False when the server does not implement dropTable, so callers can fall
    back to the Unity Catalog tables API rather than leak the table. Any other
    failure raises.
    """
    url = table_url(base, prefix, namespace, table)
    try:
        urllib.request.urlopen(request("DELETE", url, token))
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return True
        if e.code in (400, 405, 501):
            detail = e.read().decode("utf-8", errors="replace")
            print(
                f"Iceberg REST dropTable unsupported for {table}: HTTP {e.code}\n{detail}"
            )
            return False
        raise


def drop_table_via_tables_api(
    token: str, workspace_url: str, catalog: str, namespace: str, table: str
) -> None:
    """Drop a table through the Unity Catalog tables API.

    The fallback for servers whose Iceberg REST endpoint does not implement
    dropTable. Uses the same OAuth token and needs no SQL warehouse.
    """
    full_name = urllib.parse.quote(f"{catalog}.{namespace}.{table}", safe="")
    url = f"{workspace_url.rstrip('/')}/api/2.1/unity-catalog/tables/{full_name}"
    try:
        urllib.request.urlopen(request("DELETE", url, token))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Unity Catalog tables API delete failed for {full_name}: "
            f"HTTP {e.code}\n{detail}"
        ) from e


def load_credentials(
    token: str, base: str, prefix: str, namespace: str, table: str
) -> dict[str, Any]:
    """Fetch the vended storage credentials for a table.

    The response carries live credentials, so callers must never print its
    values. `credential_expiry` reports the shape without the secrets.
    """
    url = f"{table_url(base, prefix, namespace, table)}/credentials"
    return _get_json(
        request("GET", url, token, delegation=True),
        f"Unity Catalog loadCredentials for {table}",
    )


def credential_expiry(
    response: dict[str, Any], now_ms: int | None = None
) -> tuple[str | None, int | None]:
    """Which expiry property the catalog reported, and how long it has to live.

    Returns (property_name, seconds_until_expiry), either of which may be None
    when the catalog reports no expiry at all. In that case Materialize falls
    back to VENDED_CREDENTIAL_DEFAULT_TTL rather than scheduling against a
    reported deadline, which changes how long a test must run to observe a
    refresh.

    Picks the longest prefix, matching how `VendedCredentialLoader` chooses
    among several vended credentials.
    """
    credentials = response.get("storage-credentials", [])
    if not credentials:
        return None, None
    chosen = max(credentials, key=lambda c: len(c.get("prefix", "")))
    config = chosen.get("config", {})

    if now_ms is None:
        now_ms = int(time.time() * 1000)
    for prop in [S3_SESSION_TOKEN_EXPIRES_AT_MS, *ALTERNATE_EXPIRY_PROPERTIES]:
        raw = config.get(prop)
        if raw is None:
            continue
        try:
            return prop, max(0, (int(raw) - now_ms) // 1000)
        except (TypeError, ValueError):
            print(f"vended credential property {prop} is not a timestamp: {raw!r}")
    return None, None
