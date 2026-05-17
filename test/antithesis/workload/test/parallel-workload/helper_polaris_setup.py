#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis first command: provision Polaris + minio for iceberg sinks.

The parallel_workload framework's `CreateIcebergSinkAction` issues
`CREATE SINK … INTO ICEBERG CATALOG CONNECTION polaris_conn
(NAMESPACE 'default_namespace', TABLE …) USING AWS CONNECTION
aws_conn …`.  For the sink to actually land, Polaris needs:

  1. A REST catalog named `default_catalog` configured with
     `s3://test-bucket/` as its `default-base-location` plus minio
     credentials in the catalog's `s3.*` properties.
  2. A namespace named `default_namespace` inside that catalog.
  3. The `test-bucket` minio bucket itself.

Upstream's `setup_polaris_for_iceberg` does all of this from inside an
mzcompose Composition by `c.exec`'ing into an mc helper container and
issuing HTTP calls against Polaris's admin/catalog APIs.  That helper
isn't available inside the Antithesis workload container, so we do the
same work here using the local `mc` binary (installed in the workload
image) plus `requests` for the Polaris HTTP calls.

Antithesis runs `first_*` commands once per execution history, after
`setup_complete` and before any other commands start.  This script
lives only in the `parallel-workload` template, so it runs only when
Antithesis selects that template — kafka / pg-cdc / mysql-cdc
templates don't reach this code path.

Idempotent: re-running against an already-provisioned Polaris is a
no-op (creates return 409, which we treat as success).
"""

from __future__ import annotations

import json
import subprocess
import sys
import time

import helper_logging
import requests

from antithesis.assertions import reachable

LOG = helper_logging.setup_logging("first.polaris_setup")

# Polaris is reachable from the workload container by its compose
# service name.  :8181 is the catalog/management API, :8182 is the
# Quarkus health endpoint.
POLARIS_HOST = "polaris"
POLARIS_API_PORT = 8181
POLARIS_HEALTH_PORT = 8182

# minio is universal across all groups; the workload container reaches
# it by service name on the antithesis-net bridge.
MINIO_HOST = "minio"
MINIO_PORT = 9000

# Names match what `IcebergSink.create()` in upstream
# parallel_workload/database.py references in its CREATE SINK template.
# Changing these requires changing the framework too — not worth the
# divergence; stick with the upstream defaults.
BUCKET_NAME = "test-bucket"
CATALOG_NAME = "default_catalog"
NAMESPACE_NAME = "default_namespace"

# Single-tenant Antithesis sandbox, so the static minio root creds are
# fine here — no need to provision a per-test minio user the way
# upstream's `create_minio_user` does for shared mzcompose CI.
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Polaris bootstrap credentials: `POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,root`
# is set in the Polaris service env, so we authenticate the admin API
# with these exact strings.
POLARIS_CLIENT_ID = "root"
POLARIS_CLIENT_SECRET = "root"

# Polaris's first-start sequence (bootstrap container creates the
# realm, then the API server initialises the database) takes a few
# seconds.  Generous timeout because this only runs once per history.
HEALTH_TIMEOUT_S = 120.0
HEALTH_POLL_INTERVAL_S = 1.0

# `requests`'s default no-timeout would let a deadlocked Polaris
# block this driver forever.  Short timeouts plus retry-via-bake-loop.
REQUEST_TIMEOUT_S = 10.0


def wait_for_polaris() -> None:
    """Poll Polaris's health endpoint until it returns 200 or the budget elapses."""
    deadline = time.monotonic() + HEALTH_TIMEOUT_S
    url = f"http://{POLARIS_HOST}:{POLARIS_HEALTH_PORT}/q/health/live"
    while time.monotonic() < deadline:
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT_S)
            if resp.status_code == 200:
                LOG.info("polaris reports healthy")
                return
        except requests.RequestException as exc:
            LOG.debug("polaris health probe failed: %s", exc)
        time.sleep(HEALTH_POLL_INTERVAL_S)
    raise RuntimeError(
        f"polaris did not report healthy within {HEALTH_TIMEOUT_S}s"
    )


def create_bucket() -> None:
    """Create the iceberg-backing minio bucket via the mc CLI.

    `mc alias set` is idempotent; `mc mb --ignore-existing` no-ops if
    the bucket already exists.  Failures here are real environment
    errors and propagate to abort the first command.
    """
    subprocess.run(
        [
            "mc",
            "alias",
            "set",
            "antithesis",
            f"http://{MINIO_HOST}:{MINIO_PORT}",
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["mc", "mb", "--ignore-existing", f"antithesis/{BUCKET_NAME}"],
        check=True,
        capture_output=True,
    )
    LOG.info("minio bucket %s ready", BUCKET_NAME)


def get_polaris_token() -> str:
    """Acquire an OAuth client-credentials token from Polaris's catalog API."""
    resp = requests.post(
        f"http://{POLARIS_HOST}:{POLARIS_API_PORT}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": POLARIS_CLIENT_ID,
            "client_secret": POLARIS_CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=REQUEST_TIMEOUT_S,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(
            f"polaris oauth response missing access_token: {resp.text}"
        )
    return token


def create_catalog(token: str) -> None:
    """Create the `default_catalog` REST catalog in Polaris.

    409 (already exists) is treated as success so the first command
    stays idempotent across Antithesis re-runs of the same template.
    """
    payload = {
        "name": CATALOG_NAME,
        "type": "INTERNAL",
        "properties": {
            "default-base-location": f"s3://{BUCKET_NAME}/",
            "s3.endpoint": f"http://{MINIO_HOST}:{MINIO_PORT}",
            "s3.path-style-access": "true",
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": "minio",
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [f"s3://{BUCKET_NAME}/*"],
            "endpoint": f"http://{MINIO_HOST}:{MINIO_PORT}",
            "endpointInternal": f"http://{MINIO_HOST}:{MINIO_PORT}",
            "pathStyleAccess": True,
        },
    }
    resp = requests.post(
        f"http://{POLARIS_HOST}:{POLARIS_API_PORT}/api/management/v1/catalogs",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=REQUEST_TIMEOUT_S,
    )
    if resp.status_code == 409:
        LOG.info("polaris catalog %s already exists", CATALOG_NAME)
        return
    resp.raise_for_status()
    LOG.info("polaris catalog %s created", CATALOG_NAME)


def create_namespace(token: str) -> None:
    """Create the `default_namespace` namespace inside `default_catalog`."""
    resp = requests.post(
        f"http://{POLARIS_HOST}:{POLARIS_API_PORT}"
        f"/api/catalog/v1/{CATALOG_NAME}/namespaces",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps({"namespace": [NAMESPACE_NAME]}),
        timeout=REQUEST_TIMEOUT_S,
    )
    if resp.status_code == 409:
        LOG.info("polaris namespace %s already exists", NAMESPACE_NAME)
        return
    resp.raise_for_status()
    LOG.info("polaris namespace %s created", NAMESPACE_NAME)


def main() -> int:
    LOG.info("first_polaris_setup starting")
    wait_for_polaris()
    create_bucket()
    token = get_polaris_token()
    create_catalog(token)
    create_namespace(token)
    reachable(
        "first_polaris_setup: catalog + namespace + bucket ready",
        {
            "catalog": CATALOG_NAME,
            "namespace": NAMESPACE_NAME,
            "bucket": BUCKET_NAME,
        },
    )
    LOG.info("first_polaris_setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
