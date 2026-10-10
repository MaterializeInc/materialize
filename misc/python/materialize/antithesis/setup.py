# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Workload pod entrypoint: bring up the environment, then signal readiness.

Creates the Materialize CR once orchestratord has registered its CRD, waits for
the first generation to serve SQL, writes the state epoch sentinel that
`state.open_db` checks, and emits `setup_complete`. Test commands only start
after that signal, so they can assume a serving environment.
"""

from __future__ import annotations

import base64
import os
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import boto3
import botocore.exceptions
import psycopg
import yaml
from antithesis.lifecycle import (  # pyright: ignore[reportMissingModuleSource]
    setup_complete,
)
from kubernetes import client  # type: ignore

from materialize import orchestratord
from materialize.antithesis import sql, state
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

CRD_TIMEOUT = 20 * 60
ROLLOUT_TIMEOUT = 40 * 60
SQL_TIMEOUT = 20 * 60


def log(message: str) -> None:
    print(f"setup: {message}", flush=True)


def ensure_blob_bucket(env: Environment) -> bool:
    """Create the persist bucket named by the backend Secret, if missing.

    MinIO starts with no buckets, and environmentd retries opening blob
    storage forever rather than failing, so this must run before the CR.
    Returns whether the bucket exists; S3 errors (MinIO still starting) are
    reported as `False` so the caller retries.
    """
    secret = client.CoreV1Api().read_namespaced_secret(
        "materialize-backend", env.endpoints.namespace
    )
    assert secret.data is not None
    url = urlparse(base64.b64decode(secret.data["persist_backend_url"]).decode())
    query = parse_qs(url.query)
    s3 = boto3.client(
        "s3",
        endpoint_url=query["endpoint"][0],
        region_name=query.get("region", ["us-east-1"])[0],
        aws_access_key_id=unquote(url.username or ""),
        aws_secret_access_key=unquote(url.password or ""),
    )
    bucket = url.hostname
    assert bucket is not None
    try:
        try:
            s3.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError:
            s3.create_bucket(Bucket=bucket)
            log(f"created persist bucket {bucket}")
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
        log(f"waiting for blob storage: {e}")
        return False
    return True


def create_materialize(env: Environment) -> None:
    if env.materialize.get() is not None:
        log("Materialize CR already exists")
        return
    configmap = client.CoreV1Api().read_namespaced_config_map(
        "materialize-cr", env.endpoints.namespace
    )
    assert configmap.data is not None
    body = yaml.safe_load(configmap.data["materialize.yaml"])
    env.materialize.create(body)
    log(f"created Materialize CR {env.endpoints.environment}")


def write_state_epoch(state_dir: Path) -> None:
    """Write the state epoch sentinel (see `state`) unless one exists.

    A restarted setup keeps the existing epoch, so state that survived the
    restart stays valid.
    """
    if state.read_epoch(state_dir) is not None:
        log("state epoch already present, reusing workload state")
        return
    state_dir.mkdir(parents=True, exist_ok=True)
    tmp = state_dir / f"{state.EPOCH_FILE}.tmp"
    tmp.write_text(f"{rng.getrandbits(64):016x}")
    os.replace(tmp, state_dir / state.EPOCH_FILE)


def wait_for_sql(env: Environment) -> None:
    deadline = time.monotonic() + SQL_TIMEOUT
    while True:
        try:
            with sql.connection(env.sql_host(), connect_timeout=5) as conn:
                conn.execute("SELECT 1")
                return
        except (psycopg.Error, OSError, RuntimeError) as e:
            if time.monotonic() >= deadline:
                raise
            log(f"waiting for SQL: {e}")
            time.sleep(5)


def main() -> int:
    env = Environment()
    log("waiting for the Materialize CRD")
    orchestratord.wait_until(
        orchestratord.crd_established, CRD_TIMEOUT, description="Materialize CRD"
    )
    orchestratord.wait_until(
        lambda: ensure_blob_bucket(env),
        CRD_TIMEOUT,
        description="persist bucket",
    )
    orchestratord.wait_until(
        lambda: (create_materialize(env), True)[1],
        CRD_TIMEOUT,
        description="Materialize CR creation",
    )
    log("waiting for the first generation to be applied")
    orchestratord.wait_until(
        env.materialize.is_up_to_date,
        ROLLOUT_TIMEOUT,
        interval=5,
        description="first rollout",
    )
    wait_for_sql(env)
    write_state_epoch(env.endpoints.state_dir)
    marker = env.endpoints.state_dir / state.SETUP_MARKER
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(env.sql_host())
    setup_complete({"environment": env.endpoints.environment})
    log("setup complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
