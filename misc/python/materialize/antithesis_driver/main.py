# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Startup: bring the system up, report setup complete, run the state machine.

Every timeline branches from the snapshot Antithesis takes during the wait for
`/workload-active`, which the `first_start_workload` command creates. So
nothing before that wait draws randomness: setup is the same in every
timeline, and everything that varies is drawn inside the state machine.

The waits retry without deadlines of their own. `--setup-timeout` bounds them
in Antithesis, and the local loop's caller bounds them locally.
"""

from __future__ import annotations

import base64
import os
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    reachable,
)
from antithesis.lifecycle import (  # pyright: ignore[reportMissingModuleSource]
    setup_complete,
)

from materialize.antithesis.environment import Environment
from materialize.antithesis_driver.client import Target, connect
from materialize.antithesis_driver.context import Config, log

WORKLOAD_ACTIVE = Path(
    os.environ.get("MZ_ANTITHESIS_WORKLOAD_ACTIVE", "/workload-active")
)
DERIVED_CLUSTER = "driver"
SOURCE_CLUSTER = "sources"
"""Holds the sources and sinks. The disruptions leave its replica count alone."""
STANDALONE_SYSTEM_PARAMETERS = {
    # Strong session serializable is the only isolation level with
    # per-session guarantees. The Kubernetes config sets these in the
    # `system-params` ConfigMap, which environmentd syncs and which would
    # override `ALTER SYSTEM`.
    "enable_session_timelines": "true",
    "max_connections": "1000",
    "allow_real_time_recency": "true",
}


def retry(name: str, step: Callable[[], None]) -> None:
    """Run `step` until it succeeds, logging each new reason it fails."""
    last = None
    while True:
        try:
            step()
            return
        except Exception as e:
            # Setup has no deadline of its own: the upstreams and the broker
            # can fail in any way while they start.
            reason = f"{type(e).__name__}: {str(e).strip().splitlines()[0] if str(e).strip() else ''}"
            if reason != last:
                log(f"waiting for {name}: {reason}")
                last = reason
            time.sleep(2)


def ensure_blob_bucket(env: Environment) -> bool:
    """Create the persist bucket named by the backend Secret, if missing.

    MinIO starts with no buckets, and environmentd retries opening blob
    storage forever rather than failing, so this must run before the CR.
    Returns whether the bucket exists. S3 errors (MinIO still starting) are
    reported as `False` so the caller retries.
    """
    import boto3
    import botocore.exceptions
    from kubernetes import client  # type: ignore

    from materialize import orchestratord

    secret = client.CoreV1Api().read_namespaced_secret(
        "materialize-backend",
        env.endpoints.namespace,
        _request_timeout=orchestratord.K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
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


def create_materialize(env: Environment) -> bool:
    """Create the Materialize CR from the `materialize-cr` ConfigMap.

    The CR is not in the manifests because orchestratord registers its CRD
    at startup.
    """
    import yaml
    from kubernetes import client  # type: ignore

    from materialize import orchestratord

    if env.materialize.get() is None:
        configmap = client.CoreV1Api().read_namespaced_config_map(
            "materialize-cr",
            env.endpoints.namespace,
            _request_timeout=orchestratord.K8S_TIMEOUT_SECONDS,  # pyright: ignore[reportCallIssue]
        )
        assert configmap.data is not None
        env.materialize.create(yaml.safe_load(configmap.data["materialize.yaml"]))
        log(f"created Materialize CR {env.endpoints.environment}")
    return True


def bring_up_kubernetes() -> str:
    """Create the Materialize CR and wait for its first generation. Returns
    the SQL host."""
    from materialize import orchestratord

    env = Environment()
    forever = float("inf")
    log("waiting for the Materialize CRD")
    orchestratord.wait_until(
        orchestratord.crd_established, forever, description="Materialize CRD"
    )
    orchestratord.wait_until(
        lambda: ensure_blob_bucket(env), forever, description="persist bucket"
    )
    orchestratord.wait_until(
        lambda: create_materialize(env), forever, description="Materialize CR creation"
    )
    log("waiting for the first generation to be applied")
    orchestratord.wait_until(
        env.materialize.is_up_to_date, forever, interval=5, description="first rollout"
    )
    host: list[str] = []
    retry("the SQL host", lambda: host.append(env.sql_host()))
    return host[0]


def admin_setup(config: Config, statements: list[str]) -> None:
    system = Target(config.host, internal=True)
    user = Target(config.host)

    def system_setup() -> None:
        with connect(system, "driver-setup") as conn:
            if not config.kubernetes:
                for name, value in STANDALONE_SYSTEM_PARAMETERS.items():
                    conn.execute(f"ALTER SYSTEM SET {name} = {value}".encode())
            # The sink checks read views `AS OF` a past frontier, which needs
            # their history retained. The Kubernetes ConfigMap does not sync
            # this flag, so `ALTER SYSTEM` holds there too.
            conn.execute(b"ALTER SYSTEM SET enable_logical_compaction_window = true")
            for cluster in (DERIVED_CLUSTER, SOURCE_CLUSTER):
                exists = conn.execute(
                    b"SELECT 1 FROM mz_clusters WHERE name = %s", (cluster,)
                ).fetchone()
                if exists is None:
                    conn.execute(
                        f"CREATE CLUSTER {cluster}"
                        f" (SIZE '{config.replica_sizes[0]}', REPLICATION FACTOR 1)".encode()
                    )
                    conn.execute(
                        f"GRANT ALL ON CLUSTER {cluster} TO materialize".encode()
                    )

    def user_setup() -> None:
        with connect(user, "driver-setup") as conn:
            conn.execute(b"SET statement_timeout = '60s'")
            for statement in statements:
                conn.execute(statement.encode())

    retry("the system setup", system_setup)
    retry("the schema setup", user_setup)


def wait_for_workload_active() -> None:
    log(f"setup complete, waiting for {WORKLOAD_ACTIVE}")
    while not WORKLOAD_ACTIVE.exists():
        time.sleep(0.1)


def main(argv: list[str] | None = None) -> int:
    """`argv` are passed on to hegel, see `machine.run`."""
    import sys

    from materialize.antithesis_driver import machine

    host = os.environ.get("MZ_ANTITHESIS_SQL_HOST")
    kubernetes = host is None
    if host is None:
        host = bring_up_kubernetes()
    config = Config.from_env(host, kubernetes)
    log(f"environment serves SQL at {host}")

    def serving() -> None:
        with connect(Target(host), "driver-setup") as conn:
            conn.execute(b"SELECT 1")

    retry("SQL", serving)
    c = machine.prepare(config)
    retry("the upstream setup", c.setup_upstreams)
    admin_setup(config, c.setup_statements())
    retry("SQL", serving)
    setup_complete({"host": host, "kubernetes": kubernetes})
    # The pod's readiness probe.
    state_dir = Path(os.environ.get("MZ_ANTITHESIS_STATE_DIR", "/tmp"))
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "setup-complete").touch()
    wait_for_workload_active()
    reachable("the workload starts", {"host": host})
    machine.run(sys.argv[1:] if argv is None else argv)
    return 0
