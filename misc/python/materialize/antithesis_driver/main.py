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

import os
import time
from collections.abc import Callable
from pathlib import Path

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    reachable,
)
from antithesis.lifecycle import (  # pyright: ignore[reportMissingModuleSource]
    setup_complete,
)

from materialize.antithesis_driver.client import Target, connect
from materialize.antithesis_driver.context import Config, log

WORKLOAD_ACTIVE = Path(
    os.environ.get("MZ_ANTITHESIS_WORKLOAD_ACTIVE", "/workload-active")
)
DERIVED_CLUSTER = "driver"
STANDALONE_SYSTEM_PARAMETERS = {
    # Strong session serializable is the only isolation level with
    # per-session guarantees. The Kubernetes config sets these in the
    # `system-params` ConfigMap, which environmentd syncs and which would
    # override `ALTER SYSTEM`.
    "enable_session_timelines": "true",
    "max_connections": "1000",
}


def retry(name: str, step: Callable[[], None]) -> None:
    """Run `step` until it succeeds, logging each new reason it fails."""
    last = None
    while True:
        try:
            step()
            return
        except (psycopg.Error, OSError, RuntimeError) as e:
            reason = f"{type(e).__name__}: {str(e).strip().splitlines()[0] if str(e).strip() else ''}"
            if reason != last:
                log(f"waiting for {name}: {reason}")
                last = reason
            time.sleep(2)


def bring_up_kubernetes() -> str:
    """Create the Materialize CR and wait for its first generation, as the
    legacy workload's `setup` does. Returns the SQL host."""
    from materialize import orchestratord
    from materialize.antithesis import setup
    from materialize.antithesis.environment import Environment

    env = Environment()
    forever = float("inf")
    log("waiting for the Materialize CRD")
    orchestratord.wait_until(
        orchestratord.crd_established, forever, description="Materialize CRD"
    )
    orchestratord.wait_until(
        lambda: setup.ensure_blob_bucket(env), forever, description="persist bucket"
    )
    orchestratord.wait_until(
        lambda: (setup.create_materialize(env), True)[1],
        forever,
        description="Materialize CR creation",
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
            exists = conn.execute(
                b"SELECT 1 FROM mz_clusters WHERE name = %s", (DERIVED_CLUSTER,)
            ).fetchone()
            if exists is None:
                conn.execute(
                    f"CREATE CLUSTER {DERIVED_CLUSTER}"
                    f" (SIZE '{config.replica_sizes[0]}', REPLICATION FACTOR 1)".encode()
                )
                conn.execute(
                    f"GRANT ALL ON CLUSTER {DERIVED_CLUSTER} TO materialize".encode()
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
    admin_setup(config, machine.setup_statements())
    retry("SQL", serving)
    setup_complete({"host": host, "kubernetes": kubernetes})
    # The pod's readiness probe.
    state_dir = Path(os.environ.get("MZ_ANTITHESIS_STATE_DIR", "/tmp"))
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "setup-complete").touch()
    wait_for_workload_active()
    reachable("the workload starts", {"host": host})
    machine.run(config, sys.argv[1:] if argv is None else argv)
    return 0
