# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""What every component of the driver shares: configuration and sessions."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field

from materialize.antithesis_driver.client import Pool, Target

SESSIONS = 12
"""General sessions that rules address by index. Long-lived interactions
such as subscriptions get sessions of their own."""

RULE_WAIT_SECONDS = 1.0
"""How long a rule waits for its operation before leaving it pending."""


def log(message: str) -> None:
    print(f"driver: {message}", flush=True)


@dataclass(frozen=True)
class Config:
    host: str
    """Host that routes SQL to the active environmentd generation."""
    kubernetes: bool
    """Whether the driver runs next to an orchestratord-managed environment
    and may use the Kubernetes API."""
    replica_sizes: tuple[str, ...]
    """Replica sizes the driver may create, smallest first."""
    eventually_after_seconds: float | None
    """Local loop only: run the eventually after this many seconds of
    workload, whatever the draws decide."""
    eventually_disabled: bool
    """Local loop only: never run the eventually."""
    upstream_postgres_url: str | None
    """The upstream Postgres of the Postgres source, as the driver reaches it.
    Without it, the Postgres source is off."""
    upstream_postgres_mz_host: str | None
    """The same server's `host` or `host:port` as Materialize reaches it, if
    that differs."""
    upstream_mysql_url: str | None
    upstream_mysql_mz_host: str | None
    """As the two above, for the MySQL source."""
    upstream_sql_server_url: str | None
    upstream_sql_server_mz_host: str | None
    """As the two above, for the SQL Server source."""
    kafka_broker: str | None
    """The Kafka broker, as the driver reaches it. Without it, the Kafka source
    and sinks are off."""
    kafka_mz_broker: str | None
    """The same broker as Materialize reaches it."""

    @staticmethod
    def from_env(host: str, kubernetes: bool) -> Config:
        after = os.environ.get("MZ_ANTITHESIS_EVENTUALLY_AFTER")
        return Config(
            host=host,
            kubernetes=kubernetes,
            replica_sizes=tuple(
                os.environ.get(
                    "MZ_ANTITHESIS_REPLICA_SIZES", "antithesis-1,antithesis-2"
                ).split(",")
            ),
            eventually_after_seconds=float(after) if after else None,
            eventually_disabled=os.environ.get("MZ_ANTITHESIS_NO_EVENTUALLY") == "1",
            upstream_postgres_url=os.environ.get("MZ_ANTITHESIS_UPSTREAM_POSTGRES_URL"),
            upstream_postgres_mz_host=os.environ.get(
                "MZ_ANTITHESIS_UPSTREAM_POSTGRES_MZ_HOST"
            ),
            upstream_mysql_url=os.environ.get("MZ_ANTITHESIS_UPSTREAM_MYSQL_URL"),
            upstream_mysql_mz_host=os.environ.get(
                "MZ_ANTITHESIS_UPSTREAM_MYSQL_MZ_HOST"
            ),
            upstream_sql_server_url=os.environ.get(
                "MZ_ANTITHESIS_UPSTREAM_SQL_SERVER_URL"
            ),
            upstream_sql_server_mz_host=os.environ.get(
                "MZ_ANTITHESIS_UPSTREAM_SQL_SERVER_MZ_HOST"
            ),
            kafka_broker=os.environ.get("MZ_ANTITHESIS_KAFKA_BROKER"),
            kafka_mz_broker=os.environ.get(
                "MZ_ANTITHESIS_KAFKA_MZ_BROKER",
                os.environ.get("MZ_ANTITHESIS_KAFKA_BROKER"),
            ),
        )


@dataclass
class Context:
    config: Config
    sessions: Pool
    """Sessions as the `materialize` user."""
    system: Pool
    """Sessions as `mz_system` on the internal port, for introspection and
    `ALTER SYSTEM`."""
    started: float = field(default_factory=time.monotonic)
    quiesced: threading.Event = field(default_factory=threading.Event)
    """Set once the eventually has stopped faults. Rules that disrupt the
    system skip from then on."""

    @staticmethod
    def create(config: Config) -> Context:
        return Context(
            config=config,
            sessions=Pool("s", Target(config.host), SESSIONS),
            system=Pool("sys", Target(config.host, internal=True), 2),
        )
