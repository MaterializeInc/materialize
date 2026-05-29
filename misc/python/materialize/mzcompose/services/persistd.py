# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
    ServiceDependency,
)


class Persistd(Service):
    """Standalone persist committer service.

    Wraps the persistd binary so compositions can run the committer as its
    own container. Compositions that use it should point the materialized
    service's MZ_PERSIST_COMMITTER_URL at http://<name>:6882 instead of the
    default 127.0.0.1.
    """

    def __init__(
        self,
        name: str = "persistd",
        mzbuild: str = "persistd",
        consensus_url: str = (
            "postgres://root@postgres-metadata:26257?options=--search_path=consensus"
        ),
        listen_port: int = 6882,
        max_cached_shards: int = 10000,
        cache_refresh_interval: str = "5s",
        stats_heartbeat_interval: str = "30s",
        command: list[str] | None = None,
        depends_on: list[str] = [],
        environment_extra: list[str] = [],
    ) -> None:
        if command is None:
            command = [
                f"--listen-addr=0.0.0.0:{listen_port}",
                f"--consensus-url={consensus_url}",
                f"--max-cached-shards={max_cached_shards}",
                f"--cache-refresh-interval={cache_refresh_interval}",
                f"--stats-heartbeat-interval={stats_heartbeat_interval}",
            ]

        depends_graph: dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }
        environment = [
            "MZ_NO_TELEMETRY=1",
            *environment_extra,
        ]
        config: ServiceConfig = {
            "init": True,
            "mzbuild": mzbuild,
            "command": command,
            "ports": [listen_port],
            "environment": environment,
            "depends_on": depends_graph,
        }

        super().__init__(name=name, config=config)
