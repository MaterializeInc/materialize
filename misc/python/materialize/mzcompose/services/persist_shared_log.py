# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.service import Service, ServiceDependency, ServiceHealthcheck


class PersistSharedLog(Service):
    """The persist shared log consensus service.

    Runs a gRPC server that implements the PersistSharedLog service
    (compare_and_set, head, scan, truncate, list_keys). Environmentd
    connects via ``--persist-consensus-url=rpc://persist-shared-log:6890``.

    The service needs its own persist backend (blob + consensus) for the
    shard it uses internally. Pass ``blob_url`` and ``consensus_url`` to
    configure durable storage; omit both for in-memory (non-durable).
    """

    DEFAULT_PORT = 6890
    METRICS_PORT = 6891

    def __init__(
        self,
        name: str = "persist-shared-log",
        blob_url: str | None = None,
        consensus_url: str | None = None,
        shard_id: str | None = None,
        depends_on: dict[str, ServiceDependency] | None = None,
        healthcheck: ServiceHealthcheck | None = None,
    ) -> None:
        environment: list[str] = []
        volumes: list[str] = []

        if blob_url and consensus_url:
            environment.append(f"PERSIST_BLOB_URL={blob_url}")
            environment.append(f"PERSIST_CONSENSUS_URL={consensus_url}")
            # If using file blob storage, mount a volume for persistence.
            if blob_url.startswith("file://"):
                volumes.append("mzdata:/mzdata")

        if shard_id:
            environment.append(f"PERSIST_SHARD_ID={shard_id}")

        if healthcheck is None:
            healthcheck = {
                "test": [
                    "CMD-SHELL",
                    "curl -sf http://localhost:6891/metrics > /dev/null",
                ],
                "interval": "1s",
                "start_period": "30s",
            }

        config = {
            "mzbuild": "persist-shared-log",
            "ports": [PersistSharedLog.DEFAULT_PORT],
            "environment": environment,
            "healthcheck": healthcheck,
        }

        if volumes:
            config["volumes"] = volumes

        if depends_on:
            config["depends_on"] = depends_on

        super().__init__(name=name, config=config)
