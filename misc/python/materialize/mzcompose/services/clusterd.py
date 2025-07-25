# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json

from materialize.mzcompose import DEFAULT_MZ_ENVIRONMENT_ID, DEFAULT_MZ_VOLUMES
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)


class Clusterd(Service):
    def __init__(
        self,
        name: str = "clusterd",
        image: str | None = None,
        environment_id: str | None = None,
        environment_extra: list[str] = [],
        memory: str | None = None,
        cpu: str | None = None,
        options: list[str] = [],
        restart: str = "no",
        stop_grace_period: str = "120s",
        scratch_directory: str = "/scratch",
        volumes: list[str] = [],
        workers: int = 1,
        processes: list[tuple[str, int, int]] = [],
        ports: list[int] | None = None,
        mz_service: str = "materialized:6879",
    ) -> None:
        if not ports:
            ports = [2100, 2101, 2102, 2103, 6881]
        environment = [
            "CLUSTERD_LOG_FILTER",
            f"CLUSTERD_GRPC_HOST={name}",
            "MZ_SOFT_ASSERTIONS=1",
            "MZ_EAT_MY_DATA=1",
            f"CLUSTERD_STORAGE_CONTROLLER_LISTEN_ADDR=0.0.0.0:{ports[0]}",
            f"CLUSTERD_COMPUTE_CONTROLLER_LISTEN_ADDR=0.0.0.0:{ports[1]}",
            f"CLUSTERD_INTERNAL_HTTP_LISTEN_ADDR=0.0.0.0:{ports[4]}",
            f"CLUSTERD_PERSIST_PUBSUB_URL=http://{mz_service}",
            *environment_extra,
        ]

        if not environment_id:
            environment_id = DEFAULT_MZ_ENVIRONMENT_ID

        environment += [f"CLUSTERD_ENVIRONMENT_ID={environment_id}"]

        processes = processes or [(name, ports[2], ports[3])]
        process_index = -1
        for i, p in enumerate(processes):
            if p[0] == name:
                process_index = i
        assert process_index >= 0, f"Couldn't find process name {name} in {processes}"
        compute_timely_config = timely_config(
            [(p[0], p[1]) for p in processes], workers, 16
        )
        storage_timely_config = timely_config(
            [(p[0], p[2]) for p in processes], workers, 1337
        )

        environment += [
            f"CLUSTERD_PROCESS={process_index}",
            f"CLUSTERD_COMPUTE_TIMELY_CONFIG={compute_timely_config}",
            f"CLUSTERD_STORAGE_TIMELY_CONFIG={storage_timely_config}",
        ]

        options = [f"--scratch-directory={scratch_directory}", *options]

        config: ServiceConfig = {}

        if image:
            config["image"] = image
        else:
            config["mzbuild"] = "clusterd"

        # Depending on the Docker Compose version, this may either work or be
        # ignored with a warning. Unfortunately no portable way of setting the
        # memory limit is known.
        if memory or cpu:
            limits = {}
            if memory:
                limits["memory"] = memory
            if cpu:
                limits["cpus"] = cpu
            config["deploy"] = {"resources": {"limits": limits}}

        config.update(
            {
                "command": options,
                "ports": ports,
                "environment": environment,
                "volumes": volumes or DEFAULT_MZ_VOLUMES,
                "restart": restart,
                "stop_grace_period": stop_grace_period,
            }
        )

        super().__init__(name=name, config=config)


def timely_config(
    processes: list[tuple[str, int]],
    workers: int,
    arrangement_exert_proportionality: int,
) -> str:
    config = {
        "workers": workers,
        "process": 0,
        "addresses": [f"{n}:{port}" for n, port in processes],
        "arrangement_exert_proportionality": arrangement_exert_proportionality,
        "enable_zero_copy": False,
        "enable_zero_copy_lgalloc": False,
        "zero_copy_limit": None,
    }
    return json.dumps(config)
