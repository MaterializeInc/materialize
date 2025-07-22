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
        process_names: list[str] = [],
    ) -> None:
        environment = [
            "CLUSTERD_LOG_FILTER",
            f"CLUSTERD_GRPC_HOST={name}",
            "MZ_SOFT_ASSERTIONS=1",
            "MZ_EAT_MY_DATA=1",
            *environment_extra,
        ]

        if not environment_id:
            environment_id = DEFAULT_MZ_ENVIRONMENT_ID

        environment += [f"CLUSTERD_ENVIRONMENT_ID={environment_id}"]

        process_names = process_names if process_names else [name]
        process_index = process_names.index(name)
        compute_timely_config = timely_config(process_names, 2102, workers, 16)
        storage_timely_config = timely_config(process_names, 2103, workers, 1337)

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
                "ports": [2100, 2101, 6878],
                "environment": environment,
                "volumes": volumes or DEFAULT_MZ_VOLUMES,
                "restart": restart,
                "stop_grace_period": stop_grace_period,
            }
        )

        super().__init__(name=name, config=config)


def timely_config(
    process_names: list[str],
    port: int,
    workers: int,
    arrangement_exert_proportionality: int,
) -> str:
    config = {
        "workers": workers,
        "process": 0,
        "addresses": [f"{n}:{port}" for n in process_names],
        "arrangement_exert_proportionality": arrangement_exert_proportionality,
        "enable_zero_copy": False,
        "enable_zero_copy_lgalloc": False,
        "zero_copy_limit": None,
    }
    return json.dumps(config)
