# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose import (
    DEFAULT_SYSTEM_PARAMETERS,
)
from materialize.mzcompose.service import (
    Service,
)


class SqlLogicTest(Service):
    def __init__(
        self,
        name: str = "sqllogictest",
        mzbuild: str = "sqllogictest",
        environment: list[str] = [
            "MZ_SOFT_ASSERTIONS=1",
        ],
        volumes: list[str] = ["../..:/workdir"],
        depends_on: list[str] = ["cockroach"],
    ) -> None:
        environment += [
            "MZ_SYSTEM_PARAMETER_DEFAULT="
            + ";".join(
                f"{key}={value}" for key, value in DEFAULT_SYSTEM_PARAMETERS.items()
            )
        ]

        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "environment": environment,
                "volumes": volumes,
                "tmpfs": ["/tmp"],
                "depends_on": depends_on,
                "propagate_uid_gid": True,
                "init": True,
            },
        )
