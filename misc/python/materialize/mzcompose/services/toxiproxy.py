# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import random

from materialize.mzcompose.service import (
    Service,
)


class Toxiproxy(Service):
    def __init__(
        self,
        name: str = "toxiproxy",
        # renovate: datasource=docker depName=jauderho/toxiproxy versioning=docker
        image: str = "jauderho/toxiproxy:v2.12.0@sha256:543c2d986ffd6e745888314d0ecf7b61d2fabec618d3fbb1ea80f7b39d3d195c",
        port: int = 8474,
        seed: int = random.randrange(2**63),
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "command": ["-host=0.0.0.0", f"-seed={seed}"],
                "ports": [port],
                "healthcheck": {
                    "test": ["CMD", "nc", "-z", "localhost", "8474"],
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )
