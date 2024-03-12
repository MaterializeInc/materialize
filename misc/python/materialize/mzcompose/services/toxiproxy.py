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
        image: str = "jauderho/toxiproxy:v2.8.0",
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
