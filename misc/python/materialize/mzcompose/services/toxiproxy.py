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
)


class Toxiproxy(Service):
    def __init__(
        self,
        name: str = "toxiproxy",
        image: str = "shopify/toxiproxy:2.1.4",
        port: int = 8474,
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [port],
                "healthcheck": {
                    "test": ["CMD", "nc", "-z", "localhost", "8474"],
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )
