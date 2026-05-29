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


class Moto(Service):
    """A `moto_server` container that mocks AWS services.

    Used for testing AWS surfaces that LocalStack Community doesn't cover
    (notably Glue / Glue Schema Registry).
    """

    def __init__(
        self,
        name: str = "moto",
        image: str = "motoserver/moto:5.2.1",
        port: int = 5000,
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "init": True,
                "ports": [port],
                "healthcheck": {
                    "test": [
                        "CMD-SHELL",
                        f"python -c 'import urllib.request; urllib.request.urlopen(\"http://localhost:{port}/moto-api/\")'",
                    ],
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )
