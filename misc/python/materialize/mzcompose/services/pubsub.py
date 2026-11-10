# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.service import Service, ServiceConfig


class Pubsub(Service):
    """An external relay whose lifetime is independent of deployment containers."""

    def __init__(self, name: str = "pubsub", image: str | None = None) -> None:
        config: ServiceConfig = {
            "entrypoint": ["tini", "--", "pubsubd"],
            "command": [
                "--listen-addr=0.0.0.0:6879",
                "--internal-http-listen-addr=127.0.0.1:6880",
            ],
            "healthcheck": {
                "test": ["CMD", "curl", "--fail", "localhost:6880/api/livez"],
                "interval": "1s",
            },
        }
        if image is None:
            config["mzbuild"] = "materialized"
        else:
            config["image"] = image
        super().__init__(name=name, config=config)
