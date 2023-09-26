# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import tempfile

import toml

from materialize.mzcompose import (
    loader,
)
from materialize.mzcompose.service import (
    Service,
)


class Mz(Service):
    def __init__(
        self,
        *,
        name: str = "mz",
        region: str = "aws/us-east-1",
        environment: str = "staging",
        app_password: str,
    ) -> None:
        # We must create the temporary config file in a location
        # that is accessible on the same path in both the ci-builder
        # container and the host that runs the docker daemon
        # $TMP does not guarantee that, but loader.composition_path does.
        config = tempfile.NamedTemporaryFile(
            dir=loader.composition_path,
            prefix="tmp_",
            suffix=".toml",
            mode="w",
            delete=False,
        )

        # Production env does not require to specify an endpoint.
        if environment == "production":
            cloud_endpoint = None
            admin_endpoint = None
        else:
            cloud_endpoint = f"https://api.{environment}.cloud.materialize.com"
            admin_endpoint = f"https://admin.{environment}.cloud.materialize.com"

        toml.dump(
            {
                "profile": "default",
                "profiles": {
                    "default": {
                        "app-password": app_password,
                        "region": region,
                        "cloud-endpoint": cloud_endpoint,
                        "admin-endpoint": admin_endpoint,
                    },
                },
            },
            config,
        )
        config.close()
        super().__init__(
            name=name,
            config={
                "mzbuild": "mz",
                "volumes": [f"{config.name}:/root/.config/materialize/mz.toml"],
            },
        )
