# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import hashlib
import os

import toml

from materialize import MZ_ROOT
from materialize.mzcompose import loader
from materialize.mzcompose.service import Service


class Mz(Service):
    def __init__(
        self,
        *,
        name: str = "mz",
        region: str = "aws/us-east-1",
        environment: str = "staging",
        app_password: str,
    ) -> None:

        # Production env does not require to specify an endpoint.
        if environment == "production":
            cloud_endpoint = None
            admin_endpoint = None
        else:
            cloud_endpoint = f"https://api.{environment}.cloud.materialize.com"
            admin_endpoint = f"https://admin.{environment}.cloud.materialize.com"

        config_str = toml.dumps(
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
        )

        # We must create the temporary config file in a location
        # that is accessible on the same path in both the ci-builder
        # container and the host that runs the docker daemon
        # $TMP does not guarantee that, but loader.composition_path does.
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()
        config_name = (loader.composition_path or MZ_ROOT) / f"tmp_{config_hash}.toml"

        with open(config_name, "w") as f:
            f.write(config_str)
        os.chmod(config_name, 0o777)
        super().__init__(
            name=name,
            config={
                "mzbuild": "mz",
                "volumes": [
                    f"{config_name}:/home/materialize/.config/materialize/mz.toml"
                ],
            },
        )
