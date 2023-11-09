# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import MZ_ROOT
from materialize.mzcompose import (
    loader,
)
from materialize.mzcompose.service import (
    Service,
)


class SshBastionHost(Service):
    def __init__(
        self,
        name: str = "ssh-bastion-host",
        max_startups: str | None = None,
        aliases: list[str] | None = None,
    ) -> None:
        setup_path = os.path.relpath(
            MZ_ROOT / "misc" / "images" / "sshd" / "setup.sh",
            loader.composition_path,
        )

        if aliases is None:
            aliases = ["other_ssh_bastion"]

        super().__init__(
            name=name,
            config={
                "image": "quay.io/panubo/sshd:1.7.1",
                "init": True,
                "ports": ["22"],
                "environment": [
                    "SSH_USERS=mz:1000:1000",
                    "TCP_FORWARDING=true",
                    *([f"MAX_STARTUPS={max_startups}"] if max_startups else []),
                ],
                "volumes": [f"{setup_path}:/etc/entrypoint.d/setup.sh"],
                "networks": {"default": {"aliases": aliases}},
                "healthcheck": {
                    "test": "[ -f /var/run/sshd/sshd.pid ]",
                    "timeout": "5s",
                    "interval": "1s",
                    "start_period": "60s",
                },
            },
        )
