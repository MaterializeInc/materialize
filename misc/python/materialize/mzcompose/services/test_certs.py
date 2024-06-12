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


class TestCerts(Service):
    __test__ = False

    def __init__(
        self,
        name: str = "test-certs",
    ) -> None:
        super().__init__(
            name="test-certs",
            config={
                # Container must stay alive indefinitely to be considered
                # healthy by `docker compose up --wait`.
                "command": ["sleep", "infinity"],
                "init": True,
                "mzbuild": "test-certs",
                "volumes": ["secrets:/secrets"],
            },
        )
