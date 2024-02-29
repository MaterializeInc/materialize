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
    ServiceDependency,
)
from materialize.ui import UIError


class FronteggMock(Service):
    def __init__(
        self,
        users: str,
        issuer: str,
        encoding_key: str | None = None,
        encoding_key_file: str | None = None,
        decoding_key: str | None = None,
        decoding_key_file: str | None = None,
        name: str = "frontegg-mock",
        mzbuild: str = "frontegg-mock",
        volumes: list[str] = [],
        depends_on: list[str] = [],
    ) -> None:
        command = [
            "--listen-addr=0.0.0.0:6880",
            "--users",
            users,
            "--issuer",
            issuer,
        ]
        if encoding_key:
            command += ["--encoding-key", encoding_key]
        elif encoding_key_file:
            command += ["--encoding-key-file", encoding_key_file]
        else:
            raise UIError("FronteggMock service must specify encoding-key[-file]")

        if decoding_key:
            command += ["--decoding-key", decoding_key]
        elif decoding_key_file:
            command += ["--decoding-key-file", decoding_key_file]
        else:
            raise UIError("FronteggMock service must specify decoding-key[-file]")

        depends_graph: dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }

        super().__init__(
            name=name,
            config={
                "mzbuild": mzbuild,
                "command": command,
                "ports": [6880],
                "volumes": volumes,
                "depends_on": depends_graph,
            },
        )
