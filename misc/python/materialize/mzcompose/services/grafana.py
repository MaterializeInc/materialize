# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize import MZ_ROOT
from materialize.mzcompose.service import (
    Service,
)


class Grafana(Service):
    def __init__(self, name: str = "grafana") -> None:
        super().__init__(
            name=name,
            config={
                # renovate: datasource=docker depName=grafana/grafana versioning=docker
                "image": "grafana/grafana:13.0.1@sha256:0f86bada30d65ef9d0183b90c1e2682ac92d53d95da8bed322b984ea78a4a73a",
                "ports": ["3000"],
                "environment": [
                    "GF_AUTH_ANONYMOUS_ENABLED=true",
                    "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin",
                ],
                "volumes": [
                    str(MZ_ROOT / "misc" / "mzcompose" / "grafana" / "datasources")
                    + ":/etc/grafana/provisioning/datasources",
                ],
            },
        )
