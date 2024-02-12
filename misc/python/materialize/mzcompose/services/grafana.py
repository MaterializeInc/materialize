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
                "image": "grafana/grafana:9.3.2",
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
