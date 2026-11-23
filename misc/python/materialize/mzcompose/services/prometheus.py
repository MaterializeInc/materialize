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


class Prometheus(Service):
    def __init__(self, name: str = "prometheus") -> None:
        super().__init__(
            name=name,
            config={
                # renovate: datasource=docker depName=prom/prometheus versioning=docker
                "image": "prom/prometheus:v3.11.3@sha256:e4254400b85610324913f0dc4acf92603d9984e7519414c5a12811aa6146acc3",
                "ports": ["9090"],
                "volumes": [
                    str(
                        MZ_ROOT / "misc" / "mzcompose" / "prometheus" / "prometheus.yml"
                    )
                    + ":/etc/prometheus/prometheus.yml",
                    "mzdata:/mnt/mzdata",
                ],
            },
        )
