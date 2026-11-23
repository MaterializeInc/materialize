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


class Squid(Service):
    """
    An HTTP forward proxy, used in some workflows to test whether Materialize can correctly route
    traffic via the proxy.
    """

    def __init__(
        self,
        name: str = "squid",
        # renovate: datasource=docker depName=sameersbn/squid versioning=docker
        image: str = "sameersbn/squid:3.5.27-2@sha256:e98299069f0c6e3d9b9188903518e2f44ac36b1fa5007e879af518e1c0a234af",
        port: int = 3128,
        volumes: list[str] = ["./squid.conf:/etc/squid/squid.conf"],
    ) -> None:
        super().__init__(
            name=name,
            config={"image": image, "ports": [port], "volumes": volumes},
        )
