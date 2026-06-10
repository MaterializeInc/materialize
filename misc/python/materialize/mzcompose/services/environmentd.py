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
    ServiceConfig,
)


class Environmentd(Service):
    def __init__(
        self,
        name: str = "environmentd",
        mzbuild: str = "environmentd",
        https_resolver_template: str | None = None,
        frontegg_resolver_template: str | None = None,
        static_resolver_addr: str | None = None,
    ) -> None:
        config: ServiceConfig = {
            "mzbuild": mzbuild,
        }
        super().__init__(
            name=name,
            config=config,
        )
