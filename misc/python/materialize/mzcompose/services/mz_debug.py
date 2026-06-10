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


class MzDebug(Service):
    def __init__(self, name: str = "mz-debug", mzbuild: str = "mz-debug") -> None:
        config: ServiceConfig = {
            "mzbuild": mzbuild,
            "entrypoint": ["/usr/local/bin/mz-debug"],
        }

        super().__init__(
            name=name,
            config=config,
        )
