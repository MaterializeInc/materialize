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


class Metabase(Service):
    def __init__(self, name: str = "metabase") -> None:
        super().__init__(
            name=name,
            config={
                "image": "metabase/metabase:v0.41.4",
                "ports": ["3000"],
                "healthcheck": {
                    "test": ["CMD", "curl", "-f", "localhost:3000/api/health"],
                    "interval": "1s",
                    "start_period": "300s",
                },
                # The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested
                # See https://github.com/metabase/metabase/issues/13119
                "platform": "linux/amd64",
            },
        )
