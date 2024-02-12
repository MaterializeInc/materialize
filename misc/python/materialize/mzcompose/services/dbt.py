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


class Dbt(Service):
    def __init__(self, name: str = "dbt", environment: list[str] = []) -> None:
        super().__init__(
            name=name,
            config={
                "mzbuild": "dbt-materialize",
                "environment": environment + ["TMPDIR=/share/tmp"],
                "volumes": [
                    ".:/workdir",
                    "secrets:/secrets",
                    "tmp:/share/tmp",
                ],
            },
        )
