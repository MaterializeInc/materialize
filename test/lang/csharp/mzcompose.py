# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, Service
from materialize.mzcompose.services import Materialized

SERVICES = [
    Materialized(),
    Service(
        name="csharp",
        config={
            "image": "mcr.microsoft.com/dotnet/sdk:5.0-focal",
            "volumes": [
                "../../../:/workdir",
            ],
        },
    ),
]


def workflow_default(c: Composition) -> None:
    c.up("materialized")
    c.wait_for_materialized()
    c.run("csharp", "/workdir/test/lang/csharp/test.sh")
