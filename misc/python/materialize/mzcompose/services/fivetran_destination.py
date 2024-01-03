# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.service import Service


class FivetranDestination(Service):
    def __init__(self, volumes_extra: list[str] = []) -> None:
        super().__init__(
            name="fivetran-destination",
            config={
                "mzbuild": "fivetran-destination",
                "ports": [6874],
                "volumes": volumes_extra,
            },
        )
