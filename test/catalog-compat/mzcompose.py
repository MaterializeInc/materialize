# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, Service
from materialize.mzcompose.services import Kafka, Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    Service(
        name="catalog-compat",
        config={
            "mzbuild": "catcompatck",
        },
    ),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["zookeeper", "kafka"])
    c.run("catcompatck")
