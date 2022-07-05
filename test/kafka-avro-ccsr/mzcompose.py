# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This can't run as part of the normal testdrive
# tests, because we need to know what IDs various schemas were given in CSR,
# in order to refer to them in later `CREATE SOURCE` statements. But testdrive
# does not support passing state from one action to another at runtime.
#
# Running in a separate composition allows us to assume we started from a clean slate,
# i.e. that the first schema in the file will be given "1", and sequentially from there.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

prerequisites = ["zookeeper", "kafka", "schema-registry", "materialized"]

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)

    c.wait_for_materialized("materialized")

    c.run("testdrive", "test.td")
