# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from textwrap import dedent

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    # TODO: Only a sample, maybe you don't need all of these services
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Postgres(),
    MySql(),
    Testdrive(no_reset=True),
    Materialized(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "materialized",
    )

    c.up("testdrive", persistent=True)
    c.testdrive(
        dedent(
            """
        > SELECT 1
        1
    """
        )
    )

    # The testdrive process is running now
    time.sleep(3600)
