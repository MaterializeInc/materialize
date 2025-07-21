# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Redpanda(),
    Postgres(),
    MySql(),
    Testdrive(),
    Materialized(system_parameter_defaults={"enable_rbac_checks": "false"}),
]


def workflow_default(c: Composition) -> None:
    c.up(
        "redpanda",
        "postgres",
        "mysql",
        "materialized",
    )
    c.up("testdrive", persistent=True)
