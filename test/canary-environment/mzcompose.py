# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Dbt, Materialized

SERVICES = [
    Materialized(),
    Dbt(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.down()
    c.up("materialized")
    c.up("dbt", persistent=True)
    c.exec("dbt", "dbt", "run", workdir="/workdir")
    workflow_test(c, parser)


def workflow_test(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.exec("dbt", "dbt", "test", "--threads", "8", workdir="/workdir")
