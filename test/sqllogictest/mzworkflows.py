# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Workflow
from materialize.mzcompose.services import Postgres, SqlLogicTest

services = [Postgres(), SqlLogicTest()]


def run_sqllogictest(w: Workflow, command: str) -> None:
    w.start_services(services=["postgres"])
    w.wait_for_postgres(dbname="postgres")
    w.run_service(service="sqllogictest-svc", command=command)


def workflow_sqllogictest(w: Workflow) -> None:
    run_sqllogictest(w, "ci/slt/slt.sh")


def workflow_sqllogictest_fast(w: Workflow) -> None:
    run_sqllogictest(w, "ci/test/slt-fast.sh")
