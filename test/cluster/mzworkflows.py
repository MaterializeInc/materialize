# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Coordd, Dataflowd, Testdrive, Workflow

daemons = [
    Dataflowd(name="dataflowd", options="--workers 1", hostname="dataflowd"),
    Coordd(
        name="materialized",
        options="--workers 1 --dataflowd-addr dataflowd:6876",
        depends_on=["dataflowd"],
    ),
]

services = [*daemons, Testdrive()]


def workflow_cluster(w: Workflow):
    w.start_and_wait_for_tcp(services=daemons)
    w.wait_for_mz()
    w.run_service(
        service="testdrive-svc",
        command="*.td",
    )
