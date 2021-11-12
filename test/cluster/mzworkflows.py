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
    Dataflowd(
        name="dataflowd_1",
        options="--workers 2 --processes 2 --process 0 --hosts dataflowd_1:2101 dataflowd_2:2101",
        hostname="dataflowd_1",
        depends_on=["dataflowd_2"],
        ports=[6876, 2101],
    ),
    Dataflowd(
        name="dataflowd_2",
        options="--workers 2 --processes 2 --process 1 --hosts dataflowd_1:2101 dataflowd_2:2101",
        hostname="dataflowd_2",
        depends_on=["dataflowd_1"],
        ports=[6876, 2101],
    ),
    Coordd(
        name="materialized",
        options="--workers 4 --dataflowd-addr dataflowd_1:6876 dataflowd_2:6876",
        depends_on=["dataflowd_1", "dataflowd_2"],
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
