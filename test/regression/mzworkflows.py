# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Materialized, Testdrive, Workflow

services = [Materialized(), Testdrive()]


def workflow_github_8021(w: Workflow) -> None:
    w.start_services()
    w.wait_for_mz()
    w.run_service(service="testdrive-svc", command="github-8021.td")

    w.kill_services()
    w.start_services()
    w.wait_for_mz()
