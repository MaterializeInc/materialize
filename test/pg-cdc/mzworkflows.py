# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
from typing import List

from materialize.mzcompose import (
    Materialized,
    Postgres,
    TestCerts,
    Testdrive,
    Workflow,
    WorkflowArgumentParser,
)

services = [
    Materialized(volumes_extra=["secrets:/share/secrets"]),
    Testdrive(volumes_extra=["secrets:/share/secrets"]),
    TestCerts(),
    Postgres(),
]


def workflow_pg_cdc(w: Workflow, args: List[str]):
    parser = WorkflowArgumentParser(w)
    parser.add_argument(
        "filter",
        nargs="*",
        default="*.td",
        help="limit to only the files matching filter",
    )
    args = parser.parse_args(args)

    if os.getenv("BUILDKITE"):
        command = ["--ci-output", *args.filter]
    else:
        command = args.filter

    w.start_services(
        services=["materialized", "test-certs", "testdrive-svc", "postgres"]
    )
    w.wait_for_mz()
    w.wait_for_postgres()
    w.run_service(service="testdrive-svc", command=command)
