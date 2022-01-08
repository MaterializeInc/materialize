# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized, Postgres, TestCerts, Testdrive

SERVICES = [
    Materialized(volumes_extra=["secrets:/share/secrets"]),
    Testdrive(volumes_extra=["secrets:/share/secrets"]),
    TestCerts(),
    Postgres(),
]


def workflow_pg_cdc(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default="*.td",
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    c.up("materialized", "test-certs", "testdrive-svc", "postgres")
    c.wait_for_materialized()
    c.wait_for_postgres()
    c.run("testdrive-svc", args.filter)
