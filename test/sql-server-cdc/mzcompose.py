# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Native SQL Server source tests, functional.
"""

import glob
import random

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Mz(app_password=""),
    Materialized(),
    Testdrive(),
    SqlServer(),
]


#
# Test that SQL Server ingestion works
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    matching_files = []
    for filter in args.filter:
        matching_files.extend(
            glob.glob(filter, root_dir=MZ_ROOT / "test" / "sql-server-cdc")
        )
    matching_files = sorted(matching_files)
    print(f"Filter: {args.filter} Files: {matching_files}")

    c.up("materialized", "sql-server")
    seed = random.getrandbits(16)

    c.test_parts(
        matching_files,
        lambda file: c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
            "--default-timeout=10s",
            str(file),
        ),
    )
