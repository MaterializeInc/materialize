# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test for Postgres source with real-time recency enabled. Queries
should block until results are available instead of returning out of date
results.
"""

import glob
import pathlib
import random

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    SqlServer(),
    Materialized(),
    Toxiproxy(),
    Testdrive(),
]


#
# Test that real-time recency works w/ slow ingest of upstream data.
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    seed = random.getrandbits(16)

    # Start with a fresh state
    c.kill("sql-server")
    c.rm("sql-server")
    c.kill("materialized")
    c.rm("materialized")

    c.run_testdrive_files(
        "--no-reset",
        "--max-errors=1",
        f"--seed={seed}",
        "setup/setup.td",
        "rtr/toxiproxy-setup.td",
        "rtr/mz-setup.td",
        "rtr/verify-rtr.td",
    )
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    matching_files: list[str] = []
    for filter in args.filter:
        matching_files.extend(
            glob.glob(
                filter, root_dir=MZ_ROOT / "test" / "sql-server-rtr-old-syntax" / "rtr"
            )
        )
    matching_files = sorted(matching_files)
    sharded_files: list[str] = buildkite.shard_list(
        sorted(matching_files), lambda file: file
    )
    print(f"Filter: {args.filter} Files: {sharded_files}")

    # Start with a fresh state
    c.kill("sql-server")
    c.rm("sql-server")
    c.kill("materialized")
    c.rm("materialized")

    c.up("materialized", "toxiproxy", "sql-server")
    seed = random.getrandbits(16)

    def run(file: pathlib.Path | str) -> None:
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
            "setup/setup.td",
            str(file),
        )

    c.test_parts(sharded_files, run)
