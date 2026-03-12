# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Run SQLancer++ against Materialize: Automated testing to find logic bugs in
database systems: https://github.com/MaterializeInc/SQLancerPlusPlus
"""

import argparse

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.sqlancer import create_services, run_sqlancer

SERVICES = create_services("sqlancerplusplus")


def _build_run_args(args: argparse.Namespace, seed: int) -> list[str]:
    return [
        "--random-seed",
        f"{seed}",
        "--timeout-seconds",
        f"{args.runtime}",
        "--num-tries",
        f"{args.num_tries}",
        "--num-threads",
        f"{args.num_threads}",
        "--qpg-enable",
        f"{args.qpg}",
        "--random-string-generation",
        "ALPHANUMERIC",
        "general",
        "--database-engine",
        "materialize",
        "--oracle",
        # WHERE, NOREC or QUERY_PARTITIONING
        args.oracle,
    ]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    run_sqlancer(
        c,
        parser,
        service_name="sqlancerplusplus",
        default_oracle="WHERE",
        build_run_args=_build_run_args,
        docker_logs_path="/workdir/sqlancerplusplus/logs/general",
        log_prefix="SQLancer++",
    )
