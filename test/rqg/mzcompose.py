# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
from dataclasses import dataclass
from enum import Enum

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.rqg import RQG

SERVICES = [RQG()]


class Dataset(Enum):
    SIMPLE = 1
    DBT3 = 2


class ReferenceImplementation(Enum):
    MZ = 1
    POSTGRES = 2

    def dsn(self) -> str:
        if self == ReferenceImplementation.MZ:
            return "dbname=materialize;host=mz_other;user=materialize;port=6875;cluster=default"
        elif self == ReferenceImplementation.POSTGRES:
            return "dbname=postgres;host=postgres;user=postgres;password=postgres"
        else:
            assert False


@dataclass
class Workload:
    name: str
    dataset: Dataset
    grammar: str
    reference_implementation: ReferenceImplementation | None
    duration: int = 30 * 60
    disabled: bool = False
    threads: int = 4


WORKLOADS = [
    Workload(
        name="simple-aggregates",
        dataset=Dataset.SIMPLE,
        grammar="conf/mz/simple-aggregates.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
    ),
    Workload(
        name="dbt3-joins",
        dataset=Dataset.DBT3,
        grammar="conf/mz/dbt3-joins.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--this-tag", help="Run Materialize with this git tag on port 16875"
    )
    parser.add_argument(
        "--other-tag",
        help="Run Materialize with this git tag on port 26875 (for workloads that compare two MZ instances)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Run the Workload for the specifid time in seconds",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Run the RQG With RQG_DEBUG=1"
    )
    parser.add_argument(
        "workloads", nargs="*", default=None, help="Run specified workloads"
    )
    args = parser.parse_args()

    c.up("rqg", persistent=True)

    if args.workloads is not None and len(args.workloads) > 0:
        workloads_to_run = [w for w in WORKLOADS if w.name in args.workloads]
    else:
        workloads_to_run = [w for w in WORKLOADS if not w.disabled]

    assert (
        len(workloads_to_run) > 0
    ), f"No matching workloads found (args was {args.workloads})"

    for workload in workloads_to_run:
        print(f"--- Running workload {workload.name}: {workload.__doc__} ...")
        run_workload(c, args, workload)


def run_workload(c: Composition, args: argparse.Namespace, workload: Workload) -> None:
    participants: list[Service] = [
        Materialized(
            name="mz_this",
            ports=["16875:6875", "16877:6877"],
            image=f"materialize/materialized:{args.this_tag}"
            if args.this_tag
            else None,
            use_default_volumes=False,
        ),
    ]

    psql_urls = ["postgresql://materialize@mz_this:6875/materialize"]

    if workload.reference_implementation == ReferenceImplementation.MZ:
        participants.append(
            Materialized(
                name="mz_other",
                image=f"materialize/materialized:{args.other_tag}"
                if args.other_tag
                else None,
                ports=["26875:6875", "26877:6877"],
                use_default_volumes=False,
            )
        )
        psql_urls.append("postgresql://materialize@mz_other:6885/materialize")

    elif workload.reference_implementation == ReferenceImplementation.POSTGRES:
        participants.append(Postgres(ports=["15432:5432"]))
        psql_urls.append("postgresql://postgres:postgres@postgres/postgres")
    else:
        assert False

    files = (
        ["dbt3-ddl.sql", "dbt3-s0.0001.dump"]
        if workload.dataset == Dataset.DBT3
        else ["simple.sql"]
    )

    dsn2 = (
        [f"--dsn2=dbi:Pg:{workload.reference_implementation.dsn()}"]
        if workload.reference_implementation is not None
        else []
    )

    duration = args.duration if args.duration else workload.duration
    assert duration is not None

    env_extra = {"RQG_DEBUG": "1"} if args.debug else {}

    with c.override(*participants):
        try:
            c.up(*[p.name for p in participants])

            for file in files:
                for psql_url in psql_urls:
                    print(f"--- Populating {psql_url} with {file} ...")
                    c.exec("rqg", "bash", "-c", f"psql -f conf/mz/{file} {psql_url}")

            if duration > 0:
                c.exec(
                    "rqg",
                    "perl",
                    "gentest.pl",
                    "--dsn1=dbi:Pg:dbname=materialize;host=mz_this;user=mz_system;port=6877",
                    *dsn2,
                    f"--grammar={workload.grammar}",
                    "--queries=10000000",
                    f"--threads={workload.threads}",
                    f"--duration={duration}",
                    env_extra=env_extra,
                )
        finally:
            c.capture_logs()
