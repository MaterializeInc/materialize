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
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS,
)
from materialize.version_list import (
    resolve_ancestor_image_tag,
)

SERVICES = [RQG()]


class Dataset(Enum):
    SIMPLE = 1
    DBT3 = 2

    def files(self) -> list[str]:
        match self:
            case Dataset.SIMPLE:
                return ["simple.sql"]
            case Dataset.DBT3:
                # With Postgres, CREATE MATERIALZIED VIEW from dbt3-ddl.sql will produce
                # a view thats is empty unless REFRESH MATERIALIZED VIEW from dbt3-ddl-refresh-mvs.sql
                # is also run after the data has been loaded by dbt3-s0.0001.dump
                return ["dbt3-ddl.sql", "dbt3-s0.0001.dump", "dbt3-ddl-refresh-mvs.sql"]
            case _:
                assert False


class ReferenceImplementation(Enum):
    MATERIALIZE = 1
    POSTGRES = 2

    def dsn(self) -> str:
        match self:
            case ReferenceImplementation.MATERIALIZE:
                return "dbname=materialize;host=mz_other;user=materialize;port=6875"
            case ReferenceImplementation.POSTGRES:
                return "dbname=postgres;host=postgres;user=postgres;password=postgres"
            case _:
                assert False


@dataclass
class Workload:
    name: str
    # All paths are relative to the CWD of the rqg container, which is /RQG and contains
    # a checked-out copy of the MaterializeInc/RQG repository
    # Use /workdir/file-name-goes-here.yy for files located in test/rqg
    grammar: str
    reference_implementation: ReferenceImplementation | None
    dataset: Dataset | None = None
    duration: int = 30 * 60
    disabled: bool = False
    threads: int = 4
    validator: str | None = None


WORKLOADS = [
    Workload(
        name="simple-aggregates",
        dataset=Dataset.SIMPLE,
        grammar="conf/mz/simple-aggregates.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
        validator="ResultsetComparatorSimplify",
    ),
    Workload(
        name="lateral-joins",
        dataset=Dataset.SIMPLE,
        grammar="conf/mz/lateral-joins.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
        validator="ResultsetComparatorSimplify",
    ),
    Workload(
        name="dbt3-joins",
        dataset=Dataset.DBT3,
        grammar="conf/mz/dbt3-joins.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
        validator="ResultsetComparatorSimplify",
    ),
    Workload(
        name="subqueries",
        dataset=Dataset.SIMPLE,
        grammar="conf/mz/subqueries.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
        validator="ResultsetComparatorSimplify",
    ),
    Workload(
        name="window-functions",
        dataset=Dataset.SIMPLE,
        grammar="conf/mz/window-functions.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
        validator="ResultsetComparatorSimplify",
    ),
    Workload(
        name="wmr",
        grammar="conf/mz/with-mutually-recursive.yy",
        # Postgres does not support WMR, so our only hope for a comparison
        # test is to use a previous Mz version via --other-tag=...
        reference_implementation=ReferenceImplementation.MATERIALIZE,
        validator="ResultsetComparatorSimplify",
    ),
    Workload(
        # A workload that performs DML that preserve the dataset's invariants
        # and also checks that those invariants are not violated
        name="banking",
        grammar="conf/mz/banking.yy",
        reference_implementation=None,
        validator="QueryProperties,RepeatableRead",
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
        "--grammar", type=str, help="Override the default grammar of the workload"
    )
    parser.add_argument(
        "--starting-rule",
        type=str,
        help="Override the default starting-rule for the workload",
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Run the Workload for the specifid time in seconds",
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Run the Workload with the specified number of concurrent threads",
    )
    parser.add_argument(
        "--sqltrace", action="store_true", help="Print all generated SQL statements"
    )
    parser.add_argument(
        "--skip-recursive-rules",
        action="store_true",
        help="Generate simpler queries by avoiding recursive productions",
    )
    parser.add_argument(
        "--seed",
        metavar="SEED",
        type=str,
        help="Random seed to use.",
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
        print(f"--- Running workload {workload.name}: {workload} ...")
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

    if args.other_tag == "common-ancestor":
        args.other_tag = resolve_ancestor_image_tag(
            ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS
        )
        print(f"Resolving --other-tag to {args.other_tag}")

    # If we have --other-tag, assume we want to run a comparison test against Materialize
    reference_implementation = (
        ReferenceImplementation.MATERIALIZE
        if args.other_tag and workload.reference_implementation is not None
        else workload.reference_implementation
    )

    match reference_implementation:
        case ReferenceImplementation.MATERIALIZE:
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
            psql_urls.append("postgresql://materialize@mz_other:6875/materialize")
        case ReferenceImplementation.POSTGRES:
            participants.append(Postgres(ports=["15432:5432"]))
            psql_urls.append("postgresql://postgres:postgres@postgres/postgres")
        case None:
            pass
        case _:
            assert False

    files = [] if workload.dataset is None else workload.dataset.files()

    dsn2 = (
        [f"--dsn2=dbi:Pg:{reference_implementation.dsn()}"]
        if reference_implementation is not None
        else []
    )

    duration = args.duration if args.duration is not None else workload.duration
    grammar = args.grammar or workload.grammar
    threads = args.threads or workload.threads

    with c.override(*participants):
        try:
            c.up(*[p.name for p in participants])

            for file in files:
                for psql_url in psql_urls:
                    print(f"--- Populating {psql_url} with {file} ...")
                    c.exec("rqg", "bash", "-c", f"psql -f conf/mz/{file} {psql_url}")

            c.exec(
                "rqg",
                "perl",
                "gentest.pl",
                "--dsn1=dbi:Pg:dbname=materialize;host=mz_this;user=materialize;port=6875",
                *dsn2,
                f"--grammar={grammar}",
                f"--validator={workload.validator}"
                if workload.validator is not None
                else "",
                f"--starting-rule={args.starting_rule}"
                if args.starting_rule is not None
                else "",
                "--queries=100000000",
                f"--threads={threads}",
                f"--duration={duration}",
                f"--seed={args.seed}",
                "--sqltrace" if args.sqltrace else "",
                "--skip-recursive-rules" if args.skip_recursive_rules else "",
            )
        finally:
            c.capture_logs()
