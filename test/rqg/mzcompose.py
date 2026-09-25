# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test Materialize with the Random Query Generator (grammar-based):
https://github.com/MaterializeInc/RQG/ Can find query errors and panics, and
correctness issues for workloads that compare against a reference
implementation (Postgres, or another Materialize version via --other-tag).
"""

import argparse
import hashlib
import random
from dataclasses import dataclass
from enum import Enum

from materialize.mzcompose.composition import (
    Composition,
    MzComposeService,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.rqg import RQG
from materialize.ui import UIError
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS,
)
from materialize.version_list import resolve_ancestor_image_tag

SERVICES = [
    RQG(),
    Materialized(name="mz_this", default_replication_factor=2),
    Materialized(name="mz_other", default_replication_factor=2),
    Postgres(),
]


class Target(Enum):
    """Which participants a dataset file is loaded into."""

    ALL = 1
    # Statements only the Postgres reference understands and needs,
    # e.g. REFRESH MATERIALIZED VIEW.
    POSTGRES_ONLY = 2


@dataclass(frozen=True)
class DatasetFile:
    # Path inside the rqg container. The CWD is /RQG, which contains a
    # checked-out copy of the MaterializeInc/RQG repository, and /workdir is
    # test/rqg. Dataset files must be idempotent and, unless POSTGRES_ONLY,
    # valid in both the Materialize and Postgres dialects, because they are
    # loaded with ON_ERROR_STOP.
    path: str
    target: Target = Target.ALL


class Dataset(Enum):
    SIMPLE = 1
    DBT3 = 2
    STAR_SCHEMA = 3
    WMR = 4
    BANKING = 5

    def files(self) -> list[DatasetFile]:
        match self:
            case Dataset.SIMPLE:
                return [DatasetFile("conf/mz/simple.sql")]
            case Dataset.DBT3:
                # The materialized views in dbt3-ddl.sql are created before
                # the data loads, so on Postgres they stay empty until the
                # REFRESH in dbt3-ddl-refresh-mvs.sql runs.
                return [
                    DatasetFile("conf/mz/dbt3-ddl.sql"),
                    DatasetFile("conf/mz/dbt3-s0.0001.dump"),
                    DatasetFile(
                        "conf/mz/dbt3-ddl-refresh-mvs.sql", Target.POSTGRES_ONLY
                    ),
                ]
            case Dataset.STAR_SCHEMA:
                return [DatasetFile("/workdir/datasets/star_schema.sql")]
            case Dataset.WMR:
                return [DatasetFile("conf/mz/wmr.sql")]
            case Dataset.BANKING:
                return [DatasetFile("conf/mz/banking.sql")]
            case _:
                raise RuntimeError(f"Not handled: {self}")


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
                raise RuntimeError("Unsupported case")


@dataclass
class Workload:
    name: str
    # All paths are relative to the CWD of the rqg container, which is /RQG and contains
    # a checked-out copy of the MaterializeInc/RQG repository
    # Use /workdir/file-name-goes-here.yy for files located in test/rqg
    grammar: str
    reference_implementation: ReferenceImplementation | None
    dataset: Dataset | None = None
    duration: int = 20 * 60
    queries: int = 100000000
    disabled: bool = False
    threads: int = 4
    validator: str | None = None

    def dataset_files(self) -> list[DatasetFile]:
        return self.dataset.files() if self.dataset is not None else []


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
        dataset=Dataset.WMR,
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
        dataset=Dataset.BANKING,
        grammar="conf/mz/banking.yy",
        reference_implementation=None,
        validator="QueryProperties,RepeatableRead",
    ),
    Workload(
        # CHAR/VARCHAR/TEXT semantics (padding, truncation, collation-aware
        # comparison) against Postgres, whose columns get COLLATE "C" via the
        # grammar's /*executor2 ...*/ annotations to match Materialize's byte
        # ordering. The grammar mixes INSERTs and SELECTs, so it must run
        # single-threaded: with concurrent writers the two servers observe
        # different statement interleavings and diverge spuriously. That is
        # also why its DDL stays in the grammar's thread1_init (the executor
        # comment annotations only work through gentest, not psql).
        name="char-varchar",
        grammar="conf/mz/char-varchar.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
        validator="ResultsetComparatorSimplify",
        threads=1,
    ),
    # Added as part of MaterializeInc/database-issues#7561.
    Workload(
        name="left-join-stacks",
        dataset=Dataset.STAR_SCHEMA,
        grammar="/workdir/grammars/left_join_stacks.yy",
        reference_implementation=ReferenceImplementation.POSTGRES,
        validator="ResultsetComparatorSimplify",
        queries=2000,  # Reduced no. of queries because the grammar is quite focused.
    ),
]


def resolve_seed(seed: str | None, workload_name: str) -> int:
    """Turn --seed into an integer seed for gentest.pl.

    gentest.pl computes per-worker seeds as seed + worker_id, and Perl
    silently numifies a non-numeric seed string. A UUID such as
    $BUILDKITE_JOB_ID numifies to just its leading digits, which collapses
    every CI run onto the same few effective seeds, so only ever pass a real
    integer.

    The workload name is mixed into the hash because Buildkite interpolates
    $BUILDKITE_JOB_ID at pipeline upload time, handing every job in a build
    the same value. An integer seed is used verbatim, so the printed
    effective seed reproduces the exact run.
    """
    if seed is None:
        return random.randrange(2**31)
    try:
        return int(seed)
    except ValueError:
        return int.from_bytes(
            hashlib.sha256(f"{seed}:{workload_name}".encode()).digest()[:4], "big"
        )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--this-tag",
        help="Run Materialize with this git tag on port 16875",
    )
    parser.add_argument(
        "--other-tag",
        action=StoreOtherTag,
        help="Run Materialize with this git tag on port 26875 (for workloads that compare two MZ instances)",
    )
    parser.add_argument(
        "--grammar",
        type=str,
        help="Override the default grammar of the workload",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        action="append",
        help="Override the dataset files for the workload",
    )
    parser.add_argument(
        "--starting-rule",
        type=str,
        help="Override the default starting-rule for the workload",
    )
    parser.add_argument(
        "--duration",
        type=int,
        help="Run the Workload for the specified time in seconds",
    )
    parser.add_argument(
        "--queries",
        type=int,
        help="Run the Workload for the specified number of queries",
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
        help="Random seed to use; non-integer values are hashed to an integer. Defaults to a random seed.",
    )

    parser.add_argument(
        "workloads", nargs="*", default=None, help="Run specified workloads"
    )
    args = parser.parse_args()

    c.up(Service("rqg", idle=True))

    if args.workloads is not None and len(args.workloads) > 0:
        workloads_to_run = [w for w in WORKLOADS if w.name in args.workloads]
    else:
        workloads_to_run = [w for w in WORKLOADS if not w.disabled]

    assert (
        len(workloads_to_run) > 0
    ), f"No matching workloads found (args was {args.workloads})"

    for workload in workloads_to_run:
        print(f"--- Running workload {workload.name}: {workload} ...")
        run_workload(c, args, workload, resolve_seed(args.seed, workload.name))


def psql_query(c: Composition, url: str, query: str) -> list[str]:
    return (
        c.exec("rqg", "psql", "-tA", "-c", query, url, capture=True, silent=True)
        .stdout.strip()
        .splitlines()
    )


def check_dataset_parity(
    c: Composition,
    mz_url: str,
    reference_url: str,
    reference_impl: ReferenceImplementation,
) -> None:
    """Assert that both sides expose identical row counts after loading.

    A dataset file that silently loaded differently on the two sides would
    otherwise surface later as a bogus result comparison failure.
    """
    if reference_impl == ReferenceImplementation.POSTGRES:
        list_relations = """
            SELECT schemaname || '.' || tablename FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT schemaname || '.' || matviewname FROM pg_matviews
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY 1
        """
    else:
        list_relations = """
            SELECT s.name || '.' || o.name
            FROM mz_objects o JOIN mz_schemas s ON o.schema_id = s.id
            WHERE o.id LIKE 'u%' AND o.type IN ('table', 'materialized-view')
            ORDER BY 1
        """
    relations = psql_query(c, reference_url, list_relations)
    print(f"--- Verifying row count parity of {len(relations)} relations ...")
    for relation in relations:
        counts = {
            url: psql_query(c, url, f"SELECT count(*) FROM {relation}")
            for url in [mz_url, reference_url]
        }
        assert (
            len(set(tuple(count) for count in counts.values())) == 1
        ), f"row count mismatch for {relation} after loading the dataset: {counts}"


def run_workload(
    c: Composition, args: argparse.Namespace, workload: Workload, seed: int
) -> None:
    def materialize_image(tag: str | None) -> str | None:
        return f"materialize/materialized:{tag}" if tag else None

    # A list of psql-compatible services participating in the test
    participants: list[MzComposeService] = [
        Materialized(
            name="mz_this",
            ports=["16875:6875", "16876:6876", "16877:6877", "16878:6878"],
            image=materialize_image(args.this_tag),
            use_default_volumes=False,
            default_replication_factor=2,
        ),
    ]

    mz_url = "postgresql://materialize@mz_this:6875/materialize"

    # If we have --other-tag, assume we want to run a comparison test against Materialize
    reference_impl = (
        ReferenceImplementation.MATERIALIZE
        if args.other_tag and workload.reference_implementation is not None
        else workload.reference_implementation
    )

    reference_url = None
    match reference_impl:
        case ReferenceImplementation.MATERIALIZE:
            participants.append(
                Materialized(
                    name="mz_other",
                    image=materialize_image(args.other_tag),
                    ports=["26875:6875", "26876:6876", "26877:6877", "26878:6878"],
                    use_default_volumes=False,
                    default_replication_factor=2,
                )
            )
            reference_url = "postgresql://materialize@mz_other:6875/materialize"
        case ReferenceImplementation.POSTGRES:
            participants.append(Postgres(ports=["15432:5432"]))
            reference_url = "postgresql://postgres:postgres@postgres/postgres"
        case None:
            pass
        case _:
            raise RuntimeError(
                f"Unsupported reference implementation: {reference_impl}"
            )

    dsn1 = "dbi:Pg:dbname=materialize;host=mz_this;user=materialize;port=6875"
    dsn2 = f"dbi:Pg:{reference_impl.dsn()}" if reference_impl else None
    dataset = (
        [DatasetFile(path) for path in args.dataset]
        if args.dataset is not None
        else workload.dataset_files()
    )
    grammar = str(args.grammar) if args.grammar is not None else workload.grammar
    queries = int(args.queries) if args.queries is not None else workload.queries
    threads = int(args.threads) if args.threads is not None else workload.threads
    duration = int(args.duration) if args.duration is not None else workload.duration

    other_tag_arg = f" --other-tag={args.other_tag}" if args.other_tag else ""
    print(f"--- Effective seed: {seed}")
    print(
        f"--- Reproduce with: bin/mzcompose --find rqg run default {workload.name} --seed={seed}{other_tag_arg}"
    )

    with c.override(*participants):
        try:
            c.up(*[p.name for p in participants])

            for file in dataset:
                for url in [mz_url, reference_url]:
                    if url is None:
                        continue
                    if file.target == Target.POSTGRES_ONLY and not (
                        url == reference_url
                        and reference_impl == ReferenceImplementation.POSTGRES
                    ):
                        continue
                    print(f"--- Populating {url} with {file.path} ...")
                    c.exec("rqg", "psql", "-v", "ON_ERROR_STOP=1", "-f", file.path, url)

            if reference_impl is not None and reference_url is not None and dataset:
                check_dataset_parity(c, mz_url, reference_url, reference_impl)

            c.exec(
                "rqg",
                "perl",
                "gentest.pl",
                f"--seed={seed}",
                f"--dsn1={dsn1}",
                f"--dsn2={dsn2}" if dsn2 else "",
                f"--grammar={grammar}",
                f"--queries={queries}",
                f"--threads={threads}",
                f"--duration={duration}",
                f"--validator={workload.validator}" if workload.validator else "",
                f"--starting-rule={args.starting_rule}" if args.starting_rule else "",
                "--sqltrace" if args.sqltrace else "",
                "--skip-recursive-rules" if args.skip_recursive_rules else "",
            )
        finally:
            c.capture_logs()


class StoreOtherTag(argparse.Action):
    """Resolve common ancestor during argument parsing"""

    def __call__(self, parser, namespace, values, option_string=None):
        if values == "common-ancestor":
            tag = resolve_ancestor_image_tag(
                ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS
            )

            if tag is None:
                raise UIError(
                    f"Could not resolve --other-tag={values} to an image tag, "
                    f"no version to compare against"
                )

            print(f"Resolving --other-tag to {tag}")
        else:
            tag = str(values)

        setattr(namespace, self.dest, tag)
