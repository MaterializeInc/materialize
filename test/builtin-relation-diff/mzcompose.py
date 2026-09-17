# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Validation harness for builtin-table-to-materialized-view migrations.

Dumps configured builtin relations from two fresh environments, one running a
baseline image and one running the locally built code, after applying the same
corpus of user objects to both, and diffs the results. A fresh environment's
builtin objects double as a large free test corpus.

System ids are not stable across builds (adding any builtin shifts
fresh-install id assignment), so dumps are canonicalized before diffing: any
cell whose value looks like a catalog id is rewritten to the qualified name of
the object it denotes on that side, and an id inside a `[<id> AS <name>]`
reference in a create_sql is dropped in favour of the name.

For an exact diff, run against the merge base of your branch with
MaterializeInc/materialize main (`upstream` in the fork-based setup that
doc/developer/guide-changes.md describes):

    bin/mzcompose --find builtin-relation-diff run default \\
        --old-commit $(git merge-base HEAD upstream/main)

Without --old-commit or --old-image the common-ancestor release image is
used. Builtins that were added or changed on main since that release then
show up as one-sided rows. Rows naming an object that does not exist on the
other side at all, or that is an object of another kind there (a builtin
table that became a materialized view), are tolerated automatically, but
changes to a builtin view's definition between the two versions are reported
and need human judgement.

To validate an already-shipped table-to-view conversion, diff against the
last release before the conversion version (see the MIGRATIONS list in
builtin_schema_migration.rs), restricted to the converted relations. Pass
--user-rows-only so relations whose builtin rows legitimately drift between
versions (builtin view definitions, builtin comments) only compare
user-created rows:

    bin/mzcompose --find builtin-relation-diff run default \\
        --old-image ghcr.io/materializeinc/materialize/materialized:vX.Y.Z \\
        --relation mz_catalog.mz_clusters --user-rows-only
"""

import argparse
import random
from typing import Any

from materialize.builtin_relation_diff.corpus import CORPUS, SYSTEM_CORPUS
from materialize.builtin_relation_diff.diff import Snapshot, compare, dump
from materialize.builtin_relation_diff.relations import RELATIONS
from materialize.docker import commit_to_image_tag, image_registry
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS,
)
from materialize.version_list import resolve_ancestor_image_tag
from materialize.workload_replay.config import (
    additional_system_parameter_defaults,
    cluster_replica_sizes,
)
from materialize.workload_replay.executor import test as replay_workload
from materialize.workload_replay.util import (
    get_paths,
    load_workload,
    update_captured_workloads_repo,
)

# `replay_workload` unconditionally resolves the console port with
# `c.port("materialized", 6874)`, which fails unless the port is published.
# Nothing here serves or reads the console, so it stays disabled.
WORKLOAD_MZ_PORTS = [6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257]

SERVICES = [
    Materialized(name="mz_old"),  # Overridden below
    Materialized(name="mz_new"),  # Overridden below
    # Workload replay drives a single service named `materialized` (see
    # `workload_snapshot`), alongside the external systems a capture's
    # connections may reference. Mirrors test/workload-replay/mzcompose.py;
    # the shared config module keeps the sizes and parameters in step.
    Materialized(
        cluster_replica_size=cluster_replica_sizes,
        additional_system_parameter_defaults=additional_system_parameter_defaults,
        ports=WORKLOAD_MZ_PORTS,
    ),
    # These mirror test/workload-replay/mzcompose.py rather than using
    # defaults. The replay framework creates Kafka topics from the host with
    # confluent_kafka.admin, so the broker needs a published host port and a
    # HOST advertised listener; Testdrive needs the vars the capture's
    # generated DDL references.
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        advertised_listeners=[
            "HOST://127.0.0.1:30123",
            "PLAINTEXT://kafka:9092",
        ],
        environment_extra=[
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Postgres(),
    MySql(),
    SqlServer(),
    SshBastionHost(allow_any_key=True),
    Testdrive(
        seed=1,
        no_reset=True,
        no_consistency_checks=True,
        entrypoint_extra=[
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
            f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
        ],
    ),
    Mz(app_password=""),
]


def snapshot(
    c: Composition,
    service: str,
    port: int,
    system_port: int,
    relations: list[str],
    user_rows_only: bool,
) -> Snapshot:
    """Apply CORPUS to one environment and dump the configured relations."""
    system_conn = c.sql_connection(service=service, port=system_port, user="mz_system")
    system_conn.autocommit = True
    system_cursor = system_conn.cursor()
    for stmt in SYSTEM_CORPUS:
        system_cursor.execute(stmt.encode())
    system_conn.close()

    conn = c.sql_connection(service=service, port=port)
    conn.autocommit = True
    cursor = conn.cursor()

    # The corpus runs on the same connection as the dumps so that temporary
    # items are alive while the relations are read.
    for stmt in CORPUS:
        cursor.execute(stmt.encode())

    try:
        return dump(cursor, relations, user_rows_only)
    finally:
        conn.close()


def workload_snapshot(
    c: Composition,
    image: str | None,
    workload: dict[str, Any],
    workload_path: Any,
    relations: list[str],
    user_rows_only: bool,
    seed: str,
    verbose: bool,
) -> Snapshot:
    """Replay a captured workload on `image` and dump the configured relations.

    Only the object-creation phase runs: no initial data, no ingestion, no
    query load. The relations this harness diffs are catalog metadata, so the
    objects are the corpus and their contents are irrelevant.

    `replay_workload` brings up a service named `materialized` itself, so the
    two sides run sequentially here rather than side by side as in corpus
    mode. The dump is taken from `during_continuous`, which the replay invokes
    once the objects exist and have hydrated.

    The seed is pinned rather than defaulted to the clock: a diff between two
    builds is meaningless if the corpus differs between them.
    """
    random.seed(seed)
    captured: dict[str, Snapshot] = {}

    def capture() -> None:
        conn = c.sql_connection(service="materialized", port=6875)
        conn.autocommit = True
        try:
            captured["snapshot"] = dump(conn.cursor(), relations, user_rows_only)
        finally:
            conn.close()

    with c.override(
        Materialized(
            image=image,
            cluster_replica_size=cluster_replica_sizes,
            additional_system_parameter_defaults=additional_system_parameter_defaults,
            ports=WORKLOAD_MZ_PORTS,
            use_default_volumes=False,
        )
    ):
        replay_workload(
            c,
            workload,
            workload_path,
            factor_initial_data=1,
            factor_ingestions=1,
            factor_queries=1,
            runtime=0,
            verbose=verbose,
            create_objects=True,
            initial_data=False,
            early_initial_data=False,
            run_ingestions=False,
            run_queries=False,
            max_concurrent_queries=1,
            during_continuous=capture,
        )

    if "snapshot" not in captured:
        raise AssertionError(
            "workload replay finished without reaching the dump callback"
        )
    return captured["snapshot"]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--old-image",
        type=str,
        default=None,
        help="baseline materialized image; defaults to the common-ancestor release",
    )
    parser.add_argument(
        "--old-commit",
        type=str,
        default=None,
        help="commit hash to resolve the baseline image from "
        "(use the merge base of your branch for an exact diff)",
    )
    parser.add_argument(
        "--relation",
        action="append",
        choices=sorted(RELATIONS),
        help="relation to diff (default: all configured relations)",
    )
    parser.add_argument(
        "--user-rows-only",
        action="store_true",
        help="for relations configuring it, compare only user-created rows; "
        "use when builtin rows legitimately drift between the two versions",
    )
    parser.add_argument(
        "--workload",
        type=str,
        default=None,
        help="replay this captured workload as the corpus instead of CORPUS, "
        "e.g. 'workload_prod_sandbox' (see test/workload-replay/README.md). "
        "Richer, but needs the captured-workloads repo and external systems, "
        "and cannot cover temporary items",
    )
    parser.add_argument(
        "--workload-seed",
        type=str,
        default="builtin-relation-diff",
        help="seed for workload replay; both sides use it, so changing it "
        "changes the corpus but never introduces a difference between builds",
    )
    parser.add_argument("--verbose", action=argparse.BooleanOptionalAction)
    args: argparse.Namespace = parser.parse_args()

    relations = args.relation or sorted(RELATIONS)
    old_image = args.old_image
    if old_image is None and args.old_commit is not None:
        tag = commit_to_image_tag(args.old_commit)
        old_image = f"{image_registry()}/materialized:{tag}"
    if old_image is None:
        tag = resolve_ancestor_image_tag(ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS)
        old_image = f"{image_registry()}/materialized:{tag}"
    print(f"Baseline image: {old_image}")

    c.down(destroy_volumes=True)

    sql_port = 6875
    system_port = 6877

    if args.workload:
        # Replay drives one `materialized` service, so the sides run one after
        # the other, each against a freshly reset environment.
        update_captured_workloads_repo()
        matches = get_paths([f"{args.workload}.yml"])
        if len(matches) != 1:
            raise UIError(
                f"--workload {args.workload!r} matched {len(matches)} capture files; "
                "pass the file's basename without the .yml suffix"
            )
        workload_path = matches[0]
        workload = load_workload(workload_path)
        print(f"Corpus: replay of {workload_path.name}")

        snapshots = []
        for label, image in (("baseline", old_image), ("new", None)):
            print(f"--- Replaying workload on the {label} build")
            snapshots.append(
                workload_snapshot(
                    c,
                    image,
                    workload,
                    workload_path,
                    relations,
                    args.user_rows_only,
                    args.workload_seed,
                    bool(args.verbose),
                )
            )
            c.down(destroy_volumes=True)
        old, new = snapshots
    else:
        with c.override(
            Materialized(
                name="mz_old",
                image=old_image,
                ports=[f"16875:{sql_port}", f"16877:{system_port}"],
                use_default_volumes=False,
            ),
            Materialized(
                name="mz_new",
                image=None,
                ports=[f"26875:{sql_port}", f"26877:{system_port}"],
                use_default_volumes=False,
            ),
        ):
            c.up("mz_old", "mz_new")
            old = snapshot(
                c, "mz_old", sql_port, system_port, relations, args.user_rows_only
            )
            new = snapshot(
                c, "mz_new", sql_port, system_port, relations, args.user_rows_only
            )

    failures = compare(relations, old, new)
    if failures:
        raise AssertionError(f"unexplained differences in: {', '.join(failures)}")
