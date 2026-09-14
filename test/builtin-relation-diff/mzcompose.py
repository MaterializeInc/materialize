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
the object it denotes on that side.

For an exact diff, run against the merge base of your branch:

    bin/mzcompose --find builtin-relation-diff run default \\
        --old-commit $(git merge-base HEAD origin/main)

Without --old-commit or --old-image the common-ancestor release image is
used. Builtins that were added or changed on main since that release then
show up as one-sided rows. Rows naming an object that does not exist on the
other side at all are tolerated automatically, but changes to a builtin
view's definition between the two versions are reported and need human
judgement.

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

from materialize.builtin_relation_diff.corpus import CORPUS, SYSTEM_CORPUS
from materialize.builtin_relation_diff.diff import Snapshot, compare, dump
from materialize.builtin_relation_diff.relations import RELATIONS
from materialize.docker import commit_to_image_tag, image_registry
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.version_ancestor_overrides import (
    ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS,
)
from materialize.version_list import resolve_ancestor_image_tag

SERVICES = [
    Materialized(name="mz_old"),  # Overridden below
    Materialized(name="mz_new"),  # Overridden below
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
