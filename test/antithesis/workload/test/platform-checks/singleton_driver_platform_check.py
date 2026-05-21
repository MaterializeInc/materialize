#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver: run one upstream `materialize.checks` Check class.

Pick a random `Check` subclass from a curated allow-list, run its
testdrive fragments through `initialize -> manipulate[0] ->
manipulate[1] -> validate -> validate`, and assert validate() passes
end-to-end.  Antithesis injects container kills concurrently — that's
the fault-injection equivalent of upstream platform-checks scenarios
like `RestartEntireMz`, `RestartCockroach`, and
`KillClusterdStorage`, which interleave the same phases with explicit
restarts.

Approach:

  * The real `materialize.checks` package is bundled into the workload
    image (see `workloads/platform-checks/mzbuild.yml`).  Check classes
    return `Testdrive(input=...)` wrappers from initialize/manipulate/
    validate; we read each wrapper's `.input` string and hand it to
    testdrive directly — no Executor protocol involvement, no
    docker-compose orchestration.

  * The allow-list keeps the driver to Checks whose testdrive fragments
    run against the Antithesis platform-checks topology (universal
    services + clusterd2; no Kafka / Postgres-CDC / MySQL / SQL Server
    upstreams).  Checks that need external sources are exercised in
    their dedicated cdc groups via the testdrive-runner singleton
    drivers and stay out of this list.

  * Singleton, not parallel: many Check classes assume they own
    `materialize.public` (no per-Check namespacing), so concurrent
    invocations on the same SUT collide on object names.  Singleton
    matches the upstream `ExecutionMode.SEQUENTIAL`.

Properties:

  * always: every validate() phase returns succeeded-or-fault-shaped
    exit, never a non-fault failure.  A non-fault validate failure is
    a real property break.
  * sometimes: at least one timeline runs every phase cleanly (no
    transient failure observed anywhere) — without this anchor an
    always(True) trivially passes when every phase bails on a fault.

Per-invocation entropy:
  * The picked Check class differs across timelines (SDK-routed
    `random_choice`), so the fuzzer explores a fresh Check per replay
    rather than re-running the same one.
"""

from __future__ import annotations

import os
import sys
import time
from typing import TYPE_CHECKING, Any

import helper_logging
import helper_random
import helper_testdrive

from antithesis.assertions import always, sometimes

if TYPE_CHECKING:
    from materialize.checks.checks import Check

LOG = helper_logging.setup_logging("driver.platform_check")

# Per-phase testdrive timeout. Long enough to span one full
# fault-orchestrator ON+OFF cycle (~80s with default 20-40 / 20-40
# windows) plus margin for the fragment itself to actually complete
# inside a quiet window.  Some checks (large_tables, threshold, …)
# do bulk inserts that take 10s+ even without faults; budgeting 300s
# gives them headroom.
PHASE_TIMEOUT_S = 300.0

# Sleep between phases.  Antithesis schedules its fault injections
# independently of driver progress, so a small pause between phases
# increases the chance that a fault lands *between* phases (the
# interesting scenario — restart between initialize and manipulate)
# vs. inside one phase (where testdrive's per-statement retry usually
# masks it).
INTER_PHASE_SLEEP_S = 1.0

# Allow-list of Check classes that run on the platform-checks topology
# (universal services + clusterd2; no external sources).  Keyed by
# class name so files containing multiple Check subclasses can pick
# the SUT-only ones.  The platform-checks group is the smallest one
# that exercises this driver — adding new Materialize-only Check
# classes upstream means adding them here.
ALLOWED_CHECK_NAMES: frozenset[str] = frozenset(
    {
        # aggregation.py
        "Aggregation",
        # alter_table.py
        "AlterTableAddColumn",
        # array_type.py
        "ArrayType",
        # boolean_type.py
        "BooleanType",
        # cast.py
        "Cast",
        # cluster.py
        "CreateCluster",
        "AlterCluster",
        "DropCluster",
        # comment.py
        "Comment",
        # commit.py
        "Commit",
        # constant_plan.py
        "ConstantPlan",
        # create_index.py
        "CreateIndex",
        # create_table.py
        "CreateTable",
        # databases.py
        "CheckDatabaseCreate",
        "CheckDatabaseDrop",
        # default_privileges.py
        "DefaultPrivileges",
        # delete.py
        "Delete",
        # drop_index.py
        "DropIndex",
        # drop_table.py
        "DropTable",
        # explain_catalog_item.py
        "ExplainCatalogItem",
        # float_types.py
        "DoubleType",
        "RealType",
        # having.py
        "Having",
        # insert_select.py
        "InsertSelect",
        # join_implementations.py
        "DeltaJoin",
        "LinearJoin",
        # join_types.py
        "JoinTypes",
        # jsonb_type.py
        "JsonbType",
        # large_tables.py — table-only stress, no external sources
        "WideRows",
        "ManyRows",
        # like.py
        "Like",
        # managed_cluster.py
        "CreateManagedCluster",
        "DropManagedCluster",
        # materialized_views.py
        "MaterializedViews",
        "MaterializedViewsAssertNotNull",
        "MaterializedViewsRefresh",
        "MaterializedViewReplacement",
        # materialize_type.py
        "MaterializeType",
        # nested_types.py
        "NestedTypes",
        # null_value.py
        "NullValue",
        # numeric_types.py
        "NumericTypes",
        # optimizer_notices.py
        "OptimizerNotices",
        # peek_cancellation.py
        "PeekCancellation",
        # peek_persist.py
        "PeekPersist",
        # ranges.py
        "Range",
        # regex.py
        "Regex",
        # rename_cluster.py
        "RenameCluster",
        # rename_index.py
        "RenameIndex",
        # rename_replica.py
        "RenameReplica",
        # rename_schema.py
        "RenameSchema",
        # rename_table.py
        "RenameTable",
        # rename_view.py
        "RenameView",
        # replica_targeted_mv.py
        "ReplicaTargetedMaterializedViews",
        # roles.py
        "CreateRole",
        "DropRole",
        "BuiltinRoles",
        # rollback.py
        "Rollback",
        # schemas.py
        "CheckSchemas",
        # statement_logging.py
        "StatementLogging",
        # string.py
        "String",
        # swap_cluster.py
        "SwapCluster",
        # swap_schema.py
        "SwapSchema",
        # table_data.py
        "TableData",
        # temporal_types.py
        "TemporalTypes",
        # text_bytea_types.py
        "TextByteaTypes",
        # threshold.py
        "Threshold",
        # update.py
        "Update",
        # uuid.py
        "UUID",
        # window_functions.py
        "WindowFunctions",
        # with_mutually_recursive.py
        "WithMutuallyRecursive",
    }
)


def _all_check_classes() -> list[type["Check"]]:
    """Import every Check subclass and return the allow-listed ones.

    The `from materialize.checks.all_checks import *` glob discovers
    every file under `all_checks/` and registers its Check subclasses
    via the usual subclass machinery.  Anything not in
    `ALLOWED_CHECK_NAMES` gets filtered out — those need topology
    additions (Kafka, Postgres source, …) that this group doesn't
    have.
    """
    # Resolve imports lazily so a module-level failure surfaces here
    # (where we can log it) rather than at driver-import time.
    from materialize.checks import all_checks  # noqa: F401
    from materialize.checks.checks import Check

    def _walk(cls: type) -> list[type]:
        result = [cls]
        for sub in cls.__subclasses__():
            result.extend(_walk(sub))
        return result

    candidates = [
        c for c in _walk(Check) if c is not Check and c.__name__ in ALLOWED_CHECK_NAMES
    ]
    # Stable sort by name so the fuzzer's choice is reproducible-ish
    # in local-dev (the SDK-routed choice still varies per timeline).
    candidates.sort(key=lambda c: c.__name__)
    return candidates


def _run_phase(
    check_name: str,
    phase: str,
    fragment: str,
) -> tuple[bool, bool, helper_testdrive.TestdriveResult]:
    """Run one testdrive fragment.

    Returns (succeeded, looks_transient, result).  Logs the outcome so
    Antithesis triage logs include phase-by-phase progress without
    relying on always/sometimes detail blobs alone.
    """
    LOG.info("check %s phase=%s: running fragment (%d bytes)",
             check_name, phase, len(fragment))
    result = helper_testdrive.run_inline(fragment, timeout_s=PHASE_TIMEOUT_S)
    LOG.info(
        "check %s phase=%s: exit=%d transient=%s",
        check_name, phase, result.exit_code, result.looks_transient,
    )
    return result.succeeded, result.looks_transient, result


def _query_mz_version() -> Any:
    """Read the running materialized version from `SELECT mz_version()`.

    The workload image has no cargo toolchain, so we can't use
    `MzVersion.parse_cargo()` (which spawns `cargo metadata`).  Instead
    we ask the live SUT — that's also the version Check `_can_run`
    gates actually care about.
    """
    from helper_pg import query_one_retry
    from materialize.mz_version import MzVersion

    # `mz_version()` returns "v0.NN.M (commit) (build-info)"; the parser
    # tolerates the trailing parenthesized parts.
    raw_row = query_one_retry("SELECT mz_version()")
    assert raw_row is not None, "mz_version() returned no row"
    raw = raw_row[0]
    # Take only the leading "vX.Y.Z..." token; MzVersion.parse_mz strips
    # the prefix.  Any trailing build info after the first whitespace
    # is irrelevant for version-gate decisions.
    head = raw.split()[0]
    return MzVersion.parse_mz(head)


def _make_executor_sentinel(version: Any) -> Any:
    """Build an Executor-shaped object for Check._can_run checks.

    A handful of upstream Check classes override `_can_run` to gate on
    `e.current_mz_version` (version-dependent feature availability).
    The Antithesis driver only ever runs against the current build, so
    we expose the current MzVersion on a tiny shim object that matches
    the attribute Check._can_run looks at.  Anything else accessed on
    the shim would be a Check-side assumption we'll discover by
    failing loud at runtime.
    """
    from materialize.checks.executors import Executor

    e = Executor()
    e.current_mz_version = version
    return e


def _pick_check(candidates: list[type["Check"]]) -> type["Check"] | None:
    """Pick one allow-listed Check at random.

    Skips Checks whose class-level `enabled` flag is False (the
    `@disabled` decorator) — those have an explicit upstream
    @ignore_reason that we honor here too.
    """
    enabled = [c for c in candidates if getattr(c, "enabled", True)]
    if not enabled:
        return None
    return helper_random.random_choice(enabled)


def main() -> int:
    candidates = _all_check_classes()
    LOG.info("loaded %d allow-listed Check classes", len(candidates))
    if not candidates:
        LOG.warning(
            "no Check classes found in allow-list; image may be missing "
            "the materialize.checks bundle (check workloads/platform-checks/"
            "mzbuild.yml pre-image:copy globs)"
        )
        return 0

    cls = _pick_check(candidates)
    if cls is None:
        LOG.warning("all allow-listed Checks have enabled=False; nothing to run")
        return 0

    LOG.info("picked Check class: %s", cls.__name__)

    # Construct the Check instance.  The base class accepts
    # (base_version, rng); we pass an AntithesisRandom so any
    # Check that consults `self.rng` draws from the SDK rather than
    # locking in after one seed.
    base_version = _query_mz_version()
    rng = helper_random.AntithesisRandom()
    check = cls(base_version=base_version, rng=rng)
    # `current_version` is set by `Check.start_initialize` / `start_manipulate` /
    # `start_validate` in the upstream framework, right before each phase runs.
    # The Antithesis driver short-circuits the Executor protocol and calls
    # `initialize()` / `manipulate()` / `validate()` directly, so set it here.
    # A handful of Checks (StatementLogging) read `self.current_version` from
    # inside their `validate()` to branch on whether the SUT is the same
    # version as the base — under Antithesis the SUT version never changes
    # mid-timeline (no upgrade injection yet), so base == current is
    # appropriate.
    check.current_version = base_version

    # Some Check classes override `_can_run(e)` to gate on
    # MzVersion features. Call it explicitly here so the assertion
    # space stays small when a Check declines to run.
    if not check._can_run(_make_executor_sentinel(base_version)):
        LOG.info("Check %s declined to run (_can_run returned False); exiting cleanly", cls.__name__)
        sometimes(
            False,
            "platform-check: at least one timeline saw _can_run gate skip a Check",
            {"check": cls.__name__},
        )
        return 0

    # Extract every testdrive fragment up front. Any failure here is
    # a Check authoring problem in upstream, not a property break —
    # raise loud rather than swallowing.
    initialize_td = check.initialize()
    manipulate_tds = check.manipulate()
    validate_td = check.validate()

    # The framework asserts manipulate() returns exactly two phases;
    # mirror that here so an upstream drift is caught early.
    assert len(manipulate_tds) == 2, (
        f"{cls.__name__}.manipulate() must return 2 phases, got "
        f"{len(manipulate_tds)}; upstream framework assumption broken"
    )

    # The Check methods may return Testdrive *or* PyAction. Antithesis
    # doesn't drive pyactions — those need a live psycopg connection
    # held across phases. Skip silently and record a sometimes anchor.
    from materialize.checks.actions import PyAction, Testdrive
    if not isinstance(initialize_td, Testdrive) or any(
        not isinstance(td, Testdrive) for td in manipulate_tds
    ) or not isinstance(validate_td, Testdrive):
        LOG.info(
            "Check %s uses PyAction phases; not supported by this driver",
            cls.__name__,
        )
        sometimes(
            False,
            "platform-check: at least one timeline saw a PyAction-only Check skipped",
            {"check": cls.__name__},
        )
        _ = PyAction  # quiet unused-import warning
        return 0

    # Phase order mirrors `NoRestartNoUpgrade`'s actions list, doubled
    # on validate() to mirror `RestartEnvironmentdClusterdStorage`'s
    # second-validate (which is the framework's idempotence check).
    phases: list[tuple[str, str]] = [
        ("initialize", initialize_td.input),
        ("manipulate[0]", manipulate_tds[0].input),
        ("manipulate[1]", manipulate_tds[1].input),
        ("validate[0]", validate_td.input),
        ("validate[1]", validate_td.input),
    ]

    all_clean = True
    saw_transient = False
    phase_results: dict[str, dict[str, Any]] = {}

    for phase_name, fragment in phases:
        if INTER_PHASE_SLEEP_S > 0:
            time.sleep(INTER_PHASE_SLEEP_S)
        succeeded, looks_transient, result = _run_phase(
            cls.__name__, phase_name, fragment
        )
        phase_results[phase_name] = {
            "exit_code": result.exit_code,
            "succeeded": succeeded,
            "transient": looks_transient,
            "stderr_tail": result.stderr[-800:],
        }
        if not succeeded:
            if looks_transient:
                saw_transient = True
                LOG.info(
                    "check %s phase=%s: transient failure (faults likely); not a regression",
                    cls.__name__,
                    phase_name,
                )
                # On a transient failure we abort the cycle. The next
                # phase relies on objects created by the previous one;
                # continuing past a faulted CREATE TABLE would chain
                # downstream failures that aren't property-relevant.
                break
            else:
                all_clean = False
                LOG.error(
                    "check %s phase=%s: NON-TRANSIENT failure",
                    cls.__name__, phase_name,
                )
                break

    # Safety property: any non-fault failure at any phase, when run
    # against a healthy SUT, is a Check-class regression.  Always(True)
    # if every phase either succeeded or bailed on a fault-shaped
    # error.
    always(
        all_clean,
        "platform-check: Check class runs to completion without non-fault failure",
        {
            "check": cls.__name__,
            "phases": phase_results,
            "saw_transient": saw_transient,
        },
    )

    # Liveness anchor: at least one timeline ran every phase cleanly
    # (no transient failure observed anywhere).  Without this, the
    # safety always(True) could be vacuously satisfied by a timeline
    # that bails on faults in the first phase.
    sometimes(
        all_clean and not saw_transient,
        "platform-check: Check class completed every phase under no observed fault",
        {"check": cls.__name__},
    )

    LOG.info(
        "platform-check %s done; all_clean=%s saw_transient=%s",
        cls.__name__, all_clean, saw_transient,
    )
    return 0


if __name__ == "__main__":
    # Quiet unused-import lint for helper modules whose env-constant
    # side effects we rely on (driver inherits PG* env defaults from
    # helper_pg via PYTHONPATH on the workload image).
    _ = os
    sys.exit(main())
