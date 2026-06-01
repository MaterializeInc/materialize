# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Repro harness for CLU-95:

    thread 'coordinator' panicked at src/compute-client/src/as_of_selection.rs:
    failed to apply hard as-of constraint
    (id=u732, ..., reason="storage export u732 write frontier")

The panic fires during `Coordinator::bootstrap` -> `bootstrap_dataflow_as_ofs`
-> `mz_compute_client::as_of_selection::run` when, for some materialized view
`mv`, the *read frontier (since)* of one of `mv`'s storage inputs has advanced
PAST `mv`'s own durable *write frontier (upper)*. as-of selection then derives a
lower bound (input.since) that is greater than the upper bound
(step_back(mv.upper)), the hard "storage export write frontier" constraint
cannot be applied, and `soft_panic_or_log!` panics (soft-asserts are ON in these
images, so it is a hard crash).

THEORY (see CLU-95-CONTINUATION.md at the repo root for the full write-up):
The invariant `input.since <= step_back(mv.upper)` is maintained at runtime by a
read hold the controller keeps on `mv`'s inputs, gated on `mv`'s *durable* upper.
In a single read-write environment that hold is a persist *critical* SinceHandle
and never expires, so the invariant holds across restarts. During a 0dt upgrade,
the read-only "new" environment instead holds its inputs with persist *leased*
ReadHandles. Those participate in the shard `since` (it is the meet of all leased
+ critical reader sinces, `persist-client/src/internal/state.rs:update_since`),
BUT on lease *expiry* the meet is deliberately NOT recomputed
(`expire_leased_reader`, disabled `update_since`, database-issues#6885). So if the
new env's leased hold on an input lapses (process down longer than the lease, a
hold dropped+re-acquired, or never installed for some required input), the next
critical `compare_and_downgrade_since` by the *leader* recomputes the `since` over
the remaining readers and can jump it forward past a dependent MV's upper. Persist
`since` never regresses, so once corrupted the bad state survives, and every
subsequent bootstrap of the new env panics.

This harness tries to provoke that condition. It is a REPRO HUNT, not a proven
deterministic reproducer (see the "Why this may not fire" note in the
continuation doc): with a vanilla shared MV the leader's own critical hold keeps
`input.since <= mv.upper`, so the most promising lever is to make an input stop
being held by the *leader* on the MV's behalf (DROP/recreate, or a builtin/new MV
the leader lacks) while the follower's leased hold lapses. The knobs below are
meant to be turned.

Run on a Linux box, e.g.:

    bin/mzcompose --find clu-95-repro down -v
    bin/mzcompose --find clu-95-repro run zdt-soak --iterations 40 --lease-seconds 5
    bin/mzcompose --find clu-95-repro run restart-soak --iterations 60
"""

import time
from textwrap import dedent
from threading import Event, Thread

from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import (
    Materialized,
)
from materialize.mzcompose.services.metadata_store import CockroachOrPostgresMetadata
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive

# Short reader-lease duration so a follower that is down (or stalls its
# heartbeat) loses its leased read holds quickly. The leader's compaction can
# then jump shard sinces forward once the follower's hold is gone.
DEFAULT_LEASE_SECONDS = 5

SYSTEM_PARAMETER_DEFAULTS = get_default_system_parameters()
SYSTEM_PARAMETER_DEFAULTS.update(
    {
        # Make leased read holds expire fast (default is 15 min).
        "persist_reader_lease_duration": f"{DEFAULT_LEASE_SECONDS}s",
    }
)

# The panic substring the coordinator emits. ci-regexp in CLU-95 is the same.
AS_OF_PANIC = "failed to apply hard as-of constraint"

SERVICES = [
    CockroachOrPostgresMetadata(),
    Mz(app_password=""),
    Materialized(
        name="mz_old",
        sanity_restart=False,
        deploy_generation=0,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        external_metadata_store=True,
        # We want to keep the leader alive and scan logs ourselves rather than
        # have mzcompose abort on the first crash.
        propagate_crashes=False,
        default_replication_factor=1,
    ),
    Materialized(
        name="mz_new",
        sanity_restart=False,
        deploy_generation=1,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        external_metadata_store=True,
        # Let it restart-loop on the panic so its logs survive for scanning.
        restart="on-failure",
        propagate_crashes=False,
        default_replication_factor=1,
    ),
    # Single-environment env for the restart-soak workflow. Uses
    # sanity_restart-style ungraceful kills, mirroring the workload-replay
    # scenario where CLU-95 was first seen.
    Materialized(
        name="mz_solo",
        sanity_restart=False,
        deploy_generation=0,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        external_metadata_store=True,
        propagate_crashes=False,
        default_replication_factor=1,
    ),
    Testdrive(
        materialize_url="postgres://materialize@mz_old:6875",
        materialize_url_internal="postgres://materialize@mz_old:6877",
        mz_service="mz_old",
        no_reset=True,
        seed=1,
        default_timeout="120s",
    ),
]


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _scan_for_panic(c: Composition, service: str) -> str | None:
    """Return the first log line mentioning the as-of panic, else None."""
    try:
        logs = c.invoke("logs", service, capture=True).stdout
    except Exception as e:
        print(f"  (could not capture logs for {service}: {e})")
        return None
    for line in logs.splitlines():
        if AS_OF_PANIC in line:
            return line
    return None


def _fail_if_panic(c: Composition, service: str) -> None:
    line = _scan_for_panic(c, service)
    if line is not None:
        raise AssertionError(
            f"CLU-95 reproduced! {service} hit the as-of panic:\n  {line}"
        )


def _setup_objects(c: Composition, service: str) -> None:
    """Create a dependency chain of MVs plus a REFRESH MV.

    The chain (t -> mv1 -> mv2) maximizes the number of MV-output -> input
    edges, which are exactly the edges that are NOT tracked by the storage
    controller (data_source = Other) and therefore rely entirely on
    compute-controller read holds. The REFRESH MV is included because its
    write frontier moves in large jumps and lags far behind its inputs, which
    widens the window where input.since can overtake mv.upper.
    """
    c.sql(
        dedent(
            """
            DROP TABLE IF EXISTS t CASCADE;
            CREATE TABLE t (a bigint, b bigint);
            INSERT INTO t SELECT x, x FROM generate_series(1, 1000) AS x;

            CREATE MATERIALIZED VIEW mv1 AS
                SELECT a, b, a + b AS s FROM t;
            CREATE MATERIALIZED VIEW mv2 AS
                SELECT a % 100 AS k, sum(s) AS total FROM mv1 GROUP BY a % 100;

            CREATE MATERIALIZED VIEW mv_refresh
                WITH (REFRESH EVERY '10 seconds')
                AS SELECT count(*) AS n, sum(a) AS sa FROM t;

            -- The default logical compaction window (~1s) already makes the
            -- inputs' read frontiers chase their write frontiers aggressively
            -- once read holds permit, which is what we want here.
            """
        ),
        service=service,
    )


def _load_loop(c: Composition, service: str, stop: Event) -> None:
    """Continuously mutate `t` so the MVs (and their inputs) keep advancing and
    compacting on the leader."""
    i = 0
    while not stop.is_set():
        try:
            c.sql(
                f"INSERT INTO t SELECT x, x FROM generate_series({i}, {i + 200}) AS x;"
                "DELETE FROM t WHERE a < %d;" % max(0, i - 500),
                service=service,
            )
        except Exception as e:
            # The leader should stay up; log and keep going.
            print(f"  (load loop error: {e})")
            time.sleep(0.5)
        i += 100
        time.sleep(0.2)


def _wait_until_queryable(c: Composition, service: str, timeout: int = 120) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            c.sql("SELECT 1", service=service)
            return True
        except Exception:
            if _scan_for_panic(c, service) is not None:
                return False
            time.sleep(1)
    return False


# --------------------------------------------------------------------------- #
# workflows
# --------------------------------------------------------------------------- #


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    workflow_zdt_soak(c, parser)


def workflow_zdt_soak(c: Composition, parser: WorkflowArgumentParser) -> None:
    """0dt soak: leader (mz_old) + read-only follower (mz_new).

    Each iteration: (re)boot the read-only follower (its bootstrap runs as-of
    selection -> the panic site), verify it can read the MVs, scan its logs,
    then kill it and stay down longer than the reader lease while the leader
    keeps advancing and compacting. Optionally DROP/recreate an MV on the leader
    mid-soak to remove the leader's own hold on a shared input.
    """
    parser.add_argument("--iterations", type=int, default=40)
    parser.add_argument("--lease-seconds", type=int, default=DEFAULT_LEASE_SECONDS)
    parser.add_argument(
        "--drop-recreate",
        action="store_true",
        help="DROP and recreate mv2 on the leader each iteration, to drop the "
        "leader's hold on mv1 while the follower still expects mv2.",
    )
    args = parser.parse_args()

    SYSTEM_PARAMETER_DEFAULTS["persist_reader_lease_duration"] = (
        f"{args.lease_seconds}s"
    )

    c.down(destroy_volumes=True)
    c.up("mz_old")
    _setup_objects(c, "mz_old")

    stop = Event()
    loader = Thread(target=_load_loop, args=(c, "mz_old", stop), daemon=True)
    loader.start()

    try:
        for it in range(args.iterations):
            print(f"== zdt-soak iteration {it} ==")

            if args.drop_recreate:
                try:
                    c.sql(
                        "DROP MATERIALIZED VIEW IF EXISTS mv2;"
                        "CREATE MATERIALIZED VIEW mv2 AS "
                        "SELECT a % 100 AS k, sum(s) AS total FROM mv1 GROUP BY a % 100;",
                        service="mz_old",
                    )
                except Exception as e:
                    print(f"  (drop/recreate error: {e})")

            # Boot / reboot the read-only follower. This runs bootstrap ->
            # as-of selection, the panic site.
            c.up("mz_new")

            if not _wait_until_queryable(c, "mz_new"):
                _fail_if_panic(c, "mz_new")
                raise AssertionError("mz_new did not become queryable (no panic seen)")

            # Force the as-of-selected dataflows to actually be exercised.
            try:
                c.sql(
                    "SELECT count(*) FROM mv1;"
                    "SELECT count(*) FROM mv2;"
                    "SELECT * FROM mv_refresh;",
                    service="mz_new",
                )
            except Exception as e:
                print(f"  (read error on mz_new: {e})")

            _fail_if_panic(c, "mz_new")

            # Kill the follower and stay down past the lease so its leased read
            # holds expire while the leader keeps compacting.
            c.kill("mz_new")
            time.sleep(args.lease_seconds * 3)
    finally:
        stop.set()
        loader.join(timeout=10)

    # One last reboot + scan after the inputs have advanced a lot.
    c.up("mz_new")
    _wait_until_queryable(c, "mz_new")
    _fail_if_panic(c, "mz_new")
    print("zdt-soak completed without reproducing the panic")


def workflow_restart_soak(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Single-environment soak mirroring workload-replay's sanity_restart:
    ungraceful SIGKILL + restart under continuous load with MV chains, REFRESH
    MVs, and concurrent DDL. Scans for the as-of panic on every boot.
    """
    parser.add_argument("--iterations", type=int, default=60)
    args = parser.parse_args()

    c.down(destroy_volumes=True)
    c.up("mz_solo")
    _setup_objects(c, "mz_solo")

    stop = Event()
    loader = Thread(target=_load_loop, args=(c, "mz_solo", stop), daemon=True)
    loader.start()

    try:
        for it in range(args.iterations):
            print(f"== restart-soak iteration {it} ==")
            # Let the MVs make progress and inputs compact.
            time.sleep(3)

            # Occasionally churn DDL to perturb read holds.
            if it % 5 == 4:
                try:
                    c.sql(
                        "DROP MATERIALIZED VIEW IF EXISTS mv2;"
                        "CREATE MATERIALIZED VIEW mv2 AS "
                        "SELECT a % 100 AS k, sum(s) AS total FROM mv1 GROUP BY a % 100;",
                        service="mz_solo",
                    )
                except Exception as e:
                    print(f"  (ddl churn error: {e})")

            # Ungraceful restart (like kill + up in sanity_restart_mz).
            stop.set()
            loader.join(timeout=10)
            c.kill("mz_solo")
            c.up("mz_solo")

            if not _wait_until_queryable(c, "mz_solo"):
                _fail_if_panic(c, "mz_solo")
                raise AssertionError("mz_solo did not come back (no panic seen)")
            _fail_if_panic(c, "mz_solo")

            # Restart the load loop for the next iteration.
            stop = Event()
            loader = Thread(target=_load_loop, args=(c, "mz_solo", stop), daemon=True)
            loader.start()
    finally:
        stop.set()
        loader.join(timeout=10)

    _fail_if_panic(c, "mz_solo")
    print("restart-soak completed without reproducing the panic")
