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

After 2026-06-01: build 1248 evidence (see CLU-95-CONTINUATION.md) recasts
the live theory as the compute `remove_replica` hold-accounting bug
(incidents-and-escalations#39). The two newer workflows target that mechanism
directly using standalone Clusterd services so envd and clusterd can be
killed independently:

    bin/mzcompose --find clu-95-repro run cancelled-peek-reconnect --iterations 30
    bin/mzcompose --find clu-95-repro run replica-removal-under-load --iterations 40
"""

import time
from textwrap import dedent
from threading import Event, Thread

import psycopg
from psycopg import sql as psycopg_sql

from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.clusterd import Clusterd
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
        # Enable mz_unsafe.mz_sleep, used by the cancelled-peek-reconnect
        # workflow to deterministically widen the install window so the cancel
        # can land between dataflow install and replica acknowledgement.
        "unsafe_enable_unsafe_functions": "true",
    }
)

# The panic substring the coordinator emits. ci-regexp in CLU-95 is the same.
AS_OF_PANIC = "failed to apply hard as-of constraint"
# The hazard WARN added by PR #35937 as the diagnostic marker for the
# `remove_replica` hold-accounting bug (incidents-and-escalations#39). Even if
# we don't get the bootstrap panic on a given iteration, this WARN firing means
# the bug class is being triggered.
HAZARD_WARN = "dropping per-replica read hold without equivalent global read hold"

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
    #
    # support_external_clusterd=True is required by the cancelled-peek-reconnect
    # and replica-removal-under-load workflows: they create unmanaged cluster
    # replicas pointed at the standalone Clusterd services below so we can
    # docker-kill / docker-pause / restart clusterd independently of envd.
    Materialized(
        name="mz_solo",
        sanity_restart=False,
        deploy_generation=0,
        system_parameter_defaults=SYSTEM_PARAMETER_DEFAULTS,
        external_metadata_store=True,
        propagate_crashes=False,
        default_replication_factor=1,
        support_external_clusterd=True,
    ),
    # Standalone clusterd processes for the new workflows.
    # `clusterd_repro` hosts the MV/dataflows in cancelled-peek-reconnect.
    # `clusterd_writer` + `clusterd_compute` model the build 1248 setup where
    # one cluster writes the MV and a different cluster has the dataflows
    # whose replica is dropped under load.
    Clusterd(name="clusterd_repro", mz_service="mz_solo"),
    Clusterd(name="clusterd_writer", mz_service="mz_solo"),
    Clusterd(name="clusterd_compute", mz_service="mz_solo"),
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


def _scan_for_warn(c: Composition, service: str) -> str | None:
    """Return the first log line emitting the hazard WARN, else None."""
    try:
        logs = c.invoke("logs", service, capture=True).stdout
    except Exception as e:
        print(f"  (could not capture logs for {service}: {e})")
        return None
    for line in logs.splitlines():
        if HAZARD_WARN in line:
            return line
    return None


def _flag_hazard_warn(c: Composition, service: str, label: str) -> None:
    """Print (but do not fail) if the hazard WARN appears.

    The WARN at compute-client::Instance::remove_replica is the diagnostic
    marker for the underlying bug class. The bootstrap panic (CLU-95) is a
    downstream consequence; the WARN can fire without the panic firing, but if
    we ever see it during a workflow we want to know the workflow is on the
    right track.
    """
    line = _scan_for_warn(c, service)
    if line is not None:
        print(f"  [{label}] HAZARD WARN observed on {service}:\n    {line}")


def _enable_external_replicas(c: Composition, service: str) -> None:
    """Allow CREATE CLUSTER REPLICA with explicit storagectl/computectl addrs."""
    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = on;",
        port=6877,
        user="mz_system",
        service=service,
    )


def _create_unmanaged_cluster(
    c: Composition,
    service: str,
    cluster_name: str,
    clusterd_name: str,
    workers: int = 1,
) -> None:
    """Create an unmanaged cluster with one replica pinned at `clusterd_name`.

    Storage/compute addresses point at the standalone Clusterd service, so
    `c.kill(clusterd_name)` / `c.up(clusterd_name)` controls the replica's
    process lifecycle independently of envd.
    """
    c.sql(
        dedent(f"""
            DROP CLUSTER IF EXISTS {cluster_name} CASCADE;
            CREATE CLUSTER {cluster_name} REPLICAS ();
            CREATE CLUSTER REPLICA {cluster_name}.r1
                STORAGECTL ADDRESSES ['{clusterd_name}:2100'],
                STORAGE ADDRESSES ['{clusterd_name}:2103'],
                COMPUTECTL ADDRESSES ['{clusterd_name}:2101'],
                COMPUTE ADDRESSES ['{clusterd_name}:2102'],
                WORKERS {workers};
            GRANT ALL ON CLUSTER {cluster_name} TO materialize;
            """),
        port=6877,
        user="mz_system",
        service=service,
    )


def _slow_query_with_cancel(
    c: Composition,
    service: str,
    cluster: str,
    sql: str,
    cancel_after_seconds: float,
) -> None:
    """Issue `sql` on `cluster` in a thread, cancel it via psycopg after a delay.

    Cancel uses the psycopg native `Connection.cancel()`, which is the same
    pg-cancel-backend RPC the SQL `pg_cancel_backend(pid)` function issues.
    Returns when the cancel has been signaled (the thread may still be tearing
    down). The whole point is to leave the controller in the state where the
    user's peek dropped its global read holds while the replica is still in
    the middle of installing the dataflow.
    """
    port = c.port(service, 6875)
    conn_str = f"postgres://materialize@127.0.0.1:{port}/materialize"

    def _run() -> None:
        try:
            with psycopg.connect(conn_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        psycopg_sql.SQL("SET CLUSTER = {}").format(
                            psycopg_sql.Identifier(cluster)
                        )
                    )

                    def _cancel() -> None:
                        try:
                            conn.cancel_safe()
                        except Exception as exc:
                            print(f"  (cancel error: {exc})")

                    timer = Thread(
                        target=lambda: (time.sleep(cancel_after_seconds), _cancel())
                    )
                    timer.daemon = True
                    timer.start()
                    try:
                        cur.execute(psycopg_sql.SQL(sql))  # type: ignore[arg-type]
                        cur.fetchall()
                    except psycopg.errors.QueryCanceled:
                        pass
        except Exception as exc:
            # The cancel often races with connection close. Don't fail; this is
            # the desired endpoint of the operation.
            print(f"  (slow-query thread saw: {exc})")

    t = Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=cancel_after_seconds + 10)


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
        dedent("""
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
            """),
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
    """Run all repro workflows in sequence, each as its own test case."""
    parser.parse_args()

    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


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


def workflow_cancelled_peek_reconnect(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Teskje variant 1 — cancelled slow-path peek + clusterd reconnect.

    Sequence (per iteration):
      1. Issue a slow SELECT on the dedicated cluster (held by `clusterd_repro`).
      2. Cancel the query just after the controller has installed dataflow holds
         but before clusterd finishes rendering. This drops the GLOBAL read
         holds while the per-replica holds are still in flight on the replica.
      3. Force a controller-replica reconnect by killing clusterd. On reconnect
         the controller drops + reinitializes replica state, which releases
         the per-replica holds without re-installing them (because the global
         holds for the now-cancelled peek are gone).
      4. Sleep > 1 compaction window so storage controller advances input.since
         past where any lagging MV upper sits.
      5. Ungraceful restart of envd. Bootstrap reads the durable state.

    Then scan logs for the bootstrap panic + the diagnostic hazard WARN.
    """
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument(
        "--cancel-after-ms",
        type=int,
        default=500,
        help="Cancel the peek this many ms after issuing it. Should be smaller "
        "than --sleep-seconds so the cancel lands mid-render.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=5,
        help="mz_unsafe.mz_sleep duration inside the slow query, widening the "
        "window between dataflow install and rendering completion.",
    )
    parser.add_argument(
        "--post-kill-sleep",
        type=float,
        default=20.0,
        help="Seconds to wait after killing clusterd to let inputs compact "
        "forward before restarting envd.",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=int,
        default=60,
        help="REFRESH EVERY interval for the lagging MV. Larger = wider gap "
        "between the durable upper and global hold during a per-replica drop.",
    )
    args = parser.parse_args()

    c.down(destroy_volumes=True)
    c.up("mz_solo")
    _enable_external_replicas(c, "mz_solo")
    _create_unmanaged_cluster(c, "mz_solo", "crepro", "clusterd_repro", workers=2)
    c.up("clusterd_repro")

    # Build the dependency chain on the targeted cluster so its dataflows hold
    # storage-input read holds. Use a REFRESH MV so the dependent's upper lags.
    c.sql(
        dedent(f"""
            DROP TABLE IF EXISTS t CASCADE;
            CREATE TABLE t (a bigint, b bigint);
            INSERT INTO t SELECT x, x FROM generate_series(1, 5000) AS x;

            SET CLUSTER = crepro;
            CREATE MATERIALIZED VIEW mv_chain AS
                SELECT a, b, a + b AS s FROM t;
            CREATE MATERIALIZED VIEW mv_refresh
                WITH (REFRESH EVERY '{args.refresh_seconds} seconds')
                AS SELECT count(*) AS n, sum(a) AS sa FROM t;
            """),
        service="mz_solo",
    )

    stop = Event()
    loader = Thread(target=_load_loop, args=(c, "mz_solo", stop), daemon=True)
    loader.start()

    # A slow-path query that deterministically takes `--sleep-seconds`. The
    # mz_unsafe.mz_sleep call runs inside the rendered dataflow and parks the
    # worker thread non-yielding, giving us a wide, reproducible window
    # between dataflow install (per-replica holds in place) and rendering
    # completion (during which we cancel + force a clusterd reconnect).
    slow_sql = dedent(f"""
        SELECT count(*)
        FROM t AS t1
        CROSS JOIN generate_series(1, 50) AS t2
        WHERE t1.a + t2 > mz_unsafe.mz_sleep({args.sleep_seconds})::int;
        """)

    # Phase 1: perturbations under one long-running env. We do NOT restart
    # envd between iterations — that would wipe the in-memory controller
    # state we're trying to corrupt. The build 1248 manifestation came from
    # ~22 minutes of workload activity followed by a SINGLE ungraceful
    # restart.
    try:
        for it in range(args.iterations):
            print(f"== cancelled-peek-reconnect iteration {it} ==")

            _slow_query_with_cancel(
                c,
                service="mz_solo",
                cluster="crepro",
                sql=slow_sql,
                cancel_after_seconds=args.cancel_after_ms / 1000.0,
            )

            # Force controller-replica reconnect. On disconnect the controller
            # may drop replica state; on reconnect it reinitializes without
            # restoring the cancelled peek's per-replica holds.
            c.kill("clusterd_repro")
            c.up("clusterd_repro")

            # Let storage controller advance input.since past any lagging
            # MV upper.
            time.sleep(args.post_kill_sleep)

            _flag_hazard_warn(c, "mz_solo", f"iter {it}")
    finally:
        stop.set()
        loader.join(timeout=10)

    # Phase 2: one ungraceful restart, mirroring sanity_restart_mz. as-of
    # selection runs on bootstrap; if input.since has overtaken any MV's
    # durable upper, this is where the panic fires.
    print("== final sanity_restart_mz on mz_solo ==")
    c.kill("mz_solo")
    c.up("mz_solo")

    if not _wait_until_queryable(c, "mz_solo"):
        _fail_if_panic(c, "mz_solo")
        raise AssertionError("mz_solo did not come back after final restart")

    _fail_if_panic(c, "mz_solo")
    _flag_hazard_warn(c, "mz_solo", "final")
    print("cancelled-peek-reconnect completed without reproducing the panic")


def workflow_replica_removal_under_load(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Build 1248 variant — long-lived MV + replica removal under load.

    Mirrors the observed manifestation in release-qualification build 1248:
    a writer cluster hosts a slow-upper MV, a separate compute cluster runs
    dataflows that drive the global hold on the same inputs forward, then the
    compute replica is dropped. With the per-replica hold gone and the global
    hold advanced, the writer MV's input compacts past its durable upper.
    Subsequent envd restart hits the as-of bootstrap panic.

    Uses unmanaged cluster replicas pinned at separate Clusterd processes so
    we can DROP CLUSTER REPLICA / re-create cleanly with the same address.
    """
    parser.add_argument("--iterations", type=int, default=40)
    parser.add_argument(
        "--post-drop-sleep",
        type=float,
        default=8.0,
        help="Seconds to wait after the compute replica is dropped, before "
        "killing envd.",
    )
    args = parser.parse_args()

    c.down(destroy_volumes=True)
    c.up("mz_solo")
    _enable_external_replicas(c, "mz_solo")
    _create_unmanaged_cluster(c, "mz_solo", "cwriter", "clusterd_writer", workers=2)
    _create_unmanaged_cluster(c, "mz_solo", "ccompute", "clusterd_compute", workers=2)
    c.up("clusterd_writer")
    c.up("clusterd_compute")

    c.sql(
        dedent("""
            DROP TABLE IF EXISTS t CASCADE;
            CREATE TABLE t (a bigint, b bigint);
            INSERT INTO t SELECT x, x FROM generate_series(1, 5000) AS x;

            SET CLUSTER = cwriter;
            -- Writer MV: relatively large batches & a REFRESH variant so its
            -- durable upper lags behind any "now()" hold by tens of seconds.
            CREATE MATERIALIZED VIEW mv_slow
                WITH (REFRESH EVERY '20 seconds')
                AS SELECT count(*) AS n, sum(a) AS sa FROM t;
            """),
        service="mz_solo",
    )

    stop = Event()
    loader = Thread(target=_load_loop, args=(c, "mz_solo", stop), daemon=True)
    loader.start()

    # Phase 1: drive the bug class repeatedly under one long-running env.
    try:
        for it in range(args.iterations):
            print(f"== replica-removal-under-load iteration {it} ==")

            # Install + drop ephemeral dataflows on ccompute that read `t`.
            # These advance the global compute hold on `t` forward while the
            # writer MV's per-replica hold sits at step_back(mv_slow.upper).
            for i in range(5):
                try:
                    c.sql(
                        dedent(f"""
                            SET CLUSTER = ccompute;
                            CREATE MATERIALIZED VIEW mv_churn_{it}_{i} AS
                                SELECT a, b FROM t WHERE a % 7 = {i};
                            """),
                        service="mz_solo",
                    )
                except Exception as e:
                    print(f"  (churn create error: {e})")
            time.sleep(2)
            for i in range(5):
                try:
                    c.sql(
                        f"DROP MATERIALIZED VIEW IF EXISTS mv_churn_{it}_{i};",
                        service="mz_solo",
                    )
                except Exception as e:
                    print(f"  (churn drop error: {e})")

            # Drop+recreate the compute replica. The DROP CLUSTER REPLICA path
            # runs compute Instance::remove_replica; if any per-replica hold on
            # `t` is tighter than the global at that moment, the WARN fires
            # and inputs can compact past where the writer's MV needs.
            try:
                c.sql(
                    dedent("""
                        DROP CLUSTER REPLICA ccompute.r1;
                        CREATE CLUSTER REPLICA ccompute.r1
                            STORAGECTL ADDRESSES ['clusterd_compute:2100'],
                            STORAGE ADDRESSES ['clusterd_compute:2103'],
                            COMPUTECTL ADDRESSES ['clusterd_compute:2101'],
                            COMPUTE ADDRESSES ['clusterd_compute:2102'],
                            WORKERS 2;
                        """),
                    port=6877,
                    user="mz_system",
                    service="mz_solo",
                )
            except Exception as e:
                print(f"  (replica drop/recreate error: {e})")

            time.sleep(args.post_drop_sleep)
            _flag_hazard_warn(c, "mz_solo", f"iter {it}")
    finally:
        stop.set()
        loader.join(timeout=10)

    # Phase 2: one ungraceful envd restart. If the persist `since` on `t`
    # has overtaken `mv_slow.upper`, bootstrap as-of selection panics.
    print("== final sanity_restart_mz on mz_solo ==")
    c.kill("mz_solo")
    c.up("mz_solo")

    if not _wait_until_queryable(c, "mz_solo"):
        _fail_if_panic(c, "mz_solo")
        raise AssertionError("mz_solo did not come back after final restart")

    _fail_if_panic(c, "mz_solo")
    _flag_hazard_warn(c, "mz_solo", "final")
    print("replica-removal-under-load completed without reproducing the panic")
