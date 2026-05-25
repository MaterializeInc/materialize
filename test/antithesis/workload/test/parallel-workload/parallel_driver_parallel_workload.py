#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Antithesis driver wrapping the real `materialize.parallel_workload`.

Earlier versions of this file reimplemented the *idea* of parallel-workload
(a fixed pool of objects, worker threads racing CREATE/DROP/INSERT/etc.).
That diverged from the canonical stress driver and forced us to rederive the
catalog-race error catalog by hand. This module instead bundles the real
`materialize.parallel_workload` package into the workload image (see
`mzbuild.yml` + `Dockerfile`) and invokes its `Worker`, `Action`,
`ActionList`, and `Database` classes directly.

A few pieces of upstream's `parallel_workload.run()` orchestration don't
translate to the Antithesis topology:

  * Faults are injected at the container layer by Antithesis itself, so we
    don't spawn `KillAction`/`BackupRestoreAction`/`ZeroDowntimeDeployAction`
    worker threads (each calls into `composition.kill/up/exec`, which the
    workload container can't reach). We still tag the database with
    `Scenario.Kill` so each `Action.errors_to_ignore` includes
    connection-shaped errors — those are expected here. `CancelAction` is
    the one scenario worker we do spawn, since `pg_cancel_backend` racing
    DDL/DML is a SQL-only operation and adds coverage the container-fault
    path doesn't replicate.
  * `Database.create` unconditionally calls `setup_polaris_for_iceberg(...)`
    and creates `postgres_conn` / `sql_server_conn` against services that
    aren't in the Antithesis compose. We override `create` to skip that
    setup and only wire up the kafka + minio connections the topology
    actually has.
  * `parallel_workload.run()` tunes a long list of `ALTER SYSTEM SET` knobs
    and recreates the `quickstart` cluster. We skip the recreate (would
    fight with `antithesis_cluster`) and apply only the size-limit knobs.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from typing import Any

import helper_logging
import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_fault_tolerance import looks_like_fault
from helper_pg import (
    CONNECT_TIMEOUT_S,
    PGDATABASE,
    PGHOST,
    PGPORT,
    PGPORT_INTERNAL,
    PGUSER,
    PGUSER_INTERNAL,
)

from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload import executor as _pw_executor
from materialize.parallel_workload.action import (
    Action,
    CancelAction,
    ddl_action_list,
    dml_nontrans_action_list,
    fetch_action_list,
    read_action_list,
    write_action_list,
)
from materialize.parallel_workload.database import (
    MAX_CLUSTER_REPLICAS,
    MAX_CLUSTERS,
    MAX_KAFKA_SINKS,
    MAX_KAFKA_SOURCES,
    MAX_POSTGRES_SOURCES,
    MAX_ROLES,
    MAX_SCHEMAS,
    MAX_TABLES,
    MAX_VIEWS,
    MAX_WEBHOOK_SOURCES,
    Database,
)
from materialize.parallel_workload.executor import Executor
from materialize.parallel_workload.settings import Complexity, Scenario
from materialize.parallel_workload.worker import Worker
from materialize.parallel_workload.worker_exception import WorkerFailedException

# `parallel_workload.executor` declares module-level `logging: TextIO | None`
# and `lock: threading.Lock` as PEP-526 annotations only; they are bound by
# `initialize_logging()`. `Executor.log()` does `if not logging: return`,
# which raises `NameError` before that initialiser runs. We don't want the
# per-query log file (drivers run many times under Antithesis); bind both
# names to no-op values so `log()` returns immediately.
_pw_executor.logging = None
_pw_executor.lock = threading.Lock()

# Upstream gates `"canceling statement due to user request"` on
# `Scenario.Cancel`, but our `Database` is tagged `Scenario.Kill` to pick up
# the connection-shape tolerance set (the two enum tracks are
# scenario-disjoint upstream). Under Antithesis we spawn a CancelAction
# worker alongside the regular pool, so the regular workers WILL see the
# cancel string mid-DDL/DML and must tolerate it. Patch the base class so
# every subclass's `errors_to_ignore` (which all chain through
# `super().errors_to_ignore(exe)`) picks it up.
_orig_errors_to_ignore = Action.errors_to_ignore


def _antithesis_errors_to_ignore(self: Action, exe: Executor) -> list[str]:
    return _orig_errors_to_ignore(self, exe) + [
        "canceling statement due to user request",
    ]


Action.errors_to_ignore = _antithesis_errors_to_ignore  # type: ignore[method-assign]

# `data_ingest.PgExecutor.create()` defaults to `host="127.0.0.1"` because
# in a standard mzcompose run postgres' 5432 is bound to the host loopback.
# Antithesis is container-to-container with no host binding, so the
# workload container has to reach postgres-source by service name (aliased
# to `postgres` for cross-compatibility with checked-in testdrive scripts).
# Set the override before any worker imports happen.
os.environ.setdefault("MZ_DATA_INGEST_PG_HOST", "postgres")

LOG = helper_logging.setup_logging("driver.parallel_workload")

# Per-invocation runtime budget for the worker pool. Sized for substantive
# coverage per iteration: most of the framework's value (cross-action races,
# DDL-DML-fetch interleaving, source/sink wiring) only shows up once the
# seed-scoped database has accumulated objects, which costs ~10-30s of
# setup-shaped work before the interesting steady state begins. The Test
# Composer still re-launches the driver freely; Antithesis runs typically
# fit several invocations per timeline.
RUNTIME_S = float(os.environ.get("PW_RUNTIME_S", "300"))
NUM_THREADS = int(os.environ.get("PW_THREADS", "4"))

# Number of long-lived pool_cluster_<i> clusters the workload-entrypoint
# bootstrapped, one per clusterd-pool-<i> container. Must match
# `ANTITHESIS_CLUSTERD_POOL_SIZE` in test/antithesis/mzcompose.py (the
# Workload service mirrors that value into the workload container's env
# as both ANTITHESIS_CLUSTERD_POOL_SIZE and CLUSTERD_POOL_SIZE so the
# bootstrap script and driver agree).
#
# Each invocation picks a pool slot at random and runs against the
# corresponding pool_cluster_<slot>. No coordination between concurrent
# invocations: two invocations may share a pool cluster — every workload
# object is in a seed-scoped database (`db-pw-<seed>-*`) with seed-scoped
# roles, so DDL/DML never collides; the only shared state is the
# permanent pool cluster, which is purposefully shared. Antithesis still
# faults one container at a time, so the per-container fault domain is
# preserved; multiple invocations witnessing the same fault is a
# feature (more independent reproductions per failure).
CLUSTERD_POOL_SIZE = int(os.environ.get("CLUSTERD_POOL_SIZE", "2"))


def _alter_system(cur: psycopg.Cursor[Any], stmt: str) -> None:
    try:
        cur.execute(stmt.encode())
    except Exception as exc:  # noqa: BLE001
        LOG.warning("ALTER SYSTEM tolerated: %s (%s)", stmt, exc)


def _prepare_system(num_threads: int) -> None:
    """Apply the catalog-size knobs from `parallel_workload.run()` so the
    workload doesn't trip default limits. The privilege grants mirror upstream
    so most queries don't fail on permissions. Idempotent across drivers."""
    with (
        psycopg.connect(
            host=PGHOST,
            port=PGPORT_INTERNAL,
            user=PGUSER_INTERNAL,
            dbname=PGDATABASE,
            autocommit=True,
            connect_timeout=CONNECT_TIMEOUT_S,
        ) as conn,
        conn.cursor() as cur,
    ):
        _alter_system(
            cur,
            f"ALTER SYSTEM SET max_schemas_per_database = {MAX_SCHEMAS * 40 + num_threads}",
        )
        _alter_system(
            cur, f"ALTER SYSTEM SET max_tables = {MAX_TABLES * 40 + num_threads}"
        )
        _alter_system(
            cur,
            f"ALTER SYSTEM SET max_materialized_views = {MAX_VIEWS * 40 + num_threads}",
        )
        _alter_system(
            cur,
            f"ALTER SYSTEM SET max_sources = "
            f"{(MAX_WEBHOOK_SOURCES + MAX_KAFKA_SOURCES + MAX_POSTGRES_SOURCES) * 40 + num_threads}",
        )
        _alter_system(
            cur, f"ALTER SYSTEM SET max_sinks = {MAX_KAFKA_SINKS * 40 + num_threads}"
        )
        _alter_system(
            cur, f"ALTER SYSTEM SET max_roles = {MAX_ROLES * 1000 + num_threads}"
        )
        _alter_system(
            cur, f"ALTER SYSTEM SET max_clusters = {MAX_CLUSTERS * 40 + num_threads}"
        )
        _alter_system(
            cur,
            f"ALTER SYSTEM SET max_replicas_per_cluster = "
            f"{MAX_CLUSTER_REPLICAS * 40 + num_threads}",
        )
        _alter_system(cur, "ALTER SYSTEM SET max_secrets = 1000000")
        _alter_system(cur, "ALTER SYSTEM SET idle_in_transaction_session_timeout = 0")
        for object_type in (
            "TABLES",
            "TYPES",
            "SECRETS",
            "CONNECTIONS",
            "DATABASES",
            "SCHEMAS",
            "CLUSTERS",
        ):
            _alter_system(
                cur,
                f"ALTER DEFAULT PRIVILEGES FOR ALL ROLES "
                f"GRANT ALL PRIVILEGES ON {object_type} TO PUBLIC",
            )


# Expected substring matches for SQL errors raised during the setup phase when
# multiple parallel-driver invocations race the same deterministic object
# names (`role0`, `cluster-0`, etc.). Each invocation does best-effort cleanup
# + create; whoever loses the race sees one of these and continues. The same
# patterns are already tolerated by the parallel_workload framework itself in
# `action.Action.errors_to_ignore` for the DDL complexity tier, so the setup
# phase tolerates the same surface area.
_SETUP_RACE_PATTERNS = (
    "already exists",
    "unknown role",
    "unknown cluster",
    "unknown schema",
    "unknown catalog item",
    "cannot be dropped because",
    "was concurrently dropped",
    "was removed",
    "' was dropped",
    "was dropped while executing a statement",
    "another session modified the catalog",
    "object state changed while transaction was in progress",
)

# Fault-injection-shape patterns (network drops, DNS failures, broker
# timeouts, etc.) live in the shared `helper_fault_tolerance` module so
# every Antithesis driver agrees on what looks like a fault.  The
# `_SETUP_RACE_PATTERNS` list above is kept local because race shapes
# are unique to setup-phase concurrency between parallel-driver
# invocations — not a fault-injection concept.


def _matches_setup_tolerance(exc: BaseException) -> bool:
    """True if `exc` is a setup-phase error we expect to see under either
    concurrent-driver races or Antithesis fault injection. Used both inside
    `_tolerate_setup_race` (to swallow per-statement) and around the whole
    setup phase (to demote setup_failure from unexpected to a sometimes
    signal).
    """
    msg = getattr(exc, "msg", None) or str(exc)
    if looks_like_fault(msg):
        return True
    lo = msg.lower()
    return any(pat.lower() in lo for pat in _SETUP_RACE_PATTERNS)


def _worker_death_tolerable(occurred: Exception | None) -> bool:
    """True when an early-exiting worker thread is plausibly a fault-injection
    casualty rather than a bug to fail the run on.

    `parallel_workload.worker.Worker.run` performs its initial
    `psycopg.connect` / websocket / `SET` statements outside any try/except,
    so a fault that lands during worker startup kills the thread with
    `occurred_exception = None` (no captured cause). Once the worker is
    inside its main action loop, captured `QueryError`s that don't match
    `errors_to_ignore` populate `occurred_exception` — those are the ones
    we want to look at. If the captured exception matches a fault shape
    (via `helper_fault_tolerance.looks_like_fault`) it's still the fault
    that killed the worker, not a SUT correctness bug.
    """
    if occurred is None:
        return True
    return _matches_setup_tolerance(occurred)


def _tolerate_setup_race(fn, *args, **kwargs):
    """Run `fn(...)`, swallowing messages in `_SETUP_RACE_PATTERNS` or
    any pattern in the shared `helper_fault_tolerance` list, and
    propagating anything else.

    The setup phase is invoked by every parallel-driver invocation, and the
    framework picks deterministic object names from a small pool. Concurrent
    invocations therefore race to drop-then-create the same names; any
    single race outcome is fine because the per-invocation Database object
    only needs its named objects to exist by the time worker threads start.

    Fault-induced errors (container paused, DNS partitioned, socket reset)
    are absorbed for the same reason: they're expected under Antithesis,
    not workload bugs.
    """
    try:
        return fn(*args, **kwargs)
    except QueryError as exc:
        if _matches_setup_tolerance(exc):
            LOG.debug("setup tolerated: %s — %s", exc.query, exc.msg)
            return None
        raise
    except Exception as exc:  # noqa: BLE001
        if _matches_setup_tolerance(exc):
            LOG.debug("setup tolerated: %s", exc)
            return None
        raise


def _drop_seed_scoped_objects(seed: str) -> None:
    """Drop everything this invocation's seed owns: its databases and roles.

    Pool clusters are NOT dropped — they're long-lived, bootstrapped by the
    workload-entrypoint, and shared (one cluster per slot) across many
    invocations. The DROP DATABASE CASCADE here transitively drops every
    table / MV / index / source / sink the workload created on the pool
    cluster during its run, which tears down the corresponding dataflows
    on the bound clusterd container — so the cluster goes back to an idle
    baseline before the next invocation claims the same slot.

    Errors here are logged and swallowed: leftover objects only cost a bit
    of catalog footprint until the next invocation with the same seed
    re-runs (extremely unlikely since seeds are u64-random). Don't let a
    cleanup failure turn into an assertion failure.
    """
    from pg8000.native import identifier

    try:
        with (
            psycopg.connect(
                host=PGHOST,
                port=PGPORT,
                user=PGUSER,
                dbname=PGDATABASE,
                autocommit=True,
                connect_timeout=CONNECT_TIMEOUT_S,
            ) as conn,
            conn.cursor() as cur,
        ):
            # `seed` is u64-derived; safe to splice. We can't use psycopg's
            # parameter binding for `LIKE` patterns here without forcing the
            # caller to think about driver-specific placeholder syntax —
            # inline f-strings match the rest of this module.
            def _drop(sql: str) -> None:
                try:
                    cur.execute(sql.encode())
                except Exception as exc:  # noqa: BLE001
                    LOG.debug("cleanup tolerated: %s — %s", sql, exc)

            cur.execute(
                f"SELECT name FROM mz_databases WHERE name LIKE 'db-pw-{seed}-%'".encode()
            )
            for row in cur.fetchall():
                _drop(f"DROP DATABASE {identifier(row[0])} CASCADE")

            cur.execute(
                f"SELECT name FROM mz_roles WHERE name LIKE 'role-{seed}-%'".encode()
            )
            for row in cur.fetchall():
                _drop(f"DROP ROLE {identifier(row[0])}")
    except Exception as exc:  # noqa: BLE001
        LOG.warning("cleanup connection failed: %s", exc)


def _create_database_for_antithesis(database: Database, exe: Executor) -> None:
    """Stand-in for `Database.create` that only sets up connections matching
    the Antithesis topology. Upstream's `create()` also wires polaris,
    sql-server, and an external postgres source — none of those are running
    in this compose.

    Catalog sweeps are scoped to objects this invocation owns: roles
    matching `role-{seed}-%`. Pool clusters are NOT touched — they're
    long-lived state shared across many invocations (one cluster per
    pool slot, bootstrapped by the workload-entrypoint).

    The shared connections / secret (`kafka_conn`, `csr_conn`, `aws_conn`,
    `minio`) live outside any seed-scoped database and are required by every
    invocation. We never drop them — `CREATE ... IF NOT EXISTS` is
    idempotent and dropping would CASCADE through another invocation's
    in-flight sources.

    Setup-phase statements are wrapped with `_tolerate_setup_race` so a
    losing race against another invocation creating the same shared object
    (or against our own scoped leftovers being already absent) doesn't kill
    the driver.
    """
    from pg8000.native import identifier

    seed = database.seed

    for db in database.dbs:
        _tolerate_setup_race(db.drop, exe)
        _tolerate_setup_race(db.create, exe)

    # `seed` is the random_u64 the driver minted at the top of main(), so
    # it's already safe to splice into SQL literally. `Executor.execute`
    # takes a query string and doesn't support parameter binding.
    exe.execute(f"SELECT name FROM mz_roles WHERE name LIKE 'role-{seed}-%'")
    for row in exe.cur.fetchall():
        _tolerate_setup_race(exe.execute, f"DROP ROLE {identifier(row[0])}")

    _tolerate_setup_race(
        exe.execute,
        "CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA "
        "BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT",
    )
    _tolerate_setup_race(
        exe.execute,
        "CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA "
        "REGISTRY URL 'http://schema-registry:8081'",
    )
    _tolerate_setup_race(
        exe.execute, "CREATE SECRET IF NOT EXISTS minio AS 'minioadmin'"
    )
    _tolerate_setup_race(
        exe.execute,
        "CREATE CONNECTION IF NOT EXISTS aws_conn TO AWS ("
        "ENDPOINT 'http://minio:9000/', REGION 'minio', "
        "ACCESS KEY ID 'minioadmin', SECRET ACCESS KEY SECRET minio)",
    )
    # Iceberg REST catalog.  Required by `CreateIcebergSinkAction` — without
    # it those actions hit `unknown catalog item polaris_conn` and
    # silent-fail under Scenario.Kill, which is how this code path went
    # unexercised before.  Polaris listens on :8181 inside the antithesis
    # compose (see test/antithesis/mzcompose.py); credentials are the
    # bootstrap defaults (`root:root`, `default_catalog`, `PRINCIPAL_ROLE:ALL`).
    _tolerate_setup_race(
        exe.execute,
        "CREATE CONNECTION IF NOT EXISTS polaris_conn TO ICEBERG CATALOG ("
        "CATALOG TYPE = 'REST', "
        "URL = 'http://polaris:8181/api/catalog', "
        "CREDENTIAL = 'root:root', "
        "WAREHOUSE = 'default_catalog', "
        "SCOPE = 'PRINCIPAL_ROLE:ALL')",
    )

    for relation in database:
        _tolerate_setup_race(relation.create, exe)


class _AntithesisCancelAction(CancelAction):
    """`CancelAction` with an empty-pids guard.

    The framework's `CancelAction.run` does `self.rng.choice([...])` over the
    pids of currently-connected workers, which raises `IndexError` when none
    have registered yet (`worker.exe is None` or `pg_pid == -1`). Under
    Antithesis we spawn the cancel worker right after the regular workers
    so there is a real race window before any regular worker has finished
    its initial `psycopg.connect`; returning False here lets the worker
    loop retry on the next iteration instead of dying with an uncaught
    `IndexError` (Worker.run rethrows non-QueryError exceptions and would
    abort the cancel thread).
    """

    def run(self, exe: Executor) -> bool:
        eligible = [w for w in self.workers if w.exe is not None and w.exe.pg_pid != -1]
        if not eligible:
            time.sleep(0.1)
            return False
        return super().run(exe)


def _spawn_workers(
    rng: helper_random.AntithesisRandom,
    database: Database,
    end_time: float,
    num_threads: int,
) -> tuple[list[Worker], list[threading.Thread]]:
    """Build the same thread pool `parallel_workload.run()` does for
    `Complexity.DDL`, minus the per-scenario kill/cancel/backup helper.

    Each worker gets its own `AntithesisRandom` instance so the framework's
    per-Action `self.rng.choice/randint/random/sample` calls route through
    Antithesis on every draw. The framework expects a `random.Random`;
    `AntithesisRandom` is a subclass that overrides the entropy primitives
    to read from the SDK, so action selection, expression shape, DDL
    choices, and every other decision are driven by the fuzzer instead of
    being locked in after one seed.
    """
    weights = [60, 30, 30, 30, 100]
    workers: list[Worker] = []
    threads: list[threading.Thread] = []
    for i in range(num_threads):
        worker_rng = helper_random.AntithesisRandom()
        action_list = worker_rng.choices(
            [
                read_action_list,
                fetch_action_list,
                write_action_list,
                dml_nontrans_action_list,
                ddl_action_list,
            ],
            weights,
        )[0]
        actions = [
            action_class(worker_rng, None)
            for action_class in action_list.action_classes
        ]
        worker = Worker(
            worker_rng,
            actions,
            action_list.weights,
            end_time,
            action_list.autocommit,
            system=False,
            composition=None,
            action_list=action_list,
        )
        workers.append(worker)
        thread = threading.Thread(
            name=f"pw-worker-{i}",
            target=worker.run,
            args=(PGHOST, PGPORT, 6876, PGUSER, database),
        )
        thread.start()
        threads.append(thread)
    return workers, threads


def _spawn_cancel_worker(
    database: Database,
    end_time: float,
    workers: list[Worker],
) -> tuple[Worker, threading.Thread]:
    """Build the `Scenario.Cancel` worker thread, which fires
    `pg_cancel_backend(pid)` against the regular workers' sessions.

    Connects on `mz_system` (privilege requirement for `pg_cancel_backend`
    against another role's backend) and shares an `AntithesisRandom` of its
    own so target selection routes through the SDK. The action's `errors_to_ignore`
    tolerates `"must be a member of"` for the rare case `mz_system` itself
    is restricted, plus the standard fault-shaped patterns inherited from
    `Action.errors_to_ignore`.
    """
    cancel_rng = helper_random.AntithesisRandom()
    cancel_action = _AntithesisCancelAction(cancel_rng, None, workers)
    cancel_worker = Worker(
        cancel_rng,
        [cancel_action],
        [1],
        end_time,
        autocommit=False,
        system=True,
        composition=None,
    )
    cancel_thread = threading.Thread(
        name="pw-cancel",
        target=cancel_worker.run,
        args=(PGHOST, PGPORT_INTERNAL, 6876, PGUSER_INTERNAL, database),
    )
    cancel_thread.start()
    return cancel_worker, cancel_thread


# Soft-signal: bounded poll for the server-side view of worker sessions
# clearing. Upstream's `parallel_workload.run()` does an analogous 30s
# wait against `mz_internal.mz_sessions` filtered to `connection_id <>
# pg_backend_pid()`; under Antithesis we filter to the specific worker
# pids so concurrent parallel-driver invocations don't muddy the signal.
SESSION_DRAIN_BUDGET_S = 30.0
SESSION_DRAIN_POLL_S = 1.0


def _check_sessions_drained(worker_pids: list[int]) -> bool:
    """Best-effort: poll `mz_sessions` until none of `worker_pids` remain.

    Returns True if every worker pid has cleared (or `worker_pids` is
    empty), False on timeout or query failure. Pure signal — never raises.
    """
    if not worker_pids:
        return True
    deadline = time.monotonic() + SESSION_DRAIN_BUDGET_S
    while time.monotonic() < deadline:
        try:
            with (
                psycopg.connect(
                    host=PGHOST,
                    port=PGPORT,
                    user=PGUSER,
                    dbname=PGDATABASE,
                    autocommit=True,
                    connect_timeout=CONNECT_TIMEOUT_S,
                ) as conn,
                conn.cursor() as cur,
            ):
                cur.execute(
                    "SELECT count(*) FROM mz_internal.mz_sessions "
                    "WHERE connection_id = ANY(%s)",
                    (worker_pids,),
                )
                row = cur.fetchone()
                if row is not None and row[0] == 0:
                    return True
        except Exception as exc:  # noqa: BLE001
            LOG.debug("session-drain probe failed: %s", exc)
        time.sleep(SESSION_DRAIN_POLL_S)
    return False


def main() -> int:
    seed = str(helper_random.random_u64())
    # AntithesisRandom routes every getrandbits/random call through the
    # Antithesis SDK, so every decision the parallel_workload framework
    # makes downstream of this rng draws fresh entropy on each call. A
    # stdlib `random.Random(seed)` would lock the timeline in after one
    # draw and the fuzzer couldn't drive differing branches.
    rng = helper_random.AntithesisRandom()

    LOG.info(
        "parallel-workload starting: seed=%s threads=%d runtime=%ss",
        seed,
        NUM_THREADS,
        RUNTIME_S,
    )

    _prepare_system(NUM_THREADS)

    # Pick a pool slot at random. Each slot maps to a long-lived
    # pool_cluster_<slot> bootstrapped by the workload-entrypoint, with
    # one replica on the matching clusterd-pool-<slot> container.
    #
    # No coordination with other concurrent driver invocations: all
    # workload state is in a seed-scoped database, so two invocations
    # sharing a pool cluster don't collide. Antithesis still faults
    # containers, not invocations, so the per-container fault domain
    # is preserved; multiple invocations witnessing the same fault give
    # us more independent reproductions per failure.
    #
    # Keeping the cluster identity per slot is what makes clusterd reuse
    # safe across invocations (reconnects against the same cluster pass
    # `InstanceConfig::compatible_with`; reconnects against a *different*
    # cluster trip clusterd's introspection-index mismatch halt).
    pool_slot = rng.randrange(CLUSTERD_POOL_SIZE)
    cluster_name = f"pool_cluster_{pool_slot}"
    LOG.info(
        "parallel-workload using pool slot %d → cluster %s",
        pool_slot,
        cluster_name,
    )
    return _run_invocation(seed, rng, cluster_name)


def _run_invocation(
    seed: str,
    rng: helper_random.AntithesisRandom,
    cluster_name: str,
) -> int:
    """The bulk of `main()` once a pool slot has been claimed. Split out
    so the slot lock stays held across this whole call: it's released when
    the enclosing `with` block in `main()` exits.
    """

    # `Scenario.Kill` widens `Action.errors_to_ignore` to absorb connection
    # drops, which mirrors what Antithesis container-pauses look like at the
    # client. We never instantiate `KillAction` itself.
    #
    # `seed_scoped_names=True` keeps role names from colliding when
    # concurrent invocations share the SUT.
    #
    # `existing_cluster_name=cluster_name` makes the Database wrap the
    # pool cluster bootstrapped at compose-up; the framework's
    # CreateClusterAction / CreateClusterReplicaAction /
    # DropClusterReplicaAction are disabled for it and Cluster.create()
    # / Cluster.drop() are no-ops.
    database = Database(
        rng=rng,
        seed=seed,
        host=PGHOST,
        ports={
            "materialized": PGPORT,
            "mz_system": PGPORT_INTERNAL,
            "http": 6876,
            "kafka": 9092,
            "schema-registry": 8081,
            # data_ingest.PgExecutor reads this when it opens its own
            # connection to the upstream PG for CreatePostgresSourceAction.
            # The workload container reaches postgres-source on its bare
            # port; MZ_DATA_INGEST_PG_HOST (set near the top of this
            # module) handles the hostname half of the same fix.
            "postgres": 5432,
        },
        complexity=Complexity.DDL,
        scenario=Scenario.Kill,
        naughty_identifiers=False,
        seed_scoped_names=True,
        existing_cluster_name=cluster_name,
    )

    end_time = time.time() + RUNTIME_S

    setup_failure: Exception | None = None
    workers: list[Worker] = []
    threads: list[threading.Thread] = []
    worker_failed: WorkerFailedException | None = None
    try:
        try:
            with (
                psycopg.connect(
                    host=PGHOST,
                    port=PGPORT,
                    user=PGUSER,
                    dbname=PGDATABASE,
                    autocommit=True,
                    connect_timeout=CONNECT_TIMEOUT_S,
                ) as setup_conn,
                setup_conn.cursor() as setup_cur,
            ):
                setup_exe = Executor(rng, setup_cur, None, database)
                _create_database_for_antithesis(database, setup_exe)
        except Exception as exc:  # noqa: BLE001
            setup_failure = exc
            LOG.exception("parallel-workload setup failed")

        if setup_failure is None:
            workers, threads = _spawn_workers(rng, database, end_time, NUM_THREADS)
            cancel_worker, cancel_thread = _spawn_cancel_worker(
                database, end_time, workers
            )
            workers.append(cancel_worker)
            try:
                # Dead-thread detection only watches the regular workers —
                # cancel is auxiliary and its death (e.g. mz_system connect
                # failure during a fault window) shouldn't abort the run.
                while time.time() < end_time:
                    dead = [t for t in threads if not t.is_alive()]
                    if dead:
                        occurred = next(
                            (
                                w.occurred_exception
                                for w in workers
                                if w.occurred_exception
                            ),
                            None,
                        )
                        worker_failed = WorkerFailedException(
                            f"thread {dead[0].name} exited early", occurred
                        )
                        for worker in workers:
                            worker.end_time = time.time()
                        break
                    time.sleep(0.5)
            finally:
                for worker in workers:
                    worker.end_time = time.time()
                for thread in (*threads, cancel_thread):
                    thread.join(timeout=30)
    finally:
        # Snapshot the pid each worker last held while `workers` is still
        # in scope and before the cleanup connection muddies the picture.
        # After Worker.run returns the worker has closed its psycopg
        # connection, so these pids should disappear from
        # `mz_internal.mz_sessions` within a few seconds.
        worker_pids = sorted(
            {
                w.exe.pg_pid
                for w in workers
                if w.exe is not None and w.exe.pg_pid not in (None, -1)
            }
        )
        # Always free this invocation's seed-scoped state, including its
        # pool-slot cluster, so the next driver invocation can claim the
        # slot cleanly. Wrapped in try/except inside the helper; any
        # cleanup failure is logged but never escapes.
        _drop_seed_scoped_objects(seed)
        # Probe the SUT-side view of the worker sessions after cleanup
        # has released its own connection. Best-effort; signal only.
        sessions_drained = _check_sessions_drained(worker_pids)

    total_queries = sum(w.num_queries.total() for w in workers)
    total_ignored = sum(
        count
        for w in workers
        for counter in w.ignored_errors.values()
        for count in counter.values()
    )

    sometimes(
        total_queries >= NUM_THREADS,
        "parallel workload: randomized concurrent SQL executed successfully",
        {
            "queries": total_queries,
            "threads": NUM_THREADS,
            "ignored_errors": total_ignored,
        },
    )
    sometimes(
        total_ignored > 0,
        "parallel workload: expected concurrent-catalog races were observed",
        {"ignored_errors": total_ignored},
    )

    # Soft-signal liveness: the system mostly runs cleanly. Upstream
    # `parallel_workload.run()` asserts `failed < 50%` as a hard gate, but
    # Antithesis fault injection is more aggressive than the bracketed
    # upstream scenarios, so a hard `always` would tip on real fault windows.
    # `sometimes(<50%)` is the right shape: if this never fires, the workload
    # is permanently swamped by errors and the other assertions are vacuous.
    failed_rate = (total_ignored / total_queries) if total_queries > 0 else 0.0
    sometimes(
        total_queries > 0 and failed_rate < 0.5,
        "parallel workload: < 50% of queries failed (system mostly clean)",
        {
            "queries": total_queries,
            "ignored": total_ignored,
            "failed_rate": failed_rate,
        },
    )

    # Soft-signal liveness: after workers exit, their server-side sessions
    # eventually clear. Mirrors upstream's post-run session-drain check;
    # under fault injection this may legitimately fail (paused environmentd
    # never gets to reap the session), so `sometimes` not `always`.
    sometimes(
        sessions_drained,
        "parallel workload: worker sessions cleared from mz_sessions post-exit",
        {"worker_pids": worker_pids, "drained": sessions_drained},
    )

    # Setup-phase failures whose message matches `_SETUP_*_PATTERNS` are
    # either concurrent-driver races or Antithesis fault-injection
    # consequences (paused container, partitioned DNS, reset socket).
    # Neither is a SUT correctness issue, so demote them out of the
    # `always(...)` assertion and into a `sometimes(...)` for visibility.
    setup_tolerated = setup_failure is not None and _matches_setup_tolerance(
        setup_failure
    )
    sometimes(
        setup_tolerated,
        "parallel workload: setup phase tolerated a fault-injection or race error",
        {"error": str(setup_failure) if setup_failure else None},
    )

    # Worker-thread death under fault injection has the same
    # "expected-not-a-bug" shape: an uncaptured-exception death (typically
    # initial psycopg.connect failing because materialized was paused) or a
    # captured exception whose message matches a fault pattern.
    worker_tolerated = worker_failed is not None and _worker_death_tolerable(
        worker_failed.cause
    )
    sometimes(
        worker_tolerated,
        "parallel workload: worker thread death tolerated as fault-injection consequence",
        {
            "error": (
                str(worker_failed.cause)
                if worker_failed and worker_failed.cause
                else None
            ),
            "uncaptured": worker_failed is not None and worker_failed.cause is None,
        },
    )

    unexpected = None
    if setup_failure is not None and not setup_tolerated:
        unexpected = {"phase": "setup", "error": str(setup_failure)}
    elif worker_failed is not None and not worker_tolerated:
        unexpected = {
            "phase": "worker",
            "error": (
                str(worker_failed.cause) if worker_failed.cause else str(worker_failed)
            ),
        }

    always(
        unexpected is None,
        "parallel workload: no unexpected SQL errors escaped the randomized stress driver",
        {
            "unexpected": unexpected,
            "queries": total_queries,
            "ignored_errors": total_ignored,
            "threads": NUM_THREADS,
        },
    )

    LOG.info(
        "parallel-workload done: queries=%d ignored=%d unexpected=%s",
        total_queries,
        total_ignored,
        unexpected,
    )
    # Always exit 0.  The `always(unexpected is None, ...)` above is
    # the canonical signal — if it fires False the triage report
    # surfaces it under "Always assertions".  Exiting 1 here would
    # additionally trip Antithesis's built-in "Commands finish with
    # zero exit code" always-check, double-signalling the same
    # finding on two separate rows that look like independent
    # property violations.
    return 0


if __name__ == "__main__":
    sys.exit(main())
