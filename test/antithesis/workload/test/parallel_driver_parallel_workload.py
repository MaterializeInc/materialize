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
    worker threads. We still tag the database with `Scenario.Kill` so each
    `Action.errors_to_ignore` includes connection-shaped errors — those are
    expected here.
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

import contextlib
import fcntl
import logging
import os
import random
import sys
import threading
import time
from typing import Any

import helper_random
import psycopg
from antithesis.assertions import always, sometimes
from helper_pg import (
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
    ClusterdPoolMember,
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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
LOG = logging.getLogger("driver.parallel_workload")

# Antithesis Test Composer invokes drivers in tight loops, so this script is
# intentionally short. The cap exists so a single iteration can't monopolise
# the fault-injection budget; the goal is repeated short bursts.
RUNTIME_S = float(os.environ.get("PW_RUNTIME_S", "20"))
NUM_THREADS = int(os.environ.get("PW_THREADS", "4"))

# Number of clusterd-pool-{i} containers reserved for the parallel-workload
# driver. Must match the pool actually deployed in
# test/antithesis/mzcompose.py (ANTITHESIS_CLUSTERD_POOL_SIZE there →
# CLUSTERD_POOL_SIZE here). Each parallel-workload invocation claims one
# slot via `fcntl.flock` (see `_claim_pool_slot`); the lock is held for
# the lifetime of the invocation so concurrent driver processes inside
# the workload container can't pick the same clusterd.
CLUSTERD_POOL_SIZE = int(os.environ.get("CLUSTERD_POOL_SIZE", "8"))

# Workers configured per clusterd-pool-{i} process. Must match the
# `Clusterd(..., workers=...)` argument in test/antithesis/mzcompose.py
# or the unmanaged CREATE CLUSTER REPLICA's `WORKERS` count will diverge
# from what clusterd actually runs.
CLUSTERD_POOL_WORKERS = 4

# Filesystem locks let concurrent parallel-workload invocations claim
# distinct clusterd-pool members without coordinating through the SUT.
# All invocations exec inside the single `workload` container so a
# regular flock on a tmpfs path is sufficient (no cross-container
# coordination required).
POOL_SLOT_LOCK_DIR = os.environ.get(
    "CLUSTERD_POOL_SLOT_LOCK_DIR", "/tmp/clusterd-pool-slots"
)


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
            connect_timeout=15,
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


def _tolerate_setup_race(fn, *args, **kwargs):
    """Run `fn(...)`, swallowing the concurrent-race messages in
    `_SETUP_RACE_PATTERNS` and propagating anything else.

    The setup phase is invoked by every parallel-driver invocation, and the
    framework picks deterministic object names from a small pool. Concurrent
    invocations therefore race to drop-then-create the same names; any single
    race outcome is fine because the per-invocation Database object only
    needs its named objects to exist by the time worker threads start.
    """
    try:
        return fn(*args, **kwargs)
    except QueryError as exc:
        if any(pat in (exc.msg or "") for pat in _SETUP_RACE_PATTERNS):
            LOG.debug("setup tolerated: %s — %s", exc.query, exc.msg)
            return None
        raise
    except Exception as exc:  # noqa: BLE001
        if any(pat in str(exc) for pat in _SETUP_RACE_PATTERNS):
            LOG.debug("setup tolerated: %s", exc)
            return None
        raise


@contextlib.contextmanager
def _claim_pool_slot(rng: random.Random):
    """Hold an exclusive `fcntl.flock` on a pool-slot lockfile for the
    duration of the `with` block. Yields the slot index, or `None` if every
    slot is busy.

    Slots are tried in a randomized order so the slot a driver lands on
    doesn't correlate with deterministic state (test composer seed, wall
    clock). The lock is released when the context exits — either normally
    or via exception — so a crashing driver doesn't strand the slot.

    All parallel-workload driver invocations share the workload container's
    filesystem, so a plain flock on a tmpfs path under `POOL_SLOT_LOCK_DIR`
    is sufficient to serialize claims. If the path can't be created we fall
    back to yielding `None` (caller must handle: the existing setup-tolerance
    path can absorb a slot collision, it just costs us pool isolation for
    that one invocation).
    """
    try:
        os.makedirs(POOL_SLOT_LOCK_DIR, exist_ok=True)
    except OSError as exc:
        LOG.warning("pool slot lock dir %s unavailable: %s", POOL_SLOT_LOCK_DIR, exc)
        yield None
        return

    slots = list(range(CLUSTERD_POOL_SIZE))
    rng.shuffle(slots)
    for slot in slots:
        path = os.path.join(POOL_SLOT_LOCK_DIR, f"{slot}.lock")
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # Another invocation owns this slot; try the next one.
            os.close(fd)
            continue
        try:
            yield slot
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
        return
    LOG.warning("all %d pool slots are claimed; running without isolation", CLUSTERD_POOL_SIZE)
    yield None


def _drop_seed_scoped_objects(seed: str) -> None:
    """Drop everything this invocation's seed owns: its clusters, roles, and
    databases. Called from `main()`'s finally so each invocation leaves the
    catalog clean and frees its pool-slot's clusterd to be claimed by the
    next driver run (DROP CLUSTER tears down the unmanaged replica → the
    clusterd's existing controller connection ends → the next CREATE
    CLUSTER pointed at the same address claims it via fresh reconcile).

    Errors here are logged and swallowed: leftover objects only cost a bit
    of catalog footprint until the next invocation's setup sweep picks them
    up. Don't let a cleanup failure turn into an assertion failure.
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
                connect_timeout=15,
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
                f"SELECT name FROM mz_clusters WHERE name LIKE 'cluster-{seed}-%'".encode()
            )
            for row in cur.fetchall():
                _drop(f"DROP CLUSTER {identifier(row[0])} CASCADE")

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

    Catalog sweeps are scoped to objects this invocation owns: clusters
    matching `cluster-{seed}-%` and roles matching `role-{seed}-%`. The
    seed-scoped names are produced by `Database(seed_scoped_names=True)`;
    cleaning anything broader would delete state belonging to other
    concurrent invocations sharing the same SUT.

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
    exe.execute(f"SELECT name FROM mz_clusters WHERE name LIKE 'cluster-{seed}-%'")
    for row in exe.cur.fetchall():
        _tolerate_setup_race(
            exe.execute, f"DROP CLUSTER {identifier(row[0])} CASCADE"
        )

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
    _tolerate_setup_race(exe.execute, "CREATE SECRET IF NOT EXISTS minio AS 'minioadmin'")
    _tolerate_setup_race(
        exe.execute,
        "CREATE CONNECTION IF NOT EXISTS aws_conn TO AWS ("
        "ENDPOINT 'http://minio:9000/', REGION 'minio', "
        "ACCESS KEY ID 'minioadmin', SECRET ACCESS KEY SECRET minio)",
    )

    for relation in database:
        _tolerate_setup_race(relation.create, exe)


def _spawn_workers(
    rng: random.Random,
    database: Database,
    end_time: float,
    num_threads: int,
) -> tuple[list[Worker], list[threading.Thread]]:
    """Build the same thread pool `parallel_workload.run()` does for
    `Complexity.DDL`, minus the per-scenario kill/cancel/backup helper."""
    weights = [60, 30, 30, 30, 100]
    workers: list[Worker] = []
    threads: list[threading.Thread] = []
    for i in range(num_threads):
        worker_rng = random.Random(rng.randrange(1_000_000))
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


def main() -> int:
    seed = str(helper_random.random_u64())
    rng = random.Random(seed)

    LOG.info(
        "parallel-workload starting: seed=%s threads=%d runtime=%ss",
        seed,
        NUM_THREADS,
        RUNTIME_S,
    )

    _prepare_system(NUM_THREADS)

    # Claim one clusterd-pool-{i} container for this invocation. The flock
    # is held until main() returns; concurrent invocations inside the
    # workload container can't pick the same slot. If every slot is busy
    # the context manager yields `None` — we tag that with a sometimes()
    # for visibility and exit cleanly (the property surface for this
    # invocation just doesn't get exercised).
    #
    # Each parallel-workload cluster lands on its own clusterd-pool-{slot}
    # container, giving Antithesis per-cluster fault isolation. Without
    # this, every parallel-workload cluster would be a child process of
    # environmentd under the materialized container's process orchestrator,
    # and the only container-level fault would be "the whole world".
    with _claim_pool_slot(rng) as pool_slot:
        sometimes(
            pool_slot is not None,
            "parallel workload: clusterd pool slot claimed",
            {"pool_size": CLUSTERD_POOL_SIZE},
        )
        if pool_slot is None:
            LOG.info(
                "parallel-workload exiting cleanly: no pool slot available "
                "(pool_size=%d)",
                CLUSTERD_POOL_SIZE,
            )
            return 0
        pool_member = ClusterdPoolMember(
            host=f"clusterd-pool-{pool_slot}",
            workers=CLUSTERD_POOL_WORKERS,
        )
        LOG.info(
            "parallel-workload claimed pool slot %d (%s)",
            pool_slot,
            pool_member.host,
        )
        return _run_invocation(seed, rng, pool_member)


def _run_invocation(
    seed: str,
    rng: random.Random,
    pool_member: ClusterdPoolMember,
) -> int:
    """The bulk of `main()` once a pool slot has been claimed. Split out so
    the slot lock stays held across this whole call: the lock is released
    when the enclosing `with` block in `main()` exits.
    """

    # `Scenario.Kill` widens `Action.errors_to_ignore` to absorb connection
    # drops, which mirrors what Antithesis container-pauses look like at the
    # client. We never instantiate `KillAction` itself.
    #
    # `seed_scoped_names=True` keeps cluster/role names from colliding when
    # concurrent invocations share the SUT — see _SETUP_RACE_PATTERNS for
    # the fallback when they collide anyway.
    #
    # `pool_members=[pool_member]` puts this invocation's single cluster
    # on the pool member above; the framework forces managed=False and
    # emits unmanaged CREATE CLUSTER with explicit STORAGECTL/COMPUTE
    # ADDRESSES.
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
        },
        complexity=Complexity.DDL,
        scenario=Scenario.Kill,
        naughty_identifiers=False,
        seed_scoped_names=True,
        pool_members=[pool_member],
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
                    connect_timeout=15,
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
            try:
                while time.time() < end_time:
                    dead = [t for t in threads if not t.is_alive()]
                    if dead:
                        occurred = next(
                            (w.occurred_exception for w in workers if w.occurred_exception),
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
                for thread in threads:
                    thread.join(timeout=30)
    finally:
        # Always free this invocation's seed-scoped state, including its
        # pool-slot cluster, so the next driver invocation can claim the
        # slot cleanly. Wrapped in try/except inside the helper; any
        # cleanup failure is logged but never escapes.
        _drop_seed_scoped_objects(seed)

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

    unexpected = None
    if setup_failure is not None:
        unexpected = {"phase": "setup", "error": str(setup_failure)}
    elif worker_failed is not None:
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
    return 1 if unexpected is not None else 0


if __name__ == "__main__":
    sys.exit(main())
