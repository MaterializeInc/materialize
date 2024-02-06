# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import datetime
import os
import random
import sys
import threading
import time
from collections import Counter, defaultdict

import pg8000

from materialize.mzcompose import DEFAULT_SYSTEM_PARAMETERS
from materialize.mzcompose.composition import Composition
from materialize.parallel_workload.action import (
    Action,
    BackupRestoreAction,
    CancelAction,
    KillAction,
    StatisticsAction,
    action_lists,
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
from materialize.parallel_workload.executor import Executor, initialize_logging
from materialize.parallel_workload.settings import Complexity, Scenario
from materialize.parallel_workload.worker import Worker
from materialize.parallel_workload.worker_exception import WorkerFailedException

SEED_RANGE = 1_000_000
REPORT_TIME = 10


def run(
    host: str,
    ports: dict[str, int],
    seed: str,
    runtime: int,
    complexity: Complexity,
    scenario: Scenario,
    num_threads: int | None,
    naughty_identifiers: bool,
    fast_startup: bool,
    composition: Composition | None,
    catalog_store: str,
    sanity_restart: bool,
) -> None:
    num_threads = num_threads or os.cpu_count() or 10

    rng = random.Random(random.randrange(SEED_RANGE))

    print(
        f"+++ Running with: --seed={seed} --threads={num_threads} --runtime={runtime} --complexity={complexity.value} --scenario={scenario.value} {'--naughty-identifiers ' if naughty_identifiers else ''} {'--fast-startup' if fast_startup else ''}(--host={host})"
    )
    initialize_logging()

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=runtime)
    ).timestamp()

    database = Database(
        rng, seed, host, ports, complexity, scenario, naughty_identifiers, fast_startup
    )

    system_conn = pg8000.connect(
        host=host, port=ports["mz_system"], user="mz_system", database="materialize"
    )
    system_conn.autocommit = True
    with system_conn.cursor() as system_cur:
        system_exe = Executor(rng, system_cur, database)
        system_exe.execute(
            f"ALTER SYSTEM SET max_schemas_per_database = {MAX_SCHEMAS * 10 + num_threads}"
        )
        # The presence of ALTER TABLE RENAME can cause the total number of tables to exceed MAX_TABLES
        system_exe.execute(
            f"ALTER SYSTEM SET max_tables = {MAX_TABLES * 10 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_materialized_views = {MAX_VIEWS * 10 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_sources = {(MAX_WEBHOOK_SOURCES + MAX_KAFKA_SOURCES + MAX_POSTGRES_SOURCES) * 10 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_sinks = {MAX_KAFKA_SINKS * 10 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_roles = {MAX_ROLES * 10 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_clusters = {MAX_CLUSTERS * 10 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_replicas_per_cluster = {MAX_CLUSTER_REPLICAS * 10 + num_threads}"
        )
        system_exe.execute("ALTER SYSTEM SET max_secrets = 1000000")
        # Most queries should not fail because of privileges
        for object_type in [
            "TABLES",
            "TYPES",
            "SECRETS",
            "CONNECTIONS",
            "DATABASES",
            "SCHEMAS",
            "CLUSTERS",
        ]:
            system_exe.execute(
                f"ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT ALL PRIVILEGES ON {object_type} TO PUBLIC"
            )
        system_conn.close()
        conn = pg8000.connect(
            host=host,
            port=ports["materialized"],
            user="materialize",
            database="materialize",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            assert composition
            database.create(Executor(rng, cur, database), composition)
        conn.close()

    workers = []
    threads = []
    for i in range(num_threads):
        weights: list[float]
        if complexity == Complexity.DDL:
            weights = [60, 30, 30, 30, 100]
        elif complexity == Complexity.DML:
            weights = [60, 30, 30, 30, 0]
        elif complexity == Complexity.Read:
            weights = [60, 30, 0, 0, 0]
        else:
            raise ValueError(f"Unknown complexity {complexity}")
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
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
            action_class(worker_rng, composition)
            for action_class in action_list.action_classes
        ]
        worker = Worker(
            worker_rng,
            actions,
            action_list.weights,
            end_time,
            action_list.autocommit,
            system=False,
            composition=composition,
            action_list=action_list,
        )
        thread_name = f"worker_{i}"
        print(
            f"{thread_name}: {', '.join(action_class.__name__.removesuffix('Action') for action_class in action_list.action_classes)}"
        )
        workers.append(worker)

        thread = threading.Thread(
            name=thread_name,
            target=worker.run,
            args=(host, ports["materialized"], "materialize", database),
        )
        thread.start()
        threads.append(thread)

    if scenario == Scenario.Cancel:
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        worker = Worker(
            worker_rng,
            [CancelAction(worker_rng, composition, workers)],
            [1],
            end_time,
            autocommit=False,
            system=True,
            composition=composition,
        )
        workers.append(worker)
        thread = threading.Thread(
            name="cancel",
            target=worker.run,
            args=(host, ports["mz_system"], "mz_system", database),
        )
        thread.start()
        threads.append(thread)
    elif scenario == Scenario.Kill:
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        assert composition, "Kill scenario only works in mzcompose"
        worker = Worker(
            worker_rng,
            [KillAction(worker_rng, composition, catalog_store, sanity_restart)],
            [1],
            end_time,
            autocommit=False,
            system=False,
            composition=composition,
        )
        workers.append(worker)
        thread = threading.Thread(
            name="kill",
            target=worker.run,
            args=(host, ports["materialized"], "materialize", database),
        )
        thread.start()
        threads.append(thread)
    elif scenario == Scenario.TogglePersistTxn:

        def toggle_persist_txn(params: dict[str, str]) -> dict[str, str]:
            params["persist_txn_tables"] = random.choice(["off", "eager", "lazy"])
            return params

        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        assert composition, "TogglePersistTxn scenario only works in mzcompose"
        worker = Worker(
            worker_rng,
            [
                KillAction(
                    worker_rng,
                    composition,
                    catalog_store,
                    sanity_restart,
                    toggle_persist_txn,
                )
            ],
            [1],
            end_time,
            autocommit=False,
            system=False,
            composition=composition,
        )
        workers.append(worker)
        thread = threading.Thread(
            name="toggle-persist-txn",
            target=worker.run,
            args=(host, ports["materialized"], "materialize", database),
        )
        thread.start()
        threads.append(thread)
    elif scenario == Scenario.BackupRestore:
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        assert composition, "Backup & Restore scenario only works in mzcompose"
        worker = Worker(
            worker_rng,
            [BackupRestoreAction(worker_rng, composition, database)],
            [1],
            end_time,
            autocommit=False,
            system=False,
            composition=composition,
        )
        workers.append(worker)
        thread = threading.Thread(
            name="kill",
            target=worker.run,
            args=(host, ports["materialized"], "materialize", database),
        )
        thread.start()
        threads.append(thread)
    elif scenario in (Scenario.Regression, Scenario.Rename):
        pass
    else:
        raise ValueError(f"Unknown scenario {scenario}")

    if False:  # sanity check for debugging
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        worker = Worker(
            worker_rng,
            [StatisticsAction(worker_rng, composition)],
            [1],
            end_time,
            autocommit=False,
            system=True,
            composition=composition,
        )
        workers.append(worker)
        thread = threading.Thread(
            name="statistics",
            target=worker.run,
            args=(host, ports["mz_system"], "mz_system", database),
        )
        thread.start()
        threads.append(thread)

    num_queries = defaultdict(Counter)
    try:
        while time.time() < end_time:
            for thread in threads:
                if not thread.is_alive():
                    query_error = None
                    for worker in workers:
                        worker.end_time = time.time()
                        query_error = query_error or worker.failed_query_error
                    raise WorkerFailedException(
                        f"^^^ +++ Thread {thread.name} failed, exiting",
                        query_error,
                    )
            time.sleep(REPORT_TIME)
            print(
                "QPS: "
                + " ".join(
                    f"{worker.num_queries.total() / REPORT_TIME:05.1f}"
                    for worker in workers
                )
            )
            for worker in workers:
                for action in worker.num_queries.elements():
                    num_queries[worker.action_list][action] += worker.num_queries[
                        action
                    ]
                worker.num_queries.clear()
    except KeyboardInterrupt:
        print("Keyboard interrupt, exiting")
        for worker in workers:
            worker.end_time = time.time()

    stopping_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=300)
    ).timestamp()
    while time.time() < stopping_time:
        for thread in threads:
            thread.join(timeout=1)
        if all([not thread.is_alive() for thread in threads]):
            break
    else:
        for worker, thread in zip(workers, threads):
            if thread.is_alive():
                print(f"{thread.name} still running: {worker.exe.last_log}")
        print("Threads have not stopped within 5 minutes, exiting hard")
        # TODO(def-): Switch to failing exit code when #23582 is fixed
        os._exit(0)

    conn = pg8000.connect(host=host, port=ports["materialized"], user="materialize")
    conn.autocommit = True
    with conn.cursor() as cur:
        exe = Executor(rng, cur, database)
        for db in database.dbs:
            print(f"Dropping database {db}")
            db.drop(exe)
    conn.close()

    ignored_errors: defaultdict[str, Counter[type[Action]]] = defaultdict(Counter)
    num_failures = 0
    for worker in workers:
        for action_class, counter in worker.ignored_errors.items():
            ignored_errors[action_class].update(counter)
    for counter in ignored_errors.values():
        for count in counter.values():
            num_failures += count

    total_queries = sum(sub.total() for sub in num_queries.values())
    failed = 100.0 * num_failures / total_queries if total_queries else 0
    print(f"Queries executed: {total_queries} ({failed:.0f}% failed)")
    print("--- Action statistics:")
    for action_list in action_lists:
        text = ", ".join(
            [
                f"{action_class.__name__.removesuffix('Action')}: {num_queries[action_list][action_class]}"
                for action_class in action_list.action_classes
            ]
        )
        print(f"  {text}")
    print("--- Error statistics:")
    for error, counter in ignored_errors.items():
        text = ", ".join(
            f"{action_class.__name__}: {count}"
            for action_class, count in counter.items()
        )
        print(f"  {error}: {text}")


def parse_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--seed", type=str, default=str(int(time.time())))
    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    parser.add_argument(
        "--complexity",
        default="ddl",
        type=str,
        choices=[elem.value for elem in Complexity] + ["random"],
    )
    parser.add_argument(
        "--scenario",
        default="regression",
        type=str,
        choices=[elem.value for elem in Scenario] + ["random"],
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Number of threads to run, by default number of SMT threads",
    )
    parser.add_argument(
        "--naughty-identifiers",
        action="store_true",
        help="Whether to use naughty strings as identifiers, makes the queries unreadable",
    )
    parser.add_argument(
        "--fast-startup",
        action="store_true",
        help="Whether to initialize expensive parts like SQLsmith, sources, sinks (for fast local testing, reduces coverage)",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="parallel-workload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Run a parallel workload againt Materialize",
    )

    parser.add_argument("--host", default="localhost", type=str)
    parser.add_argument("--port", default=6875, type=int)
    parser.add_argument("--system-port", default=6877, type=int)
    parser.add_argument("--http-port", default=6876, type=int)
    parse_common_args(parser)

    args = parser.parse_args()

    ports: dict[str, int] = {
        "materialized": 6875,
        "mz_system": 6877,
        "http": 6876,
        "kafka": 9092,
        "schema-registry": 8081,
    }

    system_conn = pg8000.connect(
        host=args.host,
        port=ports["mz_system"],
        user="mz_system",
        database="materialize",
    )
    system_conn.autocommit = True
    with system_conn.cursor() as cur:
        # TODO: Currently the same as mzcompose default settings, add
        # more settings and shuffle them
        for key, value in DEFAULT_SYSTEM_PARAMETERS.items():
            cur.execute(f"ALTER SYSTEM SET {key} = '{value}'")
    system_conn.close()

    random.seed(args.seed)

    run(
        args.host,
        ports,
        args.seed,
        args.runtime,
        Complexity(args.complexity),
        Scenario(args.scenario),
        args.threads,
        args.naughty_identifiers,
        args.fast_startup,
        composition=None,  # only works in mzcompose
        catalog_store="",  # only works in mzcompose
        sanity_restart=False,  # only works in mzcompose
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
