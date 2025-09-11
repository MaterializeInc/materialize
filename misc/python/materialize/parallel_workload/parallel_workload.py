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
import gc
import os
import random
import sys
import threading
import time
from collections import Counter, defaultdict

import psycopg
from prettytable import PrettyTable

from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition
from materialize.parallel_workload.action import (
    Action,
    ActionList,
    BackupRestoreAction,
    CancelAction,
    KillAction,
    StatisticsAction,
    ZeroDowntimeDeployAction,
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
    replicas: int,
    composition: Composition | None,
    azurite: bool,
    sanity_restart: bool,
) -> None:
    num_threads = num_threads or os.cpu_count() or 10

    rng = random.Random(random.randrange(SEED_RANGE))

    print(
        f"+++ Running with: --seed={seed} --threads={num_threads} --runtime={runtime} --complexity={complexity.value} --scenario={scenario.value} {'--naughty-identifiers ' if naughty_identifiers else ''} --replicas={replicas} (--host={host})"
    )
    initialize_logging()

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=runtime)
    ).timestamp()

    database = Database(
        rng, seed, host, ports, complexity, scenario, naughty_identifiers
    )

    system_conn = psycopg.connect(
        host=host, port=ports["mz_system"], user="mz_system", dbname="materialize"
    )
    system_conn.autocommit = True
    with system_conn.cursor() as system_cur:
        system_exe = Executor(rng, system_cur, None, database)
        system_exe.execute(
            f"ALTER SYSTEM SET max_schemas_per_database = {MAX_SCHEMAS * 40 + num_threads}"
        )
        # The presence of ALTER TABLE RENAME can cause the total number of tables to exceed MAX_TABLES
        system_exe.execute(
            f"ALTER SYSTEM SET max_tables = {MAX_TABLES * 40 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_materialized_views = {MAX_VIEWS * 40 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_sources = {(MAX_WEBHOOK_SOURCES + MAX_KAFKA_SOURCES + MAX_POSTGRES_SOURCES) * 40 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_sinks = {MAX_KAFKA_SINKS * 40 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_roles = {MAX_ROLES * 1000 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_clusters = {MAX_CLUSTERS * 40 + num_threads}"
        )
        system_exe.execute(
            f"ALTER SYSTEM SET max_replicas_per_cluster = {MAX_CLUSTER_REPLICAS * 40 + num_threads}"
        )
        system_exe.execute("ALTER SYSTEM SET max_secrets = 1000000")
        system_exe.execute("ALTER SYSTEM SET idle_in_transaction_session_timeout = 0")
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

        if replicas > 1:
            system_exe.execute("DROP CLUSTER quickstart CASCADE")
            replica_names = [f"r{replica_id}" for replica_id in range(0, replicas)]
            replica_string = ",".join(
                f"{replica_name} (SIZE 'scale=1,workers=4')"
                for replica_name in replica_names
            )
            system_exe.execute(
                f"CREATE CLUSTER quickstart REPLICAS ({replica_string})",
            )

        system_conn.close()
        conn = psycopg.connect(
            host=host,
            port=ports["materialized"],
            user="materialize",
            dbname="materialize",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            assert composition
            database.create(Executor(rng, cur, None, database), composition)
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
        elif complexity == Complexity.DDLOnly:
            weights = [0, 0, 0, 0, 100]
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
            args=(host, ports["materialized"], ports["http"], "materialize", database),
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
            args=(host, ports["mz_system"], ports["http"], "mz_system", database),
        )
        thread.start()
        threads.append(thread)
    elif scenario == Scenario.Kill:
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        assert composition, "Kill scenario only works in mzcompose"
        worker = Worker(
            worker_rng,
            [KillAction(worker_rng, composition, azurite, sanity_restart)],
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
            args=(host, ports["materialized"], ports["http"], "materialize", database),
        )
        thread.start()
        threads.append(thread)
    elif scenario == Scenario.ZeroDowntimeDeploy:
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        assert composition, "ZeroDowntimeDeploy scenario only works in mzcompose"
        worker = Worker(
            worker_rng,
            [
                ZeroDowntimeDeployAction(
                    worker_rng,
                    composition,
                    azurite,
                    sanity_restart,
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
            name="zero-downtime-deploy",
            target=worker.run,
            args=(host, ports["materialized"], ports["http"], "materialize", database),
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
            args=(host, ports["materialized"], ports["http"], "materialize", database),
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
            args=(host, ports["mz_system"], ports["http"], "mz_system", database),
        )
        thread.start()
        threads.append(thread)

    num_queries = defaultdict(Counter)
    try:
        while time.time() < end_time:
            for thread in threads:
                if not thread.is_alive():
                    occurred_exception = None
                    for worker in workers:
                        worker.end_time = time.time()
                        occurred_exception = (
                            occurred_exception or worker.occurred_exception
                        )
                    raise WorkerFailedException(
                        f"^^^ +++ Thread {thread.name} failed, exiting",
                        occurred_exception,
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
        datetime.datetime.now() + datetime.timedelta(seconds=600)
    ).timestamp()
    while time.time() < stopping_time:
        for thread in threads:
            thread.join(timeout=1)
        if all([not thread.is_alive() for thread in threads]):
            break
    else:
        for worker, thread in zip(workers, threads):
            if thread.is_alive():
                print(
                    f"{thread.name} still running ({worker.exe.mz_service}): {worker.exe.last_log} ({worker.exe.last_status})"
                )
        print_cluster_replica_stats(host, ports)
        print_stats(num_queries, workers, num_threads, scenario)

        if num_threads >= 50:
            # Under high load some queries can't finish quickly, especially UPDATE/DELETE
            os._exit(0)
        if scenario == scenario.ZeroDowntimeDeploy:
            # With 0dt deploys connections against the currently-fenced-out
            # environmentd will be stuck forever, the promoted environmentd can
            # take > 10 minutes to become responsive as well
            os._exit(0)
        print("Threads have not stopped within 10 minutes, exiting hard")
        os._exit(1)

    try:
        conn = psycopg.connect(
            host=host, port=ports["materialized"], user="materialize"
        )
    except Exception as e:
        if scenario == Scenario.ZeroDowntimeDeploy:
            print(f"Failed connecting to materialized, using materialized2: {e}")
            conn = psycopg.connect(
                host=host, port=ports["materialized2"], user="materialize"
            )
        else:
            raise

    conn.autocommit = True

    with conn.cursor() as cur:
        # Dropping the database also releases the long running connections
        # used by database objects.
        database.drop(Executor(rng, cur, None, database))

        # Make sure all unreachable connections are closed too
        gc.collect()

        stopping_time = datetime.datetime.now() + datetime.timedelta(seconds=30)
        while datetime.datetime.now() < stopping_time:
            cur.execute(
                "SELECT * FROM mz_internal.mz_sessions WHERE connection_id <> pg_backend_pid()"
            )
            sessions = cur.fetchall()
            if len(sessions) == 0:
                break
            print(
                f"Sessions are still running even though all threads are done: {sessions}"
            )
        # TODO(def-): Why is this failing with psycopg?
        # else:
        #     raise ValueError("Sessions did not clean up within 30s of threads stopping")
    conn.close()

    print_stats(num_queries, workers, num_threads, scenario)


def print_cluster_replica_stats(host: str, ports: dict[str, int]) -> None:
    system_conn = psycopg.connect(
        host=host,
        port=ports["mz_system"],
        user="mz_system",
        dbname="materialize",
    )
    system_conn.autocommit = True
    with system_conn.cursor() as cur:
        cur.execute("SHOW cluster replicas;")
        cluster_replicas = cur.fetchall()
        clusters = set()
        for r in cluster_replicas:
            cluster = r[0]
            if cluster in clusters:
                continue
            clusters.add(cluster)
            replica = r[1]
            cur.execute(f"SET cluster = '{cluster}';".encode())
            cur.execute(f"SET cluster_replica = '{replica}';".encode())
            print(f"--- {cluster}.{replica}: Expensive dataflows")
            cur.execute(
                """SELECT
                    mdo.id,
                    mdo.name,
                    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
                FROM mz_introspection.mz_scheduling_elapsed AS mse,
                    mz_introspection.mz_dataflow_operators AS mdo,
                    mz_introspection.mz_dataflow_addresses AS mda
                WHERE mse.id = mdo.id AND mdo.id = mda.id AND list_length(address) = 1
                ORDER BY elapsed_ns DESC;"""
            )
            rows = cur.fetchall()
            assert cur.description
            headers = [desc[0] for desc in cur.description]
            table = PrettyTable()
            table.field_names = headers
            for row in rows:
                table.add_row(list(row))
            print(table)

            print(f"--- {cluster}.{replica}: Expensive operators")
            cur.execute(
                """SELECT
                    mdod.id,
                    mdod.name,
                    mdod.dataflow_name,
                    mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
                FROM mz_introspection.mz_scheduling_elapsed AS mse,
                    mz_introspection.mz_dataflow_addresses AS mda,
                    mz_introspection.mz_dataflow_operator_dataflows AS mdod
                WHERE
                    mse.id = mdod.id AND mdod.id = mda.id
                    -- exclude regions and just return operators
                    AND mda.address NOT IN (
                        SELECT DISTINCT address[:list_length(address) - 1]
                        FROM mz_introspection.mz_dataflow_addresses
                    )
                ORDER BY elapsed_ns DESC;"""
            )
            rows = cur.fetchall()
            assert cur.description
            headers = [desc[0] for desc in cur.description]
            table = PrettyTable()
            table.field_names = headers
            for row in rows:
                table.add_row(list(row))
            print(table)


def print_stats(
    num_queries: defaultdict[ActionList, Counter[type[Action]]],
    workers: list[Worker],
    num_threads: int,
    scenario: Scenario,
) -> None:
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
    if num_threads < 50 and scenario != scenario.ZeroDowntimeDeploy:
        assert failed < 50
    else:
        assert failed < 75


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
    parser.add_argument(
        "--azurite", action="store_true", help="Use Azurite as blob store instead of S3"
    )
    parser.add_argument("--replicas", type=int, default=2, help="use multiple replicas")


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="parallel-workload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Run a parallel workload against Materialize",
    )

    parser.add_argument("--host", default="127.0.0.1", type=str)
    parser.add_argument("--port", default=6875, type=int)
    parser.add_argument("--system-port", default=6877, type=int)
    parser.add_argument("--http-port", default=6876, type=int)
    parse_common_args(parser)

    args = parser.parse_args()

    ports: dict[str, int] = {
        "materialized": args.port,
        "mz_system": args.system_port,
        "http": 6876,
        "kafka": 9092,
        "schema-registry": 8081,
    }

    system_conn = psycopg.connect(
        host=args.host,
        port=ports["mz_system"],
        user="mz_system",
        dbname="materialize",
    )
    system_conn.autocommit = True
    with system_conn.cursor() as cur:
        # TODO: Currently the same as mzcompose default settings, add
        # more settings and shuffle them
        for key, value in get_default_system_parameters().items():
            cur.execute(f"ALTER SYSTEM SET {key} = '{value}'".encode())
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
        args.replicas,
        composition=None,  # only works in mzcompose
        azurite=args.azurite,
        sanity_restart=False,  # only works in mzcompose
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
