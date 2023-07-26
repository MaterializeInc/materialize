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
from typing import DefaultDict, List, Optional, Type

import pg8000

from materialize.mzcompose import Composition
from materialize.mzcompose.services import DEFAULT_SYSTEM_PARAMETERS
from materialize.parallel_workload.action import (
    Action,
    CancelAction,
    KillAction,
    ddl_action_list,
    dml_nontrans_action_list,
    fetch_action_list,
    read_action_list,
    write_action_list,
)
from materialize.parallel_workload.database import Database
from materialize.parallel_workload.executor import Executor, initialize_logging
from materialize.parallel_workload.settings import Complexity, Scenario
from materialize.parallel_workload.worker import Worker

SEED_RANGE = 1_000_000
REPORT_TIME = 10


def run(
    host: str,
    port: int,
    system_port: int,
    seed: str,
    runtime: int,
    complexity: Complexity,
    scenario: Scenario,
    num_threads: Optional[int],
    composition: Optional[Composition],
) -> None:
    num_threads = num_threads or os.cpu_count() or 10
    random.seed(seed)

    print(
        f"--- Running with: --seed={seed} --threads={num_threads} --runtime={runtime} --complexity={complexity.value} --scenario={scenario.value} (--host={host} --port={port})"
    )
    initialize_logging()

    system_conn = pg8000.connect(
        host=host, port=system_port, user="mz_system", database="materialize"
    )
    system_conn.autocommit = True
    with system_conn.cursor() as cur:
        cur.execute("ALTER SYSTEM SET enable_managed_clusters = true")
        cur.execute("ALTER SYSTEM SET max_schemas_per_database = 105")
        cur.execute("ALTER SYSTEM SET max_tables = 105")
        cur.execute("ALTER SYSTEM SET max_materialized_views = 105")
        cur.execute("ALTER SYSTEM SET max_sources = 105")
        cur.execute("ALTER SYSTEM SET max_roles = 105")
        cur.execute("ALTER SYSTEM SET max_clusters = 105")
        cur.execute("ALTER SYSTEM SET max_replicas_per_cluster = 105")
    system_conn.close()

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=runtime)
    ).timestamp()

    rng = random.Random(random.randrange(SEED_RANGE))
    database = Database(rng, seed, host, port, system_port, complexity, scenario)
    conn = pg8000.connect(host=host, port=port, user="materialize")
    conn.autocommit = True
    with conn.cursor() as cur:
        database.create(Executor(rng, cur))
    conn.close()

    conn = pg8000.connect(
        host=host, port=port, user="materialize", database=str(database)
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        database.create_relations(Executor(rng, cur))
    conn.close()

    workers = []
    threads = []
    for i in range(num_threads):
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        weights: List[float]
        if complexity == Complexity.DDL:
            weights = [60, 30, 30, 30, 10]
        elif complexity == Complexity.DML:
            weights = [60, 30, 30, 30, 0]
        elif complexity == Complexity.Read:
            weights = [60, 30, 0, 0, 0]
        else:
            raise ValueError(f"Unknown complexity {complexity}")
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
            action_class(worker_rng, database)
            for action_class in action_list.action_classes
        ]
        worker = Worker(
            worker_rng,
            actions,
            action_list.weights,
            end_time,
            action_list.autocommit,
            system=False,
        )
        thread_name = f"worker_{i}"
        print(
            f"{thread_name}: {', '.join(action_class.__name__ for action_class in action_list.action_classes)}"
        )
        workers.append(worker)

        thread = threading.Thread(
            name=thread_name,
            target=worker.run,
            args=(host, port, "materialize", str(database)),
        )
        thread.start()
        threads.append(thread)

    if scenario == Scenario.Cancel:
        worker = Worker(
            worker_rng,
            [CancelAction(worker_rng, database, workers)],
            [1],
            end_time,
            autocommit=False,
            system=True,
        )
        workers.append(worker)
        thread = threading.Thread(
            name="cancel",
            target=worker.run,
            args=(host, system_port, "mz_system", str(database)),
        )
        thread.start()
        threads.append(thread)
    elif scenario == Scenario.Kill:
        assert composition, "Kill scenario only works in mzcompose"
        worker = Worker(
            worker_rng,
            [KillAction(worker_rng, database, composition)],
            [1],
            end_time,
            autocommit=False,
            system=False,
        )
        workers.append(worker)
        thread = threading.Thread(
            name="kill",
            target=worker.run,
            args=(host, port, "materialize", str(database)),
        )
        thread.start()
        threads.append(thread)
    elif scenario == Scenario.Regression:
        pass
    else:
        raise ValueError(f"Unknown scenario {scenario}")

    num_queries = 0
    try:
        while time.time() < end_time:
            for thread in threads:
                if not thread.is_alive():
                    for worker in workers:
                        worker.end_time = time.time()
                    raise Exception(f"Thread {thread.name} failed, exiting")
            time.sleep(REPORT_TIME)
            print(
                "QPS: "
                + " ".join(
                    f"{worker.num_queries / REPORT_TIME:05.1f}" for worker in workers
                )
            )
            for worker in workers:
                num_queries += worker.num_queries
                worker.num_queries = 0
    except KeyboardInterrupt:
        print("Keyboard interrupt, exiting")
        for worker in workers:
            worker.end_time = time.time()

    for thread in threads:
        thread.join()

    conn = pg8000.connect(host=host, port=port, user="materialize")
    conn.autocommit = True
    with conn.cursor() as cur:
        print(f"Dropping database {database}")
        database.drop(Executor(rng, cur))
    conn.close()

    ignored_errors: DefaultDict[str, Counter[Type[Action]]] = defaultdict(Counter)
    num_failures = 0
    for worker in workers:
        for action_class, counter in worker.ignored_errors.items():
            ignored_errors[action_class].update(counter)
    for counter in ignored_errors.values():
        for count in counter.values():
            num_failures += count

    failed = 100.0 * num_failures / num_queries if num_queries else 0
    print(f"Queries executed: {num_queries} ({failed:.0f}% failed)")
    print("Error statistics:")
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
        choices=[elem.value for elem in Complexity],
    )
    parser.add_argument(
        "--scenario",
        default="regression",
        type=str,
        choices=[elem.value for elem in Scenario],
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Number of threads to run, by default number of SMT threads",
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
    parse_common_args(parser)

    args = parser.parse_args()

    system_conn = pg8000.connect(
        host=args.host, port=args.system_port, user="mz_system", database="materialize"
    )
    system_conn.autocommit = True
    with system_conn.cursor() as cur:
        # TODO: Currently the same as mzcompose default settings, add
        # more settings and shuffle them
        for key, value in DEFAULT_SYSTEM_PARAMETERS.items():
            cur.execute(f"ALTER SYSTEM SET {key} = '{value}'")
    system_conn.close()

    run(
        args.host,
        args.port,
        args.system_port,
        args.seed,
        args.runtime,
        Complexity(args.complexity),
        Scenario(args.scenario),
        args.threads,
        composition=None,  # only works in mzcompose
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
