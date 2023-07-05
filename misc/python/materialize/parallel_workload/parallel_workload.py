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
from typing import Optional

import pg8000

from materialize.parallel_workload.action import ddl_actions, dml_actions
from materialize.parallel_workload.database import Database
from materialize.parallel_workload.worker import Worker

SEED_RANGE = 1_000_000
REPORT_TIME = 10


def run(
    host: str,
    port: int,
    seed: str,
    runtime: int,
    complexity: str,
    num_threads: Optional[int],
) -> None:
    num_threads = num_threads or os.cpu_count() or 10
    print(f"Seed: {seed}")
    random.seed(seed)

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=runtime)
    ).timestamp()

    rng = random.Random(random.randrange(SEED_RANGE))
    database = Database(rng, seed)
    conn = pg8000.connect(host=host, port=port, user="materialize")
    conn.autocommit = True
    with conn.cursor() as cur:
        database.create(cur)
    conn.close()

    conn = pg8000.connect(
        host=host, port=port, user="materialize", database=str(database)
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        database.create_relations(cur)

    workers = []
    threads = []
    for i in range(num_threads):
        worker_rng = random.Random(rng.randrange(SEED_RANGE))
        chances = [80, 20] if complexity == "ddl" else [100, 0]
        actions = [
            (action[0](worker_rng, database), action[1])
            for action in worker_rng.choices([dml_actions, ddl_actions], chances, k=1)[
                0
            ]
        ]
        autocommit = type(actions[0][0]) == ddl_actions[0][0]
        worker = Worker(worker_rng, actions, end_time, autocommit)
        print(f"Worker{i}: {', '.join(type(action[0]).__name__ for action in actions)}")
        workers.append(worker)

        thread = threading.Thread(
            name=f"worker_{i}",
            target=worker.run,
            args=(host, port, str(database)),
        )
        thread.start()
        threads.append(thread)

    num_queries = 0
    try:
        while time.time() < end_time:
            for thread in threads:
                if not thread.is_alive():
                    for worker in workers:
                        worker.end_time = time.time()
                    raise Exception(f"Thread {thread.name} failed, exitting")
            time.sleep(REPORT_TIME)
            print(
                "".join(
                    f"[{worker.num_queries / REPORT_TIME:05.1f}]" for worker in workers
                )
            )
            for worker in workers:
                num_queries += worker.num_queries
                worker.num_queries = 0
    except KeyboardInterrupt:
        for worker in workers:
            worker.end_time = time.time()

    with conn.cursor() as cur:
        database.drop(cur)
    conn.close()

    for thread in threads:
        thread.join()

    print(f"Queries executed: {num_queries}")


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="parallel-workload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Run a parallel workload againt Materialize",
    )

    parser.add_argument("--host", default="localhost", type=str)
    parser.add_argument("--port", default=6875, type=int)
    parser.add_argument("--seed", type=str, default=str(int(time.time())))
    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    parser.add_argument("--complexity", default="ddl", type=str, choices=["dml", "ddl"])
    parser.add_argument(
        "--threads",
        type=int,
        help="Number of thread to run, by default number of SMT threads",
    )

    args = parser.parse_args()
    run(args.host, args.port, args.seed, args.runtime, args.complexity, args.threads)
    return 0


if __name__ == "__main__":
    sys.exit(main())
