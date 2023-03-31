# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time
from threading import Thread

from materialize.mzcompose import Composition, Service, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized

SERVICES = [
    # Auto-restart so we can keep testing even after we ran into a panic
    Materialized(
        restart="on-failure",
    ),
    Service(
        "sqlancer",
        {
            "mzbuild": "sqlancer",
        },
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", default=600, type=int)
    parser.add_argument("--num-tries", default=100000, type=int)
    parser.add_argument("--num-threads", default=4, type=int)
    parser.add_argument("--seed", default=None, type=int)
    parser.add_argument("--oracle", default="NOREC", type=str)
    args = parser.parse_args()

    c.up("materialized")

    c.sql(
        "ALTER SYSTEM SET max_tables TO 1000",
        user="mz_system",
        port=6877,
    )
    c.sql(
        "ALTER SYSTEM SET max_materialized_views TO 1000",
        user="mz_system",
        port=6877,
    )

    seed = args.seed or random.randint(0, 2**31)

    def kill_sqlancer_with_delay() -> None:
        time.sleep(args.runtime)
        c.kill("sqlancer", signal="SIGINT")

    killer = Thread(target=kill_sqlancer_with_delay)
    killer.start()

    result = c.run(
        "sqlancer",
        "--random-seed",
        f"{seed}",
        "--host",
        "materialized",
        "--port",
        "6875",
        "--username",
        "materialize",
        "--num-tries",
        f"{args.num_tries}",
        "--num-threads",
        f"{args.num_threads}",
        "--random-string-generation",
        "ALPHANUMERIC_SPECIALCHAR",
        "postgres",
        "--oracle",
        args.oracle,
        capture=True,
        capture_stderr=True,
        check=False,
    )

    if result.returncode not in (0, 130):
        raise Exception(
            f"[SQLancer] Unexpected return code in SQLancer: {result.returncode}\n{result.stdout}\n{result.stderr}"
        )

    in_assertion = False
    for line in result.stderr.splitlines():
        if line.startswith("--java.lang.AssertionError: "):
            in_assertion = True
            print(f"--- [SQLancer] {line.removeprefix('--java.lang.AssertionError: ')}")
        elif line == "":
            in_assertion = False
        elif in_assertion:
            print(line)
    print(f"--- {result.stdout.splitlines()[-1]}")
