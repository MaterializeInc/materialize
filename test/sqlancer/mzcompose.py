# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import random
from threading import Thread

from materialize import spawn
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


def print_logs(container_id: str) -> None:
    spawn.runv(["docker", "logs", "-f", container_id])


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--runtime", default=600, type=int)
    parser.add_argument("--num-tries", default=100000, type=int)
    parser.add_argument("--num-threads", default=4, type=int)
    parser.add_argument("--seed", default=None, type=int)
    parser.add_argument("--qpg", default=True, action=argparse.BooleanOptionalAction)
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

    print("~~~ Run in progress")
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
        "--timeout-seconds",
        f"{args.runtime}",
        "--num-tries",
        f"{args.num_tries}",
        "--num-threads",
        f"{args.num_threads}",
        "--qpg-enable",
        f"{args.qpg}",
        "--random-string-generation",
        "ALPHANUMERIC_SPECIALCHAR",
        "materialize",
        "--oracle",
        args.oracle,
        check=False,
        detach=True,
        capture=True,
    )
    container_id = result.stdout.strip()

    # Print logs in a background thread so that we get immediate output in CI,
    # and also when running SQLancer locally
    thread = Thread(target=print_logs, args=(container_id,))
    thread.start()
    # At the same time capture the logs to analyze for finding new issues
    stdout = spawn.capture(["docker", "logs", "-f", container_id])

    in_assertion = False
    for line in stdout.splitlines():
        if line.startswith("--java.lang.AssertionError: "):
            in_assertion = True
            print(f"--- [SQLancer] {line.removeprefix('--java.lang.AssertionError: ')}")
        elif line == "":
            in_assertion = False
        elif in_assertion:
            print(line)
    print(f"--- {result.stdout.splitlines()[-1]}")
