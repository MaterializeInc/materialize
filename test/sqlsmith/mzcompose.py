# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Use SQLsmith to generate random queries (AST/code based) and run them against
Materialize: https://github.com/MaterializeInc/sqlsmith The queries can be
complex, but we can't verify correctness or performance.
"""

import json
import random
import time
from datetime import datetime
from threading import Thread
from typing import Any

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.sqlsmith import known_errors

TOTAL_MEMORY = 8
NUM_SERVERS = 2
MZ_SERVERS = [f"mz_{i + 1}" for i in range(NUM_SERVERS)]

SERVICES = [
    # Auto-restart so we can keep testing even after we ran into a panic
    # Limit memory to prevent long hangs on out of memory
    # Don't use default volumes so we can run multiple instances at once
    Materialized(
        name=mz_server,
        restart="on-failure",
        memory=f"{TOTAL_MEMORY / len(MZ_SERVERS)}GB",
        use_default_volumes=False,
    )
    for mz_server in MZ_SERVERS
] + [
    Service(
        "sqlsmith",
        {
            "mzbuild": "sqlsmith",
        },
    ),
]


def is_known_error(e: str) -> bool:
    for known_error in known_errors:
        if known_error in e:
            return True
    return False


def run_sqlsmith(c: Composition, cmd: str, aggregate: dict[str, Any]) -> None:
    result = c.run(
        *cmd,
        capture=True,
        check=False,  # We still get back parsable json on failure, so keep going
    )

    if result.returncode not in (0, 1):
        if result.returncode == 137:
            raise Exception("[SQLsmith] OOMed (return code 137)")

        raise Exception(
            f"[SQLsmith] Unexpected return code in SQLsmith: {result.returncode}\n{result.stdout}"
        )

    data = json.loads(result.stdout)
    aggregate["version"] = data["version"]
    aggregate["queries"] += data["queries"]
    aggregate["errors"].extend(data["errors"])


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--num-sqlsmith", default=len(MZ_SERVERS), type=int)
    # parser.add_argument("--queries", default=10000, type=int)
    parser.add_argument("--runtime", default=600, type=int)
    # https://github.com/MaterializeInc/database-issues/issues/835
    parser.add_argument("--max-joins", default=2, type=int)
    parser.add_argument("--explain-only", action="store_true")
    parser.add_argument("--exclude-catalog", default=False, type=bool)
    parser.add_argument("--seed", default=None, type=int)
    args = parser.parse_args()

    c.up(*MZ_SERVERS)

    for mz_server in MZ_SERVERS:
        # Very simple data for our workload
        c.sql(
            """
            CREATE SOURCE tpch
              FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.00001);

            CREATE TABLE customer FROM SOURCE tpch (REFERENCE customer);
            CREATE TABLE lineitem FROM SOURCE tpch (REFERENCE lineitem);
            CREATE TABLE nation FROM SOURCE tpch (REFERENCE nation);
            CREATE TABLE orders FROM SOURCE tpch (REFERENCE orders);
            CREATE TABLE part FROM SOURCE tpch (REFERENCE part);
            CREATE TABLE partsupp FROM SOURCE tpch (REFERENCE partsupp);
            CREATE TABLE region FROM SOURCE tpch (REFERENCE region);
            CREATE TABLE supplier FROM SOURCE tpch (REFERENCE supplier);

            CREATE SOURCE auction
              FROM LOAD GENERATOR AUCTION;

            CREATE TABLE accounts FROM SOURCE auction (REFERENCE accounts);
            CREATE TABLE auctions FROM SOURCE auction (REFERENCE auctions);
            CREATE TABLE bids FROM SOURCE auction (REFERENCE bids);
            CREATE TABLE organizations FROM SOURCE auction (REFERENCE organizations);
            CREATE TABLE users FROM SOURCE auction (REFERENCE users);

            CREATE SOURCE counter
              FROM LOAD GENERATOR COUNTER;

            CREATE TABLE t (a int2, b int4, c int8, d uint2, e uint4, f uint8, g text);
            INSERT INTO t VALUES (1, 2, 3, 4, 5, 6, '7'), (3, 4, 5, 6, 7, 8, '9'), (5, 6, 7, 8, 9, 10, '11'), (7, 8, 9, 10, 11, 12, '13'), (9, 10, 11, 12, 13, 14, '15'), (11, 12, 13, 14, 15, 16, '17'), (13, 14, 15, 16, 17, 18, '19'), (15, 16, 17, 18, 19, 20, '21');
            CREATE MATERIALIZED VIEW mv AS SELECT a + b AS col1, c + d AS col2, e + f AS col3, g AS col4 FROM t;
            CREATE MATERIALIZED VIEW mv2 AS SELECT count(*) FROM mv;
            CREATE DEFAULT INDEX ON mv;

            CREATE MATERIALIZED VIEW mv3 WITH (REFRESH EVERY '2 seconds') AS SELECT * FROM counter;
            CREATE DEFAULT INDEX ON mv3;
            """,
            service=mz_server,
        )

    seed = args.seed or random.randint(0, 2**31 - args.num_sqlsmith)

    def kill_sqlsmith_with_delay() -> None:
        time.sleep(args.runtime)
        c.kill("sqlsmith", signal="SIGINT")

    killer = Thread(target=kill_sqlsmith_with_delay)
    killer.start()

    threads: list[Thread] = []
    aggregate: dict[str, Any] = {"errors": [], "version": "", "queries": 0}
    for i in range(args.num_sqlsmith):
        cmd = [
            "sqlsmith",
            # f"--max-queries={args.queries}",
            f"--max-joins={args.max_joins}",
            f"--seed={seed + i}",
            "--log-json",
            # we use mz_system to have access to all tables, including ones with restricted permissions
            f"--target=host={MZ_SERVERS[i % len(MZ_SERVERS)]} port=6877 dbname=materialize user=mz_system",
        ]
        if args.exclude_catalog:
            cmd.append("--exclude-catalog")
        if args.explain_only:
            cmd.append("--explain-only")

        thread = Thread(target=run_sqlsmith, args=[c, cmd, aggregate])
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    new_errors: dict[frozenset[tuple[str, Any]], list[dict[str, Any]]] = {}
    for error in aggregate["errors"]:
        if not is_known_error(error["message"]):
            frozen_key = frozenset(
                {x: error[x] for x in ["type", "sqlstate", "message"]}.items()
            )
            if frozen_key not in new_errors:
                new_errors[frozen_key] = []
            new_errors[frozen_key].append({x: error[x] for x in ["timestamp", "query"]})

    assert aggregate["queries"] > 0, "No queries were executed"

    print(
        f"SQLsmith: {aggregate['version']} seed: {seed} queries: {aggregate['queries']}"
    )
    for frozen_key, errors in new_errors.items():
        key = dict(frozen_key)
        occurrences = f" ({len(errors)} occurrences)" if len(errors) > 1 else ""
        # Print out crashes differently so that we don't get notified twice in ci_logged_errors_detect
        if "server closed the connection unexpectedly" in key["message"]:
            print(f"--- Server crash, check panics and segfaults {occurrences}")
        else:
            print(
                f"--- [SQLsmith] {key['type']} {key['sqlstate']}: {key['message']}{occurrences}"
            )
        if len(errors) > 1:
            from_time = datetime.fromtimestamp(errors[0]["timestamp"]).strftime(
                "%H:%M:%S"
            )
            to_time = datetime.fromtimestamp(errors[-1]["timestamp"]).strftime(
                "%H:%M:%S"
            )
            print(f"From {from_time} until {to_time}")

        # The error message indicates a panic, if we happen to get multiple
        # distinct panics we want to have all the responsible queries instead
        # of just one:
        if "server closed the connection unexpectedly" in key["message"]:
            for i, error in enumerate(errors, start=1):
                print(f"Query {i}: {error['query']}")
        else:
            shortest_query = min([error["query"] for error in errors], key=len)
            print(f"Query: {shortest_query}")
