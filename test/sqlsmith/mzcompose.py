# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import os
import random
import time
from datetime import datetime
from threading import Thread
from typing import Any, Dict, FrozenSet, List, Tuple

from materialize.mzcompose import Composition, Service, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized

if os.getenv("BUILDKITE_AGENT_META_DATA_AWS_INSTANCE_TYPE") == "c5.2xlarge":
    TOTAL_MEMORY = 12
    NUM_SERVERS = 2
else:
    TOTAL_MEMORY = 48
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


# These are only errors which have no associated issues since they are not
# considered product bugs, but SQLsmith generating bad queries. Use ci-regexp
# in Github issues for actual product bugs.
known_errors = [
    "no connection to the server",  # Expected AFTER a crash, the query before this is interesting, not the ones after
    "failed: Connection refused",  # Expected AFTER a crash, the query before this is interesting, not the ones after
    "canceling statement due to statement timeout",
    "value too long for type",
    "list_agg on char not yet supported",
    "does not allow subqueries",
    "range constructor flags argument must not be null",  # expected after https://github.com/MaterializeInc/materialize/issues/18036 has been fixed
    "function pg_catalog.array_remove(",
    "function pg_catalog.array_cat(",
    "function mz_catalog.list_append(",
    "function mz_catalog.list_prepend(",
    "does not support implicitly casting from",
    "aggregate functions that refer exclusively to outer columns not yet supported",  # https://github.com/MaterializeInc/materialize/issues/3720
    "range lower bound must be less than or equal to range upper bound",
    "violates not-null constraint",
    "division by zero",
    "zero raised to a negative power is undefined",
    "operator does not exist",  # For list types
    "couldn't parse role id",
    "unrecognized privilege type:",
    "cannot return complex numbers",
    "statement batch size cannot exceed",
    "length must be nonnegative",
    "is only defined for finite arguments",
    "more than one record produced in subquery",
    "invalid range bound flags",
    "invalid input syntax for type jsonb",
    "invalid regular expression",
    "invalid input syntax for type date",
    "invalid escape string",
    "invalid hash algorithm",
    "is defined for numbers greater than or equal to",
    "is not defined for zero",
    "is not defined for negative numbers",
    "requested character too large for encoding",
    "internal error: unrecognized configuration parameter",
    "invalid encoding name",
    "invalid time zone",
    "value out of range: overflow",
    "value out of range: underflow",
    "LIKE pattern exceeds maximum length",
    "negative substring length not allowed",
    "cannot take square root of a negative number",
    "timestamp units not yet supported",
    "step size cannot equal zero",
    "stride must be greater than zero",
    "timestamp out of range",
    "integer out of range",
    "unterminated escape sequence in LIKE",
    "null character not permitted",
    "is defined for numbers between",
    "field position must be greater than zero",
    "' not recognized",  # Expected, see https://github.com/MaterializeInc/materialize/issues/17981
    "must appear in the GROUP BY clause or be used in an aggregate function",
    "Expected joined table, found",  # Should fix for multi table join
    "Expected ON, or USING after JOIN, found",  # Should fix for multi table join
    "but expression is of type",  # Should fix, but only happens rarely
    "coalesce could not convert type map",  # Should fix, but only happens rarely
    "operator does not exist: map",  # Should fix, but only happens rarely
    "result exceeds max size of",  # Seems expected with huge queries
    "expected expression, but found reserved keyword",  # Should fix, but only happens rarely with subqueries
    "Expected right parenthesis, found left parenthesis",  # Should fix, but only happens rarely with cast+coalesce
    "invalid selection: operation may only refer to user-defined tables",  # Seems expected when using catalog tables
    "Unsupported temporal predicate",  # Expected, see https://github.com/MaterializeInc/materialize/issues/18048
    "OneShot plan has temporal constraints",  # Expected, see https://github.com/MaterializeInc/materialize/issues/18048
    "internal error: cannot evaluate unmaterializable function",  # Currently expected, see https://github.com/MaterializeInc/materialize/issues/14290
]


def is_known_error(e: str) -> bool:
    for known_error in known_errors:
        if known_error in e:
            return True
    return False


def run_sqlsmith(c: Composition, cmd: str, aggregate: Dict[str, Any]) -> None:
    result = c.run(
        *cmd,
        capture=True,
        check=False,  # We still get back parsable json on failure, so keep going
    )

    if result.returncode not in (0, 1):
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
    # https://github.com/MaterializeInc/materialize/issues/2392
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
              FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.00001)
              FOR ALL TABLES
              WITH (SIZE = '1');

            CREATE SOURCE auction
              FROM LOAD GENERATOR AUCTION (SCALE FACTOR 0.01)
              FOR ALL TABLES
              WITH (SIZE = '1');

            CREATE SOURCE counter
              FROM LOAD GENERATOR COUNTER (SCALE FACTOR 0.0001)
              WITH (SIZE = '1');

            CREATE TABLE t (a int, b int);
            INSERT INTO t VALUES (1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (11, 12), (13, 14), (15, 16);
            CREATE MATERIALIZED VIEW mv AS SELECT a + b FROM t;
            CREATE MATERIALIZED VIEW mv2 AS SELECT count(*) FROM mv;
            CREATE DEFAULT INDEX ON mv;
            """,
            service=mz_server,
        )

    seed = args.seed or random.randint(0, 2**31 - args.num_sqlsmith)

    def kill_sqlsmith_with_delay() -> None:
        time.sleep(args.runtime)
        c.kill("sqlsmith", signal="SIGINT")

    killer = Thread(target=kill_sqlsmith_with_delay)
    killer.start()

    threads: List[Thread] = []
    aggregate: Dict[str, Any] = {"errors": [], "version": "", "queries": 0}
    for i in range(args.num_sqlsmith):
        cmd = [
            "sqlsmith",
            # f"--max-queries={args.queries}",
            f"--max-joins={args.max_joins}",
            f"--seed={seed + i}",
            "--log-json",
            f"--target=host={MZ_SERVERS[i % len(MZ_SERVERS)]} port=6875 dbname=materialize user=materialize",
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

    new_errors: Dict[FrozenSet[Tuple[str, Any]], List[Dict[str, Any]]] = {}
    for error in aggregate["errors"]:
        if not is_known_error(error["message"]):
            frozen_key = frozenset(
                {x: error[x] for x in ["type", "sqlstate", "message"]}.items()
            )
            if frozen_key not in new_errors:
                new_errors[frozen_key] = []
            new_errors[frozen_key].append({x: error[x] for x in ["timestamp", "query"]})

    print(
        f"SQLsmith: {aggregate['version']} seed: {seed} queries: {aggregate['queries']}"
    )
    for frozen_key, errors in new_errors.items():
        key = dict(frozen_key)
        occurences = f" ({len(errors)} occurences)" if len(errors) > 1 else ""
        print(
            f"--- [SQLsmith] {key['type']} {key['sqlstate']}: {key['message']}{occurences}"
        )
        if len(errors) > 1:
            from_time = datetime.fromtimestamp(errors[0]["timestamp"]).strftime(
                "%H:%M:%S"
            )
            to_time = datetime.fromtimestamp(errors[-1]["timestamp"]).strftime(
                "%H:%M:%S"
            )
            print(f"From {from_time} until {to_time}")

        shortest_query = min([error["query"] for error in errors], key=len)
        print(f"Query: {shortest_query}")
