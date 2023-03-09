# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
import time
from datetime import datetime
from threading import Thread
from typing import Any, Dict, FrozenSet, List, Tuple

from materialize.mzcompose import Composition, Service, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized

SERVICES = [
    # Auto-restart so we can keep testing even after we ran into a panic
    Materialized(restart="on-failure"),
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
    "violates not-null constraint",
    "division by zero",
    "operator does not exist",  # For list types
    "more than one record produced in subquery",
    "invalid range bound flags",
    "argument list must have even number of elements",
    "mz_row_size requires a record type",
    "invalid input syntax for type jsonb",
    "invalid regular expression",
    "aggregate functions are not allowed in",
    "invalid input syntax for type date",
    "invalid escape string",
    "invalid hash algorithm",
    "nested aggregate functions are not allowed",
    "is defined for numbers greater than or equal to",
    "is not defined for zero",
    "is not defined for negative numbers",
    "requested character too large for encoding",
    "internal error: unrecognized configuration parameter",
    "invalid encoding name",
    "invalid time zone",
    "value out of range: overflow",
    "LIKE pattern exceeds maximum length",
    "negative substring length not allowed",
    "cannot take square root of a negative number",
    "timestamp units not yet supported",
    "step size cannot equal zero",
    "stride must be greater than zero",
    "timestamp out of range",
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
]


def is_known_error(e: str) -> bool:
    for known_error in known_errors:
        if known_error in e:
            return True
    return False


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    # parser.add_argument("--max-queries", default=100000, type=int)
    parser.add_argument("--runtime", default=600, type=int)
    # https://github.com/MaterializeInc/materialize/issues/2392
    parser.add_argument("--max-joins", default=2, type=int)
    parser.add_argument("--explain-only", action="store_true")
    parser.add_argument("--exclude-catalog", default=False, type=bool)
    parser.add_argument("--seed", default=None, type=int)
    args = parser.parse_args()

    c.up("materialized")

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
          WITH (SIZE = '1');"""
    )

    seed = args.seed or random.randint(0, 2**31 - 1)

    def kill_sqlsmith_with_delay() -> None:
        time.sleep(args.runtime)
        c.kill("sqlsmith", signal="SIGINT")

    killer = Thread(target=kill_sqlsmith_with_delay)
    killer.start()

    cmd = [
        "sqlsmith",
        # f"--max-queries={args.queries}",
        f"--max-joins={args.max_joins}",
        f"--seed={seed}",
        "--log-json",
        f"--target=host=materialized port=6875 dbname=materialize user=materialize",
    ]
    if args.exclude_catalog:
        cmd.append("--exclude-catalog")
    if args.explain_only:
        cmd.append("--explain-only")

    result = c.run(
        *cmd,
        capture=True,
        check=False,  # We still get back parsable json on failure, so keep going
    )

    if result.returncode not in (0, 1):
        raise Exception(
            f"[SQLsmith] Unexpected return code in SQLsmith\n{result.stdout}"
        )

    data = json.loads(result.stdout)

    new_errors: Dict[FrozenSet[Tuple[str, Any]], List[Dict[str, Any]]] = {}
    for error in data["errors"]:
        if not is_known_error(error["message"]):
            frozen_key = frozenset(
                {x: error[x] for x in ["type", "sqlstate", "message"]}.items()
            )
            if frozen_key not in new_errors:
                new_errors[frozen_key] = []
            new_errors[frozen_key].append({x: error[x] for x in ["timestamp", "query"]})

    print(f"SQLsmith: {data['version']} seed: {seed} queries: {data['queries']}")
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
