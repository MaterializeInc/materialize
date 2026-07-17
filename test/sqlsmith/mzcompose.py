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
from threading import Lock, Thread
from typing import Any

from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.sqlsmith import known_errors

TOTAL_MEMORY = 16
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
        default_replication_factor=2,
        additional_system_parameter_defaults={
            "enable_with_ordinality_legacy_fallback": "true"
        },
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


aggregate_lock = Lock()


def run_sqlsmith(c: Composition, cmd: list[str], aggregate: dict[str, Any]) -> None:
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
    with aggregate_lock:
        aggregate["version"] = data["version"]
        aggregate["queries"] += data["queries"]
        aggregate["errors"].extend(data["errors"])
        for imp in data.get("impedance", []):
            entry = aggregate["impedance"].setdefault(
                imp["prod"], {"ok": 0, "bad": 0, "retries": 0}
            )
            entry["ok"] += imp["ok"]
            entry["bad"] += imp["bad"]
            entry["retries"] += imp["retries"]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--num-sqlsmith", default=2 * len(MZ_SERVERS), type=int)
    # parser.add_argument("--queries", default=10000, type=int)
    parser.add_argument("--runtime", default=600, type=int)
    parser.add_argument("--max-joins", default=5, type=int)
    parser.add_argument("--explain-only", action="store_true")
    parser.add_argument("--exclude-catalog", action="store_true")
    parser.add_argument("--seed", default=None, type=int)
    args = parser.parse_args()

    c.up(*MZ_SERVERS)

    for mz_server in MZ_SERVERS:
        # Simple data for our workload. NULLs and type edge values
        # everywhere, an empty table (t4), a larger table (t5) so
        # non-trivial arrangement sizes occur, indexes so index-based plans
        # (lookup/delta joins) occur, and arrays/lists/maps/ranges so
        # functions over those types have interesting inputs. NOTE: t5 is
        # deliberately capped at 100 rows. Larger sizes make random
        # multi-way joins OOM the memory-capped clusterd regularly.
        c.sql(
            """
            CREATE TABLE t1 (a int2, b int4, c int8, d uint2, e uint4, f uint8, g text);
            INSERT INTO t1 VALUES (1, 2, 3, 4, 5, 6, '7'), (3, 4, 5, 6, 7, 8, '9'), (5, 6, 7, 8, 9, 10, '11'), (7, 8, 9, 10, 11, 12, '13'), (9, 10, 11, 12, 13, 14, '15'), (11, 12, 13, 14, 15, 16, '17'), (13, 14, 15, 16, 17, 18, '19'), (15, 16, 17, 18, 19, 20, '21');
            INSERT INTO t1 VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL), (0, NULL, -9223372036854775808, 0, NULL, 18446744073709551615, ''), (-32768, 2147483647, NULL, 65535, 4294967295, NULL, 'ß');
            CREATE INDEX t1_idx_a ON t1 (a);
            CREATE INDEX t1_idx_bc ON t1 (b, c);
            CREATE MATERIALIZED VIEW mv AS SELECT a + b AS col1, c + d AS col2, e + f AS col3, g AS col4 FROM t1;
            CREATE MATERIALIZED VIEW mv2 AS SELECT count(*) FROM mv;
            CREATE DEFAULT INDEX ON mv;
            CREATE VIEW v1 AS SELECT col1 + col2 AS c1, col4 AS c2 FROM mv WHERE col1 IS NOT NULL;
            CREATE TABLE t2 (
                a_bool        BOOL,
                b_float4      FLOAT4,
                c_float8      FLOAT8,
                d_numeric     NUMERIC,
                e_char        CHAR(5),
                f_varchar     VARCHAR(10),
                g_bytes       BYTES,
                h_date        DATE,
                i_time        TIME,
                k_timestamp   TIMESTAMP,
                l_timestamptz TIMESTAMPTZ,
                m_interval    INTERVAL,
                n_jsonb       JSONB,
                o_uuid        UUID
            );

            INSERT INTO t2 VALUES
            (
                TRUE,
                1.23,
                4.56,
                7.89,
                'abc',
                'hello',
                '\x68656c6c6f',
                DATE '2023-01-01',
                TIME '12:34:56',
                TIMESTAMP '2023-01-01 12:34:56',
                TIMESTAMPTZ '2023-01-01 12:34:56+00',
                INTERVAL '1 day 2 hours',
                '{"key": "value"}',
                '550e8400-e29b-41d4-a716-446655440000'
            ),
            (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            (
                FALSE,
                'NaN',
                '-inf',
                -0.000000001,
                '',
                'ß',
                '\\x00',
                DATE '0001-01-01',
                TIME '23:59:59.999999',
                TIMESTAMP '99999-12-31 23:59:59',
                TIMESTAMPTZ '0001-01-01 00:00:00+00',
                INTERVAL '-178956970 years',
                '[1, [2, {"a": null}]]',
                '00000000-0000-0000-0000-000000000000'
            );

            CREATE MATERIALIZED VIEW mv3 AS SELECT * FROM t2;
            CREATE TABLE t3 (a int4 list, b text list, c map[text=>text], d int4[], e text[], f int4range, g numeric(38,2), h char(1), i oid);
            INSERT INTO t3 VALUES (LIST[1,2,3], LIST['a','b'], '{a=>b}'::map[text=>text], ARRAY[1,2,3], ARRAY['a','b',NULL], int4range(1,10), 1234.56, 'x', 42);
            INSERT INTO t3 VALUES (LIST[]::int4 list, LIST[NULL]::text list, '{}'::map[text=>text], ARRAY[]::int4[], ARRAY[]::text[], int4range(5,5), -0.01, '', 0);
            INSERT INTO t3 VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
            CREATE TABLE t4 (a int4, b text);
            CREATE TABLE t5 (a int4, b int8, c text);
            INSERT INTO t5 SELECT a, a::int8 * 37, 'val_' || a FROM generate_series(1, 100) g(a);
            CREATE INDEX t5_idx_a ON t5 (a);
            """,
            service=mz_server,
        )
        c.sql(
            """
            ALTER SYSTEM SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            ALTER SYSTEM SET CLUSTER_REPLICA TO 'r1';
            ALTER SYSTEM SET enable_rbac_checks TO true;
            """,
            service=mz_server,
            port=6877,
            user="mz_system",
        )
        # An unprivileged role for some of the instances so access control
        # paths are exercised too. It can read the user tables but catalog
        # restrictions and ownership checks still apply.
        c.sql(
            """
            CREATE ROLE fuzz;
            GRANT USAGE ON DATABASE materialize TO fuzz;
            GRANT USAGE ON SCHEMA materialize.public TO fuzz;
            GRANT SELECT ON ALL TABLES IN SCHEMA materialize.public TO fuzz;
            GRANT USAGE ON CLUSTER quickstart TO fuzz;
            """,
            service=mz_server,
            port=6877,
            user="mz_system",
        )

    seed = args.seed or random.randint(0, 2**31 - args.num_sqlsmith)

    def kill_sqlsmith_with_delay() -> None:
        time.sleep(args.runtime)
        c.kill("sqlsmith", signal="SIGINT")

    killer = Thread(target=kill_sqlsmith_with_delay)
    killer.start()

    threads: list[Thread] = []
    aggregate: dict[str, Any] = {
        "errors": [],
        "version": "",
        "queries": 0,
        "impedance": {},
    }
    exceptions: list[Exception] = []

    def run_sqlsmith_thread(cmd: list[str]) -> None:
        try:
            run_sqlsmith(c, cmd, aggregate)
        except Exception as e:
            # store instead of raise, exceptions in threads don't
            # propagate to the workflow
            exceptions.append(e)

    for i in range(args.num_sqlsmith):
        mz_server = MZ_SERVERS[i % len(MZ_SERVERS)]
        # Most instances use mz_system to have access to all tables,
        # including ones with restricted permissions. Every fourth instance
        # uses the unprivileged role instead so authorization paths are
        # fuzzed as well.
        if i % 4 == 3:
            target = f"host={mz_server} port=6875 dbname=materialize user=fuzz"
        else:
            target = f"host={mz_server} port=6877 dbname=materialize user=mz_system"
        cmd = [
            "sqlsmith",
            # f"--max-queries={args.queries}",
            f"--max-joins={args.max_joins}",
            f"--seed={seed + i}",
            "--log-json",
            f"--target={target}",
        ]
        if args.exclude_catalog:
            cmd.append("--exclude-catalog")
        if args.explain_only:
            cmd.append("--explain-only")

        thread = Thread(target=run_sqlsmith_thread, args=[cmd])
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

    # Which productions of the grammar failed how often, so coverage
    # regressions (a production erroring so much it gets blacklisted) are
    # visible in the log.
    print("Impedance (failed/succeeded queries containing the production):")
    for prod, entry in sorted(
        aggregate["impedance"].items(), key=lambda x: -x[1]["bad"]
    ):
        blacklist_note = ""
        if entry["bad"] >= 100 and entry["bad"] / (entry["bad"] + entry["ok"]) > 0.99:
            blacklist_note = " (BLACKLISTED)"
        print(f"  {prod}: {entry['bad']}/{entry['ok']}{blacklist_note}")
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

    # Raise only after reporting the errors, they are the more useful signal
    if exceptions:
        raise Exception(
            f"{len(exceptions)} SQLsmith instance(s) failed, first failure: {exceptions[0]}"
        ) from exceptions[0]
