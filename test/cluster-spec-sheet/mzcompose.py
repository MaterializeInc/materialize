# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Deploy the current version on a real Staging Cloud, and run some basic
verifications, like ingesting data from Kafka and Redpanda Cloud using AWS
Privatelink. Runs only on main and release branches.
"""

import argparse
import os
import re
import time
from abc import abstractmethod
from textwrap import dedent
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import pg8000
import pg8000.native

from materialize import MZ_ROOT
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.mz import Mz

REGION = "aws/us-east-1"
ENVIRONMENT = os.getenv("ENVIRONMENT", "staging")
USERNAME = os.getenv("MZ_USERNAME", "infra+nightly-canary@materialize.com")
APP_PASSWORD = os.getenv("MZ_APP_PASSWORD")

SERVICES = [
    Mz(
        region=REGION,
        environment=ENVIRONMENT,
        app_password=APP_PASSWORD or "",
    ),
]


def replica_size_for_scale(scale: int) -> str:
    """
    Returns the replica size for a given scale.
    """
    return f"{scale}00cc-swap"


class ScenarioRunner:
    def __init__(
        self,
        scenario: str,
        scale: int,
        mode: str,
        connection: pg8000.native.Connection,
        results_file: Any,
        replica_size: int,
    ) -> None:
        self.scenario = scenario
        self.scale = scale
        self.mode = mode
        self.connection = connection
        self.results_file = results_file
        self.replica_size = replica_size

    def add_result(
        self, category: str, name: str, repetition: int, size_bytes: int, time: float
    ) -> None:
        self.results_file.write(
            f"{self.scenario},{self.scale},{self.mode},{category},{name},{self.replica_size},{repetition},{size_bytes},{int(time * 1_000)}\n"
        )
        self.results_file.flush()

    def run_query(self, query: str, **params) -> list | None:
        query = dedent(query).strip()
        print(f"> {query} {params or ''}")
        return self.connection.run(query, **params)

    def measure(
        self,
        category: str,
        name: str,
        query: list[str],
        repetitions: int = 1,
        size_of_index: str | None = None,
    ) -> None:
        print(
            f"Running {name} for {self.replica_size} with {repetitions} repetitions..."
        )
        for repetition in range(repetitions):
            start_time = time.time()
            for query_part in query:
                self.run_query(query_part)
            end_time = time.time()
            if size_of_index:
                time.sleep(2)
                size_bytes = self.size_of_dataflow(f"%{size_of_index}")
                print(f"Size of index {size_of_index}: {size_bytes}")
            else:
                size_bytes = None
            self.add_result(
                category, name, repetition, size_bytes, (end_time - start_time)
            )

    def size_of_dataflow(self, object: str) -> int | None:
        retries = 10
        while retries > 0:
            result = self.run_query(
                "SELECT size FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE :name;",
                name=object,
            )
            if result:
                return int(result[0][0])
            retries -= 1
            if retries > 0:
                time.sleep(1)
        return None


class Scenario:
    def __init__(self, scale: int, replica_size: str) -> None:
        self.scale = scale
        self.replica_size = replica_size

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def setup(self) -> list[str]:
        pass

    @abstractmethod
    def drop(self) -> list[str]:
        pass

    @abstractmethod
    def materialize_views(self) -> list[str]:
        pass

    @abstractmethod
    def run(self, runner: ScenarioRunner) -> None:
        pass
        raise NotImplementedError("Subclasses should implement this method")


class TpchScenario(Scenario):

    def name(self) -> str:
        return "tpch"

    def materialize_views(self) -> list[str]:
        return ["lineitem"]

    def setup(self) -> list[str]:
        return [
            "DROP SOURCE IF EXISTS lgtpch CASCADE;",
            "DROP CLUSTER IF EXISTS lg CASCADE;",
            f"CREATE CLUSTER lg SIZE '{self.replica_size}';",
            f"CREATE SOURCE lgtpch IN CLUSTER lg FROM LOAD GENERATOR TPCH (SCALE FACTOR {10*self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
        ]

    def drop(self) -> list[str]:
        return ["DROP CLUSTER IF EXISTS lg CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.run_query("DROP INDEX IF EXISTS lineitem_primary_idx CASCADE")
        runner.run_query("SELECT * FROM t;")

        runner.measure(
            "arrangement_formation",
            "create_index_primary_key",
            query=[
                "CREATE DEFAULT INDEX ON lineitem;",
                "SELECT count(*) > 0 FROM lineitem;",
            ],
            size_of_index="lineitem_primary_idx",
        )

        time.sleep(3)

        index_size = runner.size_of_dataflow("%lineitem_primary_idx")
        print(f"Index size: {index_size}")

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_fast_path",
            size_of_index="lineitem_primary_idx",
            query=[
                "SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_non_key_fast_path",
            size_of_index="lineitem_primary_idx",
            query=[
                "SELECT * FROM lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        runner.run_query("SELECT count(*) > 0 FROM lineitem;")
        runner.run_query("ALTER CLUSTER c SET (REPLICATION FACTOR 0);")
        runner.measure(
            "arrangement_formation",
            "index_restart",
            size_of_index="lineitem_primary_idx",
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
        )


class TpchScenarioMV(Scenario):

    def name(self) -> str:
        return "tpch_mv"

    def materialize_views(self) -> list[str]:
        return ["lineitem"]

    def setup(self) -> list[str]:
        return [
            "DROP SOURCE IF EXISTS lgtpch CASCADE;",
            "DROP CLUSTER IF EXISTS lg CASCADE;",
            f"CREATE CLUSTER lg SIZE '{self.replica_size}';",
            f"CREATE SOURCE lgtpch IN CLUSTER lg FROM LOAD GENERATOR TPCH (SCALE FACTOR {10*self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
        ]

    def drop(self) -> list[str]:
        return ["DROP CLUSTER IF EXISTS lg CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.run_query("DROP MATERIALIZED VIEW IF EXISTS mv_lineitem CASCADE")
        runner.run_query("SELECT * FROM t;")
        runner.measure(
            "materialized_view_formation",
            "create_materialize_view",
            query=[
                "CREATE MATERIALIZED VIEW mv_lineitem AS SELECT * FROM lineitem;",
                "SELECT count(*) > 0 FROM mv_lineitem;",
            ],
            size_of_index="mv_lineitem",
        )

        time.sleep(3)

        index_size = runner.size_of_dataflow("%mv_lineitem")
        print(f"Dataflow size: {index_size}")

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_key_slow_path",
            size_of_index="mv_lineitem",
            query=[
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_key_fast_path",
            size_of_index="mv_lineitem",
            query=[
                "SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_non_key_slow_path",
            size_of_index="mv_lineitem",
            query=[
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_tax = 123) SELECT * FROM data, t;",
            ],
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_non_key_fast_path",
            size_of_index="mv_lineitem",
            query=[
                "SELECT * FROM mv_lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        runner.run_query("SELECT count(*) > 0 FROM mv_lineitem;")
        runner.run_query("ALTER CLUSTER c SET (REPLICATION FACTOR 0);")
        runner.measure(
            "materialized_view_formation",
            "materialized_view_restart",
            size_of_index="mv_lineitem",
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
        )


class AuctionScenario(Scenario):
    def name(self) -> str:
        return "auction"

    def materialize_views(self) -> list[str]:
        return ["auctions", "bids"]

    def drop(self) -> list[str]:
        return [
            "DROP MATERIALIZED VIEW IF EXISTS auctions CASCADE;",
            "DROP MATERIALIZED VIEW IF EXISTS bids CASCADE;",
            "DROP VIEW IF EXISTS auctions_core CASCADE;",
            "DROP VIEW IF EXISTS random CASCADE;",
            "DROP VIEW IF EXISTS moments CASCADE;",
            "DROP VIEW IF EXISTS seconds CASCADE;",
            "DROP VIEW IF EXISTS minutes CASCADE;",
            "DROP VIEW IF EXISTS hours CASCADE;",
            "DROP VIEW IF EXISTS days CASCADE;",
            "DROP VIEW IF EXISTS years CASCADE;",
            "DROP VIEW IF EXISTS items CASCADE;",
            "DROP TABLE IF EXISTS empty CASCADE;",
            "DROP CLUSTER IF EXISTS lg CASCADE;",
        ]

    def setup(self) -> list[str]:
        return [
            "DROP CLUSTER IF EXISTS lg CASCADE;",
            f"CREATE CLUSTER lg SIZE '{self.replica_size}';",
            "SET cluster = 'lg';",
            "CREATE TABLE empty (e TIMESTAMP);",
            """
            -- Supporting view to translate ids into text.
            CREATE VIEW items (id, item) AS VALUES
                (0, 'Signed Memorabilia'),
                (1, 'City Bar Crawl'),
                (2, 'Best Pizza in Town'),
                (3, 'Gift Basket'),
                (4, 'Custom Art');""",
            f"""
            -- Each year-long interval of interest
            CREATE VIEW years AS
            SELECT *
            FROM generate_series(
                '1970-01-01 00:00:00+00',
                '2099-01-01 00:00:00+00',
                '1 year') year
            WHERE mz_now() BETWEEN year AND year + '1 year' + '{self.scale} day';
            """,
            f"""
            -- Each day-long interval of interest
            CREATE VIEW days AS
            SELECT * FROM (
                SELECT generate_series(year, year + '1 year' - '1 day'::interval, '1 day') as day
                FROM years
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN day AND day + '1 day' + '{self.scale} day';
            """,
            f"""
            -- Each hour-long interval of interest
            CREATE VIEW hours AS
            SELECT * FROM (
                SELECT generate_series(day, day + '1 day' - '1 hour'::interval, '1 hour') as hour
                FROM days
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN hour AND hour + '1 hour' + '{self.scale} day';
            """,
            f"""
            -- Each minute-long interval of interest
            CREATE VIEW minutes AS
            SELECT * FROM (
                SELECT generate_series(hour, hour + '1 hour' - '1 minute'::interval, '1 minute') AS minute
                FROM hours
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN minute AND minute + '1 minute' + '{self.scale} day';
            """,
            f"""
            -- Any second-long interval of interest
            CREATE VIEW seconds AS
            SELECT * FROM (
                SELECT generate_series(minute, minute + '1 minute' - '1 second'::interval, '1 second') as second
                FROM minutes
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN second AND second + '1 second' + '{self.scale} day';
            """,
            # Indexes are important to ensure we expand intervals carefully.
            "CREATE DEFAULT INDEX ON years;",
            "CREATE DEFAULT INDEX ON days;",
            "CREATE DEFAULT INDEX ON hours;",
            "CREATE DEFAULT INDEX ON minutes;",
            "CREATE DEFAULT INDEX ON seconds;",
            f"""
            -- The final view we'll want to use.
            CREATE VIEW moments AS
            SELECT second AS moment FROM seconds
            WHERE mz_now() >= second
              AND mz_now() < second + '{self.scale} day';
            """,
            """
            -- Extract pseudorandom bytes from each moment.
            CREATE VIEW random AS
            SELECT moment, digest(moment::text, 'md5') as random
            FROM moments;
            """,
            """
            -- Present as auction
            CREATE VIEW auctions_core AS
            SELECT
                moment,
                random,
                get_byte(random, 0) +
                get_byte(random, 1) * 256 +
                get_byte(random, 2) * 65536 as id,
                get_byte(random, 3) +
                get_byte(random, 4) * 256 as seller,
                get_byte(random, 5) as item,
                -- Have each auction expire after up to 256 minutes.
                moment + get_byte(random, 6) * '1 minutes'::interval as end_time
            FROM random;
            """,
            """
            -- Refine and materialize auction data.
            CREATE MATERIALIZED VIEW auctions AS
            SELECT auctions_core.id, seller, items.item, end_time
            FROM auctions_core, items
            WHERE auctions_core.item % 5 = items.id;
            """,
            """
            -- Create and materialize bid data.
            CREATE MATERIALIZED VIEW bids AS
            -- Establish per-bid records and randomness.
            WITH prework AS (
                SELECT
                    id AS auction_id,
                    moment as auction_start,
                    end_time as auction_end,
                    digest(random::text || generate_series(1, get_byte(random, 5))::text, 'md5') as random
                FROM auctions_core
            )
            SELECT
                get_byte(random, 0) +
                get_byte(random, 1) * 256 +
                get_byte(random, 2) * 65536 as id,
                get_byte(random, 3) +
                get_byte(random, 4) * 256 AS buyer,
                auction_id,
                get_byte(random, 5)::numeric AS amount,
                auction_start + get_byte(random, 6) * '1 minutes'::interval as bid_time
            FROM prework;
            """,
        ]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.run_query("SELECT * FROM t;")
        runner.run_query("DROP INDEX IF EXISTS bids_id_idx CASCADE")
        runner.measure(
            "arrangement_formation",
            "create_index_primary_key",
            size_of_index="bids_id_idx",
            query=[
                "CREATE INDEX bids_id_idx ON bids(id);",
                "SELECT count(*) > 0 FROM bids WHERE id = 123123123;",
            ],
        )

        runner.run_query("DROP INDEX IF EXISTS bids_auction_id_idx CASCADE")

        runner.measure(
            "arrangement_formation",
            "create_index_foreign_key",
            size_of_index="bids_id_idx",
            query=[
                "CREATE INDEX bids_auction_id_idx ON bids(auction_id);",
                "SELECT count(*) > 0 FROM bids WHERE auction_id = 123123123;",
            ],
        )

        time.sleep(3)

        index_size = runner.size_of_dataflow("%bids_id_idx")
        print(f"Index size: {index_size}")

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_slow_path",
            size_of_index="bids_id_idx",
            query=[
                "WITH data AS (SELECT * FROM bids WHERE id = 123412341234) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_fast_path",
            size_of_index="bids_id_idx",
            query=[
                "SELECT * FROM bids WHERE id = 123412341234",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_non_key_slow_path",
            size_of_index="bids_id_idx",
            query=[
                "WITH data AS (SELECT * FROM bids WHERE amount = 123123123) SELECT * FROM data, t;",
            ],
            repetitions=3,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_non_key_fast_path",
            size_of_index="bids_id_idx",
            query=[
                "SELECT * FROM bids WHERE amount = 123123123;",
            ],
            repetitions=3,
        )

        # Primitive operators
        runner.measure(
            "primitive_operators",
            "bids_max",
            size_of_index="bids_id_idx",
            query=[
                "SELECT max(amount) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_max_subscribe",
            size_of_index="bids_id_idx",
            query=[
                "BEGIN",
                "DECLARE c CURSOR FOR SUBSCRIBE (SELECT max(amount) FROM bids);",
                "FETCH 1 c;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_sum",
            size_of_index="bids_id_idx",
            query=[
                "SELECT sum(amount) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_sum_subscribe",
            size_of_index="bids_id_idx",
            query=[
                "BEGIN",
                "DECLARE c CURSOR FOR SUBSCRIBE (SELECT sum(amount) FROM bids);",
                "FETCH 1 c;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_count",
            size_of_index="bids_id_idx",
            query=[
                "SELECT count(1) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_count_subscribe",
            size_of_index="bids_id_idx",
            query=[
                "BEGIN",
                "DECLARE c CURSOR FOR SUBSCRIBE (SELECT count(1) FROM bids);",
                "FETCH 1 c;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_basic_list_agg",
            size_of_index="bids_id_idx",
            query=[
                "SELECT auction_id, list_agg(amount) FROM bids GROUP BY auction_id LIMIT 1;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_basic_list_agg_subscribe",
            size_of_index="bids_id_idx",
            query=[
                "BEGIN",
                # We need to limit the amount of data we retrieve to avoid
                # stalling the gRPC connection.
                """
                DECLARE c CURSOR FOR SUBSCRIBE (
                    SELECT
                        auction_id, list_agg(amount)
                    FROM
                        bids
                    GROUP BY
                        auction_id
                    HAVING
                        list_length(list_agg(amount)) + auction_id < 10000
                );""",
                "FETCH 1 c;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "join",
            size_of_index="bids_id_idx",
            query=[
                "SELECT * FROM bids, auctions WHERE bids.auction_id = auctions.id LIMIT 0;",
            ],
            repetitions=3,
        )

        # Composite operators
        runner.measure(
            "composite_operators",
            "bids_count_max_sum_min",
            size_of_index="bids_id_idx",
            query=[
                """
                SELECT
                    id, count(amount), max(amount), sum(amount), min(amount)
                FROM bids
                GROUP BY id
                LIMIT 0;
                """,
            ],
            repetitions=3,
        )
        runner.measure(
            "composite_operators",
            "bids_count_max_sum_min_subscribe",
            size_of_index="bids_id_idx",
            query=[
                "BEGIN",
                """
                DECLARE c CURSOR FOR SUBSCRIBE (
                    SELECT
                        id, count(amount), max(amount), sum(amount), min(amount)
                    FROM bids
                    GROUP BY id
                    LIMIT 0
                )
                """,
                "FETCH 1 c;",
                "ROLLBACK;",
                """
                """,
            ],
            repetitions=3,
        )

        runner.measure(
            "composite_operators",
            "join_max",
            size_of_index="bids_id_idx",
            query=[
                "SELECT auction_id, item, MAX(amount) FROM bids, auctions WHERE bids.auction_id = auctions.id GROUP BY auction_id, item HAVING auction_id + MAX(amount)::int4 < 1000;",
            ],
            repetitions=3,
        )

        # Restart index
        runner.run_query("ALTER CLUSTER c SET (REPLICATION FACTOR 0);")
        time.sleep(2)
        runner.measure(
            "peek_serving",
            "index_restart",
            size_of_index="bids_id_idx",
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM bids WHERE id = 123412341234) SELECT * FROM data, t;",
            ],
        )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Deploy the current source to the cloud and run tests."""

    parser.add_argument(
        "--cleanup",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Destroy the region at the end of the workflow.",
    )
    parser.add_argument(
        "--version-check",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Perform a version check.",
    )

    parser.parse_args()

    assert APP_PASSWORD is not None
    connection = pg8000.native.Connection(
        user=USERNAME, password=APP_PASSWORD, host=c.cloud_hostname(), port=6875
    )

    with open(f"results_{int(time.time())}.csv", "w") as f:
        f.write(
            "scenario,scale,mode,category,test_name,cluster_size,repetition,size_bytes,time_ms\n"
        )
        run_scenario_strong(
            scenario=TpchScenario(1, replica_size_for_scale(1)),
            results_file=f,
            connection=connection,
        )
        run_scenario_strong(
            scenario=TpchScenarioMV(1, replica_size_for_scale(1)),
            results_file=f,
            connection=connection,
        )
        run_scenario_strong(
            scenario=AuctionScenario(4, replica_size_for_scale(1)),
            results_file=f,
            connection=connection,
        )
        run_scenario_weak(
            scenario=AuctionScenario(4, "none"),
            results_file=f,
            connection=connection,
        )


def run_scenario_strong(
    scenario: Scenario, results_file: Any, connection: pg8000.native.Connection
) -> None:

    runner = ScenarioRunner(
        scenario.name(),
        scenario.scale,
        "strong",
        connection,
        results_file,
        replica_size="None",
    )

    for query in scenario.drop():
        runner.run_query(query)

    runner.run_query("DROP TABLE IF EXISTS t CASCADE;")
    runner.run_query("CREATE TABLE t (a int);")
    runner.run_query("INSERT INTO t VALUES (1);")

    for query in scenario.setup():
        runner.run_query(query)

    for name in scenario.materialize_views():
        runner.run_query(f"SELECT COUNT(*) > 0 FROM {name};")

    for replica_size in [
        replica_size_for_scale(scale) for scale in [1, 2, 4, 8, 16, 32]
    ]:
        # Create a cluster with the specified size
        runner.run_query("DROP CLUSTER IF EXISTS c CASCADE")
        runner.run_query(f"CREATE CLUSTER c SIZE '{replica_size}'")
        runner.run_query("SET cluster = 'c';")
        runner.run_query("SELECT * FROM t;")

        runner.replica_size = replica_size

        scenario.run(runner)


def run_scenario_weak(
    scenario: Scenario, results_file: Any, connection: pg8000.native.Connection
) -> None:

    connection.run("DROP TABLE IF EXISTS t CASCADE;")
    connection.run("CREATE TABLE t (a int);")
    connection.run("INSERT INTO t VALUES (1);")

    initial_scale = scenario.scale

    for replica_scale in [1, 2, 4, 8, 16, 32]:
        replica_size = replica_size_for_scale(replica_scale)
        scenario.replica_size = replica_size
        scenario.scale = initial_scale * replica_scale
        runner = ScenarioRunner(
            scenario.name(),
            scenario.scale,
            "weak",
            connection,
            results_file,
            replica_size,
        )
        for query in scenario.drop():
            runner.run_query(query)

        for query in scenario.setup():
            runner.run_query(query)

        for name in scenario.materialize_views():
            runner.run_query(f"SELECT COUNT(*) > 0 FROM {name};")

        # Create a cluster with the specified size
        runner.run_query("DROP CLUSTER IF EXISTS c CASCADE")
        runner.run_query(f"CREATE CLUSTER c SIZE '{replica_size}'")
        runner.run_query("SET cluster = 'c';")
        runner.run_query("SELECT * FROM t;")

        scenario.run(runner)


def workflow_plot(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Analyze the results of the workflow."""

    parser.add_argument(
        "files",
        nargs="*",
        help="Destroy the region at the end of the workflow.",
    )

    args = parser.parse_args()

    for file in args.files:
        analyze_file(file)


def analyze_file(file: str):
    df = pd.read_csv(file)
    # Cluster replica size as credits/hour
    df["credits_per_h"] = df["cluster_size"].map(
        lambda s: float(re.search(r"\d+", s).group()) / 100.0
    )
    # Cluster replica size as centi-credits/s
    df["ccredit_per_s"] = df["credits_per_h"] / 3600 * 100
    # Number of timely workers
    df["workers"] = round(df["credits_per_h"] * 1.9375)
    # Throughput in MiB/s
    df["throughput_mb_per_s"] = df["size_bytes"] / df["time_ms"] * 1000 / 1024 / 1024
    # Throughput in MiB/s/worker
    df["throughput_mb_per_s_worker"] = (
        df["size_bytes"] / 1024 / 1024 / df["time_ms"] * 1000 / df["workers"]
    )
    # Throughput in MiB/credit
    df["throughput_mb_per_credit"] = (
        df["size_bytes"] / 1024 / 1024 / df["time_ms"] * 1000 / df["ccredit_per_s"]
    )
    # Cost in centi-credits: ccredit/s * s
    df["credit_time"] = df["ccredit_per_s"] * df["time_ms"] / 1000.0

    base_name = os.path.basename(file).split(".")[0]
    plot_dir = os.path.join(MZ_ROOT, "test", "cluster-spec-sheet", "plots", base_name)
    os.makedirs(plot_dir, exist_ok=True)

    # Plot the results
    df2 = plot_time_ms(
        df,
        'category != "peek_serving" and (scenario == "tpch" or scenario == "tpch_mv")',
        "TPCH create index/MV",
    )
    plt.savefig(os.path.join(plot_dir, "tpch_time_ms.png"))
    df2.to_html(os.path.join(plot_dir, "tpch_time_ms.html"))

    df2 = plot_credit_time(
        df,
        'category != "peek_serving" and (scenario == "tpch" or scenario == "tpch_mv")',
        "TPCH create index/MV",
    )
    plt.savefig(os.path.join(plot_dir, "tpch_credits.png"))
    df2.to_html(os.path.join(plot_dir, "tpch_credits.html"))

    df2 = plot_time_ms(
        df,
        'category == "arrangement_formation" and scenario == "auction"',
        "Auction arrangement formation",
    )
    plt.savefig(os.path.join(plot_dir, "auction_arrangement_formation_time_ms.png"))
    df2.to_html(os.path.join(plot_dir, "auction_arrangement_formation_time_ms.html"))

    df2 = plot_credit_time(
        df,
        'category == "arrangement_formation" and scenario == "auction"',
        "Auction arrangement formation",
    )
    plt.savefig(os.path.join(plot_dir, "auction_arrangement_formation_credits.png"))
    df2.to_html(os.path.join(plot_dir, "auction_arrangement_formation_credits.html"))

    df2 = plot_time_ms(
        df,
        'category == "primitive_operators" and scenario == "auction" and mode == "strong"',
        "Auction primitive operators",
    )
    plt.savefig(
        os.path.join(plot_dir, "auction_primitive_operators_strong_time_ms.png")
    )
    df2.to_html(
        os.path.join(plot_dir, "auction_primitive_operators_strong_time_ms.html")
    )

    df2 = plot_credit_time(
        df,
        'category == "primitive_operators" and scenario == "auction" and mode == "strong"',
        "Auction primitive operators",
    )
    plt.savefig(
        os.path.join(plot_dir, "auction_primitive_operators_strong_credits.png")
    )
    df2.to_html(
        os.path.join(plot_dir, "auction_primitive_operators_strong_credits.html")
    )

    df2 = plot_time_ms(
        df,
        'category == "primitive_operators" and scenario == "auction" and mode == "weak"',
        "Auction primitive operators",
    )
    plt.savefig(os.path.join(plot_dir, "auction_primitive_operators_weak_time_ms.png"))
    df2.to_html(os.path.join(plot_dir, "auction_primitive_operators_weak_time_ms.html"))

    df2 = plot_credit_time(
        df,
        'category == "primitive_operators" and scenario == "auction" and mode == "weak"',
        "Auction primitive operators",
    )
    plt.savefig(os.path.join(plot_dir, "auction_primitive_operators_weak_credits.png"))
    df2.to_html(os.path.join(plot_dir, "auction_primitive_operators_weak_credits.html"))


def plot_time_ms(data: pd.DataFrame, query: str, title: str) -> pd.DataFrame:
    df2 = (
        data.query(query)
        .pivot_table(
            index=["credits_per_h"],
            columns=["scenario", "category", "test_name", "mode"],
            values=["time_ms"],
            aggfunc="min",
        )
        .sort_index(axis=1)
    )
    (level, dropped) = labels_to_drop(df2)
    filtered = df2.droplevel(level, axis=1).dropna(axis=1, how="all")
    filtered.plot(
        kind="bar",
        figsize=(12, 6),
        ylabel="Time [ms]",
        logy=False,
        title=f"{title}\n{dropped}",
    )
    return filtered


def plot_credit_time(data: pd.DataFrame, query: str, title: str) -> pd.DataFrame:
    df2 = (
        data.query(query)
        .pivot_table(
            index=["credits_per_h"],
            columns=["scenario", "category", "test_name", "mode"],
            values=["credit_time"],
            aggfunc="min",
        )
        .sort_index(axis=1)
    )
    (level, dropped) = labels_to_drop(df2)
    filtered = df2.droplevel(level, axis=1).dropna(axis=1, how="all")
    filtered.plot(
        kind="bar",
        figsize=(12, 6),
        ylabel="Cost [centi-credits]",
        logy=False,
        title=f"{title}\n{dropped}",
    )
    return filtered


def labels_to_drop(data: pd.DataFrame) -> (list[str | None], dict[str | None, str]):
    unique = []
    dropped = {}
    for level in range(data.columns.nlevels):
        labels = data.columns.get_level_values(level)
        if len(set(labels)) == 1:
            unique.append(data.columns.names[level])
            dropped[data.columns.names[level]] = labels[0]
    return (unique, dropped)
