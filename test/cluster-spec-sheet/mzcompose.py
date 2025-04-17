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
import time
from abc import abstractmethod
from textwrap import dedent
from typing import Any

import pg8000
import pg8000.native

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


class ScenarioRunner:
    def __init__(
        self,
        scenario: str,
        connection: pg8000.native.Connection,
        results_file: Any,
        replica_size: int,
    ) -> None:
        self.scenario = scenario
        self.connection = connection
        self.results_file = results_file
        self.replica_size = replica_size

    def add_result(
        self, name: str, repetition: int, size_bytes: int, time: float
    ) -> None:
        self.results_file.write(
            f"{self.scenario},{name},{self.replica_size},{repetition},{size_bytes},{int(time * 1_000)}\n"
        )

    def run_query(self, query: str, **params) -> list | None:
        print(f"> {query} {params}")
        return self.connection.run(query, **params)

    def measure(
        self,
        name: str,
        query: list[str],
        before: list[str] = [],
        repetitions: int = 1,
        size_of_index: str | None = None,
    ) -> None:
        print(
            f"Running {name} for {self.replica_size} with {repetitions} repetitions..."
        )
        if before:
            for before_query in before:
                self.run_query(before_query)
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
            self.add_result(name, repetition, size_bytes, (end_time - start_time))

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


class TpccScenario(Scenario):

    def name(self) -> str:
        return f"tpcc_sf_{10*self.scale}"

    def materialize_views(self) -> list[str]:
        return ["lineitem"]

    def setup(self) -> str:
        return [
            "DROP SOURCE IF EXISTS lgtpch CASCADE;",
            "DROP CLUSTER IF EXISTS lg CASCADE;",
            f"CREATE CLUSTER lg SIZE '{self.replica_size}';",
            f"CREATE SOURCE lgtpch IN CLUSTER lg FROM LOAD GENERATOR TPCH (SCALE FACTOR {10*self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
        ]

    def drop(self) -> str:
        return ["DROP CLUSTER lg CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.measure(
            "create_index",
            before=[
                "DROP INDEX IF EXISTS lineitem_primary_idx CASCADE",
                "SELECT * FROM t;",
            ],
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
            "peek_index_key_slow_path",
            size_of_index="lineitem_primary_idx",
            query=[
                "WITH data AS (SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_index_key_fast_path",
            size_of_index="lineitem_primary_idx",
            query=[
                "SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_index_non_key_slow_path",
            size_of_index="lineitem_primary_idx",
            query=[
                "WITH data AS (SELECT * FROM lineitem WHERE l_tax = 123) SELECT * FROM data, t;",
            ],
        )

        # Peek against index
        runner.measure(
            "peek_index_non_key_fast_path",
            size_of_index="lineitem_primary_idx",
            query=[
                "SELECT * FROM lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        runner.measure(
            "index_restart",
            size_of_index="lineitem_primary_idx",
            before=[
                "SELECT count(*) > 0 FROM lineitem;",
                "ALTER CLUSTER c SET (REPLICATION FACTOR 0);",
            ],
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "SET cluster = 'c';",
                "WITH data AS (SELECT * FROM lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
        )


class AuctionScenario(Scenario):

    def materialize_views(self) -> list[str]:
        return ["auctions", "bids"]

    def drop(self) -> str:
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

    def setup(self) -> str:
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
            """
            -- Each year-long interval of interest
            CREATE VIEW years AS
            SELECT *
            FROM generate_series(
                '1970-01-01 00:00:00+00',
                '2099-01-01 00:00:00+00',
                '1 year') year
            WHERE mz_now() BETWEEN year AND year + '1 year' + '1 day';
            """,
            """
            -- Each day-long interval of interest
            CREATE VIEW days AS
            SELECT * FROM (
                SELECT generate_series(year, year + '1 year' - '1 day'::interval, '1 day') as day
                FROM years
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN day AND day + '1 day' + '1 day';
            """,
            """
            -- Each hour-long interval of interest
            CREATE VIEW hours AS
            SELECT * FROM (
                SELECT generate_series(day, day + '1 day' - '1 hour'::interval, '1 hour') as hour
                FROM days
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN hour AND hour + '1 hour' + '1 day';
            """,
            """
            -- Each minute-long interval of interest
            CREATE VIEW minutes AS
            SELECT * FROM (
                SELECT generate_series(hour, hour + '1 hour' - '1 minute'::interval, '1 minute') AS minute
                FROM hours
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN minute AND minute + '1 minute' + '1 day';
            """,
            """
            -- Any second-long interval of interest
            CREATE VIEW seconds AS
            SELECT * FROM (
                SELECT generate_series(minute, minute + '1 minute' - '1 second'::interval, '1 second') as second
                FROM minutes
                UNION ALL SELECT * FROM empty
            )
            WHERE mz_now() BETWEEN second AND second + '1 second' + '1 day';
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
                moment + (get_byte(random, 6)::text || ' minutes')::interval as end_time
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
                auction_start + (get_byte(random, 6)::text || ' minutes')::interval as bid_time
            FROM prework;
            """,
        ]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.measure(
            "create_index",
            size_of_index="bids_primary_idx",
            before=[
                "DROP INDEX IF EXISTS bids_primary_idx CASCADE",
                "SELECT * FROM t;",
            ],
            query=[
                "CREATE DEFAULT INDEX ON bids;",
                "SELECT count(*) > 0 FROM bids;",
            ],
        )

        time.sleep(3)

        index_size = runner.size_of_dataflow("%bids_primary_idx")
        print(f"Index size: {index_size}")

        # Peek against index
        runner.measure(
            "peek_index_key_slow_path",
            size_of_index="bids_primary_idx",
            query=[
                "WITH data AS (SELECT * FROM bids WHERE id = 123412341234) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_index_key_fast_path",
            size_of_index="bids_primary_idx",
            query=[
                "SELECT * FROM bids WHERE id = 123412341234",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_index_non_key_slow_path",
            size_of_index="bids_primary_idx",
            query=[
                "WITH data AS (SELECT * FROM bids WHERE amount = 123123123) SELECT * FROM data, t;",
            ],
        )

        # Peek against index
        runner.measure(
            "peek_index_non_key_fast_path",
            size_of_index="bids_primary_idx",
            query=[
                "SELECT * FROM bids WHERE amount = 123123123;",
            ],
        )

        # Restart index
        runner.measure(
            "index_restart",
            size_of_index="bids_primary_idx",
            before=[
                "SELECT count(*) > 0 FROM bids;",
            ],
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 0);",
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "SET cluster = 'c';",
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
        f.write("scenario,test_name,cluster_size,repetition,size_bytes,time_ms\n")
        run_scenario_weak(
            scenario=TpccScenario(1, "50cc"),
            results_file=f,
            connection=connection,
        )
        run_scenario_weak(
            scenario=AuctionScenario(1, "100cc"),
            results_file=f,
            connection=connection,
        )
        run_scenario_strong(
            scenario=AuctionScenario(1, None),
            results_file=f,
            connection=connection,
        )


def run_scenario_weak(
    scenario: Scenario, results_file: Any, connection: pg8000.native.Connection
) -> None:

    runner = ScenarioRunner(
        scenario.name(), connection, results_file, replica_size="None"
    )

    for query in scenario.drop():
        runner.run_query(dedent(query))

    runner.run_query("DROP TABLE IF EXISTS t CASCADE;")
    runner.run_query("CREATE TABLE t (a int);")
    runner.run_query("INSERT INTO t VALUES (1);")

    for query in scenario.setup():
        runner.run_query(dedent(query))

    for name in scenario.materialize_views():
        runner.run_query(f"SELECT COUNT(*) > 0 FROM {name};")

    for replica_size in [
        "100cc",
        "200cc",
        "400cc",
        "800cc",
        "1600cc",
        "3200cc",
    ]:  # , "6400cc", "128C"]:
        # Create a cluster with the specified size
        runner.run_query("DROP CLUSTER IF EXISTS c CASCADE")
        runner.run_query(f"CREATE CLUSTER c SIZE '{replica_size}'")
        runner.run_query("SET cluster = 'c';")
        runner.run_query("SELECT * FROM t;")

        runner.replica_size = replica_size

        scenario.run(runner)


def run_scenario_strong(
    scenario: Scenario, results_file: Any, connection: pg8000.native.Connection
) -> None:

    connection.run("DROP TABLE IF EXISTS t CASCADE;")
    connection.run("CREATE TABLE t (a int);")
    connection.run("INSERT INTO t VALUES (1);")

    for replica_size_scale in [
        ("100cc", 1),
        ("200cc", 2),
        ("400cc", 4),
        ("800cc", 8),
        ("1600cc", 16),
        ("3200cc", 32),
    ]:  # , "6400cc", "128C"]:
        replica_size = replica_size_scale[0]
        scenario.replica_size = replica_size
        scenario.scale = replica_size_scale[1]
        runner = ScenarioRunner(
            scenario.name(), connection, results_file, replica_size="None"
        )
        for query in scenario.drop():
            runner.run_query(dedent(query))

        for query in scenario.setup():
            runner.run_query(dedent(query))

        for name in scenario.materialize_views():
            runner.run_query(f"SELECT COUNT(*) > 0 FROM {name};")

        # Create a cluster with the specified size
        runner.run_query("DROP CLUSTER IF EXISTS c CASCADE")
        runner.run_query(f"CREATE CLUSTER c SIZE '{replica_size}'")
        runner.run_query("SET cluster = 'c';")
        runner.run_query("SELECT * FROM t;")

        scenario.run(runner)
