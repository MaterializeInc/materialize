# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Reproduces cluster spec sheet results on Materialize Cloud.
"""

import argparse
import csv
import glob
import itertools
import os
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable
from textwrap import dedent
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import psycopg
from psycopg import InterfaceError, OperationalError

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose import _wait_for_pg
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.cluster_spec_sheet import (
    cluster_spec_sheet_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.ui import UIError

CLUSTER_SPEC_SHEET_VERSION = "1.0.0"  # Used for uploading test analytics results

REGION = os.getenv("REGION", "aws/us-east-1")
ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
USERNAME = os.getenv("NIGHTLY_MZ_USERNAME", "infra+bot@materialize.com")
APP_PASSWORD = os.getenv("MZ_CLI_APP_PASSWORD")

SERVICES = [
    Mz(
        region=REGION,
        environment=ENVIRONMENT,
        app_password=APP_PASSWORD or "",
    ),
    Materialized(
        propagate_crashes=True,
        additional_system_parameter_defaults={
            "memory_limiter_interval": "0s",
            "max_credit_consumption_rate": "1024",
        },
    ),
]

SCENARIO_AUCTION_STRONG = "auction_strong"
SCENARIO_AUCTION_WEAK = "auction_weak"
SCENARIO_TPCH_MV_STRONG = "tpch_mv_strong"
SCENARIO_TPCH_QUERIES_STRONG = "tpch_queries_strong"
SCENARIO_TPCH_QUERIES_WEAK = "tpch_queries_weak"
SCENARIO_TPCH_STRONG = "tpch_strong"
ALL_SCENARIOS = [
    SCENARIO_AUCTION_STRONG,
    SCENARIO_AUCTION_WEAK,
    SCENARIO_TPCH_MV_STRONG,
    SCENARIO_TPCH_QUERIES_STRONG,
    SCENARIO_TPCH_QUERIES_WEAK,
    SCENARIO_TPCH_STRONG,
]


class ConnectionHandler:
    def __init__(self, new_connection: Callable[[], psycopg.Connection]) -> None:
        self.new_connection = new_connection
        self.connection = self.new_connection()
        self.cursor: psycopg.Cursor | None = None

    def __ensure_connection(self):
        if not self.connection or self.connection.closed:
            self.connection = self.new_connection()

    def __enter__(self) -> psycopg.Cursor:
        self.__ensure_connection()
        assert not self.cursor
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self.cursor:
            self.cursor.close()
            self.cursor = None

    def retryable(self, func: Callable[[], Any], retries: int = 5) -> Any:
        while True:
            try:
                self.__ensure_connection()
                return func()
            # TODO: Catch DatabaseError until https://github.com/MaterializeInc/database-issues/issues/9496 is fixed.
            except (InterfaceError, OperationalError, psycopg.DatabaseError) as e:
                if retries <= 0:
                    raise e
                print(f"Retryable error: {e}, reconnecting...")
                time.sleep(5)
                self.connection.close()
                retries -= 1


class ScenarioRunner:
    def __init__(
        self,
        scenario: str,
        scenario_version: str,
        scale: int | float,
        mode: str,
        connection: ConnectionHandler,
        results_writer: csv.DictWriter,
        replica_size: Any,
    ) -> None:
        self.scenario = scenario
        self.scenario_version = scenario_version
        self.scale = scale
        self.mode = mode
        self.connection = connection
        self.results_writer = results_writer
        self.replica_size = replica_size

    def add_result(
        self,
        category: str,
        name: str,
        repetition: int,
        size_bytes: int | None,
        time: float,
    ) -> None:
        self.results_writer.writerow(
            {
                "scenario": self.scenario,
                "scenario_version": self.scenario_version,
                "scale": self.scale,
                "mode": self.mode,
                "category": category,
                "test_name": name,
                "cluster_size": self.replica_size,
                "repetition": repetition,
                "size_bytes": size_bytes,
                "time_ms": int(time * 1000),
            }
        )

    def run_query(self, query: str, fetch: bool = False, **params) -> list | None:
        query = dedent(query).strip()
        print(f"> {query} {params or ''}")
        with self.connection as cur:
            cur.execute(query.encode(), params)
            if fetch:
                return cur.fetchall()
            else:
                return None

    def measure(
        self,
        category: str,
        name: str,
        setup: list[str],
        query: list[str],
        after: list[str] = [],
        repetitions: int = 1,
        size_of_index: str | None = None,
        setup_delay: float = 0.0,
    ) -> None:
        print(
            f"--- Running {name} for {self.replica_size} with {repetitions} repetitions..."
        )
        for repetition in range(repetitions):

            def inner() -> None:
                for setup_part in setup:
                    self.run_query(setup_part)
                time.sleep(setup_delay)
                start_time = time.time()
                for query_part in query:
                    self.run_query(query_part)
                end_time = time.time()
                if size_of_index:
                    # We need to wait for the introspection source to catch up.
                    time.sleep(2)
                    size_bytes = self.size_of_dataflow(f"%{size_of_index}")
                    print(f"Size of index {size_of_index}: {size_bytes}")
                else:
                    size_bytes = None
                self.add_result(
                    category, name, repetition, size_bytes, (end_time - start_time)
                )
                for after_part in after:
                    self.run_query(after_part)

            self.connection.retryable(inner)

    def size_of_dataflow(self, object: str) -> int | None:
        retries = 10
        while retries > 0:
            result = self.run_query(
                "SELECT size FROM mz_introspection.mz_dataflow_arrangement_sizes WHERE name LIKE %(name)s;",
                name=object,
                fetch=True,
            )
            if result and len(result) == 1 and result[0][0] is not None:
                return int(result[0][0])
            retries -= 1
            if retries > 0:
                time.sleep(1)
        return None


class Scenario(ABC):
    # Bump this version in the individual test if it changes in a way that
    # makes comparing results between versions useless.
    VERSION: str = "1.0.0"

    def __init__(self, scale: int | float, replica_size: str | None) -> None:
        self.scale = scale
        self.replica_size = replica_size

    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def setup(self) -> list[str]:
        """
        Prepare the scenario by setting up resources like clusters, sources, and
        load generators.
        """
        ...

    @abstractmethod
    def drop(self) -> list[str]: ...

    @abstractmethod
    def materialize_views(self) -> list[str]:
        """
        Returns names of the persist objects that setup initializes.
        """
        ...

    @abstractmethod
    def run(self, runner: ScenarioRunner) -> None: ...


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
            f"CREATE SOURCE lgtpch IN CLUSTER lg FROM LOAD GENERATOR TPCH (SCALE FACTOR {self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
        ]

    def drop(self) -> list[str]:
        return ["DROP CLUSTER IF EXISTS lg CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index

        runner.measure(
            "arrangement_formation",
            "create_index_primary_key",
            setup=[
                "DROP INDEX IF EXISTS lineitem_primary_idx CASCADE",
                "SELECT * FROM t;",
            ],
            query=[
                "CREATE DEFAULT INDEX ON lineitem;",
                "SELECT count(*) > 0 FROM lineitem;",
            ],
            size_of_index="lineitem_primary_idx",
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_fast_path",
            size_of_index="lineitem_primary_idx",
            setup=[],
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
            setup=[],
            query=[
                "SELECT * FROM lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        runner.measure(
            "arrangement_formation",
            "index_restart",
            size_of_index="lineitem_primary_idx",
            setup=[
                "SELECT count(*) > 0 FROM lineitem;",
                "ALTER CLUSTER c SET (REPLICATION FACTOR 0);",
            ],
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
            f"CREATE SOURCE lgtpch IN CLUSTER lg FROM LOAD GENERATOR TPCH (SCALE FACTOR {self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
        ]

    def drop(self) -> list[str]:
        return ["DROP CLUSTER IF EXISTS lg CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create index
        runner.measure(
            "materialized_view_formation",
            "create_materialize_view",
            setup=[
                "DROP MATERIALIZED VIEW IF EXISTS mv_lineitem CASCADE",
                "SELECT * FROM t;",
            ],
            query=[
                "CREATE MATERIALIZED VIEW mv_lineitem AS SELECT * FROM lineitem;",
                "SELECT count(*) > 0 FROM mv_lineitem;",
            ],
        )

        time.sleep(3)

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_key_slow_path",
            setup=[],
            query=[
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_key_fast_path",
            setup=[],
            query=[
                "SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123",
            ],
            repetitions=10,
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_non_key_slow_path",
            setup=[],
            query=[
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_tax = 123) SELECT * FROM data, t;",
            ],
        )

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_materialized_view_non_key_fast_path",
            setup=[],
            query=[
                "SELECT * FROM mv_lineitem WHERE l_tax = 123;",
            ],
        )

        # Restart index
        runner.measure(
            "materialized_view_formation",
            "materialized_view_restart",
            setup=[
                "SELECT count(*) > 0 FROM mv_lineitem;",
                "ALTER CLUSTER c SET (REPLICATION FACTOR 0);",
            ],
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM mv_lineitem WHERE l_orderkey = 123412341234 and l_linenumber = 123) SELECT * FROM data, t;",
            ],
        )


class TpchScenarioQueriesIndexedInputs(Scenario):

    def name(self) -> str:
        return "tpch_queries_indexed_inputs"

    def materialize_views(self) -> list[str]:
        return ["lineitem"]

    def setup(self) -> list[str]:
        return [
            "DROP SOURCE IF EXISTS lgtpch CASCADE;",
            "DROP CLUSTER IF EXISTS lg CASCADE;",
            f"CREATE CLUSTER lg SIZE '{self.replica_size}';",
            f"CREATE SOURCE lgtpch IN CLUSTER lg FROM LOAD GENERATOR TPCH (SCALE FACTOR {self.scale}, TICK INTERVAL 1) FOR ALL TABLES;",
            "SELECT COUNT(*) > 0 FROM region;",
            """
            CREATE VIEW revenue (supplier_no, total_revenue) AS
            SELECT
                l_suppkey,
                sum(l_extendedprice * (1 - l_discount))
            FROM
                lineitem
            WHERE
                l_shipdate >= DATE '1996-01-01'
              AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' month
            GROUP BY
                l_suppkey;
            """,
            """
            CREATE VIEW vq01 AS
            SELECT
                l_returnflag,
                l_linestatus,
                sum(l_quantity) AS sum_qty,
                sum(l_extendedprice) AS sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
                avg(l_quantity) AS avg_qty,
                avg(l_extendedprice) AS avg_price,
                avg(l_discount) AS avg_disc,
                count(*) AS count_order
            FROM
                lineitem
            WHERE
                l_shipdate <= DATE '1998-12-01' - INTERVAL '60' day
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus;
            """,
            """
            -- name: Q02
            CREATE VIEW vq02 AS
            SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            FROM
                part, supplier, partsupp, nation, region
            WHERE
                p_partkey = ps_partkey
              AND s_suppkey = ps_suppkey
              AND p_size = CAST (15 AS smallint)
              AND p_type LIKE '%%BRASS'
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'EUROPE'
              AND ps_supplycost
                = (
                      SELECT
                          min(ps_supplycost)
                      FROM
                          partsupp, supplier, nation, region
                      WHERE
                          p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE'
                  )
            ORDER BY
                s_acctbal DESC, n_name, s_name, p_partkey;
            """,
            """
            -- name: Q03
            CREATE VIEW vq03 AS
            SELECT
                l_orderkey,
                sum(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM
                customer,
                orders,
                lineitem
            WHERE
                c_mktsegment = 'BUILDING'
              AND c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate < DATE '1995-03-15'
              AND l_shipdate > DATE '1995-03-15'
            GROUP BY
                l_orderkey,
                o_orderdate,
                o_shippriority
            ORDER BY
                revenue DESC,
                o_orderdate;
            """,
            """
            -- name: Q04
            CREATE VIEW vq04 AS
            SELECT
                o_orderpriority,
                count(*) AS order_count
            FROM
                orders
            WHERE
                o_orderdate >= DATE '1993-07-01'
              AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' month
              AND EXISTS (
                SELECT
                    *
                FROM
                    lineitem
                WHERE
                    l_orderkey = o_orderkey
                  AND l_commitdate < l_receiptdate
            )
            GROUP BY
                o_orderpriority
            ORDER BY
                o_orderpriority;
            """,
            """
            -- name: Q05
            CREATE VIEW vq05 AS
            SELECT
                n_name,
                sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
                customer,
                orders,
                lineitem,
                supplier,
                nation,
                region
            WHERE
                c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND l_suppkey = s_suppkey
              AND c_nationkey = s_nationkey
              AND s_nationkey = n_nationkey
              AND n_regionkey = r_regionkey
              AND r_name = 'ASIA'
              AND o_orderdate >= DATE '1994-01-01'
              AND o_orderdate < DATE '1995-01-01'
            GROUP BY
                n_name
            ORDER BY
                revenue DESC;
            """,
            """
            -- name: Q06
            CREATE VIEW vq06 AS
            SELECT
                sum(l_extendedprice * l_discount) AS revenue
            FROM
                lineitem
            WHERE
                l_quantity < 24
              AND l_shipdate >= DATE '1994-01-01'
              AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' year
              AND l_discount BETWEEN 0.06 - 0.01 AND 0.07;
            """,
            """
            -- name: Q07
            CREATE VIEW vq07 AS
            SELECT
                supp_nation,
                cust_nation,
                l_year,
                sum(volume) AS revenue
            FROM
                (
                    SELECT
                        n1.n_name AS supp_nation,
                        n2.n_name AS cust_nation,
                        extract(year FROM l_shipdate) AS l_year,
                        l_extendedprice * (1 - l_discount) AS volume
                    FROM
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                    WHERE
                        s_suppkey = l_suppkey
                      AND o_orderkey = l_orderkey
                      AND c_custkey = o_custkey
                      AND s_nationkey = n1.n_nationkey
                      AND c_nationkey = n2.n_nationkey
                      AND (
                        (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                            or (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
                        )
                      AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                ) AS shipping
            GROUP BY
                supp_nation,
                cust_nation,
                l_year
            ORDER BY
                supp_nation,
                cust_nation,
                l_year;
            """,
            """
            -- name: Q08
            CREATE VIEW vq08 AS
            SELECT
                o_year,
                sum(case
                        when nation = 'BRAZIL' then volume
                        else 0
                    end) / sum(volume) AS mkt_share
            FROM
                (
                    SELECT
                        extract(year FROM o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) AS volume,
                        n2.n_name AS nation
                    FROM
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                    WHERE
                        p_partkey = l_partkey
                      AND s_suppkey = l_suppkey
                      AND l_orderkey = o_orderkey
                      AND o_custkey = c_custkey
                      AND c_nationkey = n1.n_nationkey
                      AND n1.n_regionkey = r_regionkey
                      AND r_name = 'AMERICA'
                      AND s_nationkey = n2.n_nationkey
                      AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                      AND p_type = 'ECONOMY ANODIZED STEEL'
                ) AS all_nations
            GROUP BY
                o_year
            ORDER BY
                o_year;
            """,
            """
            -- name: Q09
            CREATE VIEW vq09 AS
            SELECT
                nation,
                o_year,
                sum(amount) AS sum_profit
            FROM
                (
                    SELECT
                        n_name AS nation,
                        extract(year FROM o_orderdate) AS o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                    FROM
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                    WHERE
                        s_suppkey = l_suppkey
                      AND ps_suppkey = l_suppkey
                      AND ps_partkey = l_partkey
                      AND p_partkey = l_partkey
                      AND o_orderkey = l_orderkey
                      AND s_nationkey = n_nationkey
                      AND p_name like '%%green%%'
                ) AS profit
            GROUP BY
                nation,
                o_year
            ORDER BY
                nation,
                o_year DESC;
            """,
            """
            -- name: Q10
            CREATE VIEW vq10 AS
            SELECT
                c_custkey,
                c_name,
                sum(l_extendedprice * (1 - l_discount)) AS revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            FROM
                customer,
                orders,
                lineitem,
                nation
            WHERE
                c_custkey = o_custkey
              AND l_orderkey = o_orderkey
              AND o_orderdate >= DATE '1993-10-01'
              AND o_orderdate < DATE '1994-01-01'
              AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' month
              AND l_returnflag = 'R'
              AND c_nationkey = n_nationkey
            GROUP BY
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment
            ORDER BY
                revenue DESC;
            """,
            """
            -- name: Q11
            CREATE VIEW vq11 AS
            SELECT
                ps_partkey,
                sum(ps_supplycost * ps_availqty) AS value
            FROM
                partsupp,
                supplier,
                nation
            WHERE
                ps_suppkey = s_suppkey
              AND s_nationkey = n_nationkey
              AND n_name = 'GERMANY'
            GROUP BY
                ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                    SELECT
                        sum(ps_supplycost * ps_availqty) * 0.0001
                    FROM
                        partsupp,
                        supplier,
                        nation
                    WHERE
                        ps_suppkey = s_suppkey
                      AND s_nationkey = n_nationkey
                      AND n_name = 'GERMANY'
                )
            ORDER BY
                value DESC;
            """,
            """
            -- name: Q12
            CREATE VIEW vq12 AS
            SELECT
                l_shipmode,
                sum(case
                        when o_orderpriority = '1-URGENT'
                            or o_orderpriority = '2-HIGH'
                            then 1
                        else 0
                    end) AS high_line_count,
                sum(case
                        when o_orderpriority <> '1-URGENT'
                            AND o_orderpriority <> '2-HIGH'
                            then 1
                        else 0
                    end) AS low_line_count
            FROM
                orders,
                lineitem
            WHERE
                o_orderkey = l_orderkey
              AND l_shipmode IN ('MAIL', 'SHIP')
              AND l_commitdate < l_receiptdate
              AND l_shipdate < l_commitdate
              AND l_receiptdate >= DATE '1994-01-01'
              AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' year
            GROUP BY
                l_shipmode
            ORDER BY
                l_shipmode;
            """,
            """
            -- name: Q13
            CREATE VIEW vq13 AS
            SELECT
                c_count,
                count(*) AS custdist
            FROM
                (
                    SELECT
                        c_custkey,
                        count(o_orderkey) c_count -- workaround for no column aliases
                    FROM
                        customer LEFT OUTER JOIN orders ON
                            c_custkey = o_custkey
                                AND o_comment NOT LIKE '%%special%%requests%%'
                    GROUP BY
                        c_custkey
                ) AS c_orders -- (c_custkey, c_count) -- no column aliases yet
            GROUP BY
                c_count
            ORDER BY
                custdist DESC,
                c_count DESC;
            """,
            """
            -- name: Q14
            CREATE VIEW vq14 AS
            SELECT
                100.00 * sum(case
                                 when p_type like 'PROMO%%'
                                     then l_extendedprice * (1 - l_discount)
                                 else 0
                    end) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM
                lineitem,
                part
            WHERE
                l_partkey = p_partkey
              AND l_shipdate >= DATE '1995-09-01'
              AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' month;
            """,
            """
            -- name: Q15
            CREATE VIEW vq15 AS
            SELECT
                s_suppkey,
                s_name,
                s_address,
                s_phone,
                total_revenue
            FROM
                supplier,
                revenue
            WHERE
                s_suppkey = supplier_no
              AND total_revenue = (
                SELECT
                    max(total_revenue)
                FROM
                    revenue
            )
            ORDER BY
                s_suppkey;
            """,
            """
            -- name: Q16
            CREATE VIEW vq16 AS
            SELECT
                p_brand,
                p_type,
                p_size,
                count(DISTINCT ps_suppkey) AS supplier_cnt
            FROM
                partsupp,
                part
            WHERE
                p_partkey = ps_partkey
              AND p_brand <> 'Brand#45'
              AND p_type NOT LIKE 'MEDIUM POLISHED%%'
              AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
              AND ps_suppkey NOT IN (
                SELECT
                    s_suppkey
                FROM
                    supplier
                WHERE
                    s_comment like '%%Customer%%Complaints%%'
            )
            GROUP BY
                p_brand,
                p_type,
                p_size
            ORDER BY
                supplier_cnt DESC,
                p_brand,
                p_type,
                p_size;
            """,
            """
            -- name: Q17
            CREATE VIEW vq17 AS
            SELECT
                sum(l_extendedprice) / 7.0 AS avg_yearly
            FROM
                lineitem,
                part
            WHERE
                p_partkey = l_partkey
              AND p_brand = 'Brand#23'
              AND p_container = 'MED BOX'
              AND l_quantity < (
                SELECT
                    0.2 * avg(l_quantity)
                FROM
                    lineitem
                WHERE
                    l_partkey = p_partkey
            );
            """,
            """
            -- name: Q18
            CREATE VIEW vq18 AS
            SELECT
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice,
                sum(l_quantity)
            FROM
                customer,
                orders,
                lineitem
            WHERE
                o_orderkey IN (
                    SELECT
                        l_orderkey
                    FROM
                        lineitem
                    GROUP BY
                        l_orderkey having
                        sum(l_quantity) > 300
                )
              AND c_custkey = o_custkey
              AND o_orderkey = l_orderkey
            GROUP BY
                c_name,
                c_custkey,
                o_orderkey,
                o_orderdate,
                o_totalprice
            ORDER BY
                o_totalprice DESC,
                o_orderdate;
            """,
            """
            -- name: Q19
            CREATE VIEW vq19 AS
            SELECT
                sum(l_extendedprice* (1 - l_discount)) AS revenue
            FROM
                lineitem,
                part
            WHERE
                (
                    p_partkey = l_partkey
                        AND p_brand = 'Brand#12'
                        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                        AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
                        AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    )
               or
                (
                    p_partkey = l_partkey
                        AND p_brand = 'Brand#23'
                        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                        AND l_quantity >= CAST (10 AS smallint) AND l_quantity <= CAST (10 + 10 AS smallint)
                        AND p_size BETWEEN CAST (1 AS smallint) AND CAST (10 AS smallint)
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    )
               or
                (
                    p_partkey = l_partkey
                        AND p_brand = 'Brand#34'
                        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                        AND l_quantity >= CAST (20 AS smallint) AND l_quantity <= CAST (20 + 10 AS smallint)
                        AND p_size BETWEEN CAST (1 AS smallint) AND CAST (15 AS smallint)
                        AND l_shipmode IN ('AIR', 'AIR REG')
                        AND l_shipinstruct = 'DELIVER IN PERSON'
                    );
            """,
            """
            -- name: Q20
            CREATE VIEW vq20 AS
                --SELECT
            --    s_name,
            --    s_address
            --FROM
            --    supplier,
            --    nation
            --WHERE
            --    s_suppkey IN (
            --        SELECT
            --            ps_suppkey
            --        FROM
            --            partsupp
            --        WHERE
            --            ps_partkey IN (
            --                SELECT
            --                    p_partkey
            --                FROM
            --                    part
            --                WHERE
            --                    p_name like 'forest%%'
            --            )
            --            AND ps_availqty > (
            --                SELECT
            --                    0.5 * sum(l_quantity)
            --                FROM
            --                    lineitem
            --                WHERE
            --                    l_partkey = ps_partkey
            --                    AND l_suppkey = ps_suppkey
            --                    AND l_shipdate >= DATE '1995-01-01'
            --                    AND l_shipdate < DATE '1995-01-01' + INTERVAL '1' year
            --            )
            --    )
            --    AND s_nationkey = n_nationkey
            --    AND n_name = 'CANADA'
            --ORDER BY
            --    s_name;
            SELECT
                s_name,
                s_address
            FROM
                supplier,
                nation,
                (
                    SELECT DISTINCT t2.ps_suppkey
                    FROM
                        (
                            SELECT
                                ps_partkey, ps_suppkey, ps_availqty
                            FROM
                                part, partsupp
                            WHERE
                                p_name like 'forest%%'
                              AND ps_partkey = p_partkey
                        ) AS t2
                            ,
                        (
                            SELECT
                                ps_partkey, ps_suppkey, 0.5 * sum(l_quantity) as qty
                            FROM
                                part, partsupp, lineitem
                            WHERE
                                p_name LIKE 'forest%%'
                              AND ps_partkey = p_partkey
                              AND l_partkey = ps_partkey
                              AND l_suppkey = ps_suppkey
                              AND l_shipdate >= DATE '1995-01-01'
                              AND l_shipdate < DATE '1995-01-01' + INTERVAL '1' year
                            GROUP BY ps_partkey, ps_suppkey
                        ) AS t3
                    WHERE t2.ps_partkey = t3.ps_partkey
                      AND t2.ps_suppkey = t3.ps_suppkey
                      AND t2.ps_availqty > t3.qty
                ) as sj
            WHERE
                s_nationkey = n_nationkey
              AND n_name = 'CANADA'
              AND sj.ps_suppkey = s_suppkey
            ORDER BY
                s_name;
            """,
            """
            -- name: Q21
            CREATE VIEW vq21 AS
            SELECT
                s_name,
                count(*) AS numwait
            FROM
                supplier,
                lineitem l1,
                orders,
                nation
            WHERE
                s_suppkey = l1.l_suppkey
              AND o_orderkey = l1.l_orderkey
              AND o_orderstatus = 'F'
              AND l1.l_receiptdate > l1.l_commitdate
              AND EXISTS (
                SELECT
                    *
                FROM
                    lineitem l2
                WHERE
                    l2.l_orderkey = l1.l_orderkey
                  AND l2.l_suppkey <> l1.l_suppkey
            )
              AND not EXISTS (
                SELECT
                    *
                FROM
                    lineitem l3
                WHERE
                    l3.l_orderkey = l1.l_orderkey
                  AND l3.l_suppkey <> l1.l_suppkey
                  AND l3.l_receiptdate > l3.l_commitdate
            )
              AND s_nationkey = n_nationkey
              AND n_name = 'SAUDI ARABIA'
            GROUP BY
                s_name
            ORDER BY
                numwait DESC,
                s_name;
            """,
            """
            -- name: Q22
            CREATE VIEW vq22 AS
            SELECT
                cntrycode,
                count(*) AS numcust,
                sum(c_acctbal) AS totacctbal
            FROM
                (
                    SELECT
                        substring(c_phone, 1, 2) AS cntrycode, c_acctbal
                    FROM
                        customer
                    WHERE
                        substring(c_phone, 1, 2)
                            IN ('13', '31', '23', '29', '30', '18', '17')
                      AND c_acctbal
                        > (
                              SELECT
                                  avg(c_acctbal)
                              FROM
                                  customer
                              WHERE
                                  c_acctbal > 0.00
                                AND substring(c_phone, 1, 2)
                                  IN (
                                      '13',
                                      '31',
                                      '23',
                                      '29',
                                      '30',
                                      '18',
                                      '17'
                                        )
                          )
                      AND NOT
                        EXISTS(
                        SELECT
                            *
                        FROM
                            orders
                        WHERE
                            o_custkey = c_custkey
                    )
                )
                    AS custsale
            GROUP BY
                cntrycode
            ORDER BY
                cntrycode;
            """,
        ]

    def drop(self) -> list[str]:
        return ["DROP CLUSTER IF EXISTS lg CASCADE;"]

    def run(self, runner: ScenarioRunner) -> None:
        # Create indexes on inputs
        runner.measure(
            "arrangement_formation",
            "create_index_inputs",
            setup=["SELECT * FROM t;"],
            query=[
                "CREATE INDEX pk_nation_nationkey ON nation (n_nationkey ASC);",
                "CREATE INDEX fk_nation_regionkey ON nation (n_regionkey ASC);",
                "CREATE INDEX pk_region_regionkey ON region (r_regionkey ASC);",
                "CREATE INDEX pk_part_partkey ON part (p_partkey ASC);",
                "CREATE INDEX pk_supplier_suppkey ON supplier (s_suppkey ASC);",
                "CREATE INDEX fk_supplier_nationkey ON supplier (s_nationkey ASC);",
                "CREATE INDEX pk_partsupp_partkey_suppkey ON partsupp (ps_partkey ASC, ps_suppkey ASC);",
                "CREATE INDEX fk_partsupp_partkey ON partsupp (ps_partkey ASC);",
                "CREATE INDEX fk_partsupp_suppkey ON partsupp (ps_suppkey ASC);",
                "CREATE INDEX pk_customer_custkey ON customer (c_custkey ASC);",
                "CREATE INDEX fk_customer_nationkey ON customer (c_nationkey ASC);",
                "CREATE INDEX pk_orders_orderkey ON orders (o_orderkey ASC);",
                "CREATE INDEX fk_orders_custkey ON orders (o_custkey ASC);",
                "CREATE INDEX pk_lineitem_orderkey_linenumber ON lineitem (l_orderkey ASC, l_linenumber ASC);",
                "CREATE INDEX fk_lineitem_orderkey ON lineitem (l_orderkey ASC);",
                "CREATE INDEX fk_lineitem_partkey ON lineitem (l_partkey ASC);",
                "CREATE INDEX fk_lineitem_suppkey ON lineitem (l_suppkey ASC);",
                "CREATE INDEX fk_lineitem_partsuppkey ON lineitem (l_partkey ASC, l_suppkey ASC);",
                "SELECT count(*) > 0 FROM lineitem;",
            ],
        )

        # Create index for each query
        for i in range(1, 23):
            runner.measure(
                "arrangement_formation",
                f"create_index_vq{i:02}",
                setup=["SELECT * FROM t;"],
                query=[
                    f"DROP INDEX IF EXISTS vq{i:02}_primary_idx CASCADE;",
                    f"CREATE DEFAULT INDEX ON vq{i:02};",
                    f"SELECT count(*) > 0 FROM vq{i:02};",
                ],
                after=[f"DROP INDEX IF EXISTS vq{i:02}_primary_idx CASCADE;"],
                size_of_index=f"vq{i:02}_primary_idx",
                repetitions=3,
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
            WHERE auctions_core.item %% 5 = items.id;
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
        runner.measure(
            "arrangement_formation",
            "create_index_primary_key",
            size_of_index="bids_id_idx",
            setup=[
                "SELECT * FROM t;",
                "DROP INDEX IF EXISTS bids_id_idx CASCADE",
            ],
            query=[
                "CREATE INDEX bids_id_idx ON bids(id);",
                "SELECT count(*) > 0 FROM bids WHERE id = 123123123;",
            ],
        )

        runner.measure(
            "arrangement_formation",
            "create_index_foreign_key",
            size_of_index="bids_id_idx",
            setup=["DROP INDEX IF EXISTS bids_auction_id_idx CASCADE"],
            query=[
                "CREATE INDEX bids_auction_id_idx ON bids(auction_id);",
                "SELECT count(*) > 0 FROM bids WHERE auction_id = 123123123;",
            ],
        )

        time.sleep(3)

        # Peek against index
        runner.measure(
            "peek_serving",
            "peek_index_key_slow_path",
            size_of_index="bids_id_idx",
            setup=[],
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
            setup=[],
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
            setup=[],
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
            setup=[],
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
            setup=[],
            query=[
                "SELECT max(amount) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_max_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                "DECLARE a CURSOR FOR SUBSCRIBE (SELECT max(amount) FROM bids);",
                "FETCH 1 a;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_sum",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT sum(amount) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_sum_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                "DECLARE b CURSOR FOR SUBSCRIBE (SELECT sum(amount) FROM bids);",
                "FETCH 1 b;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "bids_count",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT count(1) FROM bids;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_count_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
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
            setup=[],
            query=[
                "SELECT auction_id, list_agg(amount) FROM bids GROUP BY auction_id LIMIT 1;",
            ],
            repetitions=3,
        )
        runner.measure(
            "primitive_operators",
            "bids_basic_list_agg_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                # We need to limit the amount of data we retrieve to avoid
                # stalling the gRPC connection.
                """
                DECLARE d CURSOR FOR SUBSCRIBE (
                    SELECT
                        auction_id, list_agg(amount)
                    FROM
                        bids
                    GROUP BY
                        auction_id
                    HAVING
                        list_length(list_agg(amount)) + auction_id < 10000
                );""",
                "FETCH 1 d;",
                "ROLLBACK;",
            ],
            repetitions=3,
        )

        runner.measure(
            "primitive_operators",
            "join",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "SELECT * FROM bids, auctions WHERE bids.auction_id = auctions.id LIMIT 1;",
            ],
            repetitions=3,
        )

        # Composite operators
        runner.measure(
            "composite_operators",
            "bids_count_max_sum_min",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                """
                SELECT
                    id, count(amount), max(amount), sum(amount), min(amount)
                FROM bids
                GROUP BY id
                LIMIT 1;
                """,
            ],
            repetitions=3,
        )
        runner.measure(
            "composite_operators",
            "bids_count_max_sum_min_subscribe",
            size_of_index="bids_id_idx",
            setup=[],
            query=[
                "BEGIN",
                """
                DECLARE e CURSOR FOR SUBSCRIBE (
                    SELECT
                        id, count(amount), max(amount), sum(amount), min(amount)
                    FROM bids
                    GROUP BY id
                    LIMIT 1
                )
                """,
                "FETCH 1 e;",
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
            setup=[],
            query=[
                "SELECT auction_id, item, MAX(amount) FROM bids, auctions WHERE bids.auction_id = auctions.id GROUP BY auction_id, item HAVING auction_id + MAX(amount)::int4 < 1000;",
            ],
            repetitions=3,
        )

        # Restart index
        runner.measure(
            "peek_serving",
            "index_restart",
            size_of_index="bids_id_idx",
            setup=["ALTER CLUSTER c SET (REPLICATION FACTOR 0);"],
            query=[
                "ALTER CLUSTER c SET (REPLICATION FACTOR 1);",
                "WITH data AS (SELECT * FROM bids WHERE id = 123412341234) SELECT * FROM data, t;",
            ],
            setup_delay=2,
        )


def disable_region(c: Composition) -> None:
    print(f"Shutting down region {REGION} ...")

    try:
        c.run("mz", "region", "disable", "--hard", rm=True)
    except UIError:
        # Can return: status 404 Not Found
        pass


def wait_for_cloud(c: Composition) -> None:
    print(f"Waiting for cloud cluster to come up with username {USERNAME} ...")
    _wait_for_pg(
        host=c.cloud_hostname(),
        user=USERNAME,
        password=APP_PASSWORD,
        port=6875,
        query="SELECT 1",
        expected=[(1,)],
        timeout_secs=900,
        dbname="materialize",
        sslmode="require",
    )


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Run the bench workflow by default
    """
    parser.add_argument(
        "--cleanup",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Destroy the region at the end of the workflow.",
    )
    parser.add_argument(
        "--record",
        default=f"results_{int(time.time())}.csv",
        help="CSV file to store results.",
    )
    parser.add_argument(
        "--analyze",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Analyze results after completing test.",
    )
    parser.add_argument(
        "--target",
        default="cloud",
        choices=["cloud", "docker"],
        help="Target to deploy to (default: cloud).",
    )
    parser.add_argument(
        "--max-scale", type=int, default=32, help="Maximum scale to test."
    )
    parser.add_argument(
        "--scale-tpch", type=float, default=8, help="TPCH scale factor."
    )
    parser.add_argument(
        "--scale-tpch-queries", type=float, default=4, help="TPCH queries scale factor."
    )
    parser.add_argument(
        "--scale-auction", type=int, default=3, help="TPCH scale factor."
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        default=ALL_SCENARIOS,
        help="Scenarios to run.",
    )

    args = parser.parse_args()
    scenarios = set(args.scenarios)
    unknown = scenarios - set(ALL_SCENARIOS)
    if unknown:
        raise ValueError(f"Unknown scenarios: {unknown}")
    print(f"--- Running scenarios: {', '.join(scenarios)}")

    if args.target == "cloud":
        target: BenchTarget = CloudTarget(c)
    elif args.target == "docker":
        target = DockerTarget(c)
    else:
        raise ValueError(f"Unknown target: {args.target}")

    max_scale = args.max_scale
    if target.max_scale() is not None:
        max_scale = min(max_scale, target.max_scale())

    if args.cleanup:
        target.cleanup()

    target.initialize()
    results_file = open(args.record, "w", newline="")
    results_writer = csv.DictWriter(
        results_file,
        fieldnames=[
            "scenario",
            "scenario_version",
            "scale",
            "mode",
            "category",
            "test_name",
            "cluster_size",
            "repetition",
            "size_bytes",
            "time_ms",
        ],
    )
    results_writer.writeheader()

    def process(scenario: str) -> None:
        with c.test_case(scenario):
            conn = ConnectionHandler(target.new_connection)

            if scenario == SCENARIO_TPCH_STRONG:
                print("--- SCENARIO: Running TPC-H Index strong scaling")
                run_scenario_strong(
                    scenario=TpchScenario(
                        args.scale_tpch, target.replica_size_for_scale(1)
                    ),
                    results_writer=results_writer,
                    connection=conn,
                    target=target,
                    max_scale=max_scale,
                )
            if scenario == SCENARIO_TPCH_MV_STRONG:
                print("--- SCENARIO: Running TPC-H Materialized view strong scaling")
                run_scenario_strong(
                    scenario=TpchScenarioMV(
                        args.scale_tpch, target.replica_size_for_scale(1)
                    ),
                    results_writer=results_writer,
                    connection=conn,
                    target=target,
                    max_scale=max_scale,
                )
            if scenario == SCENARIO_TPCH_QUERIES_STRONG:
                print("--- SCENARIO: Running TPC-H Queries strong scaling")
                run_scenario_strong(
                    scenario=TpchScenarioQueriesIndexedInputs(
                        args.scale_tpch_queries, target.replica_size_for_scale(1)
                    ),
                    results_writer=results_writer,
                    connection=conn,
                    target=target,
                    max_scale=max_scale,
                )
            if scenario == SCENARIO_TPCH_QUERIES_WEAK:
                print("--- SCENARIO: Running TPC-H Queries weak scaling")
                run_scenario_weak(
                    scenario=TpchScenarioQueriesIndexedInputs(
                        args.scale_tpch_queries, None
                    ),
                    results_writer=results_writer,
                    connection=conn,
                    target=target,
                    max_scale=max_scale,
                )
            if scenario == SCENARIO_AUCTION_STRONG:
                print("--- SCENARIO: Running Auction strong scaling")
                run_scenario_strong(
                    scenario=AuctionScenario(
                        args.scale_auction, target.replica_size_for_scale(1)
                    ),
                    results_writer=results_writer,
                    connection=conn,
                    target=target,
                    max_scale=max_scale,
                )
            if scenario == SCENARIO_AUCTION_WEAK:
                print("--- SCENARIO: Running Auction weak scaling")
                run_scenario_weak(
                    scenario=AuctionScenario(args.scale_auction, None),
                    results_writer=results_writer,
                    connection=conn,
                    target=target,
                    max_scale=max_scale,
                )

    test_failed = True
    try:
        scenarios = buildkite.shard_list(sorted(args.scenarios), lambda s: s)
        c.test_parts(scenarios, process)
        test_failed = False
    finally:
        results_file.close()
        # Clean up
        if args.cleanup:
            target.cleanup()

    upload_results_to_test_analytics(c, args.record, not test_failed)

    assert not test_failed

    if buildkite.is_in_buildkite():
        buildkite.upload_artifact(args.record, cwd=MZ_ROOT, quiet=True)

    if args.analyze:
        analyze_file(args.record)


class BenchTarget:
    @abstractmethod
    def initialize(self) -> None: ...
    @abstractmethod
    def new_connection(self) -> psycopg.Connection: ...
    @abstractmethod
    def cleanup(self) -> None: ...
    @abstractmethod
    def replica_size_for_scale(self, scale: int) -> str:
        """
        Returns the replica size for a given scale.
        """
        ...

    def max_scale(self) -> int | None:
        """
        Returns the maximum scale for the target, or None if there is no limit.
        """
        return None


class CloudTarget(BenchTarget):
    def __init__(self, c: Composition) -> None:
        self.c = c
        self.new_app_password: str | None = None

    def initialize(self) -> None:
        print("Enabling region using Mz ...")
        self.c.run("mz", "region", "enable", rm=True)

        time.sleep(10)

        assert "materialize.cloud" in self.c.cloud_hostname()
        wait_for_cloud(self.c)

        # Create new app password.
        new_app_password_name = "Materialize CLI (mz) - Cluster Spec Sheet"
        output = self.c.run(
            "mz",
            "app-password",
            "create",
            new_app_password_name,
            capture=True,
            rm=True,
        )
        self.new_app_password = output.stdout.strip()
        assert "mzp_" in self.new_app_password

    def new_connection(self) -> psycopg.Connection:
        assert self.new_app_password is not None
        conn = psycopg.connect(
            host=self.c.cloud_hostname(),
            port=6875,
            user=USERNAME,
            password=self.new_app_password,
            dbname="materialize",
            sslmode="require",
        )
        conn.autocommit = True
        return conn

    def cleanup(self) -> None:
        disable_region(self.c)

    def replica_size_for_scale(self, scale: int) -> str:
        """
        Returns the replica size for a given scale.
        """
        return f"{scale}00cc"


class DockerTarget(BenchTarget):
    def __init__(self, c: Composition) -> None:
        self.c = c

    def initialize(self) -> None:
        print("Starting local Materialize instance ...")
        self.c.up("materialized")

    def new_connection(self) -> psycopg.Connection:
        return self.c.sql_connection()

    def cleanup(self) -> None:
        print("Stopping local Materialize instance ...")
        self.c.stop("materialized")

    def replica_size_for_scale(self, scale: int) -> str:
        # 100cc == 2 workers
        return f"scale=1,workers={2*scale}"

    def max_scale(self) -> int | None:
        return 16


def run_scenario_strong(
    scenario: Scenario,
    results_writer: csv.DictWriter,
    connection: ConnectionHandler,
    target: BenchTarget,
    max_scale: int,
) -> None:
    """
    Run a strong scaling scenario, where we increase the cluster size
    and keep the data size constant.
    """

    runner = ScenarioRunner(
        scenario.name(),
        scenario.VERSION,
        scenario.scale,
        "strong",
        connection,
        results_writer,
        replica_size=None,
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

    for replica_scale in [1, 2, 4, 8, 16, 32]:
        if replica_scale > max_scale:
            break
        replica_size = target.replica_size_for_scale(replica_scale)
        print(
            f"--- Running strong scenario {scenario.name()} with replica size {replica_size}"
        )
        # Create a cluster with the specified size
        runner.run_query("DROP CLUSTER IF EXISTS c CASCADE")
        runner.run_query(f"CREATE CLUSTER c SIZE '{replica_size}'")
        runner.run_query("SET cluster = 'c';")
        runner.run_query("SET transaction_isolation = 'serializable';")
        runner.run_query("SELECT * FROM t;")

        runner.replica_size = replica_size

        scenario.run(runner)


def run_scenario_weak(
    scenario: Scenario,
    results_writer: csv.DictWriter,
    connection: ConnectionHandler,
    target: BenchTarget,
    max_scale: int,
) -> None:
    """
    Run a weak scaling scenario, where we increase both the cluster size
    and the data size.
    """

    with connection as cur:
        cur.execute("DROP TABLE IF EXISTS t CASCADE;")
        cur.execute("CREATE TABLE t (a int);")
        cur.execute("INSERT INTO t VALUES (1);")

    initial_scale = scenario.scale

    for replica_scale in [1, 2, 4, 8, 16, 32]:
        if replica_scale > max_scale:
            break
        replica_size = target.replica_size_for_scale(replica_scale)
        print(
            f"--- Running weak scenario {scenario.name()} with replica size {replica_size}"
        )
        scenario.replica_size = replica_size
        scenario.scale = initial_scale * replica_scale
        runner = ScenarioRunner(
            scenario.name(),
            scenario.VERSION,
            scenario.scale,
            "weak",
            connection,
            results_writer,
            replica_size,
        )
        for query in scenario.drop():
            runner.run_query(query)

        for query in scenario.setup():
            runner.run_query(query)

        for name in scenario.materialize_views():
            runner.run_query(f"SELECT COUNT(*) > 0 FROM {name};")

        # Create a cluster with the specified size
        print(f"--- Loading complete; creating cluster with size {replica_size}")
        runner.run_query("DROP CLUSTER IF EXISTS c CASCADE")
        runner.run_query(f"CREATE CLUSTER c SIZE '{replica_size}'")
        runner.run_query("SET cluster = 'c';")
        runner.run_query("SET transaction_isolation = 'serializable';")
        runner.run_query("SELECT * FROM t;")

        scenario.run(runner)


def workflow_plot(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Analyze the results of the workflow."""

    parser.add_argument(
        "files",
        nargs="*",
        default="results_*.csv",
        type=str,
        help="Glob pattern of result files to plot.",
    )

    args = parser.parse_args()

    for file in itertools.chain(*map(glob.iglob, args.files)):
        analyze_file(str(file))


def analyze_file(file: str):
    print(f"--- Analyzing file {file} ...")

    def extract_cluster_size(s: str) -> float:
        match = re.search(r"(\d+)(?:(cc)|(C))", s)
        if match:
            if match.group(2):  # 'cc' match
                return float(match.group(1)) / 100.0
            elif match.group(3):  # 'C' matches
                return float(match.group(1))
        match = re.search(r"(?:scale=)(\d+)(?:,workers=)(\d+)", s)
        if match:
            # We don't have credits in docker, so approximate it
            # 100cc == 2 workers
            if match.group(1) and match.group(2):
                return float(match.group(1)) * float(match.group(2)) / 2
        raise ValueError(f"Invalid cluster size format: {s}")

    df = pd.read_csv(file)
    if df.empty:
        print(f"^^^ +++ File {file} is empty, skipping")
        return

    # Cluster replica size as credits/hour
    df["credits_per_h"] = df["cluster_size"].map(extract_cluster_size)
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
    plot_dir = os.path.join("test", "cluster-spec-sheet", "plots", base_name)
    os.makedirs(plot_dir, exist_ok=True)

    df_all = df
    # Plot the results
    for index in (
        df_all[["scenario", "category", "mode"]]
        .drop_duplicates()
        .itertuples(index=False)
    ):
        (benchmark, category, mode) = (index.scenario, index.category, index.mode)
        indexes = (
            (df_all["scenario"] == benchmark)
            & (df_all["category"] == category)
            & (df_all["mode"] == mode)
        )
        df = df_all[indexes]
        title = f"{str(benchmark).replace("_", " ")} - {str(category).replace('_', ' ')} ({mode})"
        slug = f"{benchmark}_{category}_{mode}".replace(" ", "_")

        plot(
            plot_dir,
            df,
            "time_ms",
            f"{title} (time)",
            f"{slug}_time_ms",
            "Time [ms]",
            "Normalized time",
        )
        plot(
            plot_dir,
            df,
            "credit_time",
            f"{title} (credits)",
            f"{slug}_credits",
            "Cost [centi-credits]",
            "Normalized cost",
        )


def save_plot(plot_dir: str, data_frame: pd.DataFrame, title: str, slug: str):
    all_files = []

    base_file_name = os.path.join(plot_dir, slug)
    file_path = f"{base_file_name}.svg"
    plt.savefig(MZ_ROOT / file_path, bbox_inches="tight")
    all_files.append(file_path)
    file_path = f"{base_file_name}.html"
    data_frame.to_html(MZ_ROOT / file_path)
    all_files.append(file_path)
    print(f"+++ Plot for {title}")
    print(data_frame.to_string())

    upload_file(all_files, title)


def upload_file(
    file_paths: list[str],
    title: str,
):
    if buildkite.is_in_buildkite():
        for file_path in file_paths:
            buildkite.upload_artifact(file_path, cwd=MZ_ROOT, quiet=True)
        print(f"+++ Plot for {title}")
        for file_path in file_paths:
            print(
                buildkite.inline_image(
                    f"artifact://{file_path}",
                    f"Plot for {title}",
                )
            )
    else:
        print(f"Saving plots to {file_paths}")


def plot(
    plot_dir: str,
    data: pd.DataFrame,
    value: str,
    title: str,
    slug: str,
    data_label: str,
    normalized_label: str,
):
    df2 = data.pivot_table(
        index=["credits_per_h"],
        columns=["test_name"],
        values=[value],
        aggfunc="min",
    ).sort_index(axis=1)
    (level, dropped) = labels_to_drop(df2)
    filtered = df2.droplevel(level, axis=1).dropna(axis=1, how="all")
    if filtered.empty:
        print(f"Warning: No data to plot for {title}")
        return
    plot = filtered.plot(
        kind="bar",
        figsize=(12, 6),
        ylabel=data_label,
        logy=False,
        title=f"{title}",
        grid=True,
    )
    plot.legend(
        loc="upper left",
        bbox_to_anchor=(1.0, 1.0),
    )
    save_plot(plot_dir, filtered, title, slug)

    filtered = filtered.div(filtered.iloc[0])
    plot = filtered.plot(
        kind="bar",
        figsize=(12, 6),
        ylabel=normalized_label,
        logy=False,
        title=f"{title}",
        grid=True,
    )
    plot.legend(
        loc="upper left",
        bbox_to_anchor=(1.0, 1.0),
    )
    save_plot(plot_dir, filtered, f"{title} (Normalized)", f"{slug}_normalized")


def labels_to_drop(
    data: pd.DataFrame,
) -> tuple[list[Hashable], dict[str, str]]:
    unique = []
    dropped = {}
    for level in range(data.columns.nlevels):
        labels = data.columns.get_level_values(level)
        if len(set(labels)) == 1:
            unique.append(data.columns.names[level])
            dropped[data.columns.names[level]] = labels[0]
    return unique, dropped


def upload_results_to_test_analytics(
    c: Composition,
    file: str,
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(c))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []

    with open(file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            result_entries.append(
                cluster_spec_sheet_result_storage.ClusterSpecSheetResultEntry(
                    scenario=row["scenario"],
                    scenario_version=row["scenario_version"],
                    scale=int(row["scale"]),
                    mode=row["mode"],
                    category=row["category"],
                    test_name=row["test_name"],
                    cluster_size=row["cluster_size"],
                    repetition=int(row["repetition"]),
                    size_bytes=int(row["size_bytes"]) if row["size_bytes"] else None,
                    time_ms=int(row["time_ms"]) if row["time_ms"] else None,
                )
            )

    test_analytics.cluster_spec_sheet_results.add_result(
        framework_version=CLUSTER_SPEC_SHEET_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)
