# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class LoadGeneratorAsOfUpTo(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SOURCE counter1 FROM LOAD GENERATOR COUNTER (AS OF 100, UP TO 200);
            > CREATE TABLE counter1_tbl FROM SOURCE counter1;

            > CREATE SOURCE auction1 FROM LOAD GENERATOR AUCTION (AS OF 100, UP TO 200);
            > CREATE TABLE accounts FROM SOURCE auction1 (REFERENCE accounts);
            > CREATE TABLE auctions FROM SOURCE auction1 (REFERENCE auctions);
            > CREATE TABLE bids FROM SOURCE auction1 (REFERENCE bids);
            > CREATE TABLE organizations FROM SOURCE auction1 (REFERENCE organizations);
            > CREATE TABLE users FROM SOURCE auction1 (REFERENCE users);
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
            > CREATE SOURCE counter2 FROM LOAD GENERATOR COUNTER (AS OF 1100, UP TO 1200);
            > CREATE TABLE counter2_tbl FROM SOURCE counter2;
                """,
                """
            > CREATE SOURCE counter3 FROM LOAD GENERATOR COUNTER (AS OF 11100, UP TO 11200);
            > CREATE TABLE counter3_tbl FROM SOURCE counter3;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SELECT COUNT(*) FROM counter1_tbl;
                200
                > SELECT COUNT(*) FROM counter2_tbl;
                1200
                > SELECT COUNT(*) FROM counter3_tbl;
                11200
                > SELECT COUNT(*) FROM users;
                4076
            """))


class LoadGeneratorTpch(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SOURCE lg_tpch FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.0001, UP TO 10);
            > CREATE TABLE lg_tpch_customer FROM SOURCE lg_tpch (REFERENCE customer);
            > CREATE TABLE lg_tpch_nation FROM SOURCE lg_tpch (REFERENCE nation);
            > CREATE TABLE lg_tpch_region FROM SOURCE lg_tpch (REFERENCE region);
            > CREATE TABLE lg_tpch_supplier FROM SOURCE lg_tpch (REFERENCE supplier);
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
            > CREATE TABLE lg_tpch_orders FROM SOURCE lg_tpch (REFERENCE orders);
            > CREATE TABLE lg_tpch_lineitem FROM SOURCE lg_tpch (REFERENCE lineitem);
                """,
                """
            > CREATE TABLE lg_tpch_part FROM SOURCE lg_tpch (REFERENCE part);
            > CREATE TABLE lg_tpch_partsupp FROM SOURCE lg_tpch (REFERENCE partsupp);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT count(*) FROM lg_tpch_nation;
            25
            > SELECT count(*) FROM lg_tpch_region;
            5
            > SELECT count(*) FROM lg_tpch_customer;
            15
            > SELECT count(*) FROM lg_tpch_supplier;
            1
            > SELECT count(*) FROM lg_tpch_orders;
            150
            > SELECT count(*) FROM lg_tpch_part;
            20
            > SELECT count(*) FROM lg_tpch_partsupp;
            80
            # Every lineitem belongs to an existing order.
            > SELECT count(*) > 0 FROM lg_tpch_lineitem;
            true
            > SELECT count(*) FROM lg_tpch_lineitem l
              LEFT JOIN lg_tpch_orders o ON l.l_orderkey = o.o_orderkey
              WHERE o.o_orderkey IS NULL;
            0
        """))


class LoadGeneratorMarketing(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SOURCE lg_marketing FROM LOAD GENERATOR MARKETING (TICK INTERVAL '100ms', UP TO 100);
            > CREATE TABLE lg_mkt_customers FROM SOURCE lg_marketing (REFERENCE customers);
            > CREATE TABLE lg_mkt_impressions FROM SOURCE lg_marketing (REFERENCE impressions);
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
            > CREATE TABLE lg_mkt_clicks FROM SOURCE lg_marketing (REFERENCE clicks);
            > CREATE TABLE lg_mkt_leads FROM SOURCE lg_marketing (REFERENCE leads);
                """,
                """
            > CREATE TABLE lg_mkt_coupons FROM SOURCE lg_marketing (REFERENCE coupons);
            > CREATE TABLE lg_mkt_predictions FROM SOURCE lg_marketing (REFERENCE conversion_predictions);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT count(*) > 0 FROM lg_mkt_customers;
            true
            > SELECT count(*) > 0 FROM lg_mkt_impressions;
            true
            > SELECT count(*) > 0 FROM lg_mkt_leads;
            true
            # Referential integrity between the marketing relations.
            > SELECT count(*) FROM lg_mkt_impressions i
              LEFT JOIN lg_mkt_customers c ON i.customer_id = c.id
              WHERE c.id IS NULL;
            0
            > SELECT count(*) FROM lg_mkt_clicks cl
              LEFT JOIN lg_mkt_impressions i ON cl.impression_id = i.id
              WHERE i.id IS NULL;
            0
            > SELECT count(*) FROM lg_mkt_coupons co
              LEFT JOIN lg_mkt_leads l ON co.lead_id = l.id
              WHERE l.id IS NULL;
            0
        """))


class LoadGeneratorKeyValue(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_load_generator_key_value = true

            > CREATE SOURCE lg_key_value1 FROM LOAD GENERATOR KEY VALUE (
                KEYS 16,
                PARTITIONS 4,
                SNAPSHOT ROUNDS 3,
                SEED 123,
                VALUE SIZE 10,
                BATCH SIZE 2
              )
              ENVELOPE UPSERT;
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
            > CREATE SOURCE lg_key_value2 FROM LOAD GENERATOR KEY VALUE (
                KEYS 8,
                PARTITIONS 2,
                SNAPSHOT ROUNDS 1,
                TRANSACTIONAL SNAPSHOT false,
                SEED 42,
                VALUE SIZE 10,
                BATCH SIZE 2
              )
              INCLUDE KEY AS named_key
              ENVELOPE UPSERT;
                """,
                """
            > CREATE MATERIALIZED VIEW lg_key_value_mv1 AS
              SELECT partition, count(*) AS cnt FROM lg_key_value1 GROUP BY partition;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT partition, count(*) FROM lg_key_value1 GROUP BY partition;
            0 4
            1 4
            2 4
            3 4
            > SELECT min(key), max(key) FROM lg_key_value1;
            0 15
            > SELECT * FROM lg_key_value_mv1;
            0 4
            1 4
            2 4
            3 4
            > SELECT min(named_key), max(named_key), count(*) FROM lg_key_value2;
            0 7 8
        """))


class LoadGeneratorMultiReplica(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.134.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            >[version>=13800] CREATE CLUSTER multi_cluster1 SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
            >[version<13800] CREATE CLUSTER multi_cluster1 SIZE 'scale=1,workers=1', REPLICATION FACTOR 1;
            >[version>=13800] CREATE CLUSTER multi_cluster2 SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
            >[version<13800] CREATE CLUSTER multi_cluster2 SIZE 'scale=1,workers=1', REPLICATION FACTOR 1;
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
            > CREATE SOURCE multi_counter1 IN CLUSTER multi_cluster1 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE SOURCE multi_counter2 IN CLUSTER multi_cluster2 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE TABLE multi_counter1_tbl FROM SOURCE multi_counter1;
            > CREATE TABLE multi_counter2_tbl FROM SOURCE multi_counter2;

            > ALTER CLUSTER multi_cluster1 SET (REPLICATION FACTOR 4);
                """,
                """
            > CREATE SOURCE multi_counter3 IN CLUSTER multi_cluster1 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE SOURCE multi_counter4 IN CLUSTER multi_cluster2 FROM LOAD GENERATOR COUNTER (UP TO 10);
            > CREATE TABLE multi_counter3_tbl FROM SOURCE multi_counter3;
            > CREATE TABLE multi_counter4_tbl FROM SOURCE multi_counter4;
            > ALTER CLUSTER multi_cluster2 SET (REPLICATION FACTOR 4);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                > SELECT COUNT(*) FROM multi_counter1_tbl;
                10
                > SELECT COUNT(*) FROM multi_counter2_tbl;
                10
                > SELECT COUNT(*) FROM multi_counter3_tbl;
                10
                > SELECT COUNT(*) FROM multi_counter4_tbl;
                10
            """))
