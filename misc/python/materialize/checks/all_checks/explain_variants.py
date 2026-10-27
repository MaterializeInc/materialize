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


class ExplainVariants(Check):
    """EXPLAIN variants beyond OPTIMIZED PLAN and TIMESTAMP: the other plan
    stages, AS JSON, output modifiers, EXPLAIN ANALYZE, and EXPLAIN
    KEY/VALUE SCHEMA for sinks.

    Plan text changes between versions, so except for one stable plan shape
    the statements are executed without asserting on their output.
    """

    def _can_run(self, e: Executor) -> bool:
        # EXPLAIN ANALYZE CLUSTER is the newest of the exercised variants.
        return self.base_version >= MzVersion.parse_mz("v26.8.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE explain_variants_table (f1 INT, f2 STRING)
            > INSERT INTO explain_variants_table VALUES (1, 'a'), (2, 'b')
            > CREATE VIEW explain_variants_view AS
              SELECT f2, count(*) AS c FROM explain_variants_table GROUP BY f2
            > CREATE INDEX explain_variants_index ON explain_variants_view (f2)
            > CREATE MATERIALIZED VIEW explain_variants_mv AS
              SELECT f1, count(*) AS c FROM explain_variants_table GROUP BY f1
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO explain_variants_table VALUES (3, 'c')

                $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
                EXPLAIN RAW PLAN FOR SELECT f1 FROM explain_variants_table WHERE f1 > 1
                EXPLAIN DECORRELATED PLAN FOR SELECT f1 FROM explain_variants_table WHERE f1 > 1
                EXPLAIN LOCALLY OPTIMIZED PLAN FOR SELECT f1 FROM explain_variants_table WHERE f1 > 1
                EXPLAIN PHYSICAL PLAN FOR SELECT f1 FROM explain_variants_table WHERE f1 > 1
                EXPLAIN OPTIMIZED PLAN WITH (arity, types) AS VERBOSE TEXT FOR SELECT f1 FROM explain_variants_table
                EXPLAIN OPTIMIZED PLAN AS JSON FOR SELECT f1 FROM explain_variants_table
                """,
                """
                > INSERT INTO explain_variants_table VALUES (4, 'd')

                $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
                EXPLAIN RAW PLAN FOR VIEW explain_variants_view
                EXPLAIN OPTIMIZED PLAN FOR MATERIALIZED VIEW explain_variants_mv
                EXPLAIN PHYSICAL PLAN FOR INDEX explain_variants_index
                EXPLAIN PLAN FOR CREATE MATERIALIZED VIEW explain_variants_mv_hypothetical AS SELECT max(f1) FROM explain_variants_table
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM explain_variants_view
            a 1
            b 1
            c 1
            d 1

            > SELECT * FROM explain_variants_mv
            1 1
            2 1
            3 1
            4 1

            $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
            EXPLAIN RAW PLAN FOR SELECT * FROM explain_variants_view
            EXPLAIN DECORRELATED PLAN FOR SELECT * FROM explain_variants_view
            EXPLAIN LOCALLY OPTIMIZED PLAN FOR SELECT * FROM explain_variants_view
            EXPLAIN OPTIMIZED PLAN AS JSON FOR SELECT * FROM explain_variants_view
            EXPLAIN PHYSICAL PLAN FOR SELECT * FROM explain_variants_view
            SET cluster_replica = r1
            EXPLAIN ANALYZE CPU FOR INDEX explain_variants_index
            EXPLAIN ANALYZE MEMORY FOR INDEX explain_variants_index
            EXPLAIN ANALYZE CPU, MEMORY WITH SKEW FOR MATERIALIZED VIEW explain_variants_mv
            EXPLAIN ANALYZE HINTS FOR MATERIALIZED VIEW explain_variants_mv
            EXPLAIN ANALYZE CPU FOR INDEX explain_variants_index AS SQL
            EXPLAIN ANALYZE CLUSTER CPU, MEMORY
            RESET cluster_replica
            EXPLAIN KEY SCHEMA FOR CREATE SINK explain_variants_sink FROM explain_variants_mv INTO KAFKA CONNECTION kafka_conn (TOPIC 'explain-variants-schema') KEY (f1) NOT ENFORCED FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE UPSERT
            EXPLAIN VALUE SCHEMA FOR CREATE SINK explain_variants_sink FROM explain_variants_mv INTO KAFKA CONNECTION kafka_conn (TOPIC 'explain-variants-schema') KEY (f1) NOT ENFORCED FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE UPSERT

            ?[version>=13500] EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM explain_variants_mv
            Explained Query:
              ReadStorage materialize.public.explain_variants_mv

            Source materialize.public.explain_variants_mv

            Target cluster: quickstart
            """))
