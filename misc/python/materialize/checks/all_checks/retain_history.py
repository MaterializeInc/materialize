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
from materialize.checks.checks import Check, disabled
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


@disabled("#24478")
class RetainHistoryOnMv(Check):
    def _can_run(self, e: Executor) -> bool:
        return e.current_mz_version >= MzVersion.parse_mz("v0.81.0")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE time (time_index INT, t TIMESTAMP);
                > INSERT INTO time VALUES (0, now());

                > CREATE TABLE retain_history_table (key INT, value INT);
                > INSERT INTO time VALUES (1, now());

                > INSERT INTO retain_history_table VALUES (1, 100), (2, 200), (3, 300);
                > INSERT INTO time VALUES (2, now());

                > CREATE MATERIALIZED VIEW retain_history_mv1 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table;

                > SELECT count(*) FROM retain_history_mv1;
                3
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > UPDATE retain_history_table SET value = value + 10 WHERE key = 1;
                > INSERT INTO time VALUES (3, now());

                > INSERT INTO retain_history_table VALUES (4, 400);
                > INSERT INTO time VALUES (4, now());

                > DELETE FROM retain_history_table WHERE key = 3;
                > INSERT INTO time VALUES (5, now());

                > CREATE MATERIALIZED VIEW retain_history_mv2 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_table;
                """,
                """
                > CREATE MATERIALIZED VIEW retain_history_mv3 WITH (RETAIN HISTORY FOR '30s') AS
                    SELECT * FROM retain_history_mv2;

                > UPDATE retain_history_table SET value = value + 1;
                > INSERT INTO time VALUES (6, now());

                > INSERT INTO retain_history_table VALUES (5, 500);
                > INSERT INTO time VALUES (7, now());

                > DELETE FROM retain_history_table WHERE key = 4;
                > INSERT INTO time VALUES (8, now());
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        time_definitions = """
                $ set-from-sql var=time0
                SELECT t::STRING FROM time WHERE time_index = 0
                $ set-from-sql var=time1
                SELECT t::STRING FROM time WHERE time_index = 1
                $ set-from-sql var=time2
                SELECT t::STRING FROM time WHERE time_index = 2
                $ set-from-sql var=time3
                SELECT t::STRING FROM time WHERE time_index = 3
                $ set-from-sql var=time4
                SELECT t::STRING FROM time WHERE time_index = 4
                $ set-from-sql var=time5
                SELECT t::STRING FROM time WHERE time_index = 5
                $ set-from-sql var=time6
                SELECT t::STRING FROM time WHERE time_index = 6
                $ set-from-sql var=time7
                SELECT t::STRING FROM time WHERE time_index = 7
                $ set-from-sql var=time8
                SELECT t::STRING FROM time WHERE time_index = 8
                """

        content_validations = "\n".join(
            f"""
                ! SELECT * FROM {mv_name} AS OF '${{time0}}'::TIMESTAMP; -- time0
                contains: is not valid for all inputs

                > SELECT count(*) FROM {mv_name} AS OF '${{time1}}'::TIMESTAMP; -- time1
                0

                > SELECT * FROM {mv_name} AS OF '${{time2}}'::TIMESTAMP; -- time2
                1 100
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time2}}'::TIMESTAMP; -- time2
                1 100
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time3}}'::TIMESTAMP; -- time3
                1 110
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time4}}'::TIMESTAMP; -- time4
                1 110
                2 200
                3 300
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time5}}'::TIMESTAMP; -- time5
                1 110
                2 200
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time6}}'::TIMESTAMP; -- time6
                1 111
                2 201
                4 401

                > SELECT * FROM {mv_name} AS OF '${{time7}}'::TIMESTAMP; -- time7
                1 111
                2 201
                4 401
                5 500

                > SELECT * FROM {mv_name} AS OF '${{time8}}'::TIMESTAMP; -- time8
                1 111
                2 201
                5 500

                > SELECT * FROM {mv_name};
                1 111
                2 201
                5 500
                """
            for mv_name in [
                "retain_history_mv1",
                "retain_history_mv2",
                "retain_history_mv3",
            ]
        )

        definition_validations = """
                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv1);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv1\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_table\\""

                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv2);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv2\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_table\\""

                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv3);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv3\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '30s', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_mv2\\""
        """

        other_validations = """
                ? EXPLAIN OPTIMIZED PLAN AS TEXT FOR SELECT * FROM retain_history_mv1
                Explained Query:
                  ReadStorage materialize.public.retain_history_mv1
                """

        return Testdrive(
            dedent(
                f"""
                {time_definitions}

                {content_validations}

                {definition_validations}

                {other_validations}
                """
            )
        )


@disabled("#24478")
class RetainHistoryOnKafkaSource(Check):
    def _can_run(self, e: Executor) -> bool:
        return e.current_mz_version >= MzVersion.parse_mz("v0.81.0")

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE time (time_index INT, t TIMESTAMP);
                > INSERT INTO time VALUES (0, now());

                # We want to have the topic created at a slightly later timestamp
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

                $ kafka-create-topic topic=retain-history

                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${keyschema} schema=${schema} repeat=5
                {"key1": "K${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                # Give the source some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

                > INSERT INTO time VALUES (1, now());

                > CREATE SOURCE retain_history_source
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-retain-history-${testdrive.seed}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT
                  WITH (RETAIN HISTORY FOR '30s')

                # Give the source some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

                > INSERT INTO time VALUES (2, now());
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${keyschema} schema=${schema} repeat=2
                {"key1": "K${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}

                # Give the source some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

                > INSERT INTO time VALUES (3, now());
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${keyschema} schema=${schema} repeat=6
                {"key1": "K${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}

                # Give the source some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

                > INSERT INTO time VALUES (4, now());

                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${keyschema} schema=${schema} repeat=1
                {"key1": "K${kafka-ingest.iteration}"} {"f1": "D${kafka-ingest.iteration}"}

                # Give the source some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="2s"

                > INSERT INTO time VALUES (5, now());
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-from-sql var=time0
                SELECT t::STRING FROM time WHERE time_index = 0
                $ set-from-sql var=time1
                SELECT t::STRING FROM time WHERE time_index = 1
                $ set-from-sql var=time2
                SELECT t::STRING FROM time WHERE time_index = 2
                $ set-from-sql var=time3
                SELECT t::STRING FROM time WHERE time_index = 3
                $ set-from-sql var=time4
                SELECT t::STRING FROM time WHERE time_index = 4
                $ set-from-sql var=time5
                SELECT t::STRING FROM time WHERE time_index = 5

                # TODO: check / compare with MV and file issue
                # ! SELECT * FROM retain_history_source AS OF '${time0}'::TIMESTAMP; -- time0
                # contains: is not valid for all inputs

                # TODO: check / compare with MV and file issue
                # ! SELECT * FROM retain_history_source AS OF '${time1}'::TIMESTAMP; -- time1
                # contains: is not valid for all inputs

                > SELECT * FROM retain_history_source AS OF '${time2}'::TIMESTAMP; -- time2
                K0 A0
                K1 A1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time3}'::TIMESTAMP; -- time3
                K0 B0
                K1 B1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time4}'::TIMESTAMP; -- time4
                K0 C0
                K1 C1
                K2 C2
                K3 C3
                K4 C4
                K5 C5

                > SELECT * FROM retain_history_source AS OF '${time5}'::TIMESTAMP; -- time5
                K0 D0
                K1 C1
                K2 C2
                K3 C3
                K4 C4
                K5 C5

                > SELECT * FROM retain_history_source;
                K0 D0
                K1 C1
                K2 C2
                K3 C3
                K4 C4
                K5 C5
                """
            )
        )
