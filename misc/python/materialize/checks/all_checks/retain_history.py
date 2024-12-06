# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import re
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check, disabled
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD

# This duration needs to be long enough for running all scenarios and the CI build!
RETAIN_HISTORY_DURATION = "60m"


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


@disabled(
    "database-issues#7310 and compaction not predicable and now() not appropriate while mz_now() not applicable"
)
class RetainHistoryOnMv(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                > CREATE TABLE time_for_mv (time_index INT, t TIMESTAMP);

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (0, now());

                > CREATE TABLE retain_history_table (key INT, value INT);

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (1, now());

                > INSERT INTO retain_history_table VALUES (1, 100), (2, 200), (3, 300);

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (2, now());

                > CREATE MATERIALIZED VIEW retain_history_mv1 WITH (RETAIN HISTORY FOR '{RETAIN_HISTORY_DURATION}') AS
                    SELECT * FROM retain_history_table;

                > SELECT count(*) FROM retain_history_mv1;
                3

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (3, now());
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                > UPDATE retain_history_table SET value = value + 10 WHERE key = 1;

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (4, now());

                > INSERT INTO retain_history_table VALUES (4, 400);

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (5, now());

                > DELETE FROM retain_history_table WHERE key = 3;

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (6, now());

                > CREATE MATERIALIZED VIEW retain_history_mv2 WITH (RETAIN HISTORY FOR '{RETAIN_HISTORY_DURATION}') AS
                    SELECT * FROM retain_history_table;

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (7, now());
                """,
                f"""
                > CREATE MATERIALIZED VIEW retain_history_mv3 WITH (RETAIN HISTORY FOR '{RETAIN_HISTORY_DURATION}') AS
                    SELECT * FROM retain_history_mv2;

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (8, now());

                > UPDATE retain_history_table SET value = value + 1;

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (9, now());

                > INSERT INTO retain_history_table VALUES (5, 500);

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (10, now());

                > DELETE FROM retain_history_table WHERE key = 4;

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_mv VALUES (11, now());
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        time_definitions = """
                $ set-from-sql var=time0
                SELECT t::STRING FROM time_for_mv WHERE time_index = 0
                $ set-from-sql var=time1
                SELECT t::STRING FROM time_for_mv WHERE time_index = 1
                $ set-from-sql var=time2
                SELECT t::STRING FROM time_for_mv WHERE time_index = 2
                $ set-from-sql var=time3
                SELECT t::STRING FROM time_for_mv WHERE time_index = 3
                $ set-from-sql var=time4
                SELECT t::STRING FROM time_for_mv WHERE time_index = 4
                $ set-from-sql var=time5
                SELECT t::STRING FROM time_for_mv WHERE time_index = 5
                $ set-from-sql var=time6
                SELECT t::STRING FROM time_for_mv WHERE time_index = 6
                $ set-from-sql var=time7
                SELECT t::STRING FROM time_for_mv WHERE time_index = 7
                $ set-from-sql var=time8
                SELECT t::STRING FROM time_for_mv WHERE time_index = 8
                $ set-from-sql var=time9
                SELECT t::STRING FROM time_for_mv WHERE time_index = 9
                $ set-from-sql var=time10
                SELECT t::STRING FROM time_for_mv WHERE time_index = 10
                $ set-from-sql var=time11
                SELECT t::STRING FROM time_for_mv WHERE time_index = 11
                """

        content_validations = "\n".join(
            f"""
                ! SELECT * FROM {mv_name} AS OF '${{time0}}'::TIMESTAMP; -- time0 (nothing exists)
                contains: is not valid for all inputs

                ! SELECT count(*) FROM {mv_name} AS OF '${{time1}}'::TIMESTAMP; -- time1 (table created)
                contains: is not valid for all inputs

                > SELECT * FROM {mv_name} AS OF '${{time2}}'::TIMESTAMP; -- time2 (table populated)
                1 100
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time3}}'::TIMESTAMP; -- time3 (mv1 created)
                1 100
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time4}}'::TIMESTAMP; -- time4 (table updated in manipulate#1)
                1 110
                2 200
                3 300

                > SELECT * FROM {mv_name} AS OF '${{time5}}'::TIMESTAMP; -- time5 (table updated in manipulate#1)
                1 110
                2 200
                3 300
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time6}}'::TIMESTAMP; -- time6 (table updated in manipulate#1)
                1 110
                2 200
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time7}}'::TIMESTAMP; -- time7 (mv2 created in manipulate#1)
                1 110
                2 200
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time8}}'::TIMESTAMP; -- time8 (mv3 created in manipulate#2)
                1 110
                2 200
                4 400

                > SELECT * FROM {mv_name} AS OF '${{time9}}'::TIMESTAMP; -- time9 (table updated in manipulate#2)
                1 111
                2 201
                4 401

                > SELECT * FROM {mv_name} AS OF '${{time10}}'::TIMESTAMP; -- time10 (table updated in manipulate#2)
                1 111
                2 201
                4 401
                5 500

                > SELECT * FROM {mv_name} AS OF '${{time11}}'::TIMESTAMP; -- time11 (table updated in manipulate#2)
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

        definition_validations = f"""
                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv1);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv1\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '{RETAIN_HISTORY_DURATION}', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_table\\""

                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv2);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv2\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '{RETAIN_HISTORY_DURATION}', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_table\\""

                > SELECT create_sql FROM (SHOW CREATE MATERIALIZED VIEW retain_history_mv3);
                "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"retain_history_mv3\\" IN CLUSTER \\"quickstart\\" WITH (RETAIN HISTORY = FOR '{RETAIN_HISTORY_DURATION}', REFRESH = ON COMMIT) AS SELECT * FROM \\"materialize\\".\\"public\\".\\"retain_history_mv2\\""
        """

        other_validations = """
                ? EXPLAIN OPTIMIZED PLAN AS TEXT FOR SELECT * FROM retain_history_mv1
                Explained Query:
                  ReadStorage materialize.public.retain_history_mv1

                Target cluster: quickstart
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


@disabled(
    "database-issues#7310 and compaction not predicable and now() not appropriate while mz_now() not applicable"
)
class RetainHistoryOnKafkaSource(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                f"""
                > CREATE TABLE time_for_source (time_index INT, t TIMESTAMP);

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (0, now());

                $ kafka-create-topic topic=retain-history

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (1, now());

                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${{keyschema}} schema=${{schema}} repeat=4
                {{"key1": "K${{kafka-ingest.iteration}}"}} {{"f1": "A${{kafka-ingest.iteration}}"}}

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (2, now());

                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${{keyschema}} schema=${{schema}} repeat=5
                {{"key1": "K${{kafka-ingest.iteration}}"}} {{"f1": "A${{kafka-ingest.iteration}}"}}

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (3, now());

                > CREATE SOURCE retain_history_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-retain-history-${{testdrive.seed}}')
                  WITH (RETAIN HISTORY FOR '{RETAIN_HISTORY_DURATION}')
                > CREATE TABLE retain_history_source FROM SOURCE retain_history_source_src (REFERENCE "testdrive-retain-history-${{testdrive.seed}}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (4, now());
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

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (5, now());
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${keyschema} schema=${schema} repeat=6
                {"key1": "K${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (6, now());

                $ kafka-ingest format=avro key-format=avro topic=retain-history key-schema=${keyschema} schema=${schema} repeat=1
                {"key1": "K${kafka-ingest.iteration}"} {"f1": "D${kafka-ingest.iteration}"}

                # Give it some time
                $ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

                > INSERT INTO time_for_source VALUES (7, now());
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ set-from-sql var=time0
                SELECT t::STRING FROM time_for_source WHERE time_index = 0
                $ set-from-sql var=time1
                SELECT t::STRING FROM time_for_source WHERE time_index = 1
                $ set-from-sql var=time2
                SELECT t::STRING FROM time_for_source WHERE time_index = 2
                $ set-from-sql var=time3
                SELECT t::STRING FROM time_for_source WHERE time_index = 3
                $ set-from-sql var=time4
                SELECT t::STRING FROM time_for_source WHERE time_index = 4
                $ set-from-sql var=time5
                SELECT t::STRING FROM time_for_source WHERE time_index = 5
                $ set-from-sql var=time6
                SELECT t::STRING FROM time_for_source WHERE time_index = 6
                $ set-from-sql var=time7
                SELECT t::STRING FROM time_for_source WHERE time_index = 7

                > SELECT * FROM retain_history_source AS OF '${time0}'::TIMESTAMP; -- time0 (nothing exists)
                K0 A0
                K1 A1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time1}'::TIMESTAMP; -- time1 (topic created)
                K0 A0
                K1 A1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time2}'::TIMESTAMP; -- time2 (added data to topic)
                K0 A0
                K1 A1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time3}'::TIMESTAMP; -- time3 (further added data to topic)
                K0 A0
                K1 A1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time4}'::TIMESTAMP; -- time4 (created source)
                K0 A0
                K1 A1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time5}'::TIMESTAMP; -- time5 (updated data in topic in manipulate#1)
                K0 B0
                K1 B1
                K2 A2
                K3 A3
                K4 A4

                > SELECT * FROM retain_history_source AS OF '${time6}'::TIMESTAMP; -- time6 (updated data in topic in manipulate#2)
                K0 C0
                K1 C1
                K2 C2
                K3 C3
                K4 C4
                K5 C5

                > SELECT * FROM retain_history_source AS OF '${time7}'::TIMESTAMP; -- time7 (updated data in topic again in manipulate#2)
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


def remove_target_cluster_from_explain(sql: str) -> str:
    return re.sub(r"\n\s*Target cluster: \w+\n", "", sql)
