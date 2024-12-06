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
from materialize.checks.checks import Check, externally_idempotent


@externally_idempotent(False)
class DebeziumPostgres(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                CREATE TABLE debezium_table (f1 TEXT, f2 INTEGER, f3 INTEGER, f4 TEXT, PRIMARY KEY (f1, f2));
                ALTER TABLE debezium_table REPLICA IDENTITY FULL;
                INSERT INTO debezium_table SELECT 'A', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);

                $ http-request method=POST url=http://debezium:8083/connectors content-type=application/json
                {
                  "name": "psql-connector",
                  "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "postgres",
                    "database.port": "5432",
                    "database.user": "postgres",
                    "database.password": "postgres",
                    "database.dbname" : "postgres",
                    "database.server.name": "postgres",
                    "schema.include.list": "public",
                    "table.include.list": "public.debezium_table",
                    "plugin.name": "pgoutput",
                    "publication.autocreate.mode": "filtered",
                    "slot.name" : "tester",
                    "database.history.kafka.bootstrap.servers": "kafka:9092",
                    "database.history.kafka.topic": "schema-changes.history",
                    "truncate.handling.mode": "include",
                    "decimal.handling.mode": "precise",
                    "topic.prefix": "postgres"
                  }
                }

                $ schema-registry-wait topic=postgres.public.debezium_table

                $ kafka-wait-topic topic=postgres.public.debezium_table

                # UPSERT is required due to https://github.com/MaterializeInc/database-issues/issues/4064
                > CREATE SOURCE debezium_source1
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.debezium_table');
                > CREATE TABLE debezium_source1_tbl FROM SOURCE debezium_source1 (REFERENCE "postgres.public.debezium_table")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM;

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO debezium_table SELECT 'B', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);

                > CREATE MATERIALIZED VIEW debezium_view1 AS SELECT f1, f3, SUM(LENGTH(f4)) FROM debezium_source1_tbl GROUP BY f1, f3;

                > SELECT * FROM debezium_view1;
                A 1 16000
                B 1 16000
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                BEGIN;
                INSERT INTO debezium_table SELECT 'C', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT;

                > CREATE SOURCE debezium_source2
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.debezium_table');
                > CREATE TABLE debezium_source2_tbl FROM SOURCE debezium_source2 (REFERENCE "postgres.public.debezium_table")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM;

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                BEGIN;
                INSERT INTO debezium_table SELECT 'D', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT;

                > CREATE MATERIALIZED VIEW debezium_view2 AS SELECT f1, f3, SUM(LENGTH(f4)) FROM debezium_source2_tbl GROUP BY f1, f3;
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                BEGIN;
                INSERT INTO debezium_table SELECT 'E', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT;

                > CREATE SOURCE debezium_source3
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.debezium_table');
                > CREATE TABLE debezium_source3_tbl FROM SOURCE debezium_source3 (REFERENCE "postgres.public.debezium_table")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM;

                $ postgres-execute connection=postgres://postgres:postgres@postgres
                BEGIN;
                INSERT INTO debezium_table SELECT 'F', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT;

                > CREATE MATERIALIZED VIEW debezium_view3 AS SELECT f1, f3, SUM(LENGTH(f4)) FROM debezium_source3_tbl GROUP BY f1, f3;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM debezium_view1;
                A 5 16000
                B 5 16000
                C 5 16000
                D 4 16000
                E 3 16000
                F 2 16000

                > SELECT * FROM debezium_view2;
                A 5 16000
                B 5 16000
                C 5 16000
                D 4 16000
                E 3 16000
                F 2 16000

                > SELECT * FROM debezium_view3;
                A 5 16000
                B 5 16000
                C 5 16000
                D 4 16000
                E 3 16000
                F 2 16000
                """
                + (
                    """
                $ set-regex match="FORMAT .*? ENVELOPE DEBEZIUM " replacement=""

                > SHOW CREATE SOURCE debezium_source1;
                materialize.public.debezium_source1 "CREATE SOURCE \\"materialize\\".\\"public\\".\\"debezium_source1\\" IN CLUSTER \\"quickstart\\" FROM KAFKA CONNECTION \\"materialize\\".\\"public\\".\\"kafka_conn\\" (TOPIC = 'postgres.public.debezium_table') EXPOSE PROGRESS AS \\"materialize\\".\\"public\\".\\"debezium_source1_progress\\""
                > SHOW CREATE SOURCE debezium_source2;
                materialize.public.debezium_source2 "CREATE SOURCE \\"materialize\\".\\"public\\".\\"debezium_source2\\" IN CLUSTER \\"quickstart\\" FROM KAFKA CONNECTION \\"materialize\\".\\"public\\".\\"kafka_conn\\" (TOPIC = 'postgres.public.debezium_table') EXPOSE PROGRESS AS \\"materialize\\".\\"public\\".\\"debezium_source2_progress\\""
                > SHOW CREATE SOURCE debezium_source3;
                materialize.public.debezium_source3 "CREATE SOURCE \\"materialize\\".\\"public\\".\\"debezium_source3\\" IN CLUSTER \\"quickstart\\" FROM KAFKA CONNECTION \\"materialize\\".\\"public\\".\\"kafka_conn\\" (TOPIC = 'postgres.public.debezium_table') EXPOSE PROGRESS AS \\"materialize\\".\\"public\\".\\"debezium_source3_progress\\""
                """
                    if not self.is_running_as_cloudtest()
                    else ""
                )
            )
        )
