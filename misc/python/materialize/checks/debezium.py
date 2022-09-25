# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class DebeziumPostgres(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                CREATE TABLE debezium_table (f1 TEXT, f2 INTEGER, f3 INTEGER, f4 TEXT, PRIMARY KEY (f1, f2));
                ALTER TABLE debezium_table REPLICA IDENTITY FULL;
                INSERT INTO debezium_table SELECT 'A', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);

                $ http-request method=POST url=http://debezium:8083/connectors content-type=application/json
                {
                    "name": "psql-connector",
                  "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "postgres-source",
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
                    "decimal.handling.mode": "precise"
                  }
                }

                $ schema-registry-wait-schema schema=postgres.public.debezium_table-value

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

                # UPSERT is requred due to https://github.com/MaterializeInc/materialize/issues/14211
                > CREATE SOURCE debezium_source1
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.debezium_table')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM UPSERT;

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                INSERT INTO debezium_table SELECT 'B', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);

                > CREATE MATERIALIZED VIEW debezium_view1 AS SELECT f1, f3, SUM(LENGTH(f4)) FROM debezium_source1 GROUP BY f1, f3;

                > SELECT * FROM debezium_view1;
                A 1 16000
                B 1 16000
                """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                BEGIN;
                INSERT INTO debezium_table SELECT 'C', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT;

                > CREATE SOURCE debezium_source2
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.debezium_table')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM UPSERT;

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                BEGIN;
                INSERT INTO debezium_table SELECT 'D', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT;

                > CREATE MATERIALIZED VIEW debezium_view2 AS SELECT f1, f3, SUM(LENGTH(f4)) FROM debezium_source2 GROUP BY f1, f3;
                """,
                """
                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                BEGIN;
                INSERT INTO debezium_table SELECT 'E', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT;

                > CREATE SOURCE debezium_source3
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'postgres.public.debezium_table')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM UPSERT;

                $ postgres-execute connection=postgres://postgres:postgres@postgres-source
                BEGIN;
                INSERT INTO debezium_table SELECT 'F', generate_series, 1, REPEAT('X', 16) FROM generate_series(1,1000);
                UPDATE debezium_table SET f3 = f3 + 1;
                COMMIT

                > CREATE MATERIALIZED VIEW debezium_view3 AS SELECT f1, f3, SUM(LENGTH(f4)) FROM debezium_source3 GROUP BY f1, f3;
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
            )
        )
