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
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


def schemas_null() -> str:
    return dedent("""
           $ set keyschema={
               "type": "record",
               "name": "Key",
               "fields": [
                   {"name": "key1", "type": "string"}
               ]
             }

           $ set schema={
               "type" : "record",
               "name" : "test",
               "fields" : [
                   {"name":"f1", "type":["null", "string"]},
                   {"name":"f2", "type":["long", "null"]}
               ]
             }
    """)


@externally_idempotent(False)
class IcebergSinkUpsert(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.10.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(schemas() + dedent("""
                $ kafka-create-topic topic=iceberg-sink-source

                $ kafka-ingest format=avro key-format=avro topic=iceberg-sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=iceberg-sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D2${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=iceberg-sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                $ kafka-ingest format=avro key-format=avro topic=iceberg-sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "D3${kafka-ingest.iteration}"} {"f1": "A${kafka-ingest.iteration}"}

                > CREATE SOURCE iceberg_sink_source_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-iceberg-sink-source-${testdrive.seed}')
                > CREATE TABLE iceberg_sink_source FROM SOURCE iceberg_sink_source_src (REFERENCE "testdrive-iceberg-sink-source-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE UPSERT

                > CREATE MATERIALIZED VIEW iceberg_sink_source_view AS SELECT LEFT(key1, 2) as l_k, LEFT(f1, 1) AS l_v, COUNT(*) AS c FROM iceberg_sink_source GROUP BY LEFT(key1, 2), LEFT(f1, 1);

                > CREATE SINK iceberg_sink_sink1 FROM iceberg_sink_source_view
                  INTO ICEBERG CATALOG CONNECTION polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE 'iceberg_sink_sink1'
                  )
                  USING AWS CONNECTION aws_conn
                  KEY (l_k, l_v)
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '1s');

                > SELECT * FROM iceberg_sink_source_view;
                D2 A 1000
                D3 A 1000
                U2 A 1000
                U3 A 1000

                > SELECT messages_committed >= 1
                  FROM mz_internal.mz_sink_statistics
                  JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id
                  WHERE mz_sinks.name = 'iceberg_sink_sink1';
                true
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro key-format=avro topic=iceberg-sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "U2${kafka-ingest.iteration}"} {"f1": "B${kafka-ingest.iteration}"}
                {"key1": "D2${kafka-ingest.iteration}"}

                > CREATE SINK iceberg_sink_sink2 FROM iceberg_sink_source_view
                  INTO ICEBERG CATALOG CONNECTION polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE 'iceberg_sink_sink2'
                  )
                  USING AWS CONNECTION aws_conn
                  KEY (l_k, l_v)
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '1s');

                > SELECT * FROM iceberg_sink_source_view;
                I2 B 1000
                U3 A 1000
                U2 B 1000
                D3 A 1000

                > SELECT messages_committed >= 1
                  FROM mz_internal.mz_sink_statistics
                  JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id
                  WHERE mz_sinks.name = 'iceberg_sink_sink2';
                true
                """,
                """
                $ kafka-ingest format=avro key-format=avro topic=iceberg-sink-source key-schema=${keyschema} schema=${schema} repeat=1000
                {"key1": "I3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "U3${kafka-ingest.iteration}"} {"f1": "C${kafka-ingest.iteration}"}
                {"key1": "D3${kafka-ingest.iteration}"}

                > CREATE SINK iceberg_sink_sink3 FROM iceberg_sink_source_view
                  INTO ICEBERG CATALOG CONNECTION polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE 'iceberg_sink_sink3'
                  )
                  USING AWS CONNECTION aws_conn
                  KEY (l_k, l_v)
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '1s');

                > SELECT * FROM iceberg_sink_source_view;
                I2 B 1000
                I3 C 1000
                U2 B 1000
                U3 C 1000

                > SELECT messages_committed >= 1
                  FROM mz_internal.mz_sink_statistics
                  JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id
                  WHERE mz_sinks.name = 'iceberg_sink_sink3';
                true
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                $ set-sql-timeout duration=60s
                > SELECT * FROM iceberg_sink_source_view;
                I2 B 1000
                I3 C 1000
                U2 B 1000
                U3 C 1000

                $ set-from-sql var=iceberg_user
                SELECT user FROM iceberg_credentials

                $ set-from-sql var=iceberg_key
                SELECT key FROM iceberg_credentials

                $ duckdb-execute name=iceberg
                CREATE SECRET s3_secret (TYPE S3, KEY_ID '${iceberg_user}', SECRET '${iceberg_key}', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'minio');
                SET unsafe_enable_version_guessing = true;

                $ duckdb-query name=iceberg
                SELECT * FROM iceberg_scan('s3://test-bucket/default_namespace/iceberg_sink_sink1') ORDER BY 1
                I2 B 1000
                I3 C 1000
                U2 B 1000
                U3 C 1000

                $ duckdb-query name=iceberg
                SELECT * FROM iceberg_scan('s3://test-bucket/default_namespace/iceberg_sink_sink2') ORDER BY 1
                I2 B 1000
                I3 C 1000
                U2 B 1000
                U3 C 1000

                $ duckdb-query name=iceberg
                SELECT * FROM iceberg_scan('s3://test-bucket/default_namespace/iceberg_sink_sink3') ORDER BY 1
                I2 B 1000
                I3 C 1000
                U2 B 1000
                U3 C 1000
            """))


@externally_idempotent(False)
class IcebergSinkTables(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.10.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(schemas() + dedent("""
                > CREATE TABLE iceberg_sink_large_transaction_table (f1 INTEGER, f2 TEXT, PRIMARY KEY (f1));
                > CREATE DEFAULT INDEX ON iceberg_sink_large_transaction_table;

                > INSERT INTO iceberg_sink_large_transaction_table SELECT generate_series, REPEAT('x', 1024) FROM generate_series(1, 100000);

                # Can be slow with a large transaction
                $ set-sql-timeout duration=120s

                > CREATE MATERIALIZED VIEW iceberg_sink_large_transaction_view AS SELECT f1 - 1 AS f1 , f2 FROM iceberg_sink_large_transaction_table;

                > CREATE CLUSTER iceberg_sink_large_transaction_sink_cluster SIZE 'scale=1,workers=4';

                > CREATE SINK iceberg_sink_large_transaction_sink
                  IN CLUSTER iceberg_sink_large_transaction_sink_cluster
                  FROM iceberg_sink_large_transaction_view
                  INTO ICEBERG CATALOG CONNECTION polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE 'iceberg_sink_large_transaction_sink'
                  )
                  USING AWS CONNECTION aws_conn
                  KEY (f1) NOT ENFORCED
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '1s');

                > SELECT messages_committed >= 100000
                  FROM mz_internal.mz_sink_statistics
                  JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id
                  WHERE mz_sinks.name = 'iceberg_sink_large_transaction_sink';
                true
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ set-sql-timeout duration=120s

                $ set-from-sql var=committed
                SELECT messages_committed::text FROM mz_internal.mz_sink_statistics JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id WHERE mz_sinks.name = 'iceberg_sink_large_transaction_sink'

                > UPDATE iceberg_sink_large_transaction_table SET f2 = REPEAT('y', 1024)

                > SELECT messages_committed >= ${committed} + 200000
                  FROM mz_internal.mz_sink_statistics
                  JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id
                  WHERE mz_sinks.name = 'iceberg_sink_large_transaction_sink';
                true
                """,
                """
                $ set-sql-timeout duration=120s

                $ set-from-sql var=committed
                SELECT messages_committed::text FROM mz_internal.mz_sink_statistics JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id WHERE mz_sinks.name = 'iceberg_sink_large_transaction_sink'
                > UPDATE iceberg_sink_large_transaction_table SET f2 = REPEAT('z', 1024)

                > SELECT messages_committed >= ${committed} + 200000
                  FROM mz_internal.mz_sink_statistics
                  JOIN mz_sinks ON mz_sink_statistics.id = mz_sinks.id
                  WHERE mz_sinks.name = 'iceberg_sink_large_transaction_sink';
                true
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                $ set-from-sql var=iceberg_user
                SELECT user FROM iceberg_credentials

                $ set-from-sql var=iceberg_key
                SELECT key FROM iceberg_credentials

                $ duckdb-execute name=iceberg
                CREATE SECRET s3_secret (TYPE S3, KEY_ID '${iceberg_user}', SECRET '${iceberg_key}', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'minio');
                SET unsafe_enable_version_guessing = true;

                # TODO: How to verify the data?
                # 12:1: error: executing DuckDB query: Not implemented Error: Equality deletes need the relevant columns to be selected: Error code 1: Unknown error code
                # $ duckdb-query name=iceberg
                # SELECT f2, count(*) FROM iceberg_scan('s3://test-bucket/default_namespace/iceberg_sink_large_transaction_sink') GROUP BY f2
                # zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz 100000
                """))


@externally_idempotent(False)
class AlterIcebergSinkMv(Check):
    """Check ALTER SINK with materialized views"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.10.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE table_alter_iceberg_mv1 (a INT);
                > INSERT INTO table_alter_iceberg_mv1 VALUES (0)
                > CREATE MATERIALIZED VIEW mv_alter_iceberg1 AS SELECT * FROM table_alter_iceberg_mv1
                > CREATE SINK sink_alter_iceberg_mv FROM mv_alter_iceberg1
                  INTO ICEBERG CATALOG CONNECTION polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE 'sink_alter_iceberg_mv'
                  )
                  USING AWS CONNECTION aws_conn
                  KEY (a) NOT ENFORCED
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '1s');
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE table_alter_iceberg_mv2 (a INT);
                > CREATE MATERIALIZED VIEW mv_alter_iceberg2 AS SELECT * FROM table_alter_iceberg_mv2

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_iceberg_mv';

                > ALTER SINK sink_alter_iceberg_mv SET FROM mv_alter_iceberg2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_iceberg_mv';
                true

                > INSERT INTO table_alter_iceberg_mv1 VALUES (10)
                > INSERT INTO table_alter_iceberg_mv2 VALUES (11)
                """,
                """
                > CREATE TABLE table_alter_iceberg_mv3 (a INT);
                > CREATE MATERIALIZED VIEW mv_alter_iceberg3 AS SELECT * FROM table_alter_iceberg_mv3

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_iceberg_mv';

                > ALTER SINK sink_alter_iceberg_mv SET FROM mv_alter_iceberg3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'sink_alter_iceberg_mv';
                true

                > INSERT INTO table_alter_iceberg_mv1 VALUES (100)
                > INSERT INTO table_alter_iceberg_mv2 VALUES (101)
                > INSERT INTO table_alter_iceberg_mv3 VALUES (102)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                $ set-from-sql var=iceberg_user
                SELECT user FROM iceberg_credentials

                $ set-from-sql var=iceberg_key
                SELECT key FROM iceberg_credentials

                $ duckdb-execute name=iceberg
                CREATE SECRET s3_secret (TYPE S3, KEY_ID '${iceberg_user}', SECRET '${iceberg_key}', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'minio');
                SET unsafe_enable_version_guessing = true;

                $ duckdb-query name=iceberg
                SELECT * FROM iceberg_scan('s3://test-bucket/default_namespace/sink_alter_iceberg_mv') ORDER BY 1
                0
                11
                102
                """))


@externally_idempotent(False)
class AlterIcebergSinkWebhook(Check):
    """Check ALTER SINK with webhook sources"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.10.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                >[version>=14700] CREATE CLUSTER iceberg_sink_webhook_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 2;
                >[version<14700] CREATE CLUSTER iceberg_sink_webhook_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 1;
                > CREATE SOURCE iceberg_webhook_alter1 IN CLUSTER iceberg_sink_webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;
                > CREATE SINK iceberg_sink_alter_wh FROM iceberg_webhook_alter1
                  INTO ICEBERG CATALOG CONNECTION polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE 'iceberg_sink_alter_wh'
                  )
                  USING AWS CONNECTION aws_conn
                  KEY (body) NOT ENFORCED
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '1s');
                $ webhook-append database=materialize schema=public name=iceberg_webhook_alter1
                1
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE SOURCE iceberg_webhook_alter2 IN CLUSTER iceberg_sink_webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_wh';

                > ALTER SINK iceberg_sink_alter_wh SET FROM iceberg_webhook_alter2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_wh';
                true

                $ webhook-append database=materialize schema=public name=iceberg_webhook_alter2
                2
                """,
                """
                > CREATE SOURCE iceberg_webhook_alter3 IN CLUSTER iceberg_sink_webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_wh';

                > ALTER SINK iceberg_sink_alter_wh SET FROM iceberg_webhook_alter3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_wh';
                true

                $ webhook-append database=materialize schema=public name=iceberg_webhook_alter3
                3
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                $ set-from-sql var=iceberg_user
                SELECT user FROM iceberg_credentials

                $ set-from-sql var=iceberg_key
                SELECT key FROM iceberg_credentials

                $ duckdb-execute name=iceberg
                CREATE SECRET s3_secret (TYPE S3, KEY_ID '${iceberg_user}', SECRET '${iceberg_key}', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'minio');
                SET unsafe_enable_version_guessing = true;

                $ duckdb-query name=iceberg
                SELECT * FROM iceberg_scan('s3://test-bucket/default_namespace/iceberg_sink_alter_wh') ORDER BY 1
                1
                2
                3
            """))


@externally_idempotent(False)
class AlterIcebergSinkOrder(Check):
    """Check ALTER SINK with a table created after the sink, see incident 131"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.10.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE iceberg_table_alter_order1 (x int, y string)
                > CREATE SINK iceberg_sink_alter_order FROM iceberg_table_alter_order1
                  INTO ICEBERG CATALOG CONNECTION polaris_conn (
                    NAMESPACE 'default_namespace',
                    TABLE 'iceberg_sink_alter_order'
                  )
                  USING AWS CONNECTION aws_conn
                  KEY (x, y) NOT ENFORCED
                  MODE UPSERT
                  WITH (COMMIT INTERVAL '1s');
                > INSERT INTO iceberg_table_alter_order1 VALUES (0, 'a')
                """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE iceberg_table_alter_order2 (x int, y string)

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_order';

                > ALTER SINK iceberg_sink_alter_order SET FROM iceberg_table_alter_order2;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_order';
                true

                > INSERT INTO iceberg_table_alter_order2 VALUES (1, 'b')
                > INSERT INTO iceberg_table_alter_order1 VALUES (10, 'aa')
                > INSERT INTO iceberg_table_alter_order2 VALUES (11, 'bb')
                """,
                """
                > CREATE TABLE iceberg_table_alter_order3 (x int, y string)

                $ set-from-sql var=running_count
                SELECT COUNT(*)::text FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_order';

                > ALTER SINK iceberg_sink_alter_order SET FROM iceberg_table_alter_order3;

                > SELECT COUNT(*) > ${running_count} FROM mz_internal.mz_sink_status_history JOIN mz_sinks ON mz_internal.mz_sink_status_history.sink_id = mz_sinks.id WHERE name = 'iceberg_sink_alter_order';
                true

                > INSERT INTO iceberg_table_alter_order3 VALUES (2, 'c')
                > INSERT INTO iceberg_table_alter_order3 VALUES (12, 'cc')
                > INSERT INTO iceberg_table_alter_order1 VALUES (100, 'aaa')
                > INSERT INTO iceberg_table_alter_order2 VALUES (101, 'bbb')
                > INSERT INTO iceberg_table_alter_order3 VALUES (102, 'ccc')
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
                $ set-from-sql var=iceberg_user
                SELECT user FROM iceberg_credentials

                $ set-from-sql var=iceberg_key
                SELECT key FROM iceberg_credentials

                $ duckdb-execute name=iceberg
                CREATE SECRET s3_secret (TYPE S3, KEY_ID '${iceberg_user}', SECRET '${iceberg_key}', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'minio');
                SET unsafe_enable_version_guessing = true;

                $ duckdb-query name=iceberg
                SELECT * FROM iceberg_scan('s3://test-bucket/default_namespace/iceberg_sink_alter_order') ORDER BY 1
                0 a
                1 b
                2 c
                11 bb
                12 cc
                102 ccc
            """))
