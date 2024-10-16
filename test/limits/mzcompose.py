# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Stresses Materialize with large number of objects, large ingestions, etc. Good
to prevent regressions in basic functionality for larger installations.
"""

import contextlib
import json
import sys
import uuid
from io import StringIO
from textwrap import dedent
from urllib.parse import quote

from materialize import buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.frontegg import FronteggMock
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import all_subclasses


class Generator:
    """A common class for all the individual Generators.
    Provides a set of convenience iterators.
    """

    # By default, we create that many objects of the type under test
    # unless overriden on a per-test basis.
    #
    # For tests that deal with records, the number of records processed
    # is usually COUNT * 1000
    COUNT = 1000

    @classmethod
    def header(cls) -> None:
        print(f"\n#\n# {cls}\n#\n")
        print(
            "$ postgres-connect name=mz_system url=postgres://mz_system@materialized:6877/materialize"
        )
        print("$ postgres-execute connection=mz_system")
        print("DROP SCHEMA IF EXISTS public CASCADE;")
        print(f"CREATE SCHEMA public /* {cls} */;")
        print("GRANT ALL PRIVILEGES ON SCHEMA public TO materialize")

    @classmethod
    def body(cls) -> None:
        raise NotImplementedError

    @classmethod
    def footer(cls) -> None:
        print()

    @classmethod
    def generate(cls) -> None:
        cls.header()
        cls.body()
        cls.footer()

    @classmethod
    def all(cls) -> range:
        return range(1, cls.COUNT + 1)

    @classmethod
    def no_first(cls) -> range:
        return range(2, cls.COUNT + 1)

    @classmethod
    def no_last(cls) -> range:
        return range(1, cls.COUNT)


class Connections(Generator):
    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        # three extra connections for mz_system, default connection, and one
        # since sqlparse 0.4.4. 3 reserved superuser connections since materialize#25666
        print(f"ALTER SYSTEM SET max_connections = {Connections.COUNT+4};")

        for i in cls.all():
            print(
                f"$ postgres-connect name=conn{i} url=postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require"
            )
        for i in cls.all():
            print(f"$ postgres-execute connection=conn{i}\nSELECT 1;\n")


class Tables(Generator):
    COUNT = 90  # https://github.com/MaterializeInc/database-issues/issues/3675 and https://github.com/MaterializeInc/database-issues/issues/7830

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {Tables.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {Tables.COUNT * 10};")
        for i in cls.all():
            print(f"> CREATE TABLE t{i} (f1 INTEGER);")
        for i in cls.all():
            print(f"> INSERT INTO t{i} VALUES ({i});")
        print("> BEGIN")
        for i in cls.all():
            print(f"> SELECT * FROM t{i};\n{i}")
        print("> COMMIT")


class Subscribe(Generator):
    COUNT = 100  # Each SUBSCRIBE instantiates a dataflow, so impossible to do 1K

    @classmethod
    def body(cls) -> None:
        print("> DROP TABLE IF EXISTS t1 CASCADE;")
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (-1);")
        print("> CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) FROM t1;")

        for i in cls.all():
            print(
                f"$ postgres-connect name=conn{i} url=postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require"
            )

        for i in cls.all():
            print(f"$ postgres-execute connection=conn{i}")
            print("BEGIN;")

        for i in cls.all():
            print(f"$ postgres-execute connection=conn{i}")
            print(f"DECLARE c{i} CURSOR FOR SUBSCRIBE v1")

        for i in cls.all():
            print(f"$ postgres-execute connection=conn{i}")
            print(f"FETCH ALL FROM c{i};")


class Indexes(Generator):
    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {Indexes.COUNT * 10};")
        print(
            "> CREATE TABLE t (" + ", ".join(f"f{i} INTEGER" for i in cls.all()) + ");"
        )

        print("> INSERT INTO t VALUES (" + ", ".join(str(i) for i in cls.all()) + ");")

        for i in cls.all():
            print(f"> CREATE INDEX i{i} ON t(f{i})")
        for i in cls.all():
            print(f"> SELECT f{i} FROM t;\n{i}")


class KafkaTopics(Generator):
    COUNT = min(Generator.COUNT, 20)  # CREATE SOURCE is slow

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {KafkaTopics.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {KafkaTopics.COUNT * 10};")
        print('$ set key-schema={"type": "string"}')
        print(
            '$ set value-schema={"type": "record", "name": "r", "fields": [{"name": "f1", "type": "string"}]}'
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn
                FOR CONFLUENT SCHEMA REGISTRY
                URL '${testdrive.schema-registry-url}';
                """
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """
        )

        for i in cls.all():
            topic = f"kafka-sources-{i}"
            print(f"$ kafka-create-topic topic={topic}")
            print(
                f"$ kafka-ingest format=avro topic={topic} key-format=avro key-schema=${{key-schema}} schema=${{value-schema}}"
            )
            print(f'"{i}" {{"f1": "{i}"}}')

            print(
                f"""> CREATE SOURCE s{i}
                  IN CLUSTER single_replica_cluster
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-{topic}-${{testdrive.seed}}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE;
                  """
            )

        for i in cls.all():
            print(f"> SELECT * FROM s{i};\n{i}")


class KafkaSourcesSameTopic(Generator):
    COUNT = 500  # high memory consumption

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {KafkaSourcesSameTopic.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {KafkaSourcesSameTopic.COUNT * 10};"
        )
        print('$ set key-schema={"type": "string"}')
        print(
            '$ set value-schema={"type": "record", "name": "r", "fields": [{"name": "f1", "type": "string"}]}'
        )
        print("$ kafka-create-topic topic=topic")
        print(
            "$ kafka-ingest format=avro topic=topic key-format=avro key-schema=${key-schema} schema=${value-schema}"
        )
        print('"123" {"f1": "123"}')

        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """
        )

        for i in cls.all():
            print(
                f"""> CREATE SOURCE s{i}
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic-${{testdrive.seed}}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE NONE;
              """
            )

        for i in cls.all():
            print(f"> SELECT * FROM s{i};\n123")


class KafkaPartitions(Generator):
    COUNT = min(Generator.COUNT, 100)  # It takes 5+min to process 1K partitions

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {KafkaPartitions.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {KafkaPartitions.COUNT * 10};"
        )
        # gh#12193 : topic_metadata_refresh_interval_ms is not observed so a default refresh interval of 300s applies
        print("$ set-sql-timeout duration=600s")
        print('$ set key-schema={"type": "string"}')
        print(
            '$ set value-schema={"type": "record", "name": "r", "fields": [{"name": "f1", "type": "string"}]}'
        )
        print(
            f"$ kafka-create-topic topic=kafka-partitions partitions={round(cls.COUNT/2)}"
        )
        print(
            "$ kafka-ingest format=avro topic=kafka-partitions key-format=avro key-schema=${key-schema} schema=${value-schema} partition=-1"
        )
        for i in cls.all():
            print(f'"{i}" {{"f1": "{i}"}}')

        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """
        )

        print(
            """> CREATE SOURCE s1
            IN CLUSTER single_replica_cluster
            FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-partitions-${testdrive.seed}')
            FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
            ENVELOPE NONE;
            """
        )

        print("> CREATE DEFAULT INDEX ON s1")

        print(
            f"$ kafka-add-partitions topic=kafka-partitions total-partitions={cls.COUNT}"
        )

        print(
            "$ kafka-ingest format=avro topic=kafka-partitions key-format=avro key-schema=${key-schema} schema=${value-schema} partition=-1"
        )
        for i in cls.all():
            print(f'"{i}" {{"f1": "{i}"}}')

        print(f"> SELECT COUNT(*) FROM s1;\n{cls.COUNT * 2}")


class KafkaRecordsEnvelopeNone(Generator):
    COUNT = Generator.COUNT * 10_000

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {KafkaRecordsEnvelopeNone.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {KafkaRecordsEnvelopeNone.COUNT * 10};"
        )
        print(
            '$ set kafka-records-envelope-none={"type": "record", "name": "r", "fields": [{"name": "f1", "type": "string"}]}'
        )
        print("$ kafka-create-topic topic=kafka-records-envelope-none")
        print(
            f"$ kafka-ingest format=avro topic=kafka-records-envelope-none schema=${{kafka-records-envelope-none}} repeat={cls.COUNT}"
        )
        print('{"f1": "123"}')

        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """
        )

        print(
            """> CREATE SOURCE kafka_records_envelope_none
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-records-envelope-none-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE NONE;
              """
        )

        print(f"> SELECT COUNT(*) FROM kafka_records_envelope_none;\n{cls.COUNT}")


class KafkaRecordsEnvelopeUpsertSameValue(Generator):
    COUNT = Generator.COUNT * 10_000

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_sources = {KafkaRecordsEnvelopeUpsertSameValue.COUNT * 10};"
        )
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {KafkaRecordsEnvelopeUpsertSameValue.COUNT * 10};"
        )
        print(
            '$ set kafka-records-envelope-upsert-same-key={"type": "record", "name": "Key", "fields": [ {"name": "key", "type": "string"} ] }'
        )
        print(
            '$ set kafka-records-envelope-upsert-same-value={"type" : "record", "name" : "test", "fields" : [ {"name":"f1", "type":"string"} ] }'
        )
        print("$ kafka-create-topic topic=kafka-records-envelope-upsert-same")
        print(
            f"$ kafka-ingest format=avro topic=kafka-records-envelope-upsert-same key-format=avro key-schema=${{kafka-records-envelope-upsert-same-key}} schema=${{kafka-records-envelope-upsert-same-value}} repeat={cls.COUNT}"
        )
        print('{"key": "fish"} {"f1": "fish"}')

        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """
        )

        print(
            """> CREATE SOURCE kafka_records_envelope_upsert_same
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-records-envelope-upsert-same-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE UPSERT;
              """
        )

        print("> SELECT * FROM kafka_records_envelope_upsert_same;\nfish fish")
        print("> SELECT COUNT(*) FROM kafka_records_envelope_upsert_same;\n1")


class KafkaRecordsEnvelopeUpsertDistinctValues(Generator):
    COUNT = Generator.COUNT * 1_000

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_sources = {KafkaRecordsEnvelopeUpsertDistinctValues.COUNT * 10};"
        )
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {KafkaRecordsEnvelopeUpsertDistinctValues.COUNT * 10};"
        )
        print(
            '$ set kafka-records-envelope-upsert-distinct-key={"type": "record", "name": "Key", "fields": [ {"name": "key", "type": "string"} ] }'
        )
        print(
            '$ set kafka-records-envelope-upsert-distinct-value={"type" : "record", "name" : "test", "fields" : [ {"name":"f1", "type":"string"} ] }'
        )
        print("$ kafka-create-topic topic=kafka-records-envelope-upsert-distinct")
        print(
            f"$ kafka-ingest format=avro topic=kafka-records-envelope-upsert-distinct key-format=avro key-schema=${{kafka-records-envelope-upsert-distinct-key}} schema=${{kafka-records-envelope-upsert-distinct-value}} repeat={cls.COUNT}"
        )
        print(
            '{"key": "${kafka-ingest.iteration}"} {"f1": "${kafka-ingest.iteration}"}'
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """
        )

        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """
        )

        print(
            """> CREATE SOURCE kafka_records_envelope_upsert_distinct
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-records-envelope-upsert-distinct-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE UPSERT;
              """
        )

        print(
            f"> SELECT COUNT(*), COUNT(DISTINCT f1) FROM kafka_records_envelope_upsert_distinct;\n{cls.COUNT} {cls.COUNT}"
        )

        print(
            f"$ kafka-ingest format=avro topic=kafka-records-envelope-upsert-distinct key-format=avro key-schema=${{kafka-records-envelope-upsert-distinct-key}} schema=${{kafka-records-envelope-upsert-distinct-value}} repeat={cls.COUNT}"
        )
        print('{"key": "${kafka-ingest.iteration}"}')

        print(
            "> SELECT COUNT(*), COUNT(DISTINCT f1) FROM kafka_records_envelope_upsert_distinct;\n0 0"
        )


class KafkaSinks(Generator):
    COUNT = min(Generator.COUNT, 50)  # $ kafka-verify-data is slow

    @classmethod
    def body(cls) -> None:
        print("$ set-regex match=\\d{13} replacement=<TIMESTAMP>")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_materialized_views = {KafkaSinks.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sinks = {KafkaSinks.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {KafkaSinks.COUNT * 10};")
        for i in cls.all():
            print(f"> CREATE MATERIALIZED VIEW v{i} (f1) AS VALUES ({i})")

        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """
        )

        for i in cls.all():
            print(
                dedent(
                    f"""
                     > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);
                     > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');
                     > CREATE SINK s{i}
                       IN CLUSTER single_replica_cluster
                       FROM v{i}
                       INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink-{i}')
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE DEBEZIUM;
                     $ kafka-verify-topic sink=materialize.public.s{i}
                     """
                )
            )

        for i in cls.all():
            print(
                dedent(
                    f"""
                    $ kafka-verify-data format=avro sink=materialize.public.s{i}
                    {{"before": null, "after": {{"row": {{"f1": {i}}}}}}}
                    """
                )
            )


class KafkaSinksSameSource(Generator):
    COUNT = min(Generator.COUNT, 50)  # $ kafka-verify-data is slow

    @classmethod
    def body(cls) -> None:
        print("$ set-regex match=\\d{13} replacement=<TIMESTAMP>")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sinks = {KafkaSinksSameSource.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {KafkaSinksSameSource.COUNT * 10};"
        )
        print("> CREATE MATERIALIZED VIEW v1 (f1) AS VALUES (123)")
        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);"""
        )
        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}');"""
        )

        for i in cls.all():
            print(
                dedent(
                    f"""
                     > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);
                     > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');
                     > CREATE SINK s{i}
                       IN CLUSTER single_replica_cluster
                       FROM v1
                       INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink-same-source-{i}')
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE DEBEZIUM
                     $ kafka-verify-topic sink=materialize.public.s{i}
                     """
                )
            )

        for i in cls.all():
            print(
                f'$ kafka-verify-data format=avro sink=materialize.public.s{i}\n{{"before": null, "after": {{"row": {{"f1": 123}}}}}}\n'
            )


class Columns(Generator):
    @classmethod
    def body(cls) -> None:
        print(
            "> CREATE TABLE t (" + ", ".join(f"f{i} INTEGER" for i in cls.all()) + ");"
        )
        print("> INSERT INTO t VALUES (" + ", ".join(str(i) for i in cls.all()) + ");")
        print(
            "> CREATE MATERIALIZED VIEW v AS SELECT "
            + ", ".join(f"f{i} + 1 AS f{i}" for i in cls.all())
            + " FROM t;"
        )
        print("> CREATE DEFAULT INDEX ON v")
        print("> SELECT " + ", ".join(f"f{i} + 1" for i in cls.all()) + " FROM v;")
        print(" ".join(str(i + 2) for i in cls.all()))


class TablesCommaJoinNoCondition(Generator):
    COUNT = 100  # https://github.com/MaterializeInc/database-issues/issues/3682

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = ", ".join(f"t1 as a{i}" for i in cls.all())
        print(f"> SELECT * FROM {table_list};")
        print(" ".join("1" for i in cls.all()))


class TablesCommaJoinWithJoinCondition(Generator):
    COUNT = 20  # Otherwise is very slow

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = ", ".join(f"t1 as a{i}" for i in cls.all())
        condition_list = " AND ".join(f"a{i}.f1 = a{i+1}.f1" for i in cls.no_last())
        print(f"> SELECT * FROM {table_list} WHERE {condition_list};")
        print(" ".join("1" for i in cls.all()))


class TablesCommaJoinWithCondition(Generator):
    COUNT = min(Generator.COUNT, 100)  # Otherwise is very slow

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = ", ".join(f"t1 as a{i}" for i in cls.all())
        condition_list = " AND ".join(f"a1.f1 = a{i}.f1" for i in cls.no_first())
        print(f"> SELECT * FROM {table_list} WHERE {condition_list};")
        print(" ".join("1" for i in cls.all()))


class TablesOuterJoinUsing(Generator):
    COUNT = min(Generator.COUNT, 100)  # Otherwise is very slow

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = " LEFT JOIN ".join(
            f"t1 as a{i} USING (f1)" for i in cls.no_first()
        )
        print(f"> SELECT * FROM t1 AS a1 LEFT JOIN {table_list};")
        print("1")


class TablesOuterJoinOn(Generator):
    COUNT = min(Generator.COUNT, 100)  # Otherwise is very slow

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = " LEFT JOIN ".join(
            f"t1 as a{i} ON (a{i-1}.f1 = a{i}.f1)" for i in cls.no_first()
        )
        print(f"> SELECT * FROM t1 AS a1 LEFT JOIN {table_list};")
        print(" ".join("1" for i in cls.all()))


class SubqueriesScalarSelectListWithCondition(Generator):
    COUNT = min(
        Generator.COUNT, 100
    )  # https://github.com/MaterializeInc/database-issues/issues/2626

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        select_list = ", ".join(
            f"(SELECT f1 FROM t1 AS a{i} WHERE a{i}.f1 + 1 = t1.f1 + 1)"
            for i in cls.no_first()
        )
        print(f"> SELECT {select_list} FROM t1;")
        print(" ".join("1" for i in cls.no_first()))


class SubqueriesScalarWhereClauseAnd(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2626

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        where_clause = " AND ".join(
            f"(SELECT * FROM t1 WHERE f1 <= {i}) = 1" for i in cls.all()
        )
        print(f"> SELECT 1 WHERE {where_clause}")
        print("1")


class SubqueriesExistWhereClause(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2626

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        where_clause = " AND ".join(
            f"EXISTS (SELECT * FROM t1 WHERE f1 <= {i})" for i in cls.all()
        )
        print(f"> SELECT 1 WHERE {where_clause}")
        print("1")


class SubqueriesInWhereClauseCorrelated(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/6189

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        where_clause = " AND ".join(
            f"f1 IN (SELECT * FROM t1 WHERE f1 = a1.f1 AND f1 <= {i})"
            for i in cls.all()
        )
        print(f"> SELECT * FROM t1 AS a1 WHERE {where_clause}")
        print("1")


class SubqueriesInWhereClauseUncorrelated(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/6189

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        where_clause = " AND ".join(
            f"f1 IN (SELECT * FROM t1 WHERE f1 <= {i})" for i in cls.all()
        )
        print(f"> SELECT * FROM t1 AS a1 WHERE {where_clause}")
        print("1")


class SubqueriesWhereClauseOr(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2630

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        where_clause = " OR ".join(
            f"(SELECT * FROM t1 WHERE f1 = {i}) = 1" for i in cls.all()
        )
        print(f"> SELECT 1 WHERE {where_clause}")
        print("1")


class SubqueriesNested(Generator):
    COUNT = min(
        Generator.COUNT, 40
    )  # Otherwise we exceed the 128 limit to nested expressions

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        print("> SELECT 1 WHERE 1 = ")
        print("\n".join("  (SELECT * FROM t1 WHERE f1 = " for i in cls.all()))
        print("  1" + "".join("  )" for i in cls.all()) + ";")
        print("1")


class ViewsNested(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2626

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {ViewsNested.COUNT * 10};")
        print("> CREATE TABLE t (f1 INTEGER);")
        print("> INSERT INTO t VALUES (0);")
        print("> CREATE VIEW v0 (f1) AS SELECT f1 FROM t;")

        for i in cls.all():
            print(f"> CREATE VIEW v{i} AS SELECT f1 + 1 AS f1 FROM v{i-1};")

        print(f"> SELECT * FROM v{cls.COUNT};")
        print(f"{cls.COUNT}")


class ViewsMaterializedNested(Generator):
    COUNT = min(
        Generator.COUNT, 25
    )  # https://github.com/MaterializeInc/database-issues/issues/3958

    @classmethod
    def body(cls) -> None:
        print("$ set-sql-timeout duration=300s")
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_materialized_views = {ViewsMaterializedNested.COUNT * 10};"
        )
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {ViewsMaterializedNested.COUNT * 10};"
        )
        print("> CREATE TABLE t (f1 INTEGER);")
        print("> INSERT INTO t VALUES (0);")
        print("> CREATE MATERIALIZED VIEW v0 (f1) AS SELECT f1 FROM t;")

        for i in cls.all():
            print(
                f"> CREATE MATERIALIZED VIEW v{i} AS SELECT f1 + 1 AS f1 FROM v{i-1};"
            )

        print(f"> SELECT * FROM v{cls.COUNT};")
        print(f"{cls.COUNT}")


class CTEs(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2628

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        cte_list = ", ".join(
            f"a{i} AS (SELECT * FROM t1 WHERE f1 = 1)" for i in cls.all()
        )
        table_list = ", ".join(f"a{i}" for i in cls.all())
        print(f"> WITH {cte_list} SELECT * FROM {table_list};")
        print(" ".join("1" for i in cls.all()))


class NestedCTEsIndependent(Generator):
    COUNT = min(
        Generator.COUNT, 9
    )  # https://github.com/MaterializeInc/database-issues/issues/2628 and https://github.com/MaterializeInc/database-issues/issues/7830

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES " + ", ".join(f"({i})" for i in cls.all()))
        cte_list = ", ".join(
            f"a{i} AS (SELECT f1 + 1 AS f1 FROM a{i-1} WHERE f1 <= {i})"
            for i in cls.no_first()
        )
        table_list = ", ".join(f"a{i}" for i in cls.all())
        print(
            f"> WITH a{1} AS (SELECT * FROM t1 WHERE f1 <= 1), {cte_list} SELECT * FROM {table_list};"
        )
        print(" ".join(f"{a}" for a in cls.all()))


class NestedCTEsChained(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2629

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1)")
        cte_list = ", ".join(
            f"a{i} AS (SELECT a{i-1}.f1 + 0 AS f1 FROM a{i-1}, t1 WHERE a{i-1}.f1 = t1.f1)"
            for i in cls.no_first()
        )
        print(
            f"> WITH a{1} AS (SELECT * FROM t1), {cte_list} SELECT * FROM a{cls.COUNT};"
        )
        print("1")


class DerivedTables(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2630

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1)")
        table_list = ", ".join(
            f"(SELECT t1.f1 + {i} AS f1 FROM t1 WHERE f1 <= {i}) AS a{i}"
            for i in cls.all()
        )
        print(f"> SELECT * FROM {table_list};")
        print(" ".join(f"{i+1}" for i in cls.all()))


class Lateral(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2631

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1)")
        table_list = ", LATERAL ".join(
            f"(SELECT t1.f1 + {i-1} AS f1 FROM t1 WHERE f1 <= a{i-1}.f1) AS a{i}"
            for i in cls.no_first()
        )
        print(f"> SELECT * FROM t1 AS a1 , LATERAL {table_list};")
        print(" ".join(f"{i}" for i in cls.all()))


class SelectExpression(Generator):
    # Stack exhaustion with COUNT=1000 due to unprotected path:
    # https://github.com/MaterializeInc/database-issues/issues/3107
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        column_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({column_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        const_expression = " + ".join(f"{i}" for i in cls.all())
        print(f"> SELECT {const_expression} FROM t1;")
        print(f"{sum(cls.all())}")

        expression = " + ".join(f"f{i}" for i in cls.all())
        print(f"> SELECT {expression} FROM t1;")
        print(f"{cls.COUNT}")


class WhereExpression(Generator):
    # Stack exhaustion with COUNT=1000 due to unprotected path:
    # https://github.com/MaterializeInc/database-issues/issues/3107
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        column_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({column_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        expression = " + ".join(f"{i}" for i in cls.all())
        print(f"> SELECT f1 FROM t1 WHERE {expression} = {sum(cls.all())};")
        print("1")


class WhereConditionAnd(Generator):
    # Stack overflow, see https://github.com/MaterializeInc/database-issues/issues/5731
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        column_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({column_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        where_condition = " AND ".join(f"f{i} = 1" for i in cls.all())
        print(f"> SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class WhereConditionAndSameColumn(Generator):
    # Stack overflow, see https://github.com/MaterializeInc/database-issues/issues/5731
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")

        print("> INSERT INTO t1 VALUES (1);")

        where_condition = " AND ".join(f"f1 <= {i}" for i in cls.all())
        print(f"> SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class WhereConditionOr(Generator):
    # Stack overflow, see https://github.com/MaterializeInc/database-issues/issues/5731
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        create_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({create_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        where_condition = " OR ".join(f"f{i} = 1" for i in cls.all())
        print(f"> SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class WhereConditionOrSameColumn(Generator):
    # Stack overflow, see https://github.com/MaterializeInc/database-issues/issues/5731
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")

        print("> INSERT INTO t1 VALUES (1);")

        where_condition = " OR ".join(f"f1 = {i}" for i in cls.all())
        print(f"> SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class InList(Generator):
    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print(f"> INSERT INTO t1 VALUES ({cls.COUNT})")

        in_list = ", ".join(f"{i}" for i in cls.all())
        print(f"> SELECT * FROM t1 WHERE f1 IN ({in_list})")
        print(f"{cls.COUNT}")


class JoinUsing(Generator):
    COUNT = min(Generator.COUNT, 10)  # Slow

    @classmethod
    def body(cls) -> None:
        create_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({create_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        column_list = ", ".join(f"f{i}" for i in cls.all())
        print(f"> SELECT * FROM t1 AS a1 LEFT JOIN t1 AS a2 USING ({column_list})")
        print(" ".join("1" for i in cls.all()))


class JoinOn(Generator):
    COUNT = min(Generator.COUNT, 10)  # Slow

    @classmethod
    def body(cls) -> None:
        create_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({create_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        on_clause = " AND ".join(f"a1.f{i} = a2.f1" for i in cls.all())
        print(f"> SELECT COUNT(*) FROM t1 AS a1 LEFT JOIN t1 AS a2 ON({on_clause})")
        print("1")


class Aggregates(Generator):
    @classmethod
    def body(cls) -> None:
        create_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({create_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        aggregate_list = ", ".join(f"AVG(f{i})" for i in cls.all())
        print(f"> SELECT {aggregate_list} FROM t1")
        print(" ".join("1" for i in cls.all()))


class AggregateExpression(Generator):
    # Stack exhaustion with COUNT=1000 due to unprotected path:
    # https://github.com/MaterializeInc/database-issues/issues/3107
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        create_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({create_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        aggregate_expr = " + ".join(f"f{i}" for i in cls.all())
        print(f"> SELECT AVG({aggregate_expr}) FROM t1")
        print(cls.COUNT)


class GroupBy(Generator):
    @classmethod
    def body(cls) -> None:
        create_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({create_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        column_list_select = ", ".join(f"f{i} + 1 AS f{i}" for i in cls.all())
        column_list_group_by = ", ".join(f"f{i} + 1" for i in cls.all())
        print(
            f"> CREATE MATERIALIZED VIEW v AS SELECT COUNT(*), {column_list_select} FROM t1 GROUP BY {column_list_group_by};"
        )
        print("> CREATE DEFAULT INDEX ON v")
        print("> SELECT * FROM v")
        print("1 " + " ".join("2" for i in cls.all()))


class Unions(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2628

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (0)")

        union_list = " UNION DISTINCT ".join(
            f"(SELECT f1 + {i} FROM t1 AS a{i})" for i in cls.all()
        )
        print(f">SELECT COUNT(*) FROM ({union_list})")
        print(f"{cls.COUNT}")


class UnionsNested(Generator):
    COUNT = min(
        Generator.COUNT, 40
    )  # Otherwise we exceed the 128 limit to nested expressions

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1)")

        print("> SELECT f1 + 0 FROM t1 UNION DISTINCT ")
        print(
            "\n".join(
                f"  (SELECT f1 + {i} - {i} FROM t1 UNION DISTINCT " for i in cls.all()
            )
        )
        print("  SELECT * FROM t1 " + "".join("  )" for i in cls.all()) + ";")
        print("1")


class CaseWhen(Generator):
    # Originally this was working with 1000, but after moving lowering and
    # decorrelation from the `plan_~` to the `sequence_~` method we had to
    # reduce it a bit in order to avoid overflowing the stack. See database-issues#7216
    # and database-issues#7407 for the latest occurrences of this.
    COUNT = 600

    @classmethod
    def body(cls) -> None:
        print(
            "> CREATE TABLE t (" + ", ".join(f"f{i} INTEGER" for i in cls.all()) + ");"
        )
        print("> INSERT INTO t DEFAULT VALUES")

        print(
            "> CREATE MATERIALIZED VIEW v AS SELECT CASE "
            + " ".join(f"WHEN f{i} IS NOT NULL THEN f{i}" for i in cls.all())
            + " ELSE 123 END FROM t"
        )
        print("> CREATE DEFAULT INDEX ON v")
        print("> SELECT * FROM v")
        print("123")


class Coalesce(Generator):
    @classmethod
    def body(cls) -> None:
        print(
            "> CREATE TABLE t (" + ", ".join(f"f{i} INTEGER" for i in cls.all()) + ");"
        )
        print("> INSERT INTO t DEFAULT VALUES")

        print(
            "> CREATE MATERIALIZED VIEW v AS SELECT COALESCE("
            + ",".join(f"f{i}" for i in cls.all())
            + ", 123) FROM t"
        )
        print("> CREATE DEFAULT INDEX ON v")
        print("> SELECT * FROM v")
        print("123")


class Concat(Generator):
    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t (f STRING)")
        print("> INSERT INTO t VALUES (REPEAT('A', 1024))")

        print(
            "> CREATE MATERIALIZED VIEW v AS SELECT CONCAT("
            + ",".join("f" for i in cls.all())
            + ") AS c FROM t"
        )
        print("> CREATE DEFAULT INDEX ON v")
        print("> SELECT LENGTH(c) FROM v")
        print(f"{cls.COUNT*1024}")


class ArrayAgg(Generator):
    COUNT = 50

    @classmethod
    def body(cls) -> None:
        slt = dedent(
            f"""
            > CREATE TABLE t ({
                ", ".join(
                    ", ".join([
                        f"a{i} STRING",
                        f"b{i} STRING",
                        f"c{i} STRING",
                        f"d{i} STRING[]",
                    ])
                    for i in cls.all()
                )
            });

            > INSERT INTO t DEFAULT VALUES;

            > CREATE MATERIALIZED VIEW v2 AS SELECT {
                ", ".join(
                    f"ARRAY_AGG(a{i} ORDER BY b1) FILTER (WHERE 's{i}' = ANY(d{i})) AS r{i}"
                    for i in cls.all()
                )
            } FROM t GROUP BY a1;

            > CREATE DEFAULT INDEX ON v2;

            > SELECT COUNT(*) FROM v2;
            1
            """
        ).strip()
        print(slt)


class FilterSubqueries(Generator):
    """
    Regression test for database-issues#6189.

    Without the database-issues#6189 fix in materialize#20702 this will cause `environmend` to OOM
    because of excessive memory allocations in the `RedundantJoin` transform.
    """

    COUNT = 100

    @classmethod
    def body(cls) -> None:
        slt = dedent(
            f"""
            > CREATE TABLE t1 (f1 INTEGER);

            > INSERT INTO t1 VALUES (1);

            # Increase SQL timeout to 10 minutes (~5 should be enough).
            #
            # Update: Now 20 minutes, not 10. This query appears to scale
            # super-linear with COUNT.
            $ set-sql-timeout duration=1200s

            > SELECT * FROM t1 AS a1 WHERE {
                " AND ".join(
                    f"f1 IN (SELECT * FROM t1 WHERE f1 = a1.f1 AND f1 <= {i})"
                    for i in cls.all()
                )
            };
            1
            """
        ).strip()
        print(slt)


#
# Column width
#


class Column(Generator):
    COUNT = 100_000_000

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 STRING);")
        print(f"> INSERT INTO t1 VALUES (REPEAT('x', {cls.COUNT}));")

        print("> SELECT COUNT(DISTINCT f1) FROM t1;")
        print("1")

        print("> SELECT LENGTH(f1) FROM t1;")
        print(f"{cls.COUNT}")


#
# Table size
#


class Rows(Generator):
    COUNT = 10_000_000

    @classmethod
    def body(cls) -> None:
        print("> SET statement_timeout='60s'")
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print(f"> INSERT INTO t1 SELECT * FROM generate_series(1, {cls.COUNT})")

        print("> SELECT COUNT(*) FROM t1;")
        print(f"{cls.COUNT}")


class RowsAggregate(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> SELECT COUNT(*), MIN(generate_series), MAX(generate_series), COUNT(DISTINCT generate_series) FROM generate_series(1, {cls.COUNT});"
        )
        print(f"{cls.COUNT} 1 {cls.COUNT} {cls.COUNT}")


class RowsOrderByLimit(Generator):
    COUNT = 10_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> SELECT * FROM generate_series(1, {cls.COUNT}) ORDER BY generate_series DESC LIMIT 1;"
        )
        print(f"{cls.COUNT}")


class RowsJoinOneToOne(Generator):
    COUNT = 10_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM generate_series(1, {cls.COUNT});"
        )
        print(
            "> SELECT COUNT(*) FROM v1 AS a1, v1 AS a2 WHERE a1.generate_series = a2.generate_series;"
        )
        print(f"{cls.COUNT}")


class RowsJoinOneToMany(Generator):
    COUNT = 10_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM generate_series(1, {cls.COUNT});"
        )
        print("> SELECT COUNT(*) FROM v1 AS a1, (SELECT 1) AS a2;")
        print(f"{cls.COUNT}")


class RowsJoinCross(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM generate_series(1, {cls.COUNT});"
        )
        print("> SELECT COUNT(*) FROM v1 AS a1, v1 AS a2;")
        print(f"{cls.COUNT**2}")


class RowsJoinLargeRetraction(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        print("> SET statement_timeout='60s'")
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print(f"> INSERT INTO t1 SELECT * FROM generate_series(1, {cls.COUNT});")

        print(
            "> CREATE MATERIALIZED VIEW v1 AS SELECT a1.f1 AS col1 , a2.f1 AS col2 FROM t1 AS a1, t1 AS a2 WHERE a1.f1 = a2.f1;"
        )

        print("> SELECT COUNT(*) > 0 FROM v1;")
        print("true")

        print("> DELETE FROM t1")

        print("> SELECT COUNT(*) FROM v1;")
        print("0")


class RowsJoinDifferential(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT generate_series AS f1, generate_series AS f2 FROM (SELECT * FROM generate_series(1, {cls.COUNT}));"
        )
        print("> SELECT COUNT(*) FROM v1 AS a1, v1 AS a2 WHERE a1.f1 = a2.f1;")
        print(f"{cls.COUNT}")


class RowsJoinOuter(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT generate_series AS f1, generate_series AS f2 FROM (SELECT * FROM generate_series(1, {cls.COUNT}));"
        )
        print("> SELECT COUNT(*) FROM v1 AS a1 LEFT JOIN v1 AS a2 USING (f1);")
        print(f"{cls.COUNT}")


class PostgresSources(Generator):
    COUNT = 300  # high memory consumption, slower  with source tables

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {Tables.COUNT * 10};")
        print("$ postgres-execute connection=postgres://postgres:postgres@postgres")
        print("ALTER USER postgres WITH replication;")
        print("DROP SCHEMA IF EXISTS public CASCADE;")
        print("DROP PUBLICATION IF EXISTS mz_source;")
        print("CREATE SCHEMA public;")
        for i in cls.all():
            print(f"CREATE TABLE t{i} (c int);")
            print(f"ALTER TABLE t{i} REPLICA IDENTITY FULL;")
            print(f"INSERT INTO t{i} VALUES ({i});")
        print("CREATE PUBLICATION mz_source FOR ALL TABLES;")
        print("> CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'")
        print(
            """> CREATE CONNECTION pg TO POSTGRES (
                HOST postgres,
                DATABASE postgres,
                USER postgres,
                PASSWORD SECRET pgpass
            )"""
        )
        for i in cls.all():
            print(
                f"""> CREATE SOURCE p{i}
              IN CLUSTER single_replica_cluster
              FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
              """
            )
            print(
                f"""> CREATE TABLE t{i}
              FROM SOURCE p{i} (REFERENCE t{i})
              """
            )
        for i in cls.all():
            print(f"> SELECT * FROM t{i};\n{i}")


class MySqlSources(Generator):
    COUNT = 300  # high memory consumption, slower with source tables

    @classmethod
    def body(cls) -> None:
        print("$ set-sql-timeout duration=300s")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {Tables.COUNT * 10};")
        print(
            f"$ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}"
        )
        print("$ mysql-execute name=mysql")
        print(f"SET GLOBAL max_connections={cls.COUNT * 2 + 2}")
        print("DROP DATABASE IF EXISTS public;")
        print("CREATE DATABASE public;")
        print("USE public;")
        for i in cls.all():
            print(f"CREATE TABLE t{i} (c int);")
            print(f"INSERT INTO t{i} VALUES ({i});")
        print(
            f"> CREATE SECRET IF NOT EXISTS mysqlpass AS '{MySql.DEFAULT_ROOT_PASSWORD}'"
        )
        print(
            """> CREATE CONNECTION mysql TO MYSQL (
                HOST mysql,
                USER root,
                PASSWORD SECRET mysqlpass
            )"""
        )
        for i in cls.all():
            print(
                f"""> CREATE SOURCE m{i}
              IN CLUSTER single_replica_cluster
              FROM MYSQL CONNECTION mysql
              """
            )
            print(
                f"""> CREATE TABLE t{i}
              FROM SOURCE m{i} (REFERENCE public.t{i})
              """
            )
        for i in cls.all():
            print(f"> SELECT * FROM t{i};\n{i}")


class WebhookSources(Generator):
    COUNT = 100  # TODO: Remove when database-issues#8508 is fixed

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        for i in cls.all():
            print(
                f"> CREATE SOURCE w{i} IN CLUSTER single_replica_cluster FROM WEBHOOK BODY FORMAT TEXT;"
            )

        for i in cls.all():
            print(f"$ webhook-append database=materialize schema=public name=w{i}")
            print(f"text{i}")

        for i in cls.all():
            print(f"> SELECT * FROM w{i}")
            print(f"text{i}")


TENANT_ID = str(uuid.uuid4())
ADMIN_USER = "u1@example.com"
ADMIN_ROLE = "MaterializePlatformAdmin"
OTHER_ROLE = "MaterializePlatform"
USERS = {
    ADMIN_USER: {
        "email": ADMIN_USER,
        "password": str(uuid.uuid4()),
        "id": str(uuid.uuid4()),
        "tenant_id": TENANT_ID,
        "initial_api_tokens": [
            {
                "client_id": str(uuid.uuid4()),
                "secret": str(uuid.uuid4()),
            }
        ],
        "roles": [OTHER_ROLE, ADMIN_ROLE],
    }
}
FRONTEGG_URL = "http://frontegg-mock:6880"


def app_password(email: str) -> str:
    api_token = USERS[email]["initial_api_tokens"][0]
    password = f"mzp_{api_token['client_id']}{api_token['secret']}".replace("-", "")
    return password


MAX_CLUSTERS = 8
MAX_REPLICAS = 4
MAX_NODES = 4

SERVICES = [
    Zookeeper(),
    Kafka(),
    Postgres(max_wal_senders=Generator.COUNT, max_replication_slots=Generator.COUNT),
    MySql(),
    SchemaRegistry(),
    # We create all sources, sinks and dataflows by default with SIZE '1'
    # The workflow_instance_size workflow is testing multi-process clusters
    Testdrive(
        default_timeout="120s",
        materialize_url=f"postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require",
        materialize_use_https=True,
        no_reset=True,
    ),
    TestCerts(),
    FronteggMock(
        issuer=FRONTEGG_URL,
        encoding_key_file="/secrets/frontegg-mock.key",
        decoding_key_file="/secrets/frontegg-mock.crt",
        users=json.dumps(list(USERS.values())),
        depends_on=["test-certs"],
        volumes=[
            "secrets:/secrets",
        ],
    ),
    Balancerd(
        command=[
            "service",
            "--pgwire-listen-addr=0.0.0.0:6875",
            "--https-listen-addr=0.0.0.0:6876",
            "--internal-http-listen-addr=0.0.0.0:6878",
            "--frontegg-resolver-template=materialized:6880",
            "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
            f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
            f"--frontegg-admin-role={ADMIN_ROLE}",
            "--https-resolver-template=materialized:6881",
            "--tls-key=/secrets/balancerd.key",
            "--tls-cert=/secrets/balancerd.crt",
        ],
        depends_on=["test-certs"],
        volumes=[
            "secrets:/secrets",
        ],
    ),
    Materialized(
        memory="8G",
        default_size=1,
        options=[
            # Enable TLS on the public port to verify that balancerd is connecting to the balancerd port.
            "--tls-mode=require",
            "--tls-key=/secrets/materialized.key",
            "--tls-cert=/secrets/materialized.crt",
            f"--frontegg-tenant={TENANT_ID}",
            "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
            f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
            f"--frontegg-admin-role={ADMIN_ROLE}",
        ],
        depends_on=["test-certs"],
        volumes_extra=[
            "secrets:/secrets",
        ],
        sanity_restart=False,
    ),
]

for cluster_id in range(1, MAX_CLUSTERS + 1):
    for replica_id in range(1, MAX_REPLICAS + 1):
        for node_id in range(1, MAX_NODES + 1):
            SERVICES.append(
                Clusterd(name=f"clusterd_{cluster_id}_{replica_id}_{node_id}")
            )


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue

        with c.test_case(name):
            c.workflow(name)


def workflow_main(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run all the limits tests against a multi-node, multi-replica cluster"""

    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )

    parser.add_argument(
        "--workers",
        type=int,
        metavar="N",
        default=2,
        help="set the default number of workers",
    )
    args = parser.parse_args()

    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "postgres",
        "mysql",
        "materialized",
        "balancerd",
        "frontegg-mock",
        "clusterd_1_1_1",
        "clusterd_1_2_1",
        "clusterd_2_1_1",
        "clusterd_2_2_1",
        "clusterd_3_1_1",
        "clusterd_3_2_1",
    )

    c.sql(
        "ALTER SYSTEM SET enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.sql(
        f"""
        DROP CLUSTER quickstart cascade;
        CREATE CLUSTER quickstart REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd_1_1_1:2100', 'clusterd_1_2_1:2100'],
                STORAGE ADDRESSES ['clusterd_1_1_1:2103', 'clusterd_1_2_1:2103'],
                COMPUTECTL ADDRESSES ['clusterd_1_1_1:2101', 'clusterd_1_2_1:2101'],
                COMPUTE ADDRESSES ['clusterd_1_1_1:2102', 'clusterd_1_2_1:2102'],
                WORKERS {args.workers}
            ),
            replica2 (
                STORAGECTL ADDRESSES ['clusterd_2_1_1:2100', 'clusterd_2_2_1:2100'],
                STORAGE ADDRESSES ['clusterd_2_1_1:2103', 'clusterd_2_2_1:2103'],
                COMPUTECTL ADDRESSES ['clusterd_2_1_1:2101', 'clusterd_2_2_1:2101'],
                COMPUTE ADDRESSES ['clusterd_2_1_1:2102', 'clusterd_2_2_1:2102'],
                WORKERS {args.workers}
            )
        );
        DROP CLUSTER IF EXISTS single_replica_cluster CASCADE;
        CREATE CLUSTER single_replica_cluster REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd_3_1_1:2100', 'clusterd_3_2_1:2100'],
                STORAGE ADDRESSES ['clusterd_3_1_1:2103', 'clusterd_3_2_1:2103'],
                COMPUTECTL ADDRESSES ['clusterd_3_1_1:2101', 'clusterd_3_2_1:2101'],
                COMPUTE ADDRESSES ['clusterd_3_1_1:2102', 'clusterd_3_2_1:2102'],
                WORKERS {args.workers}
            )
        );
        GRANT ALL PRIVILEGES ON CLUSTER single_replica_cluster TO materialize;
    """,
        port=6877,
        user="mz_system",
    )

    c.up("testdrive", persistent=True)

    scenarios = buildkite.shard_list(
        (
            [globals()[args.scenario]]
            if args.scenario
            else list(all_subclasses(Generator))
        ),
        lambda s: s.__name__,
    )
    print(
        f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {scenarios}"
    )

    for scenario in scenarios:
        print(f"--- Running scenario {scenario.__name__}")
        f = StringIO()
        with contextlib.redirect_stdout(f):
            scenario.generate()
            sys.stdout.flush()
        c.testdrive(f.getvalue())


def workflow_instance_size(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Create multiple clusters with multiple nodes and replicas each"""

    parser.add_argument(
        "--workers",
        type=int,
        metavar="N",
        default=2,
        help="set the default number of workers",
    )

    parser.add_argument(
        "--clusters",
        type=int,
        metavar="N",
        default=8,
        help="set the number of clusters to create",
    )
    parser.add_argument(
        "--replicas",
        type=int,
        metavar="N",
        default=4,
        help="set the number of replicas per cluster",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        metavar="N",
        default=4,
        help="set the number of nodes per cluster replica",
    )
    args = parser.parse_args()

    assert args.clusters <= MAX_CLUSTERS, "SERVICES have to be static"
    assert args.replicas <= MAX_REPLICAS, "SERVICES have to be static"
    assert args.nodes <= MAX_NODES, "SERVICES have to be static"

    c.up("testdrive", persistent=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "materialized",
        "balancerd",
        "frontegg-mock",
    )

    # Construct the requied Clusterd instances and peer them into clusters
    nodes = []
    for cluster_id in range(1, args.clusters + 1):
        for replica_id in range(1, args.replicas + 1):
            for node_id in range(1, args.nodes + 1):
                node_name = f"clusterd_{cluster_id}_{replica_id}_{node_id}"
                nodes.append(node_name)

    with c.override(
        Testdrive(
            seed=1,
            materialize_url=f"postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require",
            materialize_use_https=True,
            no_reset=True,
        )
    ):
        c.up(*nodes)

        # Increase resource limits
        c.testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                ALTER SYSTEM SET max_clusters = {args.clusters * 10}
                ALTER SYSTEM SET max_replicas_per_cluster = {args.replicas * 10}

                CREATE CLUSTER single_replica_cluster SIZE = '4';
                GRANT ALL ON CLUSTER single_replica_cluster TO materialize;
                """
            )
        )
        # Create some input data
        c.testdrive(
            dedent(
                """
                > CREATE TABLE ten (f1 INTEGER);
                > INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

                $ set schema={
                    "type" : "record",
                    "name" : "test",
                    "fields" : [
                        {"name":"f1", "type":"string"}
                    ]
                  }

                $ kafka-create-topic topic=instance-size

                $ kafka-ingest format=avro topic=instance-size schema=${schema} repeat=10000
                {"f1": "fish"}
                """
            )
        )

        # Construct the required CREATE CLUSTER statements
        for cluster_id in range(1, args.clusters + 1):
            replica_definitions = []
            for replica_id in range(1, args.replicas + 1):
                nodes = []
                for node_id in range(1, args.nodes + 1):
                    node_name = f"clusterd_{cluster_id}_{replica_id}_{node_id}"
                    nodes.append(node_name)

                replica_name = f"replica_u{cluster_id}_{replica_id}"

                replica_definitions.append(
                    f"{replica_name} (STORAGECTL ADDRESSES ["
                    + ", ".join(f"'{n}:2100'" for n in nodes)
                    + "], STORAGE ADDRESSES ["
                    + ", ".join(f"'{n}:2103'" for n in nodes)
                    + "], COMPUTECTL ADDRESSES ["
                    + ", ".join(f"'{n}:2101'" for n in nodes)
                    + "], COMPUTE ADDRESSES ["
                    + ", ".join(f"'{n}:2102'" for n in nodes)
                    + f"], WORKERS {args.workers})"
                )

            c.sql(
                "ALTER SYSTEM SET enable_unorchestrated_cluster_replicas = true;",
                port=6877,
                user="mz_system",
            )
            c.sql(
                f"CREATE CLUSTER cluster_u{cluster_id} REPLICAS ("
                + ",".join(replica_definitions)
                + ")",
                port=6877,
                user="mz_system",
            )
            c.sql(
                f"GRANT ALL PRIVILEGES ON CLUSTER cluster_u{cluster_id} TO materialize",
                port=6877,
                user="mz_system",
            )

        # Construct some dataflows in each cluster
        for cluster_id in range(1, args.clusters + 1):
            cluster_name = f"cluster_u{cluster_id}"

            c.testdrive(
                dedent(
                    f"""
                     > SET cluster={cluster_name}

                     > CREATE DEFAULT INDEX ON ten;

                     > CREATE MATERIALIZED VIEW v_{cluster_name} AS
                       SELECT COUNT(*) AS c1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4;

                     > CREATE CONNECTION IF NOT EXISTS kafka_conn
                       TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);

                     > CREATE CONNECTION IF NOT EXISTS csr_conn
                       FOR CONFLUENT SCHEMA REGISTRY
                       URL '${{testdrive.schema-registry-url}}';

                     > CREATE SOURCE s_{cluster_name}
                       IN CLUSTER single_replica_cluster
                       FROM KAFKA CONNECTION kafka_conn (TOPIC
                       'testdrive-instance-size-${{testdrive.seed}}')
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE NONE
                 """
                )
            )

        # Validate that each individual cluster is operating properly
        for cluster_id in range(1, args.clusters + 1):
            cluster_name = f"cluster_u{cluster_id}"

            c.testdrive(
                dedent(
                    f"""
                     > SET cluster={cluster_name}

                     > SELECT c1 FROM v_{cluster_name};
                     10000

                     > SELECT COUNT(*) FROM s_{cluster_name}
                     10000
                 """
                )
            )
