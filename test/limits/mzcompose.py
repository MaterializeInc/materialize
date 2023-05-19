# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import contextlib
import os
import sys
import tempfile
from textwrap import dedent

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)


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
        print("> DROP SCHEMA IF EXISTS public CASCADE;")
        print(f"> CREATE SCHEMA public /* {cls} */;")
        print(
            "$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}"
        )

    @classmethod
    def body(cls) -> None:
        assert False, "body() must be overriden"

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
        # since sqlparse 0.4.4
        print(f"ALTER SYSTEM SET max_connections = {Connections.COUNT+3};")

        for i in cls.all():
            print(
                f"$ postgres-connect name=conn{i} url=postgres://materialize:materialize@${{testdrive.materialize-sql-addr}}"
            )
        for i in cls.all():
            print(f"$ postgres-execute connection=conn{i}\nSELECT 1;\n")


class Tables(Generator):
    COUNT = 100  # https://github.com/MaterializeInc/materialize/issues/12773

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
                f"$ postgres-connect name=conn{i} url=postgres://materialize:materialize@${{testdrive.materialize-sql-addr}}"
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
    COUNT = min(
        Generator.COUNT, 100
    )  # https://github.com/MaterializeInc/materialize/issues/11134

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
            TO KAFKA (BROKER '${testdrive.kafka-addr}');
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
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-{topic}-${{testdrive.seed}}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE;
                  """
            )

        for i in cls.all():
            print(f"> SELECT * FROM s{i};\n{i}")


class KafkaSourcesSameTopic(Generator):
    COUNT = min(Generator.COUNT, 50)  # 400% CPU usage in librdkafka

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
            TO KAFKA (BROKER '${testdrive.kafka-addr}');
            """
        )

        for i in cls.all():
            print(
                f"""> CREATE SOURCE s{i}
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
            TO KAFKA (BROKER '${testdrive.kafka-addr}');
            """
        )

        print(
            """> CREATE SOURCE s1
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
            TO KAFKA (BROKER '${testdrive.kafka-addr}');
            """
        )

        print(
            """> CREATE SOURCE kafka_records_envelope_none
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
            TO KAFKA (BROKER '${testdrive.kafka-addr}');
            """
        )

        print(
            """> CREATE SOURCE kafka_records_envelope_upsert_same
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
            TO KAFKA (BROKER '${testdrive.kafka-addr}');
            """
        )

        print(
            """> CREATE SOURCE kafka_records_envelope_upsert_distinct
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
        print("$ set-regex match=\d{13} replacement=<TIMESTAMP>")
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
                     > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}');
                     > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');
                     > CREATE SINK s{i} FROM v{i}
                       INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink-{i}')
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE DEBEZIUM;
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
        print("$ set-regex match=\d{13} replacement=<TIMESTAMP>")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sinks = {KafkaSinksSameSource.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {KafkaSinksSameSource.COUNT * 10};"
        )
        print("> CREATE MATERIALIZED VIEW v1 (f1) AS VALUES (123)")
        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');"""
        )
        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}');"""
        )

        for i in cls.all():
            print(
                dedent(
                    f"""
                     > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}');
                     > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');
                     > CREATE SINK s{i} FROM v1
                       INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink-same-source-{i}')
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE DEBEZIUM
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
        print("> SELECT " + ", ".join(f"f{i}" for i in cls.all()) + " FROM t;")
        print(" ".join(str(i) for i in cls.all()))


class TablesCommaJoinNoCondition(Generator):
    COUNT = 100  # https://github.com/MaterializeInc/materialize/issues/12806

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
    )  # https://github.com/MaterializeInc/materialize/issues/8598

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
    )  # https://github.com/MaterializeInc/materialize/issues/8598

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
    )  # https://github.com/MaterializeInc/materialize/issues/8598

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
    )  # https://github.com/MaterializeInc/materialize/issues/8605

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
    )  # https://github.com/MaterializeInc/materialize/issues/8605

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
    )  # https://github.com/MaterializeInc/materialize/issues/8602

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
    )  # https://github.com/MaterializeInc/materialize/issues/8598

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
    )  # https://github.com/MaterializeInc/materialize/issues/13840

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
    )  # https://github.com/MaterializeInc/materialize/issues/8600

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
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/materialize/issues/8600

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
    )  # https://github.com/MaterializeInc/materialize/issues/8601

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
    )  # https://github.com/MaterializeInc/materialize/issues/8602

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
    )  # https://github.com/MaterializeInc/materialize/issues/8603

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
    # https://github.com/MaterializeInc/materialize/issues/10496
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
    # https://github.com/MaterializeInc/materialize/issues/10496
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
    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")

        print("> INSERT INTO t1 VALUES (1);")

        where_condition = " AND ".join(f"f1 <= {i}" for i in cls.all())
        print(f"> SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class WhereConditionOr(Generator):
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
    # https://github.com/MaterializeInc/materialize/issues/10496
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

        column_list = ", ".join(f"f{i}" for i in cls.all())
        print(f"> SELECT COUNT(*), {column_list} FROM t1 GROUP BY {column_list};")
        print("1 " + " ".join("1" for i in cls.all()))


class Unions(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/materialize/issues/8600

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


SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    # We create all sources, sinks and dataflows by default with SIZE '1'
    # The workflow_instance_size workflow is testing multi-process clusters
    Materialized(memory="8G", default_size=1),
    Testdrive(default_timeout="120s"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
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

    c.up("zookeeper", "kafka", "schema-registry")

    c.up("materialized")

    nodes = [
        Clusterd(name="clusterd_1_1"),
        Clusterd(name="clusterd_1_2"),
        Clusterd(name="clusterd_2_1"),
        Clusterd(name="clusterd_2_2"),
    ]
    with c.override(*nodes):
        c.up(*[n.name for n in nodes])

        c.sql(
            "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
            port=6877,
            user="mz_system",
        )

        c.sql(
            f"""
            DROP CLUSTER DEFAULT cascade;
            CREATE CLUSTER default REPLICAS (
                replica1 (
                    STORAGECTL ADDRESSES ['clusterd_1_1:2100', 'clusterd_1_2:2100'],
                    STORAGE ADDRESSES ['clusterd_1_1:2103', 'clusterd_1_2:2103'],
                    COMPUTECTL ADDRESSES ['clusterd_1_1:2101', 'clusterd_1_2:2101'],
                    COMPUTE ADDRESSES ['clusterd_1_1:2102', 'clusterd_1_2:2102'],
                    WORKERS {args.workers}
                ),
                replica2 (
                    STORAGECTL ADDRESSES ['clusterd_2_1:2100', 'clusterd_2_2:2100'],
                    STORAGE ADDRESSES ['clusterd_2_1:2103', 'clusterd_2_2:2103'],
                    COMPUTECTL ADDRESSES ['clusterd_2_1:2101', 'clusterd_2_2:2101'],
                    COMPUTE ADDRESSES ['clusterd_2_1:2102', 'clusterd_2_2:2102'],
                    WORKERS {args.workers}
                )
            )
        """
        )

        c.up("testdrive", persistent=True)

        scenarios = (
            [globals()[args.scenario]] if args.scenario else Generator.__subclasses__()
        )

        for scenario in scenarios:
            with tempfile.NamedTemporaryFile(mode="w", dir=c.path) as tmp:
                with contextlib.redirect_stdout(tmp):
                    scenario.generate()
                    sys.stdout.flush()
                    c.exec("testdrive", os.path.basename(tmp.name))


def workflow_instance_size(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Create multiple clusters with multiple nodes and replicas each"""
    c.up("zookeeper", "kafka", "schema-registry")

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
        "--nodes",
        type=int,
        metavar="N",
        default=4,
        help="set the number of nodes per cluster",
    )
    parser.add_argument(
        "--replicas",
        type=int,
        metavar="N",
        default=4,
        help="set the number of replicas per cluster",
    )
    args = parser.parse_args()

    c.up("testdrive", persistent=True)
    c.up("materialized")

    # Construct the requied Clusterd instances and peer them into clusters
    cluster_replicas = []
    for cluster_id in range(0, args.clusters):
        for replica_id in range(0, args.replicas):
            nodes = []
            for node_id in range(0, args.nodes):
                node_name = f"compute_u{cluster_id}_{replica_id}_{node_id}"
                nodes.append(node_name)

            for node_id in range(0, args.nodes):
                cluster_replicas.append(Clusterd(name=nodes[node_id]))

    with c.override(*cluster_replicas):
        with c.override(Testdrive(seed=1, no_reset=True)):

            for n in cluster_replicas:
                c.up(n.name)

            # Increase resource limits
            c.testdrive(
                dedent(
                    """
                    $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

                    """
                    f"""
                    $ postgres-execute connection=mz_system
                    ALTER SYSTEM SET max_clusters = {args.clusters * 10}
                    ALTER SYSTEM SET max_replicas_per_cluster = {args.replicas * 10}
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
            for cluster_id in range(0, args.clusters):
                replica_definitions = []
                for replica_id in range(0, args.replicas):
                    nodes = []
                    for node_id in range(0, args.nodes):
                        node_name = f"compute_u{cluster_id}_{replica_id}_{node_id}"
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
                    "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
                    port=6877,
                    user="mz_system",
                )
                c.sql(
                    f"CREATE CLUSTER cluster_u{cluster_id} REPLICAS ("
                    + ",".join(replica_definitions)
                    + ")"
                )

            # Construct some dataflows in each cluster
            for cluster_id in range(0, args.clusters):
                cluster_name = f"cluster_u{cluster_id}"

                c.testdrive(
                    dedent(
                        f"""
                         > SET cluster={cluster_name}

                         > CREATE DEFAULT INDEX ON ten;

                         > CREATE MATERIALIZED VIEW v_{cluster_name} AS
                           SELECT COUNT(*) AS c1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4;

                         > CREATE CONNECTION IF NOT EXISTS kafka_conn
                           TO KAFKA (BROKER '${{testdrive.kafka-addr}}');

                         > CREATE CONNECTION IF NOT EXISTS csr_conn
                           FOR CONFLUENT SCHEMA REGISTRY
                           URL '${{testdrive.schema-registry-url}}';

                         > CREATE SOURCE s_{cluster_name}
                           FROM KAFKA CONNECTION kafka_conn (TOPIC
                           'testdrive-instance-size-${{testdrive.seed}}')
                           FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                           ENVELOPE NONE
                     """
                    )
                )

            # Validate that each individual cluster is operating properly
            for cluster_id in range(0, args.clusters):
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
