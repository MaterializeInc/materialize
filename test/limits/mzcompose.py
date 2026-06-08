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
import csv
import json
import math
import re
import sys
import time
import traceback
import uuid
from io import StringIO
from textwrap import dedent
from urllib.parse import quote

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.foundationdb import foundationdb_services
from materialize.mzcompose.services.frontegg import FronteggMock
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import (
    METADATA_STORE,
    metadata_store_services,
)
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres, PostgresMetadata
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.mzcompose.services.test_certs import TestCerts
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.product_limits import (
    product_limits_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.util import all_subclasses

PRODUCT_LIMITS_FRAMEWORK_VERSION = "1.0.0"


class Statistics:
    def __init__(self, wallclock: float, explain_wallclock: float | None):
        self.wallclock = wallclock
        self.explain_wallclock = explain_wallclock

    def __str__(self) -> str:
        return f"""  wallclock: {self.wallclock:>7.2f}
  explain_wallclock: {self.explain_wallclock:>7.2f}ms"""


class Generator:
    """A common class for all the individual Generators.
    Provides a set of convenience iterators.
    """

    # By default, we create that many objects of the type under test
    # unless overriden on a per-test basis.
    #
    # For tests that deal with records, the number of records processed
    # is usually COUNT * 1000
    COUNT: int = 1000

    VERSION: str = "1.0.0"

    EXPLAIN: str | None = None

    MAX_COUNT: int | None = None

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
        print(f'GRANT ALL PRIVILEGES ON SCHEMA public TO "{ADMIN_USER}"')
        print(
            f'GRANT ALL PRIVILEGES ON CLUSTER single_replica_cluster TO "{ADMIN_USER}";',
        )
        print(
            f'GRANT ALL PRIVILEGES ON CLUSTER single_worker_cluster TO "{ADMIN_USER}";',
        )
        print(
            f'GRANT ALL PRIVILEGES ON CLUSTER quickstart TO "{ADMIN_USER}";',
        )

    @classmethod
    def body(cls) -> None:
        raise NotImplementedError

    @classmethod
    def store_explain_and_run(cls, query: str) -> str | None:
        cls.EXPLAIN = f"EXPLAIN {query}"
        print(f"> {query}")

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
        # try bumping limit a bit further since this is sometimes flaky
        print(f"ALTER SYSTEM SET max_connections = {Connections.COUNT+10};")

        for i in cls.all():
            print(
                f"$ postgres-connect name=conn{i} url=postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require"
            )
        for i in cls.all():
            print(f"$ postgres-execute connection=conn{i}\nSELECT 1;\n")


class Tables(Generator):
    COUNT = 90  # https://github.com/MaterializeInc/database-issues/issues/3675 and https://github.com/MaterializeInc/database-issues/issues/7830

    MAX_COUNT = 2880  # Too long-running with 5760 tables

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        for i in cls.all():
            print(f"> CREATE TABLE t{i} (f1 INTEGER);")
        for i in cls.all():
            print(f"> INSERT INTO t{i} VALUES ({i});")
        print("> BEGIN")
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM t{i}")
            print(f"{i}")
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
    MAX_COUNT = 2000  # Too long-running with count=2562

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print(
            "> CREATE TABLE t (" + ", ".join(f"f{i} INTEGER" for i in cls.all()) + ");"
        )

        print("> INSERT INTO t VALUES (" + ", ".join(str(i) for i in cls.all()) + ");")

        for i in cls.all():
            print(f"> CREATE INDEX i{i} ON t(f{i})")
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT f{i} FROM t")
            print(f"{i}")


class IndexedViews(Generator):
    MAX_COUNT = 1000  # TODO: Bump when https://github.com/MaterializeInc/database-issues/issues/9307 is fixed

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print(
            "> CREATE TABLE t (f0 INTEGER, f1 INTEGER, f2 INTEGER, f3 INTEGER, f4 INTEGER, f5 INTEGER, f6 INTEGER, f7 INTEGER);"
        )
        print("> INSERT INTO t VALUES (0, 1, 2, 3, 4, 5, 6, 7);")

        for i in cls.all():
            print(f"> CREATE VIEW v{i} AS SELECT * FROM t;")
            print(f"> CREATE DEFAULT INDEX i{i} ON v{i}")
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT *, {i} FROM v{i}")
            print(f"0 1 2 3 4 5 6 7 {i}")


class ArrangementSizesHistory(Generator):
    """Exercises the coordinator's periodic snapshot of
    `mz_object_arrangement_sizes` at scale: creates many indexed views
    and waits for a `collection_timestamp` that records every
    `(replica_id, object_id)` pair in
    `mz_object_arrangement_size_history`.

    `COUNT` is capped to keep clusterd memory bounded — each index
    holds ~30 MiB of arranged state per replica, so cluster memory grows
    as `30 MiB × COUNT × N_replicas`. Raising `COUNT` requires also
    shrinking per-index arrangement size (fewer/narrower rows in `t`).
    """

    # `quickstart` is the default cluster the indexes below land on; it is
    # configured with two replicas in `setup()`. Each index produces one
    # `(replica_id, object_id)` row per replica.
    NUM_REPLICAS = 2

    COUNT = min(Generator.COUNT, 100)
    MAX_COUNT = 200

    @classmethod
    def body(cls) -> None:
        expected_pairs = cls.COUNT * cls.NUM_REPLICAS
        index_names = ", ".join(f"'i{i}'" for i in cls.all())

        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        # Tighten the collection cadence so the test doesn't wait an hour
        # for the default 1h interval.
        print("$ postgres-execute connection=mz_system")
        print("ALTER SYSTEM SET arrangement_size_history_collection_interval = '5s';")

        # 30K rows × ~1 KiB → ~30 MiB arrangement per object, comfortably
        # above the 10 MiB floor in `mz_object_arrangement_sizes`.
        print("> CREATE TABLE t (f0 INTEGER, f1 TEXT);")
        print(
            "> INSERT INTO t SELECT g, repeat('x', 1024) "
            "FROM generate_series(1, 30000) g;"
        )

        for i in cls.all():
            print(f"> CREATE VIEW v{i} AS SELECT f0 + {i} AS k, f1 FROM t;")
            print(f"> CREATE DEFAULT INDEX i{i} ON v{i}")

        # `>` retries until the result matches; this waits for a
        # `collection_timestamp` whose row count for our indexes equals
        # `COUNT × N_replicas`, i.e. every expected pair was recorded at
        # the same snapshot.
        print(
            "> SELECT EXISTS ("
            "  SELECT 1"
            "    FROM mz_internal.mz_object_arrangement_size_history h"
            "    JOIN mz_objects o ON o.id = h.object_id"
            f"  WHERE o.name IN ({index_names})"
            "   GROUP BY h.collection_timestamp"
            f"  HAVING COUNT(*) = {expected_pairs}"
            ")"
        )
        print("true")


class KafkaTopics(Generator):
    COUNT = min(Generator.COUNT, 20)  # CREATE SOURCE is slow

    MAX_COUNT = 640  # Too long-running with count=1280

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print('$ set key-schema={"type": "string"}')
        print(
            '$ set value-schema={"type": "record", "name": "r", "fields": [{"name": "f1", "type": "string"}]}'
        )

        print("""> CREATE CONNECTION IF NOT EXISTS csr_conn
                FOR CONFLUENT SCHEMA REGISTRY
                URL '${testdrive.schema-registry-url}';
                """)

        print("""> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """)

        for i in cls.all():
            topic = f"kafka-sources-{i}"
            print(f"$ kafka-create-topic topic={topic}")
            print(
                f"$ kafka-ingest format=avro topic={topic} key-format=avro key-schema=${{key-schema}} schema=${{value-schema}}"
            )
            print(f'"{i}" {{"f1": "{i}"}}')

            print(f"""> CREATE SOURCE s{i}
                  IN CLUSTER single_replica_cluster
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-{topic}-${{testdrive.seed}}')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE;
                  """)

        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM s{i}")
            print(f"{i}")


class KafkaSourcesSameTopic(Generator):
    COUNT = 500  # high memory consumption

    MAX_COUNT = COUNT  # Too long-running with 750 sources

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print('$ set key-schema={"type": "string"}')
        print(
            '$ set value-schema={"type": "record", "name": "r", "fields": [{"name": "f1", "type": "string"}]}'
        )
        print("$ kafka-create-topic topic=topic")
        print(
            "$ kafka-ingest format=avro topic=topic key-format=avro key-schema=${key-schema} schema=${value-schema}"
        )
        print('"123" {"f1": "123"}')

        print("""> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """)

        print("""> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """)

        for i in cls.all():
            print(f"""> CREATE SOURCE s{i}
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic-${{testdrive.seed}}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE NONE;
              """)

        print("$ set-sql-timeout duration=600s")

        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM s{i}")
            print("123")


class KafkaPartitions(Generator):
    COUNT = min(Generator.COUNT, 100)  # It takes 5+min to process 1K partitions
    MAX_COUNT = 3200  # Too long-running with 6400 partitions

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
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

        print("""> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """)

        print("""> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """)

        print("""> CREATE SOURCE s1
            IN CLUSTER single_replica_cluster
            FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-partitions-${testdrive.seed}')
            FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
            ENVELOPE NONE;
            """)

        print("> CREATE DEFAULT INDEX ON s1")

        print(
            f"$ kafka-add-partitions topic=kafka-partitions total-partitions={cls.COUNT}"
        )

        print(
            "$ kafka-ingest format=avro topic=kafka-partitions key-format=avro key-schema=${key-schema} schema=${value-schema} partition=-1"
        )
        for i in cls.all():
            print(f'"{i}" {{"f1": "{i}"}}')

        cls.store_explain_and_run("SELECT COUNT(*) FROM s1")
        print(f"{cls.COUNT * 2}")


class KafkaRecordsEnvelopeNone(Generator):
    COUNT = Generator.COUNT * 10_000

    MAX_COUNT = COUNT  # Only runs into max unsigned int size, takes a while

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print(
            '$ set kafka-records-envelope-none={"type": "record", "name": "r", "fields": [{"name": "f1", "type": "string"}]}'
        )
        print("$ kafka-create-topic topic=kafka-records-envelope-none")
        print(
            f"$ kafka-ingest format=avro topic=kafka-records-envelope-none schema=${{kafka-records-envelope-none}} repeat={cls.COUNT}"
        )
        print('{"f1": "123"}')

        print("""> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """)

        print("""> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """)

        print("""> CREATE SOURCE kafka_records_envelope_none
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-records-envelope-none-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE NONE;
              """)

        cls.store_explain_and_run("SELECT COUNT(*) FROM kafka_records_envelope_none")
        print(f"{cls.COUNT}")


class KafkaRecordsEnvelopeUpsertSameValue(Generator):
    COUNT = Generator.COUNT * 10_000

    MAX_COUNT = COUNT  # Only runs into max unsigned int size, takes a while

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
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

        print("""> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """)

        print("""> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """)

        print("""> CREATE SOURCE kafka_records_envelope_upsert_same
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-records-envelope-upsert-same-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE UPSERT;
              """)

        print("> SELECT * FROM kafka_records_envelope_upsert_same;\nfish fish")
        cls.store_explain_and_run(
            "SELECT COUNT(*) FROM kafka_records_envelope_upsert_same"
        )
        print("1")


class KafkaRecordsEnvelopeUpsertDistinctValues(Generator):
    COUNT = Generator.COUNT * 1_000
    MAX_COUNT = 64000000  # Too long-running with count 77968750

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
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

        print("""> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """)

        print("""> CREATE CONNECTION IF NOT EXISTS kafka_conn
            TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);
            """)

        print("""> CREATE SOURCE kafka_records_envelope_upsert_distinct
              IN CLUSTER single_replica_cluster
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-records-envelope-upsert-distinct-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE UPSERT;
              """)

        cls.store_explain_and_run(
            "SELECT COUNT(*), COUNT(DISTINCT f1) FROM kafka_records_envelope_upsert_distinct"
        )
        print(f"{cls.COUNT} {cls.COUNT}")

        print(
            f"$ kafka-ingest format=avro topic=kafka-records-envelope-upsert-distinct key-format=avro key-schema=${{kafka-records-envelope-upsert-distinct-key}} schema=${{kafka-records-envelope-upsert-distinct-value}} repeat={cls.COUNT}"
        )
        print('{"key": "${kafka-ingest.iteration}"}')

        print(
            "> SELECT COUNT(*), COUNT(DISTINCT f1) FROM kafka_records_envelope_upsert_distinct;\n0 0"
        )


class KafkaSinks(Generator):
    COUNT = min(Generator.COUNT, 50)  # $ kafka-verify-data is slow

    MAX_COUNT = 1600  # Too long-running with 3200 sinks

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_materialized_views = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sinks = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        for i in cls.all():
            print(f"> CREATE MATERIALIZED VIEW v{i} (f1) AS VALUES ({i})")

        print("""> CREATE CONNECTION IF NOT EXISTS csr_conn
            FOR CONFLUENT SCHEMA REGISTRY
            URL '${testdrive.schema-registry-url}';
            """)

        for i in cls.all():
            print(dedent(f"""
                     > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);
                     > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');
                     > CREATE SINK s{i}
                       IN CLUSTER single_replica_cluster
                       FROM v{i}
                       INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink-{i}')
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE DEBEZIUM;
                     $ kafka-verify-topic sink=materialize.public.s{i}
                     """))

        for i in cls.all():
            print(dedent(f"""
                    $ kafka-verify-data format=avro sink=materialize.public.s{i}
                    {{"before": null, "after": {{"row": {{"f1": {i}}}}}}}
                    """))


class KafkaSinksSameSource(Generator):
    COUNT = min(Generator.COUNT, 50)  # $ kafka-verify-data is slow

    MAX_COUNT = 3200  # Too long-running with 6400 sinks

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sinks = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("> CREATE MATERIALIZED VIEW v1 (f1) AS VALUES (123)")
        print(
            """> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);"""
        )
        print(
            """> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}');"""
        )

        for i in cls.all():
            print(dedent(f"""
                     > CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT);
                     > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${{testdrive.schema-registry-url}}');
                     > CREATE SINK s{i}
                       IN CLUSTER single_replica_cluster
                       FROM v1
                       INTO KAFKA CONNECTION kafka_conn (TOPIC 'kafka-sink-same-source-{i}')
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE DEBEZIUM
                     $ kafka-verify-topic sink=materialize.public.s{i}
                     """))

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
        cls.store_explain_and_run(
            "SELECT " + ", ".join(f"f{i} + 1" for i in cls.all()) + " FROM v;"
        )
        print(" ".join(str(i + 2) for i in cls.all()))


class TablesCommaJoinNoCondition(Generator):
    COUNT = 100  # https://github.com/MaterializeInc/database-issues/issues/3682

    MAX_COUNT = 200  # Too long-running with 400 conditions

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = ", ".join(f"t1 as a{i}" for i in cls.all())
        cls.store_explain_and_run(f"SELECT * FROM {table_list};")
        print(" ".join("1" for i in cls.all()))


class TablesCommaJoinWithJoinCondition(Generator):
    COUNT = 20  # Otherwise is very slow

    MAX_COUNT = 200  # Too long-running with 400 conditions

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = ", ".join(f"t1 as a{i}" for i in cls.all())
        condition_list = " AND ".join(f"a{i}.f1 = a{i+1}.f1" for i in cls.no_last())
        cls.store_explain_and_run(f"SELECT * FROM {table_list} WHERE {condition_list};")
        print(" ".join("1" for i in cls.all()))


class TablesCommaJoinWithCondition(Generator):
    COUNT = min(Generator.COUNT, 100)

    MAX_COUNT = 200  # Too long-running with 400 conditions

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        table_list = ", ".join(f"t1 as a{i}" for i in cls.all())
        condition_list = " AND ".join(f"a1.f1 = a{i}.f1" for i in cls.no_first())
        cls.store_explain_and_run(f"SELECT * FROM {table_list} WHERE {condition_list};")
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
        cls.store_explain_and_run(f"SELECT * FROM t1 AS a1 LEFT JOIN {table_list};")
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
        cls.store_explain_and_run(f"SELECT * FROM t1 AS a1 LEFT JOIN {table_list};")
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
        cls.store_explain_and_run(f"SELECT {select_list} FROM t1;")
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
        cls.store_explain_and_run(f"SELECT 1 WHERE {where_clause}")
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
        cls.store_explain_and_run(f"SELECT 1 WHERE {where_clause}")
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
        cls.store_explain_and_run(f"SELECT * FROM t1 AS a1 WHERE {where_clause}")
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
        cls.store_explain_and_run(f"SELECT * FROM t1 AS a1 WHERE {where_clause}")
        print("1")


class SubqueriesWhereClauseOr(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2630
    MAX_COUNT = 160  # Too long-running with count=320

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        where_clause = " OR ".join(
            f"(SELECT * FROM t1 WHERE f1 = {i}) = 1" for i in cls.all()
        )
        cls.store_explain_and_run(f"SELECT 1 WHERE {where_clause}")
        print("1")


class SubqueriesNested(Generator):
    COUNT = min(
        Generator.COUNT, 40
    )  # Otherwise we exceed the 128 limit to nested expressions

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        cls.store_explain_and_run(
            "SELECT 1 WHERE 1 = "
            + " ".join("  (SELECT * FROM t1 WHERE f1 = " for i in cls.all())
            + "  1"
            + "".join("  )" for i in cls.all())
        )
        print("1")


class ViewsNested(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2626

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("> CREATE TABLE t (f1 INTEGER);")
        print("> INSERT INTO t VALUES (0);")
        print("> CREATE VIEW v0 (f1) AS SELECT f1 FROM t;")

        for i in cls.all():
            print(f"> CREATE VIEW v{i} AS SELECT f1 + 1 AS f1 FROM v{i-1};")

        cls.store_explain_and_run(f"SELECT * FROM v{cls.COUNT};")
        print(f"{cls.COUNT}")


class ViewsMaterializedNested(Generator):
    COUNT = min(
        Generator.COUNT, 25
    )  # https://github.com/MaterializeInc/database-issues/issues/3958

    MAX_COUNT = 400  # Too long-running with 800 views

    @classmethod
    def body(cls) -> None:
        print("$ set-sql-timeout duration=300s")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_materialized_views = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("> CREATE TABLE t (f1 INTEGER);")
        print("> INSERT INTO t VALUES (0);")
        print("> CREATE MATERIALIZED VIEW v0 (f1) AS SELECT f1 FROM t;")

        for i in cls.all():
            print(
                f"> CREATE MATERIALIZED VIEW v{i} AS SELECT f1 + 1 AS f1 FROM v{i-1};"
            )

        cls.store_explain_and_run(f"SELECT * FROM v{cls.COUNT};")
        print(f"{cls.COUNT}")


class ClustersWithMaterializedViews(Generator):
    """Creates many small managed clusters, each hosting a few materialized
    views. Stresses the controller's per-cluster bookkeeping and the
    orchestrator launching many replicas concurrently.

    Each cluster runs its own `scale=1,workers=1` replica, so cluster memory
    grows roughly linearly with `COUNT`. The materialized views are kept
    trivial (a projection over a tiny table) so that per-view arrangement
    memory is negligible and the test isolates the per-cluster overhead.
    """

    # Each cluster spawns its own replica process, so we cannot create as many
    # clusters as we do tables. Cap to keep the orchestrator and memory bounded
    # under the materialized memory limit.
    COUNT = min(Generator.COUNT, 50)
    MAX_COUNT = 100

    # Number of materialized views hosted on each cluster.
    MVS_PER_CLUSTER = 16

    @classmethod
    def body(cls) -> None:
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_clusters = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_materialized_views = {cls.COUNT * cls.MVS_PER_CLUSTER * 10};"
        )
        print("$ postgres-execute connection=mz_system")
        print(
            f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * cls.MVS_PER_CLUSTER * 10};"
        )
        # The testdrive default connection runs as ADMIN_USER, which needs the
        # CREATECLUSTER system privilege to create the clusters below.
        print("$ postgres-execute connection=mz_system")
        print(f'GRANT CREATECLUSTER ON SYSTEM TO "{ADMIN_USER}";')

        # Shared input for all materialized views. Kept tiny so per-view
        # arrangement memory is negligible and the test isolates the
        # per-cluster overhead.
        print("> CREATE TABLE t (f1 INTEGER);")
        print("> INSERT INTO t VALUES (1);")

        # Drop any clusters left over from a previous (smaller) run so the
        # scenario is idempotent under --find-limit, which reruns with an
        # increasing COUNT.
        for i in cls.all():
            print(f"> DROP CLUSTER IF EXISTS c{i} CASCADE;")

        for i in cls.all():
            print(f"> CREATE CLUSTER c{i} SIZE = 'scale=1,workers=1';")
            for j in range(1, cls.MVS_PER_CLUSTER + 1):
                print(
                    f"> CREATE MATERIALIZED VIEW mv{i}_{j} IN CLUSTER c{i} AS "
                    f"SELECT f1 + {j} AS f1 FROM t;"
                )

        # Validate that every materialized view on every cluster is hydrated
        # and returns the expected value.
        for i in cls.all():
            for j in range(1, cls.MVS_PER_CLUSTER + 1):
                print(f"> SELECT f1 FROM mv{i}_{j};")
                print(f"{1 + j}")

        # Drop the clusters again so they do not leak into subsequent
        # scenarios: `header()` resets the `public` schema but not clusters.
        for i in cls.all():
            print(f"> DROP CLUSTER c{i} CASCADE;")


class CTEs(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2628

    MAX_COUNT = 240  # Too long-running with count=480

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")
        cte_list = ", ".join(
            f"a{i} AS (SELECT * FROM t1 WHERE f1 = 1)" for i in cls.all()
        )
        table_list = ", ".join(f"a{i}" for i in cls.all())
        cls.store_explain_and_run(f"WITH {cte_list} SELECT * FROM {table_list}")
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
        cls.store_explain_and_run(
            f"WITH a{1} AS (SELECT * FROM t1 WHERE f1 <= 1), {cte_list} SELECT * FROM {table_list}"
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
        cls.store_explain_and_run(
            f"WITH a{1} AS (SELECT * FROM t1), {cte_list} SELECT * FROM a{cls.COUNT}"
        )
        print("1")


class DerivedTables(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2630

    MAX_COUNT = 160  # Too long-running with count=320

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1)")
        table_list = ", ".join(
            f"(SELECT t1.f1 + {i} AS f1 FROM t1 WHERE f1 <= {i}) AS a{i}"
            for i in cls.all()
        )
        cls.store_explain_and_run(f"SELECT * FROM {table_list};")
        print(" ".join(f"{i+1}" for i in cls.all()))


class Lateral(Generator):
    COUNT = min(
        Generator.COUNT, 10
    )  # https://github.com/MaterializeInc/database-issues/issues/2631

    MAX_COUNT = 160  # Too long-running with count=320

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1)")
        table_list = ", LATERAL ".join(
            f"(SELECT t1.f1 + {i-1} AS f1 FROM t1 WHERE f1 <= a{i-1}.f1) AS a{i}"
            for i in cls.no_first()
        )
        cls.store_explain_and_run(f"SELECT * FROM t1 AS a1 , LATERAL {table_list};")
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
        cls.store_explain_and_run(f"SELECT {const_expression} FROM t1;")
        print(f"{sum(cls.all())}")

        expression = " + ".join(f"f{i}" for i in cls.all())
        cls.store_explain_and_run(f"SELECT {expression} FROM t1;")
        print(f"{cls.COUNT}")


class WhereExpression(Generator):
    # Stack exhaustion with COUNT=1000 due to unprotected path:
    # https://github.com/MaterializeInc/database-issues/issues/3107
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        print("> SET statement_timeout='120s'")
        column_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({column_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        expression = " + ".join(f"{i}" for i in cls.all())
        cls.store_explain_and_run(
            f"SELECT f1 FROM t1 WHERE {expression} = {sum(cls.all())};"
        )
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
        cls.store_explain_and_run(f"SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class WhereConditionAndSameColumn(Generator):
    # Stack overflow, see https://github.com/MaterializeInc/database-issues/issues/5731
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")

        print("> INSERT INTO t1 VALUES (1);")

        where_condition = " AND ".join(f"f1 <= {i}" for i in cls.all())
        cls.store_explain_and_run(f"SELECT f1 FROM t1 WHERE {where_condition};")
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
        cls.store_explain_and_run(f"SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class WhereConditionOrSameColumn(Generator):
    # Stack overflow, see https://github.com/MaterializeInc/database-issues/issues/5731
    COUNT = min(Generator.COUNT, 500)

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")

        print("> INSERT INTO t1 VALUES (1);")

        where_condition = " OR ".join(f"f1 = {i}" for i in cls.all())
        cls.store_explain_and_run(f"SELECT f1 FROM t1 WHERE {where_condition};")
        print("1")


class InList(Generator):
    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print(f"> INSERT INTO t1 VALUES ({cls.COUNT})")

        in_list = ", ".join(f"{i}" for i in cls.all())
        cls.store_explain_and_run(f"SELECT * FROM t1 WHERE f1 IN ({in_list})")
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
        cls.store_explain_and_run(
            f"SELECT * FROM t1 AS a1 LEFT JOIN t1 AS a2 USING ({column_list})"
        )
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
        cls.store_explain_and_run(
            f"SELECT COUNT(*) FROM t1 AS a1 LEFT JOIN t1 AS a2 ON({on_clause})"
        )
        print("1")


class Aggregates(Generator):
    @classmethod
    def body(cls) -> None:
        create_list = ", ".join(f"f{i} INTEGER" for i in cls.all())
        print(f"> CREATE TABLE t1 ({create_list});")

        value_list = ", ".join("1" for i in cls.all())
        print(f"> INSERT INTO t1 VALUES ({value_list});")

        aggregate_list = ", ".join(f"AVG(f{i})" for i in cls.all())
        cls.store_explain_and_run(f"SELECT {aggregate_list} FROM t1")
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
        cls.store_explain_and_run(f"SELECT AVG({aggregate_expr}) FROM t1")
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
        cls.store_explain_and_run("SELECT * FROM v")
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
        cls.store_explain_and_run(f"SELECT COUNT(*) FROM ({union_list})")
        print(f"{cls.COUNT}")


class UnionsNested(Generator):
    COUNT = min(
        Generator.COUNT, 40
    )  # Otherwise we exceed the 128 limit to nested expressions

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1)")

        cls.store_explain_and_run(
            "SELECT f1 + 0 FROM t1 UNION DISTINCT "
            + "\n".join(
                f"  (SELECT f1 + {i} - {i} FROM t1 UNION DISTINCT " for i in cls.all()
            )
            + "\n"
            + "  SELECT * FROM t1 "
            + "".join("  )" for i in cls.all())
        )
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
        cls.store_explain_and_run("SELECT * FROM v")
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
        cls.store_explain_and_run("SELECT * FROM v")
        print("123")


class Concat(Generator):
    MAX_COUNT = 250_000  # Too long-running with 500_000

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
        cls.store_explain_and_run("SELECT LENGTH(c) FROM v")
        print(f"{cls.COUNT*1024}")


class ArrayAgg(Generator):
    COUNT = 50

    @classmethod
    def body(cls) -> None:
        print("$ set-sql-timeout duration=300s")
        print("> SET statement_timeout='300s'")
        print(f"""> CREATE TABLE t ({
            ", ".join(
                ", ".join([
                    f"a{i} STRING",
                    f"b{i} STRING",
                    f"c{i} STRING",
                    f"d{i} STRING[]",
                ])
                for i in cls.all()
            )
        });""")
        print("> INSERT INTO t DEFAULT VALUES;")
        print(f"""> CREATE MATERIALIZED VIEW v2 AS SELECT {
            ", ".join(
                f"ARRAY_AGG(a{i} ORDER BY b1) FILTER (WHERE 's{i}' = ANY(d{i})) AS r{i}"
                for i in cls.all()
            )
        } FROM t GROUP BY a1;""")
        print("> CREATE DEFAULT INDEX ON v2;")

        cls.store_explain_and_run("SELECT COUNT(*) FROM v2")
        print("1")


class FilterSubqueries(Generator):
    """
    Regression test for database-issues#6189.

    Without the database-issues#6189 fix in materialize#20702 this will cause `environmend` to OOM
    because of excessive memory allocations in the `RedundantJoin` transform.
    """

    COUNT = min(Generator.COUNT, 100)

    MAX_COUNT = 111  # Too long-running with count=200

    @classmethod
    def body(cls) -> None:
        print("> CREATE TABLE t1 (f1 INTEGER);")
        print("> INSERT INTO t1 VALUES (1);")

        # TODO: Slow optimization, possibly due to https://github.com/MaterializeInc/database-issues/issues/8777
        print("$ set-sql-timeout duration=3600s")
        print("> SET statement_timeout = '3600s'")

        cls.store_explain_and_run(
            f"SELECT * FROM t1 AS a1 WHERE {' AND '.join(f'f1 IN (SELECT * FROM t1 WHERE f1 = a1.f1 AND f1 <= {i})' for i in cls.all())}"
        )
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

        cls.store_explain_and_run("SELECT LENGTH(f1) FROM t1")
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

        cls.store_explain_and_run("SELECT COUNT(*) FROM t1")
        print(f"{cls.COUNT}")


class RowsAggregate(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        cls.store_explain_and_run(
            f"SELECT COUNT(*), MIN(generate_series), MAX(generate_series), COUNT(DISTINCT generate_series) FROM generate_series(1, {cls.COUNT})"
        )
        print(f"{cls.COUNT} 1 {cls.COUNT} {cls.COUNT}")


class RowsOrderByLimit(Generator):
    COUNT = 10_000_000

    MAX_COUNT = 80_000_000  # Too long-running with 160_000_000

    @classmethod
    def body(cls) -> None:
        cls.store_explain_and_run(
            f"SELECT * FROM generate_series(1, {cls.COUNT}) ORDER BY generate_series DESC LIMIT 1"
        )
        print(f"{cls.COUNT}")


class RowsJoinOneToOne(Generator):
    COUNT = 10_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM generate_series(1, {cls.COUNT});"
        )
        cls.store_explain_and_run(
            "SELECT COUNT(*) FROM v1 AS a1, v1 AS a2 WHERE a1.generate_series = a2.generate_series"
        )
        print(f"{cls.COUNT}")


class RowsJoinOneToMany(Generator):
    COUNT = 10_000_000

    MAX_COUNT = 80_000_000  # Too long-running with 160_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM generate_series(1, {cls.COUNT});"
        )
        cls.store_explain_and_run("SELECT COUNT(*) FROM v1 AS a1, (SELECT 1) AS a2")
        print(f"{cls.COUNT}")


class RowsJoinCross(Generator):
    COUNT = 1_000_000

    MAX_COUNT = 64_000_000  # Too long-running with 128_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM generate_series(1, {cls.COUNT});"
        )
        cls.store_explain_and_run("SELECT COUNT(*) FROM v1 AS a1, v1 AS a2")
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

        cls.store_explain_and_run("SELECT COUNT(*) FROM v1")
        print("0")


class RowsJoinDifferential(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT generate_series AS f1, generate_series AS f2 FROM (SELECT * FROM generate_series(1, {cls.COUNT}));"
        )
        cls.store_explain_and_run(
            "SELECT COUNT(*) FROM v1 AS a1, v1 AS a2 WHERE a1.f1 = a2.f1"
        )
        print(f"{cls.COUNT}")


class RowsJoinOuter(Generator):
    COUNT = 1_000_000

    @classmethod
    def body(cls) -> None:
        print(
            f"> CREATE MATERIALIZED VIEW v1 AS SELECT generate_series AS f1, generate_series AS f2 FROM (SELECT * FROM generate_series(1, {cls.COUNT}));"
        )
        cls.store_explain_and_run(
            "SELECT COUNT(*) FROM v1 AS a1 LEFT JOIN v1 AS a2 USING (f1)"
        )
        print(f"{cls.COUNT}")


class PostgresSources(Generator):
    COUNT = 300  # high memory consumption, slower with source tables
    MAX_COUNT = 600  # Too long-running with count=1200

    @classmethod
    def body(cls) -> None:
        print("> SET statement_timeout='300s'")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {cls.COUNT * 10};")
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
        print("""> CREATE CONNECTION pg TO POSTGRES (
                HOST postgres,
                DATABASE postgres,
                USER postgres,
                PASSWORD SECRET pgpass
            )""")
        for i in cls.all():
            print(f"""> CREATE SOURCE p{i}
              IN CLUSTER single_replica_cluster
              FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
              """)
            print(f"""> CREATE TABLE t{i}
              FROM SOURCE p{i} (REFERENCE t{i})
              """)
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM t{i}")
            print(f"{i}")


class PostgresTables(Generator):
    @classmethod
    def body(cls) -> None:
        print("> SET statement_timeout='300s'")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
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
        print("""> CREATE CONNECTION pg TO POSTGRES (
                HOST postgres,
                DATABASE postgres,
                USER postgres,
                PASSWORD SECRET pgpass
            )""")
        print("""> CREATE SOURCE p
          IN CLUSTER single_worker_cluster
          FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
          FOR ALL TABLES
          """)
        print("> SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';")
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM t{i}")
            print(f"{i}")


class PostgresTablesOldSyntax(Generator):
    @classmethod
    def body(cls) -> None:
        print("> SET statement_timeout='300s'")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {cls.COUNT * 10};")
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
        print("""> CREATE CONNECTION pg TO POSTGRES (
                HOST postgres,
                DATABASE postgres,
                USER postgres,
                PASSWORD SECRET pgpass
            )""")
        print("""> CREATE SOURCE p
          IN CLUSTER single_replica_cluster
          FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source') FOR SCHEMAS (public)
          """)
        print("> SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';")
        for i in cls.all():
            print("$ set-sql-timeout duration=300s")
            cls.store_explain_and_run(f"SELECT * FROM t{i}")
            print(f"{i}")


class MySqlSources(Generator):
    COUNT = 150  # high memory consumption, slower with source tables

    MAX_COUNT = 400  # Too long-running with count=473

    @classmethod
    def body(cls) -> None:
        print("$ set-sql-timeout duration=300s")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {cls.COUNT * 10};")
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
        print("""> CREATE CONNECTION mysql TO MYSQL (
                HOST mysql,
                USER root,
                PASSWORD SECRET mysqlpass
            )""")
        for i in cls.all():
            print(f"""> CREATE SOURCE m{i}
              IN CLUSTER single_replica_cluster
              FROM MYSQL CONNECTION mysql
              """)
            print(f"""> CREATE TABLE t{i}
              FROM SOURCE m{i} (REFERENCE public.t{i})
              """)
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM t{i}")
            print(f"{i}")


class SqlServerSources(Generator):
    COUNT = 300

    MAX_COUNT = 400  # Too long-running with count=450

    @classmethod
    def body(cls) -> None:
        print("$ set-sql-timeout duration=300s")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_sources = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_objects_per_schema = {cls.COUNT * 10};")
        print("$ postgres-execute connection=mz_system")
        print(f"ALTER SYSTEM SET max_tables = {cls.COUNT * 10};")
        print("$ sql-server-connect name=sql-server")
        print(
            f"server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}"
        )
        print("$ sql-server-execute name=sql-server")
        print("USE test;")
        for i in cls.all():
            print(
                f"IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_t{i}') BEGIN EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 't{i}', @capture_instance = 'dbo_t{i}'; END"
            )
            print(f"DROP TABLE IF EXISTS t{i};")
            print(f"CREATE TABLE t{i} (c int);")
            print(
                f"EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't{i}', @role_name = 'SA', @supports_net_changes = 0;"
            )
            print(f"INSERT INTO t{i} VALUES ({i});")
        print(
            f"> CREATE SECRET IF NOT EXISTS sqlserverpass AS '{SqlServer.DEFAULT_SA_PASSWORD}'"
        )
        print(f"""> CREATE CONNECTION sqlserver TO SQL SERVER (
                HOST 'sql-server',
                DATABASE test,
                USER {SqlServer.DEFAULT_USER},
                PASSWORD SECRET sqlserverpass
            )""")
        for i in cls.all():
            print(f"""> CREATE SOURCE m{i}
              IN CLUSTER single_replica_cluster
              FROM SQL SERVER CONNECTION sqlserver
              """)
            print(f"""> CREATE TABLE t{i}
              FROM SOURCE m{i} (REFERENCE t{i})
              """)
        for i in cls.all():
            cls.store_explain_and_run(f"SELECT * FROM t{i}")
            print(f"{i}")


class WebhookSources(Generator):
    COUNT = 100  # TODO: Remove when database-issues#8508 is fixed

    MAX_COUNT = 400  # timeout expired with count=800

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
            cls.store_explain_and_run(f"SELECT * FROM w{i}")
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


def make_materialized(
    metadata_store: str = METADATA_STORE,
    external_persist_committer: bool = True,
    use_committer: bool = True,
    memory: str = "8G",
    extra_system_params: dict[str, str] | None = None,
    persistd_coalesce_max_batch: int = 0,
    persistd_coalesce_concurrency: int = 8,
) -> Materialized:
    """Construct the limits `Materialized` service.

    Factored out so the `committer-comparison` workflow can override the
    metadata store and committer per variant without duplicating the
    TLS/frontegg/listener configuration.

    `external_metadata_store=True` forces an external store (see the
    `EXTERNAL_METADATA_STORE_ADDRESS` caveat below); `metadata_store` selects
    the backend. `external_persist_committer` toggles the paired persistd
    companion: True runs the committer in persistd, False keeps it in-process.
    `use_committer` gates the `persist_consensus_use_committer` flag: False is
    the no-committer baseline where clusterds compare_and_append directly to
    consensus (set as a startup default because the flag needs a restart to
    take effect). `memory` is the container limit; the default 8G matches the
    `main` scenarios' calibration, but the comparison workflow raises it so the
    committer variants don't OOM at high MV counts. `extra_system_params` merges
    additional startup `ALTER SYSTEM` defaults (used by the pool-exhaustion
    workflow to shrink `persist_consensus_connection_pool_max_size`/`_max_wait`,
    which require a restart to take effect).
    """
    params: dict[str, str] = {}
    if not use_committer:
        params["persist_consensus_use_committer"] = "false"
    if extra_system_params:
        params.update(extra_system_params)
    extra_system_params = params or None
    return Materialized(
        memory=memory,
        additional_system_parameter_defaults=extra_system_params,
        # Managed clusters (e.g. ClustersWithMaterializedViews) run their
        # replicas as clusterd subprocesses inside this container, sharing its
        # CPU cgroup. `cpu` is a ceiling, not a reservation, so raising it never
        # exceeds the host's cores nor breaks smaller CI agents.
        cpu="28",
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
        # Always use an external metadata store. `EXTERNAL_METADATA_STORE_ADDRESS`
        # defaults to `False` (internal postgres) unless the env var is set, which
        # would silently bypass the external store and disable persistd; force
        # external here and let `metadata_store` pick the backend (default
        # postgres-metadata, override with EXTERNAL_METADATA_STORE=cockroach).
        external_metadata_store=True,
        metadata_store=metadata_store,
        external_persist_committer=external_persist_committer,
        persistd_coalesce_max_batch=persistd_coalesce_max_batch,
        persistd_coalesce_concurrency=persistd_coalesce_concurrency,
        listeners_config_path=f"{MZ_ROOT}/src/materialized/ci/listener_configs/no_auth_https.json",
        support_external_clusterd=True,
    )


SERVICES = [
    Kafka(),
    Postgres(
        max_wal_senders=Generator.COUNT,
        max_replication_slots=Generator.COUNT,
        volumes=["sourcedata_512Mb:/var/lib/postgresql/data"],
    ),
    MySql(),
    SqlServer(),
    SchemaRegistry(),
    # We create all sources, sinks and dataflows by default with SIZE 'scale=1,workers=1'
    # The workflow_instance_size workflow is testing multi-process clusters
    Testdrive(
        default_timeout="60s",
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
            "--frontegg-resolver-template=materialized:6875",
            "--frontegg-jwk-file=/secrets/frontegg-mock.crt",
            f"--frontegg-api-token-url={FRONTEGG_URL}/identity/resources/auth/v1/api-token",
            f"--frontegg-admin-role={ADMIN_ROLE}",
            "--https-resolver-template=materialized:6876",
            "--tls-key=/secrets/balancerd.key",
            "--tls-cert=/secrets/balancerd.crt",
            "--internal-tls",
            # Nonsensical but we don't need cancellations here
            "--cancellation-resolver-dir=/secrets",
        ],
        depends_on=["test-certs"],
        volumes=[
            "secrets:/secrets",
        ],
    ),
    *metadata_store_services(),
    make_materialized(),
    Mz(app_password=""),
]

for cluster_id in range(1, MAX_CLUSTERS + 1):
    for replica_id in range(1, MAX_REPLICAS + 1):
        for node_id in range(1, MAX_NODES + 1):
            SERVICES.append(
                Clusterd(name=f"clusterd_{cluster_id}_{replica_id}_{node_id}", cpu="2")
            )


service_names = [
    "kafka",
    "schema-registry",
    "postgres",
    "mysql",
    "sql-server",
    "materialized",
    "balancerd",
    "frontegg-mock",
    METADATA_STORE,
    "clusterd_1_1_1",
    "clusterd_1_1_2",
    "clusterd_1_2_1",
    "clusterd_1_2_2",
    "clusterd_2_1_1",
    "clusterd_2_1_2",
    "clusterd_3_1_1",
]


def setup(c: Composition, workers: int) -> None:
    c.up(*service_names)
    setup_sql_server_testing(c)

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )
    # Ensure the admin user exists
    c.sql(
        "SELECT 1;",
        port=6875,
        user=ADMIN_USER,
        password=app_password(ADMIN_USER),
    )

    c.sql(
        f"""
        DROP CLUSTER quickstart cascade;
        CREATE CLUSTER quickstart REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd_1_1_1:2100', 'clusterd_1_1_2:2100'],
                STORAGE ADDRESSES ['clusterd_1_1_1:2103', 'clusterd_1_1_2:2103'],
                COMPUTECTL ADDRESSES ['clusterd_1_1_1:2101', 'clusterd_1_1_2:2101'],
                COMPUTE ADDRESSES ['clusterd_1_1_1:2102', 'clusterd_1_1_2:2102'],
                WORKERS {workers}
            ),
            replica2 (
                STORAGECTL ADDRESSES ['clusterd_1_2_1:2100', 'clusterd_1_2_2:2100'],
                STORAGE ADDRESSES ['clusterd_1_2_1:2103', 'clusterd_1_2_2:2103'],
                COMPUTECTL ADDRESSES ['clusterd_1_2_1:2101', 'clusterd_1_2_2:2101'],
                COMPUTE ADDRESSES ['clusterd_1_2_1:2102', 'clusterd_1_2_2:2102'],
                WORKERS {workers}
            )
        );
        DROP CLUSTER IF EXISTS single_replica_cluster CASCADE;
        CREATE CLUSTER single_replica_cluster REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd_2_1_1:2100', 'clusterd_2_1_2:2100'],
                STORAGE ADDRESSES ['clusterd_2_1_1:2103', 'clusterd_2_1_2:2103'],
                COMPUTECTL ADDRESSES ['clusterd_2_1_1:2101', 'clusterd_2_1_2:2101'],
                COMPUTE ADDRESSES ['clusterd_2_1_1:2102', 'clusterd_2_1_2:2102'],
                WORKERS {workers}
            )
        );
        GRANT ALL PRIVILEGES ON CLUSTER single_replica_cluster TO materialize;
        DROP CLUSTER IF EXISTS single_worker_cluster CASCADE;
        CREATE CLUSTER single_worker_cluster REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd_3_1_1:2100'],
                STORAGE ADDRESSES ['clusterd_3_1_1:2103'],
                COMPUTECTL ADDRESSES ['clusterd_3_1_1:2101'],
                COMPUTE ADDRESSES ['clusterd_3_1_1:2102'],
                WORKERS 1
            )
        );
        GRANT ALL PRIVILEGES ON CLUSTER single_replica_cluster TO materialize;
        GRANT ALL PRIVILEGES ON CLUSTER single_replica_cluster TO "{ADMIN_USER}";
        GRANT ALL PRIVILEGES ON CLUSTER quickstart TO "{ADMIN_USER}";
    """,
        port=6877,
        user="mz_system",
    )


def upload_results_to_test_analytics(
    c: Composition,
    stats: dict[tuple[type[Generator], int], Statistics],
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(c))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []

    for (scenario, count), stat in stats.items():
        scenario_name = scenario.__name__
        scenario_version = scenario.VERSION
        result_entries.append(
            product_limits_result_storage.ProductLimitsResultEntry(
                scenario_name=scenario_name,
                scenario_version=str(scenario_version),
                count=count,
                wallclock=stat.wallclock,
                explain_wallclock=stat.explain_wallclock,
            )
        )

    test_analytics.product_limits_results.add_result(
        framework_version=PRODUCT_LIMITS_FRAMEWORK_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return
        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


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

    parser.add_argument(
        "--find-limit",
        action="store_true",
        help="Increase limit until the test fails, record higehst limit that works",
    )
    args = parser.parse_args()

    scenarios = buildkite.shard_list(
        sorted(
            (
                [globals()[args.scenario]]
                if args.scenario
                else list(all_subclasses(Generator))
            ),
            key=lambda w: w.__name__,
        ),
        lambda s: s.__name__,
    )
    print(
        f"Scenarios in shard with index {buildkite.get_parallelism_index()}: {scenarios}"
    )

    if not scenarios:
        return

    with c.override(
        Clusterd(
            name="clusterd_1_1_1",
            workers=args.workers,
            process_names=["clusterd_1_1_1", "clusterd_1_1_2"],
        ),
        Clusterd(
            name="clusterd_1_1_2",
            workers=args.workers,
            process_names=["clusterd_1_1_1", "clusterd_1_1_2"],
        ),
        Clusterd(
            name="clusterd_1_2_1",
            workers=args.workers,
            process_names=["clusterd_1_2_1", "clusterd_1_2_2"],
        ),
        Clusterd(
            name="clusterd_1_2_2",
            workers=args.workers,
            process_names=["clusterd_1_2_1", "clusterd_1_2_2"],
        ),
        Clusterd(
            name="clusterd_2_1_1",
            workers=args.workers,
            process_names=["clusterd_2_1_1", "clusterd_2_1_2"],
        ),
        Clusterd(
            name="clusterd_2_1_2",
            workers=args.workers,
            process_names=["clusterd_2_1_1", "clusterd_2_1_2"],
        ),
        Clusterd(
            name="clusterd_3_1_1",
            workers=1,
        ),
    ):
        run_scenarios(c, scenarios, args.find_limit, args.workers)


def run_scenarios(
    c: Composition, scenarios: list[type[Generator]], find_limit: bool, workers: int
):
    c.up(Service("testdrive", idle=True))

    setup(c, workers)

    failures: list[TestFailureDetails] = []
    stats: dict[tuple[type[Generator], int], Statistics] = {}

    for scenario in scenarios:
        if find_limit:
            good_count = None
            bad_count = None
            while True:
                print(
                    f"--- Running scenario {scenario.__name__} with count {scenario.COUNT}"
                )
                f = StringIO()
                with contextlib.redirect_stdout(f):
                    scenario.generate()
                    sys.stdout.flush()
                try:
                    start_time = time.time()
                    c.testdrive(f.getvalue(), quiet=True).stdout
                    wallclock = time.time() - start_time
                except Exception as e:
                    print(
                        f"Failed scenario {scenario.__name__} with count {scenario.COUNT}: {e}"
                    )
                    i = 0
                    while True:
                        try:
                            c.kill(*service_names)
                            c.rm(*service_names)
                            c.rm_volumes("mzdata")
                            break
                        except:
                            if i > 10:
                                raise
                            i += 1
                            print(
                                re.sub(
                                    r"mzp_[a-z1-9]*",
                                    "[REDACTED]",
                                    traceback.format_exc(),
                                )
                            )
                            print("Retrying in a minute...")
                            time.sleep(60)
                    setup(c, workers)

                    bad_count = scenario.COUNT
                    previous_count = scenario.COUNT
                    scenario.COUNT = (
                        scenario.COUNT // 2
                        if good_count is None
                        else (good_count + bad_count) // 2
                    )
                    if scenario.COUNT >= bad_count:
                        if not good_count:
                            failures.append(
                                TestFailureDetails(
                                    message=str(e),
                                    details=traceback.format_exc(),
                                    test_class_name_override=f"{scenario.__name__} with count {previous_count}",
                                )
                            )
                        break
                    continue
                else:
                    explain_wallclock = None
                    explain_wallclock_str = ""
                    if scenario.EXPLAIN:
                        try:
                            with c.sql_cursor(
                                sslmode="require",
                                user=ADMIN_USER,
                                password=app_password(ADMIN_USER),
                            ) as cur:
                                start_time = time.time()
                                cur.execute(scenario.EXPLAIN.encode())
                                explain_wallclock = time.time() - start_time
                                explain_wallclock_str = (
                                    f", explain took {explain_wallclock:.2f} s"
                                )
                        except Exception as e:
                            print(
                                f"Failed explain in scenario {scenario.__name__} with count {scenario.COUNT}: {e}, ignoring"
                            )
                    print(
                        f"Scenario {scenario.__name__} with count {scenario.COUNT} took {wallclock:.2f} s{explain_wallclock_str}"
                    )
                    stats[(scenario, scenario.COUNT)] = Statistics(
                        wallclock, explain_wallclock
                    )
                good_count = scenario.COUNT
                if bad_count is None:
                    scenario.COUNT *= 2
                else:
                    scenario.COUNT = (good_count + bad_count) // 2
                if scenario.MAX_COUNT is not None:
                    scenario.COUNT = min(scenario.COUNT, scenario.MAX_COUNT)
                if scenario.COUNT <= good_count:
                    break
            print(f"Final good count: {good_count}")
        else:
            print(f"--- Running scenario {scenario.__name__}")
            f = StringIO()
            with contextlib.redirect_stdout(f):
                scenario.generate()
                sys.stdout.flush()
            try:
                c.testdrive(f.getvalue())
            except Exception as e:
                failures.append(
                    TestFailureDetails(
                        message=str(e),
                        details=traceback.format_exc(),
                        test_class_name_override=scenario.__name__,
                    )
                )

    if find_limit:
        upload_results_to_test_analytics(c, stats, not failures)

    if failures:
        raise FailedTestExecutionError(errors=failures)


def _percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile of `values` (0..100). Returns 0.0 if empty."""
    if not values:
        return 0.0
    ordered = sorted(values)
    # Nearest-rank: rank = ceil(pct/100 * n), 1-indexed.
    rank = max(1, math.ceil(pct / 100.0 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def workflow_committer_comparison(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Compare freshness and hydration across metadata stores (cockroach,
    postgres, foundationdb) and committer modes (in-process vs persistd)
    under a swept clusters x MVs workload.

    For each of the four variants the workflow brings up a clean Materialize
    with the variant's metadata store and committer, then builds the workload
    incrementally over the requested sizes. At each size it waits for all MVs
    to hydrate (`mz_compute_hydration_statuses`) and samples write-frontier
    freshness (`mz_wallclock_lag_history`, recorded every 1s). Results are
    written to a CSV and printed as a summary table.

    All SQL runs as `mz_system` on the internal port, so no balancerd /
    frontegg / external clusterd are needed: the workload uses managed clusters
    whose replicas run inside the Materialized container.
    """
    parser.add_argument(
        "--sizes",
        default="25,50,100",
        help="comma-separated cluster counts to sweep (ascending)",
    )
    parser.add_argument("--mvs-per-cluster", type=int, default=16)
    parser.add_argument(
        "--stores",
        default="cockroach,postgres-metadata",
        help="comma-separated metadata stores to include (cockroach, "
        "postgres-metadata, foundationdb) — e.g. just 'cockroach' to probe "
        "crdb's limit",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=15,
        help="number of 1s lag observations to let accumulate before sampling",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=10.0,
        help="time to wait after hydration before the lag window starts",
    )
    parser.add_argument("--hydration-timeout", type=int, default=900)
    parser.add_argument(
        "--tick-interval",
        default="500ms",
        help="default_timestamp_interval; lower = higher per-MV write rate "
        "(500ms => ~2 writes/s per MV)",
    )
    parser.add_argument(
        "--memory",
        default="32G",
        help="materialized memory limit; raised above the 8G default so the "
        "in-process committer variants don't OOM at high MV counts",
    )
    parser.add_argument(
        "--output",
        default="committer_comparison.csv",
        help="CSV output path (relative to repo root)",
    )
    parser.add_argument(
        "--coalesce-max-batch",
        type=int,
        default=0,
        help="when > 0, add a 'persistd coalesce' variant per store where the "
        "committer batches concurrent CaS requests into single backing "
        "statements of at most this many elements",
    )
    parser.add_argument(
        "--coalesce-concurrency",
        type=int,
        default=8,
        help="maximum number of coalesced batches in flight against the "
        "backing store at once (only meaningful with --coalesce-max-batch)",
    )
    parser.add_argument(
        "--skip-baselines",
        action="store_true",
        help="run only the coalesce variants (requires --coalesce-max-batch); "
        "useful when baseline CSVs already exist",
    )
    args = parser.parse_args()

    sizes = sorted(int(s) for s in args.sizes.split(","))
    mvs = args.mvs_per_cluster
    max_objects = max(sizes) * mvs

    # (label, metadata_store service name, committer, coalesce_max_batch).
    # committer=False is the baseline: no committer, clusterds
    # compare_and_append directly to consensus
    # (persist_consensus_use_committer off). committer=True runs the
    # committer in a persistd companion; coalesce_max_batch > 0 additionally
    # batches concurrent CaS requests inside that persistd.
    variants = [
        ("crdb / no committer", "cockroach", False, 0),
        ("crdb / persistd", "cockroach", True, 0),
        ("psql / no committer", "postgres-metadata", False, 0),
        ("psql / persistd", "postgres-metadata", True, 0),
        ("fdb / no committer", "foundationdb", False, 0),
        ("fdb / persistd", "foundationdb", True, 0),
    ]
    if args.skip_baselines:
        assert (
            args.coalesce_max_batch > 0
        ), "--skip-baselines needs --coalesce-max-batch"
        variants = []
    if args.coalesce_max_batch > 0:
        variants += [
            ("crdb / persistd coal", "cockroach", True, args.coalesce_max_batch),
            (
                "psql / persistd coal",
                "postgres-metadata",
                True,
                args.coalesce_max_batch,
            ),
            ("fdb / persistd coal", "foundationdb", True, args.coalesce_max_batch),
        ]
    wanted_stores = {s.strip() for s in args.stores.split(",")}
    variants = [v for v in variants if v[1] in wanted_stores]

    fieldnames = [
        "variant",
        "metadata_store",
        "committer",
        "coalesce_max_batch",
        "n_clusters",
        "mvs_per_cluster",
        "n_objects",
        "hydrate_ok",
        "batch_hydrate_wall_s",
        "hydration_ms_max",
        "hydration_ms_mean",
        "hydration_ms_p95",
        "lag_ms_max",
        "lag_ms_mean",
        "lag_ms_p95",
    ]
    results: list[dict] = []
    # Write rows incrementally so a mid-run crash (e.g. envd dying in one
    # variant) doesn't lose the completed variants' data.
    csv_file = open(args.output, "w", newline="")
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_file.flush()

    def run_all(stmts: list[str]) -> None:
        """Run a batch of statements on one short-lived mz_system connection.

        Used for the DDL burst (contiguous, no sleeps) so we don't reconnect
        per statement.
        """
        with c.sql_cursor(port=6877, user="mz_system") as cur:
            for s in stmts:
                cur.execute(s.encode())

    def fetch(sql: str) -> list:
        """Run a query on a fresh mz_system connection and return all rows.

        Fresh per call so a long idle (the freshness settle window) can't leave
        us holding a connection the server has already reaped.
        """
        with c.sql_cursor(port=6877, user="mz_system") as cur:
            cur.execute(sql.encode())
            return cur.fetchall()

    for label, store, committer, coalesce in variants:
        print(
            f"--- Variant: {label} (store={store}, committer={committer}, "
            f"coalesce={coalesce})",
            flush=True,
        )
        # Clean slate: each variant gets a fresh metadata store and mzdata so
        # state from a prior variant can't leak in.
        c.down(destroy_volumes=True)

        # Raise Postgres SSI predicate-lock limits well above defaults. The
        # default RWConflictPool sizing exhausts under the concurrent cold-start
        # CaS burst (~100 clusterds) and triggers a retry storm ("not enough
        # elements in RWConflictPool"). These values test whether the psql
        # hydration collapse is just default-limit starvation rather than a
        # fundamental direct-CaS limit.
        store_services: list[Service]
        if store == "cockroach":
            store_services = [Cockroach(in_memory=True)]
        elif store == "foundationdb":
            # Single-node fdb cluster: a `foundationdb-0` server node plus the
            # `foundationdb` init/healthcheck service materialized depends on.
            store_services = list(foundationdb_services(num_nodes=1))
        else:
            store_services = [
                PostgresMetadata(
                    extra_command=[
                        "-c",
                        "max_pred_locks_per_transaction=1024",
                        "-c",
                        "max_pred_locks_per_relation=10000",
                        "-c",
                        "max_pred_locks_per_page=512",
                    ]
                )
            ]
        with c.override(
            *store_services,
            make_materialized(
                metadata_store=store,
                external_persist_committer=committer,
                use_committer=committer,
                memory=args.memory,
                persistd_coalesce_max_batch=coalesce,
                persistd_coalesce_concurrency=args.coalesce_concurrency,
            ),
            fail_on_new_service=False,
        ):
            # The metadata store and test-certs start automatically via
            # materialized's depends_on.
            c.up("materialized")

            # Record freshness every second instead of the 60s default so the
            # lag window has enough observations to sample. Shared 1Hz-ticking
            # input `t` is kept tiny so per-MV arrangement memory is negligible
            # and the load is dominated by per-tick persist writes, not data.
            run_all(
                [
                    "ALTER SYSTEM SET wallclock_lag_recording_interval = '1s'",
                    # Use persist's Postgres-tuned CaS query (SELECT ... FOR
                    # UPDATE row lock) instead of the CRDB-flavored query, which
                    # relies on SSI and triggers "pivot" serialization-failure
                    # (40001) retry storms on Postgres under the cold-start
                    # burst. No-op on CockroachDB (gated on PostgresMode).
                    "ALTER SYSTEM SET persist_use_postgres_tuned_queries = true",
                    # Faster table tick => higher per-MV write rate, pushing
                    # steady-state committer load past what direct CaS sustains.
                    f"ALTER SYSTEM SET default_timestamp_interval = '{args.tick_interval}'",
                    f"ALTER SYSTEM SET max_clusters = {max(sizes) * 10}",
                    f"ALTER SYSTEM SET max_materialized_views = {max_objects * 10}",
                    f"ALTER SYSTEM SET max_objects_per_schema = {max_objects * 10}",
                    "CREATE TABLE t (f1 INTEGER)",
                    "INSERT INTO t VALUES (1)",
                ]
            )

            created = 0
            for n in sizes:
                # Build incrementally up to N clusters so each size reuses the
                # catalog from the previous one. The whole burst runs on one
                # connection (no sleeps) for speed; the probes below use fresh
                # connections so a long idle can't leave us on a reaped one.
                t_create = time.time()
                burst: list[str] = []
                for i in range(created + 1, n + 1):
                    burst.append(f"CREATE CLUSTER c{i} SIZE = 'scale=1,workers=1'")
                    for j in range(1, mvs + 1):
                        burst.append(
                            f"CREATE MATERIALIZED VIEW mv{i}_{j} IN CLUSTER c{i} "
                            f"AS SELECT f1 + {j} AS f1 FROM t"
                        )
                run_all(burst)
                created = n
                n_objects = n * mvs

                # Phase 1: wait for all MVs to hydrate.
                deadline = time.time() + args.hydration_timeout
                hydrated = 0
                while time.time() < deadline:
                    rows = fetch("""
                        SELECT count(*) FILTER (WHERE hs.hydrated)
                        FROM mz_internal.mz_compute_hydration_statuses hs
                        JOIN mz_materialized_views mv ON mv.id = hs.object_id
                        WHERE mv.name LIKE 'mv%'
                        """)
                    hydrated = int(rows[0][0])
                    if hydrated >= n_objects:
                        break
                    time.sleep(1.0)
                hydrate_wall = time.time() - t_create
                hydrate_ok = hydrated >= n_objects

                # Per-object hydration time from the catalog.
                hyd = [float(r[0]) for r in fetch("""
                        SELECT EXTRACT(EPOCH FROM hs.hydration_time) * 1000
                        FROM mz_internal.mz_compute_hydration_statuses hs
                        JOIN mz_materialized_views mv ON mv.id = hs.object_id
                        WHERE mv.name LIKE 'mv%' AND hs.hydration_time IS NOT NULL
                        """)]

                # Phase 2: let steady-state freshness observations accumulate,
                # then aggregate per-object worst lag over the window.
                window = args.samples
                time.sleep(args.settle_seconds + window)
                lag = [float(r[0]) for r in fetch(f"""
                        SELECT EXTRACT(EPOCH FROM max(h.lag)) * 1000
                        FROM mz_internal.mz_wallclock_lag_history h
                        JOIN mz_materialized_views mv ON mv.id = h.object_id
                        WHERE mv.name LIKE 'mv%'
                          AND h.lag IS NOT NULL
                          AND h.occurred_at > now() - INTERVAL '{window} seconds'
                        GROUP BY h.object_id
                        """)]

                row = {
                    "variant": label,
                    "metadata_store": store,
                    "committer": committer,
                    "coalesce_max_batch": coalesce,
                    "n_clusters": n,
                    "mvs_per_cluster": mvs,
                    "n_objects": n_objects,
                    "hydrate_ok": hydrate_ok,
                    "batch_hydrate_wall_s": round(hydrate_wall, 2),
                    "hydration_ms_max": round(max(hyd), 1) if hyd else 0.0,
                    "hydration_ms_mean": (
                        round(sum(hyd) / len(hyd), 1) if hyd else 0.0
                    ),
                    "hydration_ms_p95": round(_percentile(hyd, 95), 1),
                    "lag_ms_max": round(max(lag), 1) if lag else 0.0,
                    "lag_ms_mean": round(sum(lag) / len(lag), 1) if lag else 0.0,
                    "lag_ms_p95": round(_percentile(lag, 95), 1),
                }
                results.append(row)
                csv_writer.writerow(row)
                csv_file.flush()
                print(
                    f"    n={n} objects={n_objects} hydrate_ok={hydrate_ok} "
                    f"wall={row['batch_hydrate_wall_s']}s "
                    f"hyd_max={row['hydration_ms_max']}ms "
                    f"lag_max={row['lag_ms_max']}ms lag_p95={row['lag_ms_p95']}ms",
                    flush=True,
                )

    csv_file.close()
    print(f"\nWrote {len(results)} rows to {args.output}\n")

    # Summary table.
    header = (
        f"{'variant':20} {'n_obj':>6} {'hyd_ok':>6} {'wall_s':>7} "
        f"{'hyd_max':>8} {'hyd_p95':>8} {'lag_max':>8} {'lag_p95':>8}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['variant']:20} {r['n_objects']:>6} {str(r['hydrate_ok']):>6} "
            f"{r['batch_hydrate_wall_s']:>7} {r['hydration_ms_max']:>8} "
            f"{r['hydration_ms_p95']:>8} {r['lag_ms_max']:>8} {r['lag_ms_p95']:>8}"
        )


# Persist log signatures of the consensus connection-pool exhaustion incident
# (Notion prod, 2026-05). The causal chain: consensus ops block on a saturated
# connection pool ("waiting for a slot"), so reader-lease heartbeats can't
# complete ("heartbeat call took" / "between heartbeats" growing into minutes),
# leases expire ("expired due to inactivity" / "Force expiring"), processes
# halt ("lost lease"), and dataflows fall over ("reconciliation failed").
POOL_EXHAUSTION_SIGNATURES = {
    "slot_wait": "waiting for a slot to become available",
    "heartbeat_took": "heartbeat call took",
    "between_heartbeats": "between heartbeats",
    "lease_expired": "expired due to inactivity",
    "force_expiring": "Force expiring",
    "lost_lease": "lost lease",
    "reconciliation_failed": "reconciliation failed",
    "halting": "halting process",
}


def _count_signatures(logs: str) -> dict[str, int]:
    """Count occurrences of each pool-exhaustion log signature."""
    return {k: logs.count(sub) for k, sub in POOL_EXHAUSTION_SIGNATURES.items()}


def workflow_pool_exhaustion(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Reproduce the persist consensus connection-pool exhaustion incident.

    This is NOT the hydration/throughput benchmark (see
    `committer-comparison`). It reproduces the production failure where each
    clusterd's consensus connection pool to Postgres saturates: consensus ops
    (`fetch_state::scan`, `downgrade_since`) block waiting for a free pool slot
    and time out, which stalls reader-lease heartbeats until leases expire and
    dataflows halt.

    The key lever is `persist_consensus_connection_pool_max_size` (default 50
    per process): shrinking it lets a modest clusters x MVs workload exhaust the
    pool. The default variant matches production (committer off, postgres
    backend, clusterds talk to Postgres directly). Flip `--committer on` to test
    the hypothesis that routing consensus through the in-envd committer removes
    the per-clusterd pool pressure.

    Instead of measuring hydration time, the workflow ramps the workload, holds
    it under load, and scrapes the materialized container logs (where the
    in-container clusterds log) for the incident's signatures, reporting per
    pool size and cluster count.
    """
    parser.add_argument(
        "--sizes",
        default="25,50,100",
        help="comma-separated cluster counts to sweep (ascending)",
    )
    parser.add_argument("--mvs-per-cluster", type=int, default=16)
    parser.add_argument(
        "--pool-sizes",
        default="4,16,50",
        help="comma-separated persist_consensus_connection_pool_max_size values "
        "to sweep; 50 is the production default, lower forces exhaustion",
    )
    parser.add_argument(
        "--pool-max-wait",
        default="60s",
        help="persist_consensus_connection_pool_max_wait; the slot-wait timeout "
        "fires after this (production default 60s)",
    )
    parser.add_argument(
        "--committer",
        choices=["on", "off"],
        default="off",
        help="off (default) matches production: clusterds compare_and_append "
        "directly to Postgres, each with its own connection pool. on routes "
        "consensus through the in-envd committer (the fix hypothesis)",
    )
    parser.add_argument(
        "--load-seconds",
        type=float,
        default=120.0,
        help="time to hold the workload under load at each size so consensus "
        "pool pressure can build and leases can expire",
    )
    parser.add_argument(
        "--postgres-cpu",
        default="1",
        help="CPU limit for the postgres-metadata container; restricting it "
        "slows each consensus op so connections stay checked out longer, "
        "draining the pool at lower scale (empty string = unrestricted)",
    )
    parser.add_argument(
        "--tick-interval",
        default="100ms",
        help="default_timestamp_interval; lower = higher per-MV consensus rate, "
        "more pressure on the connection pool",
    )
    parser.add_argument("--hydration-timeout", type=int, default=300)
    parser.add_argument("--memory", default="32G")
    parser.add_argument(
        "--output",
        default="pool_exhaustion.csv",
        help="CSV output path (relative to repo root)",
    )
    args = parser.parse_args()

    sizes = sorted(int(s) for s in args.sizes.split(","))
    pool_sizes = [int(p) for p in args.pool_sizes.split(",")]
    mvs = args.mvs_per_cluster
    max_objects = max(sizes) * mvs
    committer = args.committer == "on"

    fieldnames = [
        "pool_max_size",
        "committer",
        "n_clusters",
        "n_objects",
        "hydrate_ok",
        "overloaded",
        "load_wall_s",
        *POOL_EXHAUSTION_SIGNATURES.keys(),
    ]
    results: list[dict] = []

    def run_all(stmts: list[str]) -> None:
        with c.sql_cursor(port=6877, user="mz_system") as cur:
            for s in stmts:
                cur.execute(s.encode())

    def fetch(sql: str) -> list:
        with c.sql_cursor(port=6877, user="mz_system") as cur:
            cur.execute(sql.encode())
            return cur.fetchall()

    for pool_size in pool_sizes:
        print(
            f"--- pool_max_size={pool_size} committer={args.committer} "
            f"(max_wait={args.pool_max_wait}, tick={args.tick_interval})"
        )
        # Clean slate per pool size: the pool size and max_wait need a restart
        # to take effect, so they are startup defaults and the env is rebuilt.
        c.down(destroy_volumes=True)

        store_service = PostgresMetadata(
            extra_command=[
                "-c",
                "max_pred_locks_per_transaction=1024",
                "-c",
                "max_pred_locks_per_relation=10000",
                "-c",
                "max_pred_locks_per_page=512",
            ],
            cpu=args.postgres_cpu or None,
        )
        with c.override(
            store_service,
            make_materialized(
                metadata_store="postgres-metadata",
                external_persist_committer=committer,
                use_committer=committer,
                memory=args.memory,
                extra_system_params={
                    "persist_consensus_connection_pool_max_size": str(pool_size),
                    "persist_consensus_connection_pool_max_wait": args.pool_max_wait,
                },
            ),
            fail_on_new_service=False,
        ):
            c.up("materialized")
            run_all(
                [
                    f"ALTER SYSTEM SET default_timestamp_interval = '{args.tick_interval}'",
                    f"ALTER SYSTEM SET max_clusters = {max(sizes) * 10}",
                    f"ALTER SYSTEM SET max_materialized_views = {max_objects * 10}",
                    f"ALTER SYSTEM SET max_objects_per_schema = {max_objects * 10}",
                    "CREATE TABLE t (f1 INTEGER)",
                    "INSERT INTO t VALUES (1)",
                ]
            )

            created = 0
            prev = {k: 0 for k in POOL_EXHAUSTION_SIGNATURES}

            def record(
                n: int, n_objects: int, hydrate_ok: bool, overloaded: bool, t0: float
            ) -> dict[str, int]:
                """Scrape the materialized container logs (the in-container
                clusterds log here) and append a per-size row of signature
                deltas. Returns the new delta for printing."""
                logs = c.invoke("logs", "materialized", capture=True).stdout
                counts = _count_signatures(logs)
                delta = {k: counts[k] - prev[k] for k in counts}
                prev.update(counts)
                results.append(
                    {
                        "pool_max_size": pool_size,
                        "committer": committer,
                        "n_clusters": n,
                        "n_objects": n_objects,
                        "hydrate_ok": hydrate_ok,
                        "overloaded": overloaded,
                        "load_wall_s": round(time.time() - t0, 1),
                        **delta,
                    }
                )
                print(
                    f"    n={n} objects={n_objects} hydrate_ok={hydrate_ok} "
                    f"overloaded={overloaded} slot_wait={delta['slot_wait']} "
                    f"lease_expired={delta['lease_expired']} "
                    f"force_expiring={delta['force_expiring']} "
                    f"reconciliation_failed={delta['reconciliation_failed']} "
                    f"halting={delta['halting']}"
                )
                return delta

            for n in sizes:
                t_load = time.time()
                n_objects = n * mvs
                burst: list[str] = []
                for i in range(created + 1, n + 1):
                    burst.append(f"CREATE CLUSTER c{i} SIZE = 'scale=1,workers=1'")
                    for j in range(1, mvs + 1):
                        burst.append(
                            f"CREATE MATERIALIZED VIEW mv{i}_{j} IN CLUSTER c{i} "
                            f"AS SELECT f1 + {j} AS f1 FROM t"
                        )
                # A severe enough overload fences/halts envd, so the DDL burst or
                # the hydration/load probes lose their connection. That crash IS
                # the terminal repro state, so on a connection error we scrape the
                # logs one last time, record the overload, and stop this variant
                # rather than letting the exception abort the whole workflow.
                try:
                    run_all(burst)
                    created = n

                    # Wait for hydration (best effort) so the steady-state
                    # consensus traffic — downgrade_since from active readers plus
                    # state scans — is what drains the pool, then hold under load.
                    deadline = time.time() + args.hydration_timeout
                    hydrated = 0
                    while time.time() < deadline:
                        rows = fetch("""
                            SELECT count(*) FILTER (WHERE hs.hydrated)
                            FROM mz_internal.mz_compute_hydration_statuses hs
                            JOIN mz_materialized_views mv ON mv.id = hs.object_id
                            WHERE mv.name LIKE 'mv%'
                            """)
                        hydrated = int(rows[0][0])
                        if hydrated >= n_objects:
                            break
                        time.sleep(1.0)
                    hydrate_ok = hydrated >= n_objects

                    # Hold under load so pool pressure builds, leases can expire.
                    time.sleep(args.load_seconds)
                    record(n, n_objects, hydrate_ok, False, t_load)
                except Exception as e:
                    print(
                        f"    n={n} objects={n_objects} envd connection lost "
                        f"(overload): {type(e).__name__}: {e}"
                    )
                    try:
                        record(n, n_objects, False, True, t_load)
                    except Exception as scrape_err:
                        print(f"    log scrape after overload failed: {scrape_err}")
                    break

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"\nWrote {len(results)} rows to {args.output}\n")

    header = (
        f"{'pool':>5} {'cmtr':>5} {'n_obj':>6} {'hyd':>4} {'ovld':>5} "
        f"{'slot_wait':>10} {'lease_exp':>10} {'force_exp':>10} "
        f"{'recon_fail':>11} {'halt':>5}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['pool_max_size']:>5} {str(r['committer']):>5} "
            f"{r['n_objects']:>6} {str(r['hydrate_ok'])[0]:>4} "
            f"{str(r['overloaded'])[0]:>5} {r['slot_wait']:>10} "
            f"{r['lease_expired']:>10} {r['force_expiring']:>10} "
            f"{r['reconciliation_failed']:>11} {r['halting']:>5}"
        )


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

    c.up(
        "kafka",
        "schema-registry",
        "materialized",
        "balancerd",
        "frontegg-mock",
        Service("testdrive", idle=True),
    )

    # Construct the requied Clusterd instances and peer them into clusters
    node_names = []
    node_overrides = []
    for cluster_id in range(1, args.clusters + 1):
        for replica_id in range(1, args.replicas + 1):
            names = [
                f"clusterd_{cluster_id}_{replica_id}_{i}"
                for i in range(1, args.nodes + 1)
            ]
            for node_name in names:
                node_names.append(node_name)
                node_overrides.append(
                    Clusterd(
                        name=node_name,
                        workers=args.workers,
                        process_names=names,
                    )
                )

    with c.override(
        Testdrive(
            seed=1,
            materialize_url=f"postgres://{quote(ADMIN_USER)}:{app_password(ADMIN_USER)}@balancerd:6875?sslmode=require",
            materialize_use_https=True,
            no_reset=True,
        ),
        *node_overrides,
    ):
        c.up(*node_names)

        # Increase resource limits
        c.testdrive(dedent(f"""
                $ postgres-execute connection=postgres://mz_system@materialized:6877/materialize
                ALTER SYSTEM SET max_clusters = {args.clusters * 10}
                ALTER SYSTEM SET max_replicas_per_cluster = {args.replicas * 10}

                CREATE CLUSTER single_replica_cluster SIZE = 'scale=1,workers=4';
                GRANT ALL ON CLUSTER single_replica_cluster TO materialize;
                GRANT ALL ON CLUSTER single_replica_cluster TO "{ADMIN_USER}";
                GRANT ALL PRIVILEGES ON SCHEMA public TO "{ADMIN_USER}";
                """))
        # Create some input data
        c.testdrive(dedent("""
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
                """))

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
                "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
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
            c.sql(
                f'GRANT ALL PRIVILEGES ON CLUSTER cluster_u{cluster_id} TO "{ADMIN_USER}"',
                port=6877,
                user="mz_system",
            )

        # Construct some dataflows in each cluster
        for cluster_id in range(1, args.clusters + 1):
            cluster_name = f"cluster_u{cluster_id}"

            c.testdrive(dedent(f"""
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

                     > CREATE TABLE s_{cluster_name}_tbl FROM SOURCE s_{cluster_name} (REFERENCE "testdrive-instance-size-${{testdrive.seed}}")
                       FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                       ENVELOPE NONE
                 """))

        # Validate that each individual cluster is operating properly
        for cluster_id in range(1, args.clusters + 1):
            cluster_name = f"cluster_u{cluster_id}"

            c.testdrive(dedent(f"""
                     > SET cluster={cluster_name}

                     > SELECT c1 FROM v_{cluster_name};
                     10000

                     > SELECT COUNT(*) FROM s_{cluster_name}_tbl
                     10000
                 """))
