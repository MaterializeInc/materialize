# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Tries to find race conditions in Materialize, mostly DDLs. Can find panics and wrong results.
"""

import datetime
import random
import time
from textwrap import dedent
from uuid import uuid4

import requests

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.helpers.iceberg import setup_polaris_for_iceberg
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.polaris import Polaris, PolarisBootstrap
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import PropagatingThread, all_subclasses

SERVICES = [
    Postgres(max_replication_slots=100000),
    MySql(),
    SqlServer(),
    PolarisBootstrap(),
    Polaris(),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://127.0.0.1:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Toxiproxy(),
    Testdrive(no_reset=True, consistent_seed=True, default_timeout="600s"),
    Mc(),
    Materialized(
        default_replication_factor=2,
        additional_system_parameter_defaults={
            "memory_limiter_interval": "0",
            "enable_replica_targeted_materialized_views": "true",
        },
    ),
]

SERVICE_NAMES = [
    "postgres",
    "mysql",
    "sql-server",
    "zookeeper",
    "kafka",
    "schema-registry",
    # Still required for backups/s3 testing even when we use Azurite as blob store
    "minio",
    "materialized",
]


class Object:
    name: str
    references: "Object | None"
    can_refer: bool = True
    enabled: bool = True

    def __init__(self, name: str, references: "Object | None", rng: random.Random):
        self.name = name
        self.references = references

    def prepare(self) -> str:
        return ""

    def create(self) -> str:
        raise NotImplementedError

    def destroy(self) -> str:
        raise NotImplementedError

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class UpsertSource(Object):
    def prepare(self) -> str:
        return dedent(f"""
            $ set keyschema={{
                "type": "record",
                "name": "Key",
                "fields": [
                    {{"name": "b", "type": "string"}},
                    {{"name": "a", "type": "long"}}
                ]
              }}

            $ set schema={{
                "type" : "record",
                "name" : "envelope",
                "fields" : [
                  {{
                    "name": "before",
                    "type": [
                      {{
                        "name": "row",
                        "type": "record",
                        "fields": [
                          {{
                              "name": "a",
                              "type": "long"
                          }},
                          {{
                            "name": "data",
                            "type": "string"
                          }},
                          {{
                              "name": "b",
                              "type": "string"
                          }}]
                       }},
                       "null"
                     ]
                  }},
                  {{
                    "name": "after",
                    "type": ["row", "null"]
                  }}
                ]
              }}

            $ kafka-create-topic topic={self.name} partitions=1

            $ kafka-ingest format=avro topic={self.name} key-format=avro key-schema=${{keyschema}} schema=${{schema}} repeat=1000000
            {{"b": "bdata", "a": ${{kafka-ingest.iteration}}}} {{"before": {{"row": {{"a": ${{kafka-ingest.iteration}}, "data": "fish", "b": "bdata"}}}}, "after": {{"row": {{"a": ${{kafka-ingest.iteration}}, "data": "fish2", "b": "bdata"}}}}}}

            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
              URL 'http://toxiproxy:8081')

            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER 'toxiproxy:9092', SECURITY PROTOCOL PLAINTEXT)""")

    def create(self) -> str:
        return dedent(f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-{self.name}-${{testdrive.seed}}')
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE "testdrive-{self.name}-${{testdrive.seed}}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM
            > COMMIT""")

    def destroy(self) -> str:
        return dedent(f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source CASCADE""")

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            # Bounce cluster to force source restart (database-issues#8698)
            lambda: dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 1);
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 2);
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Table(Object):
    def create(self) -> str:
        return f"> CREATE TABLE {self.name} (a TEXT, b TEXT)"

    def destroy(self) -> str:
        return f"> DROP TABLE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: f"> INSERT INTO {self.name} VALUES ('foo', 'bar'), ('baz', 'qux')",
            lambda: f"> UPDATE {self.name} SET b = b || 'bar'",
            lambda: f"> DELETE FROM {self.name} WHERE length(b) > 12",
            lambda: dedent(f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
"""),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class PostgresSource(Object):
    def prepare(self) -> str:
        return dedent(f"""
            $ postgres-execute connection=postgres://postgres:postgres@postgres
            DROP USER IF EXISTS {self.name}_role;
            CREATE USER {self.name}_role WITH SUPERUSER PASSWORD 'postgres';
            ALTER USER {self.name}_role WITH replication;
            DROP PUBLICATION IF EXISTS {self.name}_source;
            DROP TABLE IF EXISTS {self.name}_table;
            CREATE TABLE {self.name}_table (a TEXT, b TEXT);
            ALTER TABLE {self.name}_table REPLICA IDENTITY FULL;
            CREATE PUBLICATION {self.name}_source FOR TABLE {self.name}_table;
            INSERT INTO {self.name}_table VALUES ('foo', 'bar');

            > DROP SECRET IF EXISTS {self.name}_pass CASCADE
            > CREATE SECRET {self.name}_pass AS 'postgres'
            > DROP CONNECTION IF EXISTS {self.name}_conn CASCADE
            > CREATE CONNECTION {self.name}_conn FOR POSTGRES
              HOST 'postgres',
              DATABASE postgres,
              USER {self.name}_role,
              PASSWORD SECRET {self.name}_pass""")

    def create(self) -> str:
        return dedent(f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM POSTGRES CONNECTION {self.name}_conn
              (PUBLICATION '{self.name}_source')
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE {self.name}_table)
            > COMMIT""")

    def destroy(self) -> str:
        return dedent(f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source""")

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO {self.name}_table VALUES ('foo', 'bar');"""),
            lambda: dedent(f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                UPDATE {self.name}_table SET b = b || 'bar' WHERE true;"""),
            lambda: dedent(f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DELETE FROM {self.name}_table WHERE LENGTH(b) > 12;"""),
            lambda: dedent(f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
"""),
            # Bounce cluster to force source restart (database-issues#8698)
            lambda: dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 1);
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 2);
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class MySqlSource(Object):
    def prepare(self) -> str:
        return dedent(f"""
            $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
            $ mysql-execute name=mysql
            # create the database if it does not exist yet but do not drop it
            CREATE DATABASE IF NOT EXISTS public;
            USE public;

            CREATE USER IF NOT EXISTS {self.name}_role IDENTIFIED BY 'mysql';
            GRANT REPLICATION SLAVE ON *.* TO {self.name}_role;
            GRANT ALL ON public.* TO {self.name}_role;

            DROP TABLE IF EXISTS {self.name}_table;
            CREATE TABLE {self.name}_table (a TEXT, b TEXT);
            INSERT INTO {self.name}_table VALUES ('foo', 'bar');

            > DROP SECRET IF EXISTS {self.name}_pass CASCADE
            > CREATE SECRET {self.name}_pass AS 'mysql'
            > DROP CONNECTION IF EXISTS {self.name}_conn CASCADE
            > CREATE CONNECTION {self.name}_conn TO MYSQL (
                HOST 'mysql',
                USER {self.name}_role,
                PASSWORD SECRET {self.name}_pass
              )""")

    def create(self) -> str:
        return dedent(f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM MYSQL CONNECTION {self.name}_conn
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE public.{self.name}_table)
            > COMMIT
            """)

    def destroy(self) -> str:
        return dedent(f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source""")

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                $ mysql-execute name=mysql
                USE public;
                INSERT INTO {self.name}_table VALUES ('foo', 'bar');"""),
            lambda: dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                $ mysql-execute name=mysql
                USE public;
                UPDATE {self.name}_table SET b = CONCAT(b, 'bar') WHERE true;"""),
            lambda: dedent(f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                $ mysql-execute name=mysql
                USE public;
                DELETE FROM {self.name}_table WHERE LENGTH(b) > 12;"""),
            lambda: dedent(f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
"""),
            # Bounce cluster to force source restart (database-issues#8698)
            lambda: dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 1);
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 2);
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class SqlServerSource(Object):
    def prepare(self) -> str:
        return dedent(f"""
            $ sql-server-connect name=sql-server
            server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

            $ sql-server-execute name=sql-server
            USE test;
            IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_{self.name}_table') BEGIN EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = '{self.name}_table', @capture_instance = 'dbo_{self.name}_table'; END
            DROP TABLE IF EXISTS {self.name}_table;
            CREATE TABLE {self.name}_table (a VARCHAR(1024), b VARCHAR(1024));
            EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '{self.name}_table', @role_name = 'SA', @supports_net_changes = 0;

            > DROP SECRET IF EXISTS {self.name}_pass CASCADE
            > CREATE SECRET {self.name}_pass AS '{SqlServer.DEFAULT_SA_PASSWORD}'
            > DROP CONNECTION IF EXISTS {self.name}_conn CASCADE
            > CREATE CONNECTION {self.name}_conn TO SQL SERVER (
                HOST 'sql-server',
                DATABASE test,
                USER {SqlServer.DEFAULT_USER},
                PASSWORD SECRET {self.name}_pass
              )""")

    def create(self) -> str:
        return dedent(f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM SQL SERVER CONNECTION {self.name}_conn
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE {self.name}_table)
            > COMMIT
            """)

    def destroy(self) -> str:
        return dedent(f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source""")

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO {self.name}_table VALUES ('foo', 'bar');"""),
            lambda: dedent(f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                UPDATE {self.name}_table SET b = CONCAT(b, 'bar') WHERE 1 = 1;"""),
            lambda: dedent(f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                DELETE FROM {self.name}_table WHERE LEN(b) > 12;"""),
            lambda: dedent(f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
"""),
            # Bounce cluster to force source restart (database-issues#8698)
            lambda: dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 1);
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 2);
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class LoadGeneratorSource(Object):
    def __init__(self, name: str, references: "Object | None", rng: random.Random):
        super().__init__(name, references, rng)
        self.tick_interval = rng.choice(["1ms", "10ms", "100ms", "1s", "10s"])

    def create(self) -> str:
        return f"> CREATE SOURCE {self.name} IN CLUSTER quickstart FROM LOAD GENERATOR COUNTER (TICK INTERVAL '{self.tick_interval}')"

    def destroy(self) -> str:
        return f"> DROP SOURCE {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP SOURCE IF EXISTS {self.name}_tmp_source
                > ALTER SOURCE {self.name} RENAME TO {self.name}_tmp_source
                > ALTER SOURCE {self.name}_tmp_source RENAME TO {self.name}
"""),
            # Bounce cluster to force source restart (database-issues#8698)
            lambda: dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 1);
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 2);
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class AuctionLoadGeneratorSource(Object):
    AUCTION_TABLES = ["accounts", "auctions", "bids", "organizations", "users"]

    def __init__(self, name: str, references: "Object | None", rng: random.Random):
        super().__init__(name, references, rng)
        self.tick_interval = rng.choice(["100ms", "1s", "10s"])
        self.ref_table = rng.choice(self.AUCTION_TABLES)

    def create(self) -> str:
        return dedent(f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM LOAD GENERATOR AUCTION (TICK INTERVAL '{self.tick_interval}')
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE {self.ref_table})
            > COMMIT""")

    def destroy(self) -> str:
        return dedent(f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source CASCADE""")

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
"""),
            # Bounce cluster to force source restart (database-issues#8698)
            lambda: dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 1);
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 2);
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class TpchLoadGeneratorSource(Object):
    TPCH_TABLES = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]

    def __init__(self, name: str, references: "Object | None", rng: random.Random):
        super().__init__(name, references, rng)
        self.scale_factor = rng.choice([0.01, 0.1])
        self.ref_table = rng.choice(self.TPCH_TABLES)

    def create(self) -> str:
        return dedent(f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM LOAD GENERATOR TPCH (SCALE FACTOR {self.scale_factor})
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE {self.ref_table})
            > COMMIT""")

    def destroy(self) -> str:
        return dedent(f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source CASCADE""")

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
"""),
            # Bounce cluster to force source restart (database-issues#8698)
            lambda: dedent("""
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 1);
                ALTER CLUSTER quickstart SET (REPLICATION FACTOR 2);
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class WebhookSource(Object):
    def __init__(self, name: str, references: "Object | None", rng: random.Random):
        super().__init__(name, references, rng)
        self.body_format = rng.choice(["TEXT", "JSON", "JSON ARRAY", "BYTES"])

    def create(self) -> str:
        return dedent(f"""
            > DROP CLUSTER IF EXISTS {self.name}_cluster
            > CREATE CLUSTER {self.name}_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 1
            > CREATE SOURCE {self.name} IN CLUSTER {self.name}_cluster FROM WEBHOOK BODY FORMAT {self.body_format}
            """)

    def destroy(self) -> str:
        return dedent(f"""
            > DROP CLUSTER {self.name}_cluster CASCADE
            """)

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP SOURCE IF EXISTS {self.name}_tmp_source
                > ALTER SOURCE {self.name} RENAME TO {self.name}_tmp_source
                > ALTER SOURCE {self.name}_tmp_source RENAME TO {self.name}
"""),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class KafkaSink(Object):
    can_refer: bool = False

    def __init__(self, name: str, references: Object | None, rng: random.Random):
        super().__init__(name, references, rng)
        self.format = rng.choice(
            [
                "JSON",
                "AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn",
            ]
        )

    def create(self) -> str:
        # Always create an intermediary MV with a known schema so that
        # KEY (a) works regardless of what the referenced object provides.
        self.references_str = f"{self.name}_mv"
        cmds = [
            f"> CREATE MATERIALIZED VIEW IF NOT EXISTS {self.references_str} AS SELECT 'foo' AS a, 'bar' AS b",
        ]

        # See database-issues#9048, topic has to be unique
        topic = f"{self.name}-{uuid4()}"

        cmds.append(dedent(f"""
            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT)
            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                URL '${{testdrive.schema-registry-url}}'
              )
            > CREATE SINK {self.name}
              IN CLUSTER quickstart
              FROM {self.references_str}
              INTO KAFKA CONNECTION kafka_conn (TOPIC '{topic}')
              KEY (a) NOT ENFORCED
              FORMAT {self.format}
              ENVELOPE DEBEZIUM"""))
        return "\n".join(cmds)

    def destroy(self) -> str:
        return f"> DROP SINK {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_tmp_mv
                > CREATE MATERIALIZED VIEW {self.name}_tmp_mv AS SELECT * FROM {self.references_str}
                > ALTER SINK {self.name} SET FROM {self.name}_tmp_mv
                > ALTER SINK {self.name} SET FROM {self.references_str}
                > DROP MATERIALIZED VIEW {self.name}_tmp_mv
                """),
            lambda: dedent(f"""
                > DROP SINK IF EXISTS {self.name}_tmp_sink
                > ALTER SINK {self.name} RENAME TO {self.name}_tmp_sink
                > ALTER SINK {self.name}_tmp_sink RENAME TO {self.name}
"""),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class IcebergSink(Object):
    can_refer: bool = False
    _mode: str = "UPSERT"

    def __init__(self, name: str, references: Object | None, rng: random.Random):
        super().__init__(name, references, rng)

    def create(self) -> str:
        # Always create an intermediary MV with a known schema so that
        # KEY (a) works regardless of what the referenced object provides.
        self.references_str = f"{self.name}_mv"
        cmds = [
            f"> CREATE MATERIALIZED VIEW IF NOT EXISTS {self.references_str} AS SELECT 'foo' AS a, 'bar' AS b",
        ]

        # See database-issues#9048, topic has to be unique
        table = f"{self.name}-{uuid4()}"

        key_clause = " KEY (a) NOT ENFORCED" if self._mode == "UPSERT" else ""
        cmds.append(dedent(f"""
            > CREATE SINK {self.name}
              IN CLUSTER quickstart
              FROM {self.references_str}
              INTO ICEBERG CATALOG CONNECTION polaris_conn (NAMESPACE 'default_namespace', TABLE '{table}')
              USING AWS CONNECTION aws_conn{key_clause}
              MODE {self._mode} WITH (COMMIT INTERVAL '1s')"""))
        return "\n".join(cmds)

    def destroy(self) -> str:
        return f"> DROP SINK {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            # TODO: https://github.com/MaterializeInc/database-issues/issues/10026
            # lambda: dedent(
            #     f"""
            #     > DROP MATERIALIZED VIEW IF EXISTS {self.name}_tmp_mv
            #     > CREATE MATERIALIZED VIEW {self.name}_tmp_mv AS SELECT * FROM {self.references_str}
            #     > ALTER SINK {self.name} SET FROM {self.name}_tmp_mv
            #     > ALTER SINK {self.name} SET FROM {self.references_str}
            #     > DROP MATERIALIZED VIEW {self.name}_tmp_mv
            #     """
            # ),
            lambda: dedent(f"""
                > DROP SINK IF EXISTS {self.name}_tmp_sink
                > ALTER SINK {self.name} RENAME TO {self.name}_tmp_sink
                > ALTER SINK {self.name}_tmp_sink RENAME TO {self.name}
"""),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class IcebergAppendSink(IcebergSink):
    _mode: str = "APPEND"


class View(Object):
    def create(self) -> str:
        select = (
            "* FROM " + self.references.name
            if self.references
            else "'foo' AS a, 'bar' AS b"
        )
        return f"> CREATE VIEW {self.name} AS SELECT {select}"

    def destroy(self) -> str:
        return f"> DROP VIEW {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP VIEW IF EXISTS {self.name}_tmp_view
                > ALTER VIEW {self.name} RENAME TO {self.name}_tmp_view
                > ALTER VIEW {self.name}_tmp_view RENAME TO {self.name}
"""),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class MaterializedView(Object):
    def create(self) -> str:
        self.select = (
            "* FROM " + self.references.name
            if self.references
            else "'foo' AS a, 'bar' AS b"
        )
        return f"> CREATE MATERIALIZED VIEW {self.name} AS SELECT {self.select}"

    def destroy(self) -> str:
        return f"> DROP MATERIALIZED VIEW {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_tmp_mv
                > ALTER MATERIALIZED VIEW {self.name} RENAME TO {self.name}_tmp_mv
                > ALTER MATERIALIZED VIEW {self.name}_tmp_mv RENAME TO {self.name}
"""),
            # TODO: Deal with 'The materialized view has already computed its output until the end of time, so replacing its definition would have no effect.'
            # lambda: dedent(
            #     f"""
            #     > DROP MATERIALIZED VIEW IF EXISTS {self.name}_replacement
            #     > CREATE REPLACEMENT MATERIALIZED VIEW {self.name}_replacement FOR {self.name} AS SELECT {self.select}
            #     > ALTER MATERIALIZED VIEW {self.name} APPLY REPLACEMENT {self.name}_replacement
            #     """
            # ),
            lambda: dedent(f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_replacement
                > CREATE REPLACEMENT MATERIALIZED VIEW {self.name}_replacement FOR {self.name} AS SELECT {self.select}
                > DROP MATERIALIZED VIEW {self.name}_replacement
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class ReplicaTargetedMaterializedView(Object):
    def create(self) -> str:
        self.select = (
            "* FROM " + self.references.name
            if self.references
            else "'foo' AS a, 'bar' AS b"
        )
        return f"> CREATE MATERIALIZED VIEW {self.name} IN CLUSTER quickstart REPLICA r1 AS SELECT {self.select}"

    def destroy(self) -> str:
        return f"> DROP MATERIALIZED VIEW {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_tmp_mv
                > ALTER MATERIALIZED VIEW {self.name} RENAME TO {self.name}_tmp_mv
                > ALTER MATERIALIZED VIEW {self.name}_tmp_mv RENAME TO {self.name}
"""),
            lambda: dedent(f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_replacement
                > CREATE REPLACEMENT MATERIALIZED VIEW {self.name}_replacement FOR {self.name} AS SELECT {self.select}
                > DROP MATERIALIZED VIEW {self.name}_replacement
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class DefaultIndex(Object):
    can_refer: bool = False

    def create(self) -> str:
        return (
            f"> CREATE DEFAULT INDEX ON {self.references.name}"
            if self.references
            else ""
        )

    def destroy(self) -> str:
        return (
            f"> DROP INDEX {self.references.name}_primary_idx"
            if self.references
            else ""
        )

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP INDEX IF EXISTS {self.references.name}_tmp_idx
                > ALTER INDEX {self.references.name}_primary_idx RENAME TO {self.references.name}_tmp_idx
                > ALTER INDEX {self.references.name}_tmp_idx RENAME TO {self.references.name}_primary_idx
""") if self.references else "",
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Cluster(Object):
    can_refer: bool = False

    def __init__(self, name: str, references: "Object | None", rng: random.Random):
        super().__init__(name, references, rng)
        self.size = rng.choice(["scale=1,workers=1", "scale=1,workers=2"])
        self.replication_factor = rng.choice([1, 2])

    def create(self) -> str:
        return f"> CREATE CLUSTER {self.name} SIZE '{self.size}', REPLICATION FACTOR {self.replication_factor}"

    def destroy(self) -> str:
        return f"> DROP CLUSTER {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: f"> ALTER CLUSTER {self.name} SET (REPLICATION FACTOR 1)",
            lambda: f"> ALTER CLUSTER {self.name} SET (REPLICATION FACTOR 2)",
            lambda: f"> ALTER CLUSTER {self.name} SET (SIZE 'scale=1,workers=1')",
            lambda: f"> ALTER CLUSTER {self.name} SET (SIZE 'scale=1,workers=2')",
            lambda: dedent(f"""
                > DROP CLUSTER IF EXISTS {self.name}_tmp_cluster CASCADE
                > ALTER CLUSTER {self.name} RENAME TO {self.name}_tmp_cluster
                > ALTER CLUSTER {self.name}_tmp_cluster RENAME TO {self.name}
"""),
            lambda: dedent(f"""
                > DROP CLUSTER IF EXISTS {self.name}_swap_partner CASCADE
                > CREATE CLUSTER {self.name}_swap_partner SIZE '{self.size}', REPLICATION FACTOR {self.replication_factor}
                > ALTER CLUSTER {self.name} SWAP WITH {self.name}_swap_partner
                > ALTER CLUSTER {self.name} SWAP WITH {self.name}_swap_partner
                > DROP CLUSTER {self.name}_swap_partner CASCADE
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Secret(Object):
    can_refer: bool = False

    def create(self) -> str:
        return f"> CREATE SECRET {self.name} AS 'initial_secret_value'"

    def destroy(self) -> str:
        return f"> DROP SECRET {self.name}"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: f"> ALTER SECRET {self.name} AS 'rotated_secret_value'",
            lambda: f"> ALTER SECRET {self.name} AS 'another_secret_value'",
            lambda: dedent(f"""
                > DROP SECRET IF EXISTS {self.name}_tmp_secret
                > ALTER SECRET {self.name} RENAME TO {self.name}_tmp_secret
                > ALTER SECRET {self.name}_tmp_secret RENAME TO {self.name}
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Schema(Object):
    can_refer: bool = False

    def create(self) -> str:
        return f"> CREATE SCHEMA {self.name}"

    def destroy(self) -> str:
        return f"> DROP SCHEMA {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(f"""
                > DROP SCHEMA IF EXISTS {self.name}_tmp_schema CASCADE
                > ALTER SCHEMA {self.name} RENAME TO {self.name}_tmp_schema
                > ALTER SCHEMA {self.name}_tmp_schema RENAME TO {self.name}
                """),
            lambda: dedent(f"""
                > CREATE SCHEMA IF NOT EXISTS {self.name}_swap_partner
                > ALTER SCHEMA {self.name} SWAP WITH {self.name}_swap_partner
                > ALTER SCHEMA {self.name} SWAP WITH {self.name}_swap_partner
                > DROP SCHEMA IF EXISTS {self.name}_swap_partner CASCADE
                """),
            lambda: dedent(f"""
                > CREATE TABLE IF NOT EXISTS {self.name}.tmp_t (a TEXT)
                > INSERT INTO {self.name}.tmp_t VALUES ('foo')
                > DROP TABLE IF EXISTS {self.name}.tmp_t
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Role(Object):
    can_refer: bool = False

    def create(self) -> str:
        return f"> CREATE ROLE {self.name}"

    def destroy(self) -> str:
        # Route through mz_system: role may have been granted SUPERUSER
        # or system privileges, and only a superuser can alter/drop it.
        return dedent(f"""
            $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
            REVOKE ALL PRIVILEGES ON SYSTEM FROM {self.name};
            ALTER ROLE {self.name} NOSUPERUSER;
            DROP ROLE {self.name};
            """)

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: f"> ALTER ROLE {self.name} INHERIT",
            lambda: f"> ALTER ROLE {self.name} LOGIN",
            lambda: f"> ALTER ROLE {self.name} NOLOGIN",
            # SUPERUSER/NOSUPERUSER require the caller to already be a
            # superuser, so route through mz_system.
            lambda: dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER ROLE {self.name} SUPERUSER;
                ALTER ROLE {self.name} NOSUPERUSER;
                """),
            # CREATECLUSTER/CREATEDB/CREATEROLE are not supported as role
            # attributes; system privileges must be granted explicitly
            # (run as mz_system since only superusers can grant them).
            lambda: dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                GRANT CREATECLUSTER ON SYSTEM TO {self.name};
                REVOKE CREATECLUSTER ON SYSTEM FROM {self.name};
                """),
            lambda: dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                GRANT CREATEDB ON SYSTEM TO {self.name};
                REVOKE CREATEDB ON SYSTEM FROM {self.name};
                """),
            lambda: dedent(f"""
                $ postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                GRANT CREATEROLE ON SYSTEM TO {self.name};
                REVOKE CREATEROLE ON SYSTEM FROM {self.name};
                """),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class Executor:
    def execute(self, td: str) -> None:
        raise NotImplementedError


class Scenario:
    def __init__(self, c: Composition, rng: random.Random, num_objects: int):
        self.c = c
        self.rng = rng
        self.num_objects = num_objects
        # Set to True when a transient error was detected and the catalog
        # may now be in an inconsistent state (e.g. a BEGIN/COMMIT transaction
        # rolled back after testdrive's retry logic masked the COMMIT failure).
        # The outer workflow loop must do a full reset before the next run.
        self.needs_reset = False

    def _impl(self, num_executions: int) -> str:
        raise NotImplementedError

    def print(self) -> None:
        print(self._impl(1))

    # Errors that are transient and safe to ignore in race-condition testing.
    # When one of these is encountered the catalog is likely in an
    # inconsistent state, so the scenario signals for a full reset.
    ignorable_errors: list[str] = [
        # TODO: https://github.com/MaterializeInc/database-issues/issues/9690
        "another session modified the catalog while this DDL transaction was open",
        # Downstream effect: a prior catalog-conflict rolled back a
        # BEGIN/COMMIT transaction, so the object never existed by the time
        # destroy() tried to drop it.
        "unknown catalog item",
    ]

    def run_fragment(self, text: str, tries: int = 1) -> None:
        if not text:
            return
        for i in range(tries):
            try:
                self.c.testdrive(text, quiet=True)
                return
            except Exception as e:
                # FailedTestExecutionError's str() is just a summary ("At least
                # one test failed"); the actual testdrive error text lives in
                # e.errors[].{message,details}. Check all of them.
                haystack = str(e)
                for err in getattr(e, "errors", []) or []:
                    haystack += f"\n{getattr(err, 'message', '')}\n{getattr(err, 'details', '')}"
                if any(msg in haystack for msg in self.ignorable_errors):
                    print(
                        f"Transient error detected, will reset state before next scenario: {e}"
                    )
                    self.needs_reset = True
                    return
                print(e)
                if i == tries - 1:
                    print("Failed to run fragment, giving up")
                    raise
                print(f"Failed to run fragment, retrying ({i+1}/{tries})")

    def run(self, num_executions: int) -> None:
        self.run_fragment(self._impl(num_executions))


class Concurrent(Scenario):
    def __init__(self, c: Composition, rng: random.Random, num_objects: int):
        super().__init__(c, rng, num_objects)
        objects = [o for o in list(all_subclasses(Object)) if o.enabled]
        self.objs = [
            rng.choice([o for o in objects if o.can_refer])("o_base", None, rng)
        ]
        self.manipulators = []
        for i in range(num_objects):
            self.objs.append(rng.choice(objects)(f"o_{i}", self.objs[0], rng))
            self.manipulators.append(self.rng.randrange(100))

    def print(self) -> None:
        pass  # TODO: print

    def run(self, num_executions: int) -> None:
        for i in range(num_executions):
            # Clean up old state
            self.c.down(destroy_volumes=True)
            self.c.up(*SERVICE_NAMES, Service("testdrive", idle=True))
            setup_sql_server_testing(self.c)

            for obj in self.objs:
                self.run_fragment(obj.prepare())

            self.run_fragment(self.objs[0].create())

            def run(o: Object, m: int) -> None:
                try:
                    self.run_fragment(o.create(), tries=100)
                    self.run_fragment(o.manipulate(m), tries=100)
                finally:
                    try:
                        self.run_fragment(o.destroy(), tries=100)
                    except:
                        # Might be in a half-finished state, ignore
                        pass

            threads = [
                PropagatingThread(target=lambda: run(obj, manipulator))
                for obj, manipulator in zip(self.objs[1:], self.manipulators)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.run_fragment(self.objs[0].destroy())


class Subsequent(Scenario):
    def __init__(self, c: Composition, rng: random.Random, num_objects: int):
        super().__init__(c, rng, num_objects)
        objects = [o for o in list(all_subclasses(Object)) if o.enabled]
        self.objs = [
            rng.choice([o for o in objects if o.can_refer])("o_base", None, rng)
        ]
        self.manipulators = []
        for i in range(num_objects):
            self.objs.append(rng.choice(objects)(f"o_{i}", self.objs[0], rng))
            self.manipulators.append(self.rng.randrange(100))

    def _impl(self, num_executions: int) -> str:
        result = ""
        for i in range(num_executions):
            if i == 0:
                for obj in self.objs:
                    result += obj.prepare() + "\n"
            result += self.objs[0].create() + "\n"
            for obj, manipulator in zip(self.objs[1:], self.manipulators):
                result += obj.create() + "\n"
                result += obj.manipulate(manipulator) + "\n"
                result += obj.destroy() + "\n"
            result += self.objs[0].destroy() + "\n"
        return result


class SubsequentChain(Scenario):
    def __init__(self, c: Composition, rng: random.Random, num_objects: int):
        super().__init__(c, rng, num_objects)
        objects = [o for o in list(all_subclasses(Object)) if o.enabled]
        self.objs = [
            rng.choice([o for o in objects if o.can_refer])("o_base", None, rng)
        ]
        self.manipulators = []
        for i in range(num_objects):
            self.objs.append(
                rng.choice([o for o in objects if o.can_refer])(
                    f"o_{i}", self.objs[-1], rng
                )
            )
            self.manipulators.append(self.rng.randrange(100))

    def _impl(self, num_executions: int) -> str:
        result = ""
        for i in range(num_executions):
            if i == 0:
                for obj in self.objs:
                    result += obj.prepare() + "\n"
            for obj in self.objs:
                result += obj.create() + "\n"
            for obj, manipulator in zip(self.objs[1:], self.manipulators):
                result += obj.manipulate(manipulator) + "\n"
            for obj in reversed(self.objs):
                result += obj.destroy() + "\n"
        return result


def setup(c: Composition) -> None:
    """Bring services up and configure Materialize for the test.

    Called at startup and again after a reset (down + up), so the operations
    here must be safe to run against a fresh volume-destroyed environment.
    """
    c.up(
        *SERVICE_NAMES,
        Service("testdrive", idle=True),
        Service("polaris-bootstrap", idle=True),
        Service("polaris", idle=True),
    )
    setup_sql_server_testing(c)

    iceberg_credentials = setup_polaris_for_iceberg(c)
    c.sql(f"CREATE SECRET iceberg_secret AS '{iceberg_credentials[1]}'")
    c.sql(
        f"CREATE CONNECTION aws_conn TO AWS (ACCESS KEY ID = '{iceberg_credentials[0]}', SECRET ACCESS KEY = SECRET iceberg_secret, ENDPOINT = 'http://toxiproxy:9000/', REGION = 'us-east-1');"
    )
    c.sql(
        "CREATE CONNECTION polaris_conn TO ICEBERG CATALOG (CATALOG TYPE = 'REST', URL = 'http://toxiproxy:8181/api/catalog', CREDENTIAL = 'root:root', WAREHOUSE = 'default_catalog', SCOPE = 'PRINCIPAL_ROLE:ALL');"
    )
    c.sql(
        "ALTER SYSTEM SET max_schemas_per_database = 1000000",
        user="mz_system",
        port=6877,
    )
    c.sql("ALTER SYSTEM SET max_tables = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_materialized_views = 1000000", user="mz_system", port=6877
    )
    c.sql("ALTER SYSTEM SET max_sources = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_sinks = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_roles = 1000000", user="mz_system", port=6877)
    c.sql("ALTER SYSTEM SET max_clusters = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET max_replicas_per_cluster = 1000000",
        user="mz_system",
        port=6877,
    )
    c.sql("ALTER SYSTEM SET max_secrets = 1000000", user="mz_system", port=6877)
    c.sql(
        "ALTER SYSTEM SET webhook_concurrent_request_limit = 1000000",
        user="mz_system",
        port=6877,
    )
    # enable_replica_targeted_materialized_views is set via
    # additional_system_parameter_defaults on the Materialized service so it
    # survives c.down()/c.up() cycles in the Concurrent scenario.


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--seed", type=str, default=random.randrange(1000000))
    parser.add_argument("--runtime", default=600, type=int, help="Runtime in seconds")
    parser.add_argument(
        "--repetitions", default=100, type=int, help="Repeatitions per scenario"
    )
    parser.add_argument(
        "--scenario",
        default="subsequent",
        type=str,
        choices=["subsequent", "subsequent-chain", "concurrent"],
    )
    parser.add_argument(
        "--num-objects",
        default=5,
        type=int,
    )
    parser.add_argument(
        "--jitter",
        default=10,
        type=int,
    )
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
    ).timestamp()

    toxiproxy_start(c, args.jitter)
    setup(c)

    seed = args.seed
    # Longest observed scenario runtime so far; used to decide whether a new
    # scenario can safely start within the remaining budget. The whole scenario
    # runs as a single testdrive invocation and cannot be interrupted mid-flight,
    # so starting one without enough budget would overrun the CI wall-clock.
    max_scenario_duration = 0.0

    while time.time() < end_time:
        remaining = end_time - time.time()
        if max_scenario_duration > 0 and remaining < max_scenario_duration:
            print(
                f"--- Remaining budget {remaining:.0f}s < longest observed scenario {max_scenario_duration:.0f}s, stopping early"
            )
            break

        rng = random.Random(seed)

        if args.scenario == "subsequent":
            scenario = Subsequent(c, rng, args.num_objects)
        elif args.scenario == "subsequent-chain":
            scenario = SubsequentChain(c, rng, args.num_objects)
        elif args.scenario == "concurrent":
            scenario = Concurrent(c, rng, args.num_objects)
        else:
            raise ValueError(f"Unknown scenario {args.scenario}")

        print(f"--- Scenario to run (--seed={seed})")
        scenario.print()

        print(f"--- Running scenario {args.repetitions} times")
        start = time.time()
        scenario.run(args.repetitions)
        duration = time.time() - start
        max_scenario_duration = max(max_scenario_duration, duration)
        print(f"--- Scenario finished in {duration:.0f}s")

        if scenario.needs_reset:
            print("--- Resetting state after transient error")
            c.down(destroy_volumes=True)
            toxiproxy_start(c, args.jitter)
            setup(c)

        seed = rng.randrange(1000000)


def toxiproxy_start(c: Composition, jitter: int) -> None:
    c.up("toxiproxy")

    port = c.default_port("toxiproxy")
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "schema-registry",
            "listen": "0.0.0.0:8081",
            "upstream": "schema-registry:8081",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "kafka",
            "listen": "0.0.0.0:9092",
            "upstream": "kafka:9092",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "polaris",
            "listen": "0.0.0.0:8181",
            "upstream": "polaris:8181",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r
    r = requests.post(
        f"http://localhost:{port}/proxies",
        json={
            "name": "minio",
            "listen": "0.0.0.0:9000",
            "upstream": "minio:9000",
            "enabled": True,
        },
    )
    assert r.status_code == 201, r

    if not jitter:
        return

    r = requests.post(
        f"http://localhost:{port}/proxies/schema-registry/toxics",
        json={
            "name": "schema-registry",
            "type": "latency",
            "attributes": {"latency": 0, "jitter": jitter},
        },
    )
    assert r.status_code == 200, r
    r = requests.post(
        f"http://localhost:{port}/proxies/kafka/toxics",
        json={
            "name": "kafka",
            "type": "latency",
            "attributes": {"latency": 0, "jitter": jitter},
        },
    )
    assert r.status_code == 200, r
    r = requests.post(
        f"http://localhost:{port}/proxies/polaris/toxics",
        json={
            "name": "polaris",
            "type": "latency",
            "attributes": {"latency": 0, "jitter": jitter},
        },
    )
    assert r.status_code == 200, r
    r = requests.post(
        f"http://localhost:{port}/proxies/minio/toxics",
        json={
            "name": "minio",
            "type": "latency",
            "attributes": {"latency": 0, "jitter": jitter},
        },
    )
    assert r.status_code == 200, r
