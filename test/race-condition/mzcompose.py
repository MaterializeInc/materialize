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

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.util import PropagatingThread, all_subclasses

SERVICES = [
    Postgres(max_replication_slots=100000),
    MySql(),
    SqlServer(),
    Zookeeper(),
    Kafka(
        auto_create_topics=False,
        ports=["30123:30123"],
        allow_host_ports=True,
        environment_extra=[
            "KAFKA_ADVERTISED_LISTENERS=HOST://localhost:30123,PLAINTEXT://kafka:9092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=HOST:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        ],
    ),
    SchemaRegistry(),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Testdrive(no_reset=True, consistent_seed=True, default_timeout="600s"),
    Mc(),
    Materialized(
        default_replication_factor=2,
        additional_system_parameter_defaults={"memory_limiter_interval": "0"},
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
        return dedent(
            f"""
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
              URL '${{testdrive.schema-registry-url}}')

            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT)"""
        )

    def create(self) -> str:
        return dedent(
            f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-{self.name}-${{testdrive.seed}}')
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE "testdrive-{self.name}-${{testdrive.seed}}")
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM
            > COMMIT"""
        )

    def destroy(self) -> str:
        return dedent(
            f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source CASCADE"""
        )

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
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
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


# TODO: How to handle things like clusters, replicas?

# TODO: Add more manipulations: inserts, updates, deletes, ALTER RENAME (twice)


# TODO: Reenable when https://github.com/MaterializeInc/database-issues/issues/9302 is fixed
# class PostgresSource(Object):
#     def prepare(self) -> str:
#         return dedent(
#             f"""
#             $ postgres-execute connection=postgres://postgres:postgres@postgres
#             DROP USER IF EXISTS {self.name}_role;
#             CREATE USER {self.name}_role WITH SUPERUSER PASSWORD 'postgres';
#             ALTER USER {self.name}_role WITH replication;
#             DROP PUBLICATION IF EXISTS {self.name}_source;
#             DROP TABLE IF EXISTS {self.name}_table;
#             CREATE TABLE {self.name}_table (a TEXT, b TEXT);
#             ALTER TABLE {self.name}_table REPLICA IDENTITY FULL;
#             CREATE PUBLICATION {self.name}_source FOR TABLE {self.name}_table;
#             INSERT INTO {self.name}_table VALUES ('foo', 'bar');
#
#             > DROP SECRET IF EXISTS {self.name}_pass CASCADE
#             > CREATE SECRET {self.name}_pass AS 'postgres'
#             > DROP CONNECTION IF EXISTS {self.name}_conn CASCADE
#             > CREATE CONNECTION {self.name}_conn FOR POSTGRES
#               HOST 'postgres',
#               DATABASE postgres,
#               USER {self.name}_role,
#               PASSWORD SECRET {self.name}_pass"""
#         )
#
#     def create(self) -> str:
#         return dedent(
#             f"""
#             > BEGIN
#             > CREATE SOURCE {self.name}_source
#               IN CLUSTER quickstart
#               FROM POSTGRES CONNECTION {self.name}_conn
#               (PUBLICATION '{self.name}_source')
#             > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE {self.name}_table)
#             > COMMIT"""
#         )
#
#     def destroy(self) -> str:
#         return dedent(
#             f"""
#             > DROP TABLE {self.name} CASCADE
#             > DROP SOURCE IF EXISTS {self.name}_source"""
#         )
#
#     def manipulate(self, kind: int) -> str:
#         manipulations = [
#             lambda: "",
#             lambda: dedent(
#                 f"""
#                 $ postgres-execute connection=postgres://postgres:postgres@postgres
#                 INSERT INTO {self.name}_table VALUES ('foo', 'bar');"""
#             ),
#             lambda: dedent(
#                 f"""
#                 $ postgres-execute connection=postgres://postgres:postgres@postgres
#                 UPDATE {self.name}_table SET b = b || 'bar' WHERE true;"""
#             ),
#             lambda: dedent(
#                 f"""
#                 $ postgres-execute connection=postgres://postgres:postgres@postgres
#                 DELETE FROM {self.name}_table WHERE LENGTH(b) > 12;"""
#             ),
#             lambda: dedent(
#                 f"""
#                 > DROP TABLE IF EXISTS {self.name}_tmp_table
#                 > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
#                 > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
#                 """
#             ),
#         ]
#         return manipulations[kind % len(manipulations)]()
#
#     def verify(self) -> str:
#         raise NotImplementedError


# TODO: Can't set up with an empty table in mysql? ERROR: reference to public.o_0_table not found in source
class MySqlSource(Object):
    def prepare(self) -> str:
        return dedent(
            f"""
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
              )"""
        )

    def create(self) -> str:
        return dedent(
            f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM MYSQL CONNECTION {self.name}_conn
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE public.{self.name}_table)
            > COMMIT
            """
        )

    def destroy(self) -> str:
        return dedent(
            f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source"""
        )

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                $ mysql-execute name=mysql
                USE public;
                INSERT INTO {self.name}_table VALUES ('foo', 'bar');"""
            ),
            lambda: dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                $ mysql-execute name=mysql
                USE public;
                UPDATE {self.name}_table SET b = CONCAT(b, 'bar') WHERE true;"""
            ),
            lambda: dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}
                $ mysql-execute name=mysql
                USE public;
                DELETE FROM {self.name}_table WHERE LENGTH(b) > 12;"""
            ),
            lambda: dedent(
                f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
                """
            ),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class SqlServerSource(Object):
    def prepare(self) -> str:
        return dedent(
            f"""
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
              )"""
        )

    def create(self) -> str:
        return dedent(
            f"""
            > BEGIN
            > CREATE SOURCE {self.name}_source
              IN CLUSTER quickstart
              FROM SQL SERVER CONNECTION {self.name}_conn
            > CREATE TABLE {self.name} FROM SOURCE {self.name}_source (REFERENCE {self.name}_table)
            > COMMIT
            """
        )

    def destroy(self) -> str:
        return dedent(
            f"""
            > DROP TABLE {self.name} CASCADE
            > DROP SOURCE IF EXISTS {self.name}_source"""
        )

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO {self.name}_table VALUES ('foo', 'bar');"""
            ),
            lambda: dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                UPDATE {self.name}_table SET b = CONCAT(b, 'bar') WHERE 1 = 1;"""
            ),
            lambda: dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}

                $ sql-server-execute name=sql-server
                USE test;
                DELETE FROM {self.name}_table WHERE LEN(b) > 12;"""
            ),
            lambda: dedent(
                f"""
                > DROP TABLE IF EXISTS {self.name}_tmp_table
                > ALTER TABLE {self.name} RENAME TO {self.name}_tmp_table
                > ALTER TABLE {self.name}_tmp_table RENAME TO {self.name}
                """
            ),
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
            lambda: dedent(
                f"""
                > DROP SOURCE IF EXISTS {self.name}_tmp_source
                > ALTER SOURCE {self.name} RENAME TO {self.name}_tmp_source
                > ALTER SOURCE {self.name}_tmp_source RENAME TO {self.name}
                """
            ),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class WebhookSource(Object):
    def __init__(self, name: str, references: "Object | None", rng: random.Random):
        super().__init__(name, references, rng)
        self.body_format = rng.choice(["TEXT", "JSON", "JSON ARRAY", "BYTES"])

    def create(self) -> str:
        return dedent(
            f"""
            > DROP CLUSTER IF EXISTS {self.name}_cluster
            > CREATE CLUSTER {self.name}_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 1
            > CREATE SOURCE {self.name} IN CLUSTER {self.name}_cluster FROM WEBHOOK BODY FORMAT {self.body_format}
            """
        )

    def destroy(self) -> str:
        return dedent(
            f"""
            > DROP CLUSTER {self.name}_cluster CASCADE
            """
        )

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(
                f"""
                > DROP SOURCE IF EXISTS {self.name}_tmp_source
                > ALTER SOURCE {self.name} RENAME TO {self.name}_tmp_source
                > ALTER SOURCE {self.name}_tmp_source RENAME TO {self.name}
                """
            ),
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
        self.references_str = (
            self.references.name if self.references else f"{self.name}_view"
        )
        cmds = []
        if not self.references:
            cmds.append(
                f"> CREATE MATERIALIZED VIEW IF NOT EXISTS {self.references_str} AS SELECT 'foo' AS a, 'bar' AS b"
            )
        elif isinstance(self.references, View):
            self.references_str = f"{self.name}_mv"
            cmds.append(
                f"> CREATE MATERIALIZED VIEW IF NOT EXISTS {self.references_str} AS SELECT * FROM {self.references.name}"
            )

        # See database-issues#9048, topic has to be unique
        topic = f"{self.name}-{uuid4()}"

        cmds.append(
            dedent(
                f"""
            > CREATE CONNECTION IF NOT EXISTS kafka_conn
              TO KAFKA (BROKER '${{testdrive.kafka-addr}}', SECURITY PROTOCOL PLAINTEXT)
            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                URL '${{testdrive.schema-registry-url}}'
              )
            > CREATE SINK {self.name}
              IN CLUSTER quickstart
              FROM {self.references_str}
              INTO KAFKA CONNECTION kafka_conn (TOPIC '{topic}')
              FORMAT {self.format}
              ENVELOPE DEBEZIUM"""
            )
        )
        return "\n".join(cmds)

    def destroy(self) -> str:
        return f"> DROP SINK {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(
                f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_tmp_mv
                > CREATE MATERIALIZED VIEW {self.name}_tmp_mv AS SELECT * FROM {self.references_str}
                > ALTER SINK {self.name} SET FROM {self.name}_tmp_mv
                > ALTER SINK {self.name} SET FROM {self.references_str}
                > DROP MATERIALIZED VIEW {self.name}_tmp_mv
                """
            ),
            lambda: dedent(
                f"""
                > DROP SINK IF EXISTS {self.name}_tmp_sink
                > ALTER SINK {self.name} RENAME TO {self.name}_tmp_sink
                > ALTER SINK {self.name}_tmp_sink RENAME TO {self.name}
                """
            ),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


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
            lambda: dedent(
                f"""
                > DROP VIEW IF EXISTS {self.name}_tmp_view
                > ALTER VIEW {self.name} RENAME TO {self.name}_tmp_view
                > ALTER VIEW {self.name}_tmp_view RENAME TO {self.name}
                """
            ),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class MaterializedView(Object):
    def create(self) -> str:
        select = (
            "* FROM " + self.references.name
            if self.references
            else "'foo' AS a, 'bar' AS b"
        )
        return f"> CREATE MATERIALIZED VIEW {self.name} AS SELECT {select}"

    def destroy(self) -> str:
        return f"> DROP MATERIALIZED VIEW {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        manipulations = [
            lambda: "",
            lambda: dedent(
                f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_tmp_mv
                > ALTER MATERIALIZED VIEW {self.name} RENAME TO {self.name}_tmp_mv
                > ALTER MATERIALIZED VIEW {self.name}_tmp_mv RENAME TO {self.name}
                """
            ),
        ]
        return manipulations[kind % len(manipulations)]()

    def verify(self) -> str:
        raise NotImplementedError


class ReplacementMaterializedView(Object):
    def create(self) -> str:
        select = (
            "* FROM " + self.references.name
            if self.references
            else "'foo' AS a, 'bar' AS b"
        )
        return f"> CREATE MATERIALIZED VIEW {self.name} AS SELECT {select}"

    def destroy(self) -> str:
        return f"> DROP MATERIALIZED VIEW {self.name} CASCADE"

    def manipulate(self, kind: int) -> str:
        select = (
            "* FROM " + self.references.name
            if self.references
            else "'foo' AS a, 'bar' AS b"
        )
        manipulations = [
            lambda: "",
            lambda: dedent(
                f"""
                > DROP MATERIALIZED VIEW IF EXISTS {self.name}_replacement
                > CREATE MATERIALIZED VIEW {self.name}_replacement REPLACING {self.name} AS SELECT {select}
                > ALTER MATERIALIZED VIEW {self.name} APPLY REPLACEMENT {self.name}_replacement
                """
            ),
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

    def _impl(self, num_executions: int) -> str:
        raise NotImplementedError

    def print(self) -> None:
        print(self._impl(1))

    def run_fragment(self, text: str, tries: int = 1) -> None:
        if not text:
            return
        for i in range(tries):
            try:
                self.c.testdrive(text, quiet=True)
                return
            except Exception as e:
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
        objects = list(all_subclasses(Object))
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
        objects = list(all_subclasses(Object))
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
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")

    end_time = (
        datetime.datetime.now() + datetime.timedelta(seconds=args.runtime)
    ).timestamp()

    c.up(*SERVICE_NAMES, Service("testdrive", idle=True))
    setup_sql_server_testing(c)

    seed = args.seed

    while time.time() < end_time:
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
        scenario.run(args.repetitions)
        seed = rng.randrange(1000000)
