# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import time
from typing import Any

import confluent_kafka  # type: ignore
import pg8000
from confluent_kafka.admin import AdminClient  # type: ignore
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient  # type: ignore
from confluent_kafka.schema_registry.avro import AvroSerializer  # type: ignore
from confluent_kafka.serialization import (  # type: ignore
    MessageField,
    SerializationContext,
)
from pg8000.exceptions import InterfaceError
from pg8000.native import identifier

from materialize.data_ingest.data_type import Backend
from materialize.data_ingest.field import Field, formatted_value
from materialize.data_ingest.query_error import QueryError
from materialize.data_ingest.row import Operation
from materialize.data_ingest.transaction import Transaction


class Executor:
    num_transactions: int
    ports: dict[str, int]
    mz_conn: pg8000.Connection
    fields: list[Field]
    database: str
    schema: str

    def __init__(
        self,
        ports: dict[str, int],
        fields: list[Field] = [],
        database: str = "",
        schema: str = "public",
    ) -> None:
        self.num_transactions = 0
        self.ports = ports
        self.fields = fields
        self.database = database
        self.schema = schema
        self.reconnect()

    def reconnect(self) -> None:
        self.mz_conn = pg8000.connect(
            host="localhost",
            port=self.ports["materialized"],
            user="materialize",
            database=self.database,
        )
        self.mz_conn.autocommit = True

    def create(self) -> None:
        raise NotImplementedError

    def run(self, transaction: Transaction) -> None:
        raise NotImplementedError

    def execute(self, cur: pg8000.Cursor, query: str) -> None:
        try:
            cur.execute(query)
        except InterfaceError:
            # Can happen after Mz disruptions if we running queries against Mz
            print("Network error, retrying")
            time.sleep(0.01)
            self.reconnect()
            with self.mz_conn.cursor() as cur:
                self.execute(cur, query)
        except Exception as e:
            print(f"Query failed: {query} {e}")
            raise QueryError(str(e), query)

    def execute_with_retry_on_error(
        self,
        cur: pg8000.Cursor,
        query: str,
        required_error_message_substrs: list[str],
        max_tries: int = 5,
        wait_time_in_sec: int = 1,
    ) -> None:
        for try_count in range(1, max_tries + 1):
            try:
                self.execute(cur, query)
                return
            except Exception as e:
                if not any([s in e.__str__() for s in required_error_message_substrs]):
                    raise
                elif try_count == max_tries:
                    raise
                else:
                    time.sleep(wait_time_in_sec)


class PrintExecutor(Executor):
    def create(self) -> None:
        pass

    def run(self, transaction: Transaction) -> None:
        print("Transaction:")
        print("  ", transaction.row_lists)


def delivery_report(err: str, msg: Any) -> None:
    assert err is None, f"Delivery failed for User record {msg.key()}: {err}"


class KafkaExecutor(Executor):
    producer: confluent_kafka.Producer
    avro_serializer: AvroSerializer
    key_avro_serializer: AvroSerializer
    serialization_context: SerializationContext
    key_serialization_context: SerializationContext
    topic: str
    table: str

    def __init__(
        self,
        num: int,
        ports: dict[str, int],
        fields: list[Field],
        database: str,
        schema: str = "public",
    ):
        super().__init__(ports, fields, database, schema)

        self.topic = f"data-ingest-{num}"
        self.table = f"kafka_table{num}"

    def create(self) -> None:
        schema = {
            "type": "record",
            "name": "value",
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.data_type.name(Backend.AVRO)).lower(),
                }
                for field in self.fields
                if not field.is_key
            ],
        }

        key_schema = {
            "type": "record",
            "name": "key",
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.data_type.name(Backend.AVRO)).lower(),
                }
                for field in self.fields
                if field.is_key
            ],
        }

        kafka_conf = {"bootstrap.servers": f"localhost:{self.ports['kafka']}"}

        a = AdminClient(kafka_conf)
        fs = a.create_topics(
            [
                confluent_kafka.admin.NewTopic(  # type: ignore
                    self.topic, num_partitions=1, replication_factor=1
                )
            ]
        )
        for topic, f in fs.items():
            f.result()

        # NOTE: this _could_ be refactored, but since we are fairly certain at
        # this point there will be exactly one topic it should be fine.
        topic = list(fs.keys())[0]

        schema_registry_conf = {
            "url": f"http://localhost:{self.ports['schema-registry']}"
        }
        registry = SchemaRegistryClient(schema_registry_conf)

        self.avro_serializer = AvroSerializer(
            registry, json.dumps(schema), lambda d, ctx: d
        )

        self.key_avro_serializer = AvroSerializer(
            registry, json.dumps(key_schema), lambda d, ctx: d
        )

        registry.register_schema(
            f"{topic}-value", Schema(json.dumps(schema), schema_type="AVRO")
        )
        registry.register_schema(
            f"{topic}-key", Schema(json.dumps(key_schema), schema_type="AVRO")
        )

        self.serialization_context = SerializationContext(
            self.topic, MessageField.VALUE
        )
        self.key_serialization_context = SerializationContext(
            self.topic, MessageField.KEY
        )

        self.producer = confluent_kafka.Producer(kafka_conf)

        self.mz_conn.autocommit = True
        with self.mz_conn.cursor() as cur:
            self.execute(
                cur,
                f"""CREATE SOURCE {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.table)}
                    FROM KAFKA CONNECTION materialize.public.kafka_conn (TOPIC '{self.topic}')
                    FORMAT AVRO
                    USING CONFLUENT SCHEMA REGISTRY CONNECTION materialize.public.csr_conn
                    ENVELOPE UPSERT""",
            )
        self.mz_conn.autocommit = False

    def run(self, transaction: Transaction) -> None:
        for row_list in transaction.row_lists:
            for row in row_list.rows:
                if (
                    row.operation == Operation.INSERT
                    or row.operation == Operation.UPSERT
                ):
                    self.producer.produce(
                        topic=self.topic,
                        key=self.key_avro_serializer(
                            {
                                field.name: value
                                for field, value in zip(row.fields, row.values)
                                if field.is_key
                            },
                            self.key_serialization_context,
                        ),
                        value=self.avro_serializer(
                            {
                                field.name: value
                                for field, value in zip(row.fields, row.values)
                                if not field.is_key
                            },
                            self.serialization_context,
                        ),
                        on_delivery=delivery_report,
                    )
                elif row.operation == Operation.DELETE:
                    self.producer.produce(
                        topic=self.topic,
                        key=self.key_avro_serializer(
                            {
                                field.name: value
                                for field, value in zip(row.fields, row.values)
                                if field.is_key
                            },
                            self.key_serialization_context,
                        ),
                        value=None,
                        on_delivery=delivery_report,
                    )
                else:
                    raise ValueError(f"Unexpected operation {row.operation}")
        self.producer.flush()


class PgExecutor(Executor):
    pg_conn: pg8000.Connection
    table: str
    source: str
    num: int

    def __init__(
        self,
        num: int,
        ports: dict[str, int],
        fields: list[Field],
        database: str,
        schema: str = "public",
    ):
        super().__init__(ports, fields, database, schema)
        self.table = f"table{num}"
        self.source = f"postgres_source{num}"
        self.num = num

    def create(self) -> None:
        self.pg_conn = pg8000.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=self.ports["postgres"],
        )

        values = [
            f"{identifier(field.name)} {str(field.data_type.name(Backend.POSTGRES)).lower()}"
            for field in self.fields
        ]
        keys = [field.name for field in self.fields if field.is_key]

        self.pg_conn.autocommit = True
        with self.pg_conn.cursor() as cur:
            self.execute(
                cur,
                f"""DROP TABLE IF EXISTS {identifier(self.table)};
                    CREATE TABLE {identifier(self.table)} (
                        {", ".join(values)},
                        PRIMARY KEY ({", ".join([identifier(key) for key in keys])}));
                    ALTER TABLE {identifier(self.table)} REPLICA IDENTITY FULL;
                    CREATE USER postgres{self.num} WITH SUPERUSER PASSWORD 'postgres';
                    ALTER USER postgres{self.num} WITH replication;
                    DROP PUBLICATION IF EXISTS postgres_source;
                    CREATE PUBLICATION postgres_source FOR ALL TABLES;""",
            )
        self.pg_conn.autocommit = False

        self.mz_conn.autocommit = True
        with self.mz_conn.cursor() as cur:
            self.execute(cur, f"CREATE SECRET pgpass{self.num} AS 'postgres'")
            self.execute(
                cur,
                f"""CREATE CONNECTION pg{self.num} FOR POSTGRES
                    HOST 'postgres',
                    DATABASE postgres,
                    USER postgres{self.num},
                    PASSWORD SECRET pgpass{self.num}""",
            )
            self.execute(
                cur,
                f"""CREATE SOURCE {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.source)}
                    FROM POSTGRES CONNECTION pg{self.num} (PUBLICATION 'postgres_source')
                    FOR TABLES ({identifier(self.table)} AS {identifier(self.table)})""",
            )
        self.mz_conn.autocommit = False

    def run(self, transaction: Transaction) -> None:
        with self.pg_conn.cursor() as cur:
            for row_list in transaction.row_lists:
                for row in row_list.rows:
                    if row.operation == Operation.INSERT:
                        values_str = ", ".join(
                            str(formatted_value(value)) for value in row.values
                        )
                        self.execute(
                            cur,
                            f"""INSERT INTO {identifier(self.table)}
                                VALUES ({values_str})
                            """,
                        )
                    elif row.operation == Operation.UPSERT:
                        values_str = ", ".join(
                            str(formatted_value(value)) for value in row.values
                        )
                        keys_str = ", ".join(
                            identifier(field.name)
                            for field in row.fields
                            if field.is_key
                        )
                        update_str = ", ".join(
                            f"{identifier(field.name)} = EXCLUDED.{identifier(field.name)}"
                            for field in row.fields
                        )
                        self.execute(
                            cur,
                            f"""INSERT INTO {identifier(self.table)}
                                VALUES ({values_str})
                                ON CONFLICT ({keys_str})
                                DO UPDATE SET {update_str}
                            """,
                        )
                    elif row.operation == Operation.DELETE:
                        cond_str = " AND ".join(
                            f"{identifier(field.name)} = {formatted_value(value)}"
                            for field, value in zip(row.fields, row.values)
                            if field.is_key
                        )
                        self.execute(
                            cur,
                            f"""DELETE FROM {identifier(self.table)}
                                WHERE {cond_str}
                            """,
                        )
                    else:
                        raise ValueError(f"Unexpected operation {row.operation}")
        self.pg_conn.commit()


class KafkaRoundtripExecutor(Executor):
    table: str
    table_original: str
    topic: str
    known_keys: set[tuple[str]]
    num: int

    def __init__(
        self,
        num: int,
        ports: dict[str, int],
        fields: list[Field],
        database: str,
        schema: str = "public",
    ):
        super().__init__(ports, fields, database, schema)
        self.table_original = f"table_rt_source{num}"
        self.table = f"table_rt{num}"
        self.topic = f"data-ingest-rt-{num}"
        self.num = num
        self.known_keys = set()

    def create(self) -> None:
        values = [
            f"{field.name} {str(field.data_type.name(Backend.POSTGRES)).lower()}"
            for field in self.fields
        ]
        keys = [field.name for field in self.fields if field.is_key]

        self.mz_conn.autocommit = True
        with (self.mz_conn.cursor() as cur):
            self.execute(cur, f"DROP TABLE IF EXISTS {identifier(self.table_original)}")
            self.execute(
                cur,
                f"""CREATE TABLE {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.table_original)} (
                        {", ".join(values)},
                        PRIMARY KEY ({", ".join(keys)}));""",
            )
            self.execute(
                cur,
                f"""CREATE SINK {identifier(self.database)}.{identifier(self.schema)}.sink{self.num} FROM {identifier(self.table_original)}
                    INTO KAFKA CONNECTION kafka_conn (TOPIC '{self.topic}')
                    KEY ({", ".join([identifier(key) for key in keys])})
                    FORMAT AVRO
                    USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                    ENVELOPE DEBEZIUM""",
            )
            self.execute_with_retry_on_error(
                cur,
                f"""CREATE SOURCE {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.table)}
                    FROM KAFKA CONNECTION kafka_conn (TOPIC '{self.topic}')
                    FORMAT AVRO
                    USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                    ENVELOPE DEBEZIUM""",
                wait_time_in_sec=1,
                max_tries=15,
                required_error_message_substrs=[
                    "No value schema found",
                    "Key schema is required for ENVELOPE DEBEZIUM",
                ],
            )
        self.mz_conn.autocommit = False

    def run(self, transaction: Transaction) -> None:
        with self.mz_conn.cursor() as cur:
            for row_list in transaction.row_lists:
                for row in row_list.rows:
                    key_values = tuple(
                        value
                        for field, value in zip(row.fields, row.values)
                        if field.is_key
                    )
                    if row.operation == Operation.INSERT:
                        values_str = ", ".join(
                            str(formatted_value(value)) for value in row.values
                        )
                        self.execute(
                            cur,
                            f"""INSERT INTO {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.table_original)}
                                VALUES ({values_str})
                            """,
                        )
                        self.known_keys.add(key_values)
                    elif row.operation == Operation.UPSERT:
                        if key_values in self.known_keys:
                            non_key_values = tuple(
                                (field, value)
                                for field, value in zip(row.fields, row.values)
                                if not field.is_key
                            )
                            # Can't update anything if there are no values, only a key, and the key is already in the table
                            if non_key_values:
                                cond_str = " AND ".join(
                                    f"{identifier(field.name)} = {formatted_value(value)}"
                                    for field, value in zip(row.fields, row.values)
                                    if field.is_key
                                )
                                set_str = ", ".join(
                                    f"{identifier(field.name)} = {formatted_value(value)}"
                                    for field, value in non_key_values
                                )
                                self.mz_conn.autocommit = True
                                self.execute(
                                    cur,
                                    f"""UPDATE {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.table_original)}
                                        SET {set_str}
                                        WHERE {cond_str}
                                    """,
                                )
                                self.mz_conn.autocommit = False
                        else:
                            values_str = ", ".join(
                                str(formatted_value(value)) for value in row.values
                            )
                            self.execute(
                                cur,
                                f"""INSERT INTO {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.table_original)}
                                    VALUES ({values_str})
                                """,
                            )
                            self.known_keys.add(key_values)
                    elif row.operation == Operation.DELETE:
                        cond_str = " AND ".join(
                            f"{identifier(field.name)} = {formatted_value(value)}"
                            for field, value in zip(row.fields, row.values)
                            if field.is_key
                        )
                        self.mz_conn.autocommit = True
                        self.execute(
                            cur,
                            f"""DELETE FROM {identifier(self.database)}.{identifier(self.schema)}.{identifier(self.table_original)}
                                WHERE {cond_str}
                            """,
                        )
                        self.mz_conn.autocommit = False
                        self.known_keys.discard(key_values)
                    else:
                        raise ValueError(f"Unexpected operation {row.operation}")
        self.mz_conn.commit()
