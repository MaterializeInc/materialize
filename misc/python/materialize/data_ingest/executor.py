# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from typing import Any, Dict, List, Optional

import pg8000
from confluent_kafka import Producer  # type: ignore
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient  # type: ignore
from confluent_kafka.schema_registry.avro import AvroSerializer  # type: ignore
from confluent_kafka.serialization import (  # type: ignore
    MessageField,
    SerializationContext,
)

from materialize.data_ingest.data_type import Backend
from materialize.data_ingest.field import Field
from materialize.data_ingest.row import Operation
from materialize.data_ingest.transaction import Transaction


class Executor:
    num_transactions: int

    def __init__(self) -> None:
        self.num_transactions = 0

    def run(self, transaction: Transaction) -> None:
        raise NotImplementedError

    def print_progress(self) -> None:
        if self.num_transactions % 100 == 0:
            print(f"{type(self).__name__}: {self.num_transactions}")
        self.num_transactions += 1

    def execute(self, cur: pg8000.Cursor, query: str) -> None:
        try:
            cur.execute(query)
        except:
            print(f"Query failed: {query}")
            raise


class PrintExecutor(Executor):
    def run(self, transaction: Transaction) -> None:
        print("Transaction:")
        print("  ", transaction.row_lists)


def delivery_report(err: str, msg: Any) -> None:
    assert err is None, f"Delivery failed for User record {msg.key()}: {err}"


class KafkaExecutor(Executor):
    producer: Producer
    avro_serializer: AvroSerializer
    key_avro_serializer: AvroSerializer
    topic: str
    table: str
    fields: List[Field]

    def __init__(
        self,
        num: int,
        conn: pg8000.Connection,
        ports: Dict[str, int],
        fields: List[Field],
    ):
        super().__init__()
        self.topic = f"data-ingest-{num}"
        self.table = f"kafka_table{num}"
        self.fields = fields

        schema = {
            "type": "record",
            "name": "value",
            "fields": [
                {
                    "name": field.name,
                    "type": str(field.data_type.name(Backend.AVRO)).lower(),
                }
                for field in fields
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
                for field in fields
                if field.is_key
            ],
        }

        kafka_conf = {"bootstrap.servers": f"localhost:{ports['kafka']}"}

        a = AdminClient(kafka_conf)
        fs = a.create_topics(
            [NewTopic(self.topic, num_partitions=1, replication_factor=1)]
        )
        for topic, f in fs.items():
            f.result()
            print(f"Topic {topic} created")

        schema_registry_conf = {"url": f"http://localhost:{ports['schema-registry']}"}
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

        self.producer = Producer(kafka_conf)

        conn.autocommit = True
        with conn.cursor() as cur:
            self.execute(
                cur,
                f"""CREATE SOURCE {self.table}
                    FROM KAFKA CONNECTION kafka_conn (TOPIC '{self.topic}')
                    FORMAT AVRO
                    USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                    ENVELOPE UPSERT""",
            )
        conn.autocommit = False

    def run(self, transaction: Transaction) -> None:
        self.print_progress()
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
                                field.name: field.value
                                for field in row.fields
                                if field.is_key
                            },
                            SerializationContext(self.topic, MessageField.KEY),
                        ),
                        value=self.avro_serializer(
                            {
                                field.name: field.value
                                for field in row.fields
                                if not field.is_key
                            },
                            SerializationContext(self.topic, MessageField.VALUE),
                        ),
                        on_delivery=delivery_report,
                    )
                elif row.operation == Operation.DELETE:
                    self.producer.produce(
                        topic=self.topic,
                        key=self.key_avro_serializer(
                            {
                                field.name: field.value
                                for field in row.fields
                                if field.is_key
                            },
                            SerializationContext(self.topic, MessageField.KEY),
                        ),
                        value=None,
                        on_delivery=delivery_report,
                    )
                else:
                    raise ValueError(f"Unexpected operation {row.operation}")
        self.producer.flush()


class PgExecutor(Executor):
    conn: pg8000.Connection
    table: str

    def __init__(
        self,
        num: int,
        conn: Optional[pg8000.Connection],
        ports: Dict[str, int],
        fields: List[Field],
    ):
        super().__init__()
        self.conn = pg8000.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=ports["postgres"],
        )
        self.table = f"cdc{num}" if conn else f"pg{num}"

        values = [
            f"{field.name} {str(field.data_type.name(Backend.POSTGRES)).lower()}"
            for field in fields
        ]
        keys = [field.name for field in fields if field.is_key]

        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            self.execute(
                cur,
                f"""DROP TABLE IF EXISTS {self.table};
                    CREATE TABLE {self.table} (
                        {", ".join(values)},
                        PRIMARY KEY ({", ".join(keys)}));
                    ALTER TABLE {self.table} REPLICA IDENTITY FULL;""",
            )
            if conn:
                self.execute(
                    cur,
                    f"""CREATE USER postgres{num} WITH SUPERUSER PASSWORD 'postgres';
                    ALTER USER postgres{num} WITH replication;
                    DROP PUBLICATION IF EXISTS postgres_source;
                    CREATE PUBLICATION postgres_source FOR ALL TABLES;""",
                )
        self.conn.autocommit = False

        if conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                self.execute(cur, f"CREATE SECRET pgpass{num} AS 'postgres'")
                self.execute(
                    cur,
                    f"""CREATE CONNECTION pg{num} FOR POSTGRES
                        HOST 'postgres',
                        DATABASE postgres,
                        USER postgres{num},
                        PASSWORD SECRET pgpass{num}""",
                )
                self.execute(
                    cur,
                    f"""CREATE SOURCE postgres_source{num}
                        FROM POSTGRES CONNECTION pg{num} (PUBLICATION 'postgres_source')
                        FOR TABLES ({self.table} AS {self.table})""",
                )
            conn.autocommit = False

    def run(self, transaction: Transaction) -> None:
        self.print_progress()
        with self.conn.cursor() as cur:
            for row_list in transaction.row_lists:
                for row in row_list.rows:
                    if row.operation == Operation.INSERT:
                        values_str = ", ".join(
                            str(field.formatted_value()) for field in row.fields
                        )
                        keys_str = ", ".join(
                            field.name for field in row.fields if field.is_key
                        )
                        self.execute(
                            cur,
                            f"""INSERT INTO {self.table}
                                VALUES ({values_str})
                            """,
                        )
                    elif row.operation == Operation.UPSERT:
                        values_str = ", ".join(
                            str(field.formatted_value()) for field in row.fields
                        )
                        keys_str = ", ".join(
                            field.name for field in row.fields if field.is_key
                        )
                        update_str = ", ".join(
                            f"{field.name} = EXCLUDED.{field.name}"
                            for field in row.fields
                        )
                        self.execute(
                            cur,
                            f"""INSERT INTO {self.table}
                                VALUES ({values_str})
                                ON CONFLICT ({keys_str})
                                DO UPDATE SET {update_str}
                            """,
                        )
                    elif row.operation == Operation.DELETE:
                        cond_str = " AND ".join(
                            f"{field.name} = {field.formatted_value()}"
                            for field in row.fields
                            if field.is_key
                        )
                        self.execute(
                            cur,
                            f"""DELETE FROM {self.table}
                                WHERE {cond_str}
                            """,
                        )
                    else:
                        raise ValueError(f"Unexpected operation {row.operation}")
        self.conn.commit()
