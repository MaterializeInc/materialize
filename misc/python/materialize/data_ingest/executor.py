# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from typing import Any, Dict, List

import pg8000
from confluent_kafka import Producer  # type: ignore
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient  # type: ignore
from confluent_kafka.schema_registry.avro import AvroSerializer  # type: ignore
from confluent_kafka.serialization import (  # type: ignore
    MessageField,
    SerializationContext,
)

from materialize.data_ingest.row import Operation
from materialize.data_ingest.transaction import Transaction


class Executor:
    def run(self, transactions: List[Transaction]) -> None:
        raise NotImplementedError


class PrintExecutor(Executor):
    def run(self, transactions: List[Transaction]) -> None:
        for transaction in transactions:
            print("Transaction:")
            print("  ", transaction.row_lists)


def idfn(d: str, ctx: Any) -> str:
    return d


def delivery_report(err: str, msg: Any) -> None:
    assert err is None, f"Delivery failed for User record {msg.key()}: {err}"
    # print('User record {} successfully produced to {} [{}] at offset {}'.format(
    #    msg.key(), msg.topic(), msg.partition(), msg.offset()))


class KafkaExecutor(Executor):
    producer: Producer
    topic: str
    table: str

    def __init__(self, num: int, conn: pg8000.Connection, ports: Dict[str, int]):
        self.topic = f"data-ingest-{num}"
        self.table = f"kafka_table{num}"

        schema = {
            "type": "record",
            "name": "test",
            "fields": [
                # {"name":"f1", "type": "string"}
                {"name": "f1", "type": "int"}
            ],
        }

        key_schema = {
            "type": "record",
            "name": "Key",
            "fields": [
                # {"name": "key1", "type": "string"}
                {"name": "key1", "type": "int"}
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

        self.avro_serializer = AvroSerializer(registry, json.dumps(schema), idfn)

        self.key_avro_serializer = AvroSerializer(
            registry, json.dumps(key_schema), idfn
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
            cur.execute(
                f"""CREATE SOURCE {self.table}
                    FROM KAFKA CONNECTION kafka_conn (TOPIC '{self.topic}')
                    FORMAT AVRO
                    USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                    ENVELOPE UPSERT"""
            )
        conn.autocommit = False

    def run(self, transactions: List[Transaction]) -> None:
        for transaction in transactions:
            for row_list in transaction.row_lists:
                for row in row_list.rows:
                    if (
                        row.operation == Operation.INSERT
                        or row.operation == Operation.UPSERT
                    ):
                        self.producer.produce(
                            topic=self.topic,
                            key=self.key_avro_serializer(
                                {"key1": row.key},
                                SerializationContext(self.topic, MessageField.KEY),
                            ),
                            value=self.avro_serializer(
                                {"f1": row.value},
                                SerializationContext(self.topic, MessageField.VALUE),
                            ),
                            on_delivery=delivery_report,
                        )
                    elif row.operation == Operation.DELETE:
                        self.producer.produce(
                            topic=self.topic,
                            key=self.key_avro_serializer(
                                {"key1": row.key},
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

    def __init__(self, num: int, ports: Dict[str, int]):
        self.conn = pg8000.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=ports["postgres"],
        )
        self.table = f"table{num}"
        with self.conn.cursor() as cur:
            cur.execute(
                f"""DROP TABLE IF EXISTS {self.table};
                    CREATE TABLE {self.table} (col1 int, col2 int, PRIMARY KEY (col1))"""
            )

    def run(self, transactions: List[Transaction]) -> None:
        for transaction in transactions:
            with self.conn.cursor() as cur:
                for row_list in transaction.row_lists:
                    for row in row_list.rows:
                        if (
                            row.operation == Operation.INSERT
                            or row.operation == Operation.UPSERT
                        ):
                            cur.execute(
                                f"INSERT INTO {self.table} VALUES ({row.key}, {row.value}) ON CONFLICT (col1) DO UPDATE SET col2 = EXCLUDED.col2"
                            )
                        elif row.operation == Operation.DELETE:
                            cur.execute(
                                f"DELETE FROM {self.table} WHERE col1 = {row.key}"
                            )
                        else:
                            raise ValueError(f"Unexpected operation {row.operation}")
        self.conn.commit()


class PgCdcExecutor(Executor):
    conn: pg8000.Connection
    table: str

    def __init__(self, num: int, conn: pg8000.Connection, ports: Dict[str, int]):
        self.conn = pg8000.connect(
            host="localhost",
            user="postgres",
            password="postgres",
            port=ports["postgres"],
        )
        self.table = f"cdc{num}"
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            cur.execute(
                f"""DROP TABLE IF EXISTS {self.table};
                    CREATE TABLE {self.table} (col1 int, col2 int, PRIMARY KEY (col1));
                    ALTER TABLE {self.table} REPLICA IDENTITY FULL;
                    CREATE USER postgres{num} WITH SUPERUSER PASSWORD 'postgres';
                    ALTER USER postgres{num} WITH replication;
                    DROP PUBLICATION IF EXISTS postgres_source;
                    CREATE PUBLICATION postgres_source FOR ALL TABLES;"""
            )
        self.conn.autocommit = False

        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE SECRET pgpass{num} AS 'postgres'")
            cur.execute(
                f"""CREATE CONNECTION pg{num} FOR POSTGRES
                    HOST 'postgres',
                    DATABASE postgres,
                    USER postgres{num},
                    PASSWORD SECRET pgpass{num}"""
            )
            cur.execute(
                f"""CREATE SOURCE postgres_source{num}
                    FROM POSTGRES CONNECTION pg{num} (PUBLICATION 'postgres_source')
                    FOR TABLES ({self.table} AS {self.table})"""
            )
        conn.autocommit = False

    def run(self, transactions: List[Transaction]) -> None:
        for transaction in transactions:
            with self.conn.cursor() as cur:
                for row_list in transaction.row_lists:
                    for row in row_list.rows:
                        if (
                            row.operation == Operation.INSERT
                            or row.operation == Operation.UPSERT
                        ):
                            cur.execute(
                                f"""INSERT INTO {self.table}
                                    VALUES ({row.key}, {row.value})
                                    ON CONFLICT (col1) DO
                                        UPDATE SET col2 = EXCLUDED.col2"""
                            )
                        elif row.operation == Operation.DELETE:
                            cur.execute(
                                f"DELETE FROM {self.table} WHERE col1 = {row.key}"
                            )
                        else:
                            raise ValueError(f"Unexpected operation {row.operation}")
        self.conn.commit()
