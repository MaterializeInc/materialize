# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
import time
from enum import Enum
from typing import List

import pg8000
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)


def idfn(d, ctx):
    return d


def delivery_report(err, msg):
    assert err is None, f"Delivery failed for User record {msg.key()}: {err}"
    # print('User record {} successfully produced to {} [{}] at offset {}'.format(
    #    msg.key(), msg.topic(), msg.partition(), msg.offset()))


class Operation(Enum):
    INSERT = 1
    UPSERT = 2
    DELETE = 3


class Row:
    key: List[str]
    value: List[str]
    operation: Operation

    def __init__(self, key: List[str], operation: Operation, value: List[str] = None):
        self.key = key
        self.value = value
        self.operation = operation

    def __repr__(self) -> str:
        return f"Row({self.key}, {self.value}, {self.operation})"


class RowList:
    rows: List[Row]
    # generator_properties

    def __init__(self, rows: List[Row]):
        self.rows = rows

    def __repr__(self) -> str:
        return f"RowList({','.join([str(row) for row in self.rows])})"


class Transaction:
    row_lists: List[RowList]

    def __init__(self, row_lists: List[RowList]):
        self.row_lists = row_lists

    def __repr__(self) -> str:
        return (
            f"Transaction({','.join([str(row_list) for row_list in self.row_lists])})"
        )


class Executor:
    def run(self, transactions: List[Transaction]):
        raise NotImplementedError


class PrintExecutor(Executor):
    def run(self, transactions: List[Transaction]):
        for transaction in transactions:
            print("Transaction:")
            print("  ", transaction.row_lists)


class KafkaExecutor(Executor):
    producer: Producer
    topic: str
    table: str

    def __init__(self, num: int, conn: pg8000.Connection):
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

        kafka_conf = {"bootstrap.servers": "kafka:9092"}

        a = AdminClient(kafka_conf)
        fs = a.create_topics(
            [NewTopic(self.topic, num_partitions=1, replication_factor=1)]
        )
        for topic, f in fs.items():
            f.result()
            print(f"Topic {topic} created")

        schema_registry_conf = {"url": "http://schema-registry:8081"}
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
                f"CREATE SOURCE {self.table} FROM KAFKA CONNECTION kafka_conn (TOPIC '{self.topic}') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn ENVELOPE UPSERT"
            )
        conn.autocommit = False

    def run(self, transactions: List[Transaction]):
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

    def __init__(self, num: int):
        self.conn = pg8000.connect(
            host="postgres", user="postgres", password="postgres"
        )
        self.table = f"table{num}"
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {self.table}")
            cur.execute(
                f"CREATE TABLE {self.table} (col1 int, col2 int, PRIMARY KEY (col1))"
            )

    def run(self, transactions: List[Transaction]):
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

    def __init__(self, num: int, conn: pg8000.Connection):
        self.conn = pg8000.connect(
            host="postgres", user="postgres", password="postgres"
        )
        self.table = f"cdc{num}"
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {self.table}")
            cur.execute(
                f"CREATE TABLE {self.table} (col1 int, col2 int, PRIMARY KEY (col1))"
            )
            cur.execute(f"ALTER TABLE {self.table} REPLICA IDENTITY FULL")
            cur.execute(f"CREATE USER postgres{num} WITH SUPERUSER PASSWORD 'postgres'")
            cur.execute(f"ALTER USER postgres{num} WITH replication")

            cur.execute("DROP PUBLICATION IF EXISTS postgres_source")
            cur.execute("CREATE PUBLICATION postgres_source FOR ALL TABLES")
        self.conn.autocommit = False

        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE SECRET pgpass{num} AS 'postgres'")
            cur.execute(
                f"CREATE CONNECTION pg{num} FOR POSTGRES HOST 'postgres', DATABASE postgres, USER postgres{num}, PASSWORD SECRET pgpass{num}"
            )
            cur.execute(
                f"CREATE SOURCE postgres_source{num} FROM POSTGRES CONNECTION pg{num} (PUBLICATION 'postgres_source') FOR TABLES ({self.table} AS {self.table});"
            )
        conn.autocommit = False

    def run(self, transactions: List[Transaction]):
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


class Records(Enum):
    ONE = 1
    MANY = 2
    ALL = 3


class RecordSize(Enum):
    TINY = 1
    SMALL = 2
    MEDIUM = 3
    LARGE = 4


class Keyspace(Enum):
    SINGLE_VALUE = 1
    LARGE = 2
    EXISTING = 3


class Target(Enum):
    KAFKA = 1
    POSTGRES = 2
    PRINT = 3


class Definition:
    def generate(self) -> List[Transaction]:
        raise NotImplementedError


class Insert(Definition):
    def __init__(self, count: Records, record_size: RecordSize):
        self.count = count
        self.record_size = record_size
        self.current_key = 1

    def generate(self) -> List[Transaction]:
        key = self.current_key
        self.current_key += 1

        if self.count == Records.ONE:
            count = 1
        elif self.count == Records.MANY:
            count = 1000
        else:
            raise ValueError(f"Unexpected count {self.count}")

        if self.record_size == RecordSize.TINY:
            value = random.randint(-127, 128)
        elif self.record_size == RecordSize.SMALL:
            value = random.randint(-32768, 32767)
        elif self.record_size == RecordSize.MEDIUM:
            value = random.randint(-2147483648, 2147483647)
        elif self.record_size == RecordSize.LARGE:
            value = random.randint(-9223372036854775808, 9223372036854775807)
        else:
            raise ValueError(f"Unexpected count {self.count}")

        transactions = []
        for i in range(count):
            transactions.append(
                Transaction(
                    [RowList([Row(key=key, value=value, operation=Operation.INSERT)])]
                )
            )

        return transactions


class Upsert(Definition):
    def __init__(self, keyspace: Keyspace, count: Records, record_size: RecordSize):
        self.keyspace = keyspace
        self.count = count
        self.record_size = record_size

    def generate(self) -> List[Transaction]:
        if self.keyspace == Keyspace.SINGLE_VALUE:
            key = 1
        elif self.keyspace == Keyspace.LARGE:
            key = random.randint(0, 1_000_000)
        else:
            raise ValueError(f"Unexpected keyspace {self.keyspace}")

        if self.count == Records.ONE:
            count = 1
        elif self.count == Records.MANY:
            count = 1000
        else:
            raise ValueError(f"Unexpected count {self.count}")

        if self.record_size == RecordSize.TINY:
            value = random.randint(-127, 128)
        elif self.record_size == RecordSize.SMALL:
            value = random.randint(-32768, 32767)
        elif self.record_size == RecordSize.MEDIUM:
            value = random.randint(-2147483648, 2147483647)
        elif self.record_size == RecordSize.LARGE:
            value = random.randint(-9223372036854775808, 9223372036854775807)
        else:
            raise ValueError(f"Unexpected count {self.count}")

        transactions = []
        for i in range(count):
            transactions.append(
                Transaction(
                    [RowList([Row(key=key, value=value, operation=Operation.UPSERT)])]
                )
            )

        return transactions


class Delete(Definition):
    def __init__(self, number_of_records: Records):
        self.number_of_records = number_of_records

    def generate(self) -> List[Transaction]:
        transactions = []

        if self.number_of_records == Records.ONE:
            raise NotImplementedError
        elif self.number_of_records == Records.MANY:
            raise NotImplementedError
        elif self.number_of_records == Records.ALL:
            for key in range(1000):
                transactions.append(
                    Transaction([RowList([Row(key=key, operation=Operation.DELETE)])])
                )
        else:
            raise ValueError(f"Unexpected number of records {self.number_of_records}")

        return transactions


class Workload:
    cycle: List[Definition]

    def generate(self) -> List[Transaction]:
        transactions = []
        for i in range(100):
            for definition in self.cycle:
                transactions.extend(definition.generate())
        return transactions


class SingleSensorUpdating(Workload):
    def __init__(self):
        self.cycle: List[Definition] = [
            Upsert(
                keyspace=Keyspace.SINGLE_VALUE,
                count=Records.ONE,
                record_size=RecordSize.SMALL,
            )
        ]


class DeleteDataAtEndOfDay(Workload):
    def __init__(self):
        self.cycle: List[Definition] = [
            Insert(
                count=Records.MANY,
                record_size=RecordSize.SMALL,
            ),
            Delete(number_of_records=Records.ALL),
        ]


class ProgressivelyEnrichRecords(Workload):
    def __init__(self):
        self.cycle: List[Definition] = [
            # TODO
        ]


def execute_workload(
    executor_classes, workload: Workload, conn: pg8000.Connection, num: int
):
    transactions = workload.generate()
    print(transactions)

    pg_executor = PgExecutor(num)
    print_executor = PrintExecutor()
    executors = [executor_class(num, conn) for executor_class in executor_classes]

    for executor in [print_executor, pg_executor] + executors:
        executor.run(transactions)

    with pg_executor.conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {pg_executor.table}")
        expected_result = cur.fetchall()
        print(f"Expected (via Postgres): {expected_result}")

    for executor in executors:
        sleep_time = 0.1
        while sleep_time < 60:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {executor.table}")
                actual_result = cur.fetchall()
            conn.autocommit = False
            print(f"{type(executor).__name__}: {actual_result}")
            if actual_result == expected_result:
                break
            print(f"Results don't match, sleeping for {sleep_time}s")
            time.sleep(sleep_time)
            sleep_time *= 2
        else:
            raise ValueError(f"Unexpected result {actual_result} != {expected_result}")


def main():
    conn = pg8000.connect(host="materialized", port=6875, user="materialize")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER 'kafka:9092'"
        )
        cur.execute(
            "CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL 'http://schema-registry:8081'"
        )
    conn.autocommit = False

    executor_classes = [KafkaExecutor, PgCdcExecutor]

    for i, workload in enumerate([SingleSensorUpdating(), DeleteDataAtEndOfDay()]):
        execute_workload(executor_classes, workload, conn, i)


main()
