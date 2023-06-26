# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum
from typing import List

import pg8000
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)


def idfn(d, ctx):
    return d


def delivery_report(err, msg):
    assert err is not None, f"Delivery failed for User record {msg.key()}: {err}"
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

    def __init__(self, key: List[str], value: List[str], operation: Operation):
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
    kafka_topic: str

    def __init__(self):
        with open(f"data-ingest/user.avsc") as f:
            schema_str = f.read()

        with open(f"data-ingest/key.avsc") as f:
            key_schema_str = f.read()

        # docker port data-ingest-schema-registry-1 8081
        schema_registry_conf = {"url": "http://schema-registry:8081/"}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        # Can call register_schema and get_schema: https://github.com/confluentinc/confluent-kafka-python/blob/842e2df13b3eebc5ae5562f050008c1932c8332d/src/confluent_kafka/schema_registry/schema_registry_client.py#L252

        avro_serializer = AvroSerializer(schema_registry_client, schema_str, idfn)

        key_avro_serializer = AvroSerializer(
            schema_registry_client, key_schema_str, idfn
        )

        # docker port data-ingest-kafka-1 9092
        producer_conf = {"bootstrap.servers": "kafka:9092"}
        producer = Producer(producer_conf)

        # Have to copy
        self.kafka_topic = "testdrive-upsert-insert-2074592892"

    def run(self, transactions: List[Transaction]):
        pass


class PgExecutor(Executor):
    conn: pg8000.Connection

    def __init__(self):
        self.conn = pg8000.connect(
            host="postgres", user="postgres", password="postgres"
        )
        #with conn.cursor() as cur:
        #    cur.execute("DROP TABLE IF EXISTS table1")
        #    cur.execute("CREATE TABLE table1 (col1 int, col2 int)")

    def run(self, transactions: List[Transaction]):
        for transaction in transactions:
            with self.conn.cursor() as cur:
                for row_list in transaction.row_lists:
                    for row in row_list.rows:
                        if row.operation == Operation.INSERT or row.operation == Operation.UPSERT:
                            #cur.execute(f"INSERT INTO table1 VALUES ({row.key}, {row.value}) ON CONFLICT (col1) DO UPDATE SET col2 = EXCLUDED.col2")
                            cur.execute("INSERT INTO table1 VALUES (1, 2) ON CONFLICT (col1) DO UPDATE SET col2 = EXCLUDED.col2")
                        elif row.operation == Operation.DELETE:
                            cur.execute(f"DELETE FROM table1 WHERE col1 = {row.key}")
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


class Target(Enum):
    KAFKA = 1
    POSTGRES = 2
    PRINT = 3


class Definition:
    def generate(self) -> Row:
        raise NotImplementedError


class Insert(Definition):
    def __init__(self, count: Records, record_size: RecordSize):
        self.count = count
        self.record_size = record_size

    def generate(self) -> Row:
        return Row(key="key1", value="value1", operation=Operation.INSERT)


class Upsert(Definition):
    def __init__(self, keyspace: Keyspace, count: Records, record_size: RecordSize):
        self.keyspace = keyspace
        self.count = count
        self.record_size = record_size

    def generate(self) -> Row:
        return Row(key="key1", value="value1", operation=Operation.UPSERT)


class Delete(Definition):
    def __init__(self, number_of_records: Records):
        self.number_of_records = number_of_records

    def generate(self) -> Row:
        return Row(key="key1", value="value1", operation=Operation.DELETE)


class Workload:
    cycle: List[Definition]

    def generate(self) -> List[Transaction]:
        transactions = []
        for definition in self.cycle:
            transactions.append(Transaction([RowList([definition.generate()])]))
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


def main():
    workload = SingleSensorUpdating()
    transactions = workload.generate()
    print(transactions)
    print_executor = PrintExecutor()
    print_executor.run(transactions)
    kafka_executor = KafkaExecutor()
    kafka_executor.run(transactions)
    pg_executor = PgExecutor()
    pg_executor.run(transactions)

    # with open(f"data-ingest/user.avsc") as f:
    #    schema_str = f.read()

    # with open(f"data-ingest/key.avsc") as f:
    #    key_schema_str = f.read()

    ## docker port data-ingest-schema-registry-1 8081
    # schema_registry_conf = {'url': "http://schema-registry:8081/"}
    # schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    ## Can call register_schema and get_schema: https://github.com/confluentinc/confluent-kafka-python/blob/842e2df13b3eebc5ae5562f050008c1932c8332d/src/confluent_kafka/schema_registry/schema_registry_client.py#L252

    # avro_serializer = AvroSerializer(schema_registry_client,
    #                                 schema_str,
    #                                 idfn)

    # key_avro_serializer = AvroSerializer(schema_registry_client,
    #                                     key_schema_str,
    #                                     idfn)

    ## docker port data-ingest-kafka-1 9092
    # producer_conf = {'bootstrap.servers': "kafka:9092"}
    # producer = Producer(producer_conf)

    # producer.poll(0.0)
    ## Have to copy
    # topic = "testdrive-upsert-insert-2074592892"
    ## 6 seconds for 1 million productions, 18k/s, should be performant enough
    ## 1 Python threads can load 15 clusterd threads at ~100% each
    # for i in range(1000):
    #    producer.produce(topic=topic,
    #                     #partition=0,
    #                     key=key_avro_serializer({"key1": f"A{i}"}, SerializationContext(topic, MessageField.KEY)),
    #                    value=avro_serializer({"f1": f"A{i*2}"}, SerializationContext(topic, MessageField.VALUE)),
    #                    on_delivery=delivery_report)
    #    if i % 100_000 == 0:
    #        producer.flush()
    # producer.flush()


main()
