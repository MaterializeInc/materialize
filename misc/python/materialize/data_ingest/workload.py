# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time
from copy import deepcopy
from typing import Any, Dict, Iterator, List

import pg8000

from materialize.data_ingest.data_type import (
    DoubleType,
    FloatType,
    IntType,
    LongType,
    StringType,
)
from materialize.data_ingest.definition import (
    Delete,
    Insert,
    Keyspace,
    Records,
    RecordSize,
    Upsert,
)
from materialize.data_ingest.executor import PgExecutor, PrintExecutor
from materialize.data_ingest.field import Field
from materialize.data_ingest.transaction import Transaction
from materialize.data_ingest.transaction_def import TransactionDef, TransactionSize


class Workload:
    cycle: List[TransactionDef]

    def generate(self, fields: List[Field]) -> Iterator[Transaction]:
        while True:
            cycle = deepcopy(self.cycle)
            for transaction_def in cycle:
                for transaction in transaction_def.generate(fields):
                    yield transaction


class SingleSensorUpdating(Workload):
    def __init__(self) -> None:
        self.cycle = [
            TransactionDef(
                [
                    Upsert(
                        keyspace=Keyspace.SINGLE_VALUE,
                        count=Records.ONE,
                        record_size=RecordSize.SMALL,
                    )
                ]
            )
        ]


class DeleteDataAtEndOfDay(Workload):
    def __init__(self) -> None:
        insert = Insert(
            count=Records.ONE,
            record_size=RecordSize.SMALL,
        )
        insert_phase = TransactionDef(
            size=TransactionSize.HUGE,
            operations=[insert],
        )
        # Delete all records in a single transaction
        delete_phase = TransactionDef(
            [
                Delete(
                    number_of_records=Records.ALL,
                    record_size=RecordSize.SMALL,
                    num=insert.max_key(),
                )
            ]
        )
        self.cycle = [
            insert_phase,
            delete_phase,
        ]


# TODO: Implement
# class ProgressivelyEnrichRecords(Workload):
#    def __init__(self) -> None:
#        self.cycle: List[Definition] = [
#        ]


# TODO: Disruptions in workloads


def execute_workload(
    executor_classes: Any,
    workload: Workload,
    conn: pg8000.Connection,
    num: int,
    ports: Dict[str, int],
    runtime: int,
    verbose: bool,
) -> None:
    fields = []

    types = (StringType, IntType, LongType, FloatType, DoubleType)

    for i in range(random.randint(1, 10)):
        fields.append(Field(f"key{i}", random.choice(types), True))
    for i in range(random.randint(0, 20)):
        fields.append(Field(f"value{i}", random.choice(types), False))
    print(f"With fields: {fields}")

    executors = [
        executor_class(num, conn, ports, fields) for executor_class in executor_classes
    ]
    pg_executor = PgExecutor(num, conn=None, ports=ports, fields=fields)

    start = time.time()

    run_executors = ([PrintExecutor()] if verbose else []) + [pg_executor] + executors
    for i, transaction in enumerate(workload.generate(fields)):
        duration = time.time() - start
        if duration > runtime:
            print(f"Ran {i} transactions in {duration} s")
            assert i > 0
            break
        for executor in run_executors:
            executor.run(transaction)

    order_str = ", ".join(str(i + 1) for i in range(len(fields)))

    with pg_executor.conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {pg_executor.table} ORDER BY {order_str}")
        expected_result = cur.fetchall()
        print(f"Expected (via Postgres): {expected_result}")

    for executor in executors:
        correct_once = False
        sleep_time = 0.1
        while sleep_time < 60:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {executor.table} ORDER BY {order_str}")
                actual_result = cur.fetchall()
            conn.autocommit = False
            if actual_result == expected_result:
                if correct_once:
                    break
                print("Check for correctness again to make sure the result is stable")
                correct_once = True
                time.sleep(sleep_time)
                continue
            else:
                print(f"Unexpected ({type(executor).__name__}): {actual_result}")
            print(f"Results don't match, sleeping for {sleep_time}s")
            time.sleep(sleep_time)
            sleep_time *= 2
        else:
            raise ValueError(f"Unexpected result {actual_result} != {expected_result}")
