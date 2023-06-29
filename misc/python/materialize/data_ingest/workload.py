# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from typing import Any, Dict, List

import pg8000

from materialize.data_ingest.definition import (
    Definition,
    Delete,
    Insert,
    Keyspace,
    Records,
    RecordSize,
    Upsert,
)
from materialize.data_ingest.executor import PgExecutor, PrintExecutor
from materialize.data_ingest.transaction import Transaction


class Workload:
    cycle: List[Definition]

    def generate(self) -> List[Transaction]:
        transactions = []
        for i in range(100):
            for definition in self.cycle:
                transactions.extend(definition.generate())
        return transactions


class SingleSensorUpdating(Workload):
    def __init__(self) -> None:
        self.cycle: List[Definition] = [
            Upsert(
                keyspace=Keyspace.SINGLE_VALUE,
                count=Records.ONE,
                record_size=RecordSize.SMALL,
            )
        ]


class DeleteDataAtEndOfDay(Workload):
    def __init__(self) -> None:
        self.cycle: List[Definition] = [
            Insert(
                count=Records.MANY,
                record_size=RecordSize.SMALL,
            ),
            Delete(number_of_records=Records.ALL),
        ]


class ProgressivelyEnrichRecords(Workload):
    def __init__(self) -> None:
        self.cycle: List[Definition] = [
            # TODO
        ]


def execute_workload(
    executor_classes: Any,
    workload: Workload,
    conn: pg8000.Connection,
    num: int,
    ports: Dict[str, int],
) -> None:
    transactions = workload.generate()
    print(transactions)

    pg_executor = PgExecutor(num, ports)
    print_executor = PrintExecutor()
    executors = [
        executor_class(num, conn, ports) for executor_class in executor_classes
    ]

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
