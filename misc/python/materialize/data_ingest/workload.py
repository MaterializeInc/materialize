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
import traceback
from collections.abc import Iterator
from typing import Any

import psycopg

from materialize.data_ingest.data_type import DATA_TYPES_FOR_AVRO, DATA_TYPES_FOR_KEY
from materialize.data_ingest.definition import (
    Delete,
    Insert,
    Keyspace,
    Records,
    RecordSize,
    Upsert,
)
from materialize.data_ingest.executor import (
    PgExecutor,
    PrintExecutor,
)
from materialize.data_ingest.field import Field
from materialize.data_ingest.transaction import Transaction
from materialize.data_ingest.transaction_def import (
    RestartMz,
    TransactionDef,
    TransactionSize,
    ZeroDowntimeDeploy,
)
from materialize.mzcompose.composition import Composition
from materialize.util import all_subclasses


class Workload:
    cycle: list[TransactionDef]
    mz_service: str
    deploy_generation: int

    def __init__(
        self, mz_service: str = "materailized", deploy_generation: int = 0
    ) -> None:
        self.mz_service = mz_service
        self.deploy_generation = deploy_generation

    def generate(self, fields: list[Field]) -> Iterator[Transaction]:
        while True:
            for transaction_def in self.cycle:
                for transaction in transaction_def.generate(fields):
                    if transaction:
                        yield transaction


class SingleSensorUpdating(Workload):
    def __init__(
        self,
        composition: Composition | None = None,
        mz_service: str = "materialized",
        deploy_generation: int = 0,
    ) -> None:
        super().__init__(mz_service, deploy_generation)
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


class SingleSensorUpdatingDisruptions(Workload):
    def __init__(
        self,
        composition: Composition | None = None,
        mz_service: str = "materialized",
        deploy_generation: int = 0,
    ) -> None:
        super().__init__(mz_service, deploy_generation)
        self.cycle = [
            TransactionDef(
                [
                    Upsert(
                        keyspace=Keyspace.SINGLE_VALUE,
                        count=Records.ONE,
                        record_size=RecordSize.SMALL,
                    ),
                ]
            ),
        ]
        if composition:
            self.cycle.append(RestartMz(composition, probability=0.1, workload=self))


class SingleSensorUpdating0dtDeploy(Workload):
    def __init__(
        self,
        composition: Composition | None = None,
        mz_service: str = "materialized",
        deploy_generation: int = 0,
    ) -> None:
        super().__init__(mz_service, deploy_generation)
        self.cycle = [
            TransactionDef(
                [
                    Upsert(
                        keyspace=Keyspace.SINGLE_VALUE,
                        count=Records.ONE,
                        record_size=RecordSize.SMALL,
                    ),
                ]
            ),
        ]
        if composition:
            self.cycle.append(
                ZeroDowntimeDeploy(composition, probability=0.1, workload=self)
            )


class DeleteDataAtEndOfDay(Workload):
    def __init__(
        self,
        composition: Composition | None = None,
        mz_service: str = "materialized",
        deploy_generation: int = 0,
    ) -> None:
        super().__init__(mz_service, deploy_generation)
        insert = Insert(
            count=Records.SOME,
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


class DeleteDataAtEndOfDayDisruptions(Workload):
    def __init__(
        self,
        composition: Composition | None = None,
        mz_service: str = "materialized",
        deploy_generation: int = 0,
    ) -> None:
        super().__init__(mz_service, deploy_generation)
        insert = Insert(
            count=Records.SOME,
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

        if composition:
            self.cycle.append(RestartMz(composition, probability=0.1, workload=self))


class DeleteDataAtEndOfDay0dtDeploys(Workload):
    def __init__(
        self,
        composition: Composition | None = None,
        mz_service: str = "materialized",
        deploy_generation: int = 0,
    ) -> None:
        super().__init__(mz_service, deploy_generation)
        insert = Insert(
            count=Records.SOME,
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

        if composition:
            self.cycle.append(
                ZeroDowntimeDeploy(composition, probability=0.1, workload=self)
            )


# TODO: Implement
# class ProgressivelyEnrichRecords(Workload):
#    def __init__(
#        self, composition: Composition | None = None, mz_service: str = "materialized", deploy_generation: int = 0
#    ) -> None:
#        super().__init__(mz_service, deploy_generation)
#        self.cycle: list[Definition] = [
#        ]


WORKLOADS = all_subclasses(Workload)


def execute_workload(
    executor_classes: list[Any],
    workload: Workload,
    num: int,
    ports: dict[str, int],
    runtime: int,
    verbose: bool,
) -> None:
    fields = []

    for i in range(random.randint(1, 10)):
        fields.append(Field(f"key{i}", random.choice(DATA_TYPES_FOR_KEY), True))
    for i in range(random.randint(0, 20)):
        fields.append(Field(f"value{i}", random.choice(DATA_TYPES_FOR_AVRO), False))
    print(f"With fields: {fields}")

    executors = [
        executor_class(
            num,
            ports,
            fields,
            "materialize",
            mz_service=workload.mz_service,
            cluster="singlereplica" if executor_class == PgExecutor else "quickstart",
        )
        for executor_class in [PgExecutor] + executor_classes
    ]
    pg_executor = executors[0]

    start = time.time()

    run_executors = ([PrintExecutor(ports)] if verbose else []) + executors
    for exe in run_executors:
        exe.create()
    for i, transaction in enumerate(workload.generate(fields)):
        duration = time.time() - start
        if duration > runtime:
            print(f"Ran {i} transactions in {duration} s")
            assert i > 0
            break
        for executor in run_executors:
            executor.mz_service = workload.mz_service
            executor.run(transaction)

    order_str = ", ".join(str(i + 1) for i in range(len(fields)))

    with pg_executor.pg_conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {pg_executor.table} ORDER BY {order_str}".encode())
        expected_result = cur.fetchall()
        print(f"Expected (via Postgres): {expected_result}")

    # Reconnect as Mz disruptions may have destroyed the previous connection
    conn = psycopg.connect(
        host="localhost",
        port=ports[workload.mz_service],
        user="materialize",
        dbname="materialize",
    )

    for executor in executors:
        executor.mz_service = workload.mz_service
        conn.autocommit = True
        correct_once = False
        sleep_time = 0.1
        # TODO: Reenable RTR when database-issues#8657 is fixed
        while sleep_time < 60:
            conn.autocommit = True
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"SELECT * FROM {executor.table} ORDER BY {order_str}".encode()
                    )
                except:
                    print(f"Comparing against {type(executor).__name__} failed")
                    print(traceback.format_exc())
                    raise
                actual_result = cur.fetchall()
            conn.autocommit = False
            if actual_result == expected_result:
                if correct_once:
                    break
                print(
                    "Results match. Check for correctness again to make sure the result is stable"
                )
                correct_once = True
                time.sleep(sleep_time)
                continue
            else:
                print(f"Unexpected ({type(executor).__name__}): {actual_result}")
            print(f"Results don't match, sleeping for {sleep_time}s")
            time.sleep(sleep_time)
            sleep_time *= 2
        else:
            raise ValueError(f"Unexpected result for {type(executor).__name__}: {actual_result} != {expected_result}")  # type: ignore
