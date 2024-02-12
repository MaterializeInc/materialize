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
from concurrent import futures
from dataclasses import dataclass
from enum import Enum

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio


class Operation(Enum):
    START_SECOND_MZ = 0
    INSERT = 1


@dataclass
class Workload:
    name: str
    persist_txn_first: str = "off"
    persist_txn_second: str = "eager"
    concurrency: int = 100
    tables: int = 1
    operation = Operation.INSERT
    second_mz_delay = 5
    operation_count = 3000
    max_transaction_size = 100


@dataclass
class SuccessfulCommit:
    table_id: int
    row_id: int
    transaction_size: int


WORKLOADS = [
    Workload(
        name="off_to_eager_simple",
    ),
    Workload(
        name="off_to_lazy_simple",
        persist_txn_first="off",
        persist_txn_second="lazy",
    ),
    Workload(
        name="eager_to_lazy_simple",
        persist_txn_first="eager",
        persist_txn_second="lazy",
    ),
    Workload(
        name="eager_to_off_simple",
        persist_txn_first="eager",
        persist_txn_second="off",
    ),
    Workload(name="off_to_eager_many_tables", tables=100),
    Workload(name="off_to_eager_many_connections", concurrency=512),
    Workload(
        name="eager_to_lazy_many_tables",
        tables=100,
        persist_txn_first="eager",
        persist_txn_second="lazy",
    ),
    Workload(
        name="eager_to_lazy_many_connections",
        concurrency=512,
        persist_txn_first="eager",
        persist_txn_second="lazy",
    ),
]

CATALOG_STORES = ["stash", "persist"]

SERVICES = [
    Minio(setup_materialize=True),
    Cockroach(setup_materialize=True),
    # Overriden below
    Materialized(name="mz_first"),
    Materialized(name="mz_second"),
]


def workflow_default(c: Composition) -> None:
    """Introduce a second Mz instance while a concurrent workload is running for the purpose of exercising fencing."""
    for workload in WORKLOADS:
        for catalog_store in CATALOG_STORES:
            run_workload(c, workload, catalog_store)


def execute_operation(
    args: tuple[Composition, Workload, Operation, int]
) -> SuccessfulCommit | None:
    c, workload, operation, id = args

    if operation == Operation.START_SECOND_MZ:
        print(
            f"Will sleep {workload.second_mz_delay} before bringing up 'mz_second' ..."
        )
        time.sleep(workload.second_mz_delay)
        print("+++ Bringing up 'mz_second'...")
        c.up("mz_second")
        print("+++ 'mz_second' is now up.")
        return None
    elif operation == Operation.INSERT:
        table_id = id % workload.tables
        mz_service = random.choices(["mz_first", "mz_second"], weights=(66, 33))[0]
        transaction = random.choice([True, False])

        if transaction:
            transaction_size = random.randrange(workload.max_transaction_size) + 1
        else:
            transaction_size = 1

        try:
            cursor = c.sql_cursor(service=mz_service)
            if transaction_size > 1:
                cursor.execute("BEGIN")
                for i in range(transaction_size):
                    cursor.execute(
                        f"INSERT INTO table{table_id} VALUES ({id}, {i}, '{mz_service}')"
                    )
                cursor.execute("COMMIT")
            else:
                cursor.execute(
                    f"INSERT INTO table{table_id} VALUES ({id}, 0, '{mz_service}')"
                )
        except Exception as e:
            str_e = str(e)
            if "running docker compose failed" in str_e:
                # The query targeted a Mz container that is not up
                return None
            elif "network error" in str_e:
                # Container died while query was in progress
                return None
            elif "Can't create a connection to host" in str_e:
                # Container died before the SQL connection was established
                return None
            else:
                assert False, f"unexpected exception: {e}"

        # No error, so we assume the INSERT successfully committed
        return SuccessfulCommit(
            table_id=table_id, row_id=id, transaction_size=transaction_size
        )


def run_workload(c: Composition, workload: Workload, catalog_store: str) -> None:
    print(
        f"+++ Running workload {workload.name} with {catalog_store} catalog implementation ..."
    )
    c.silent = True

    c.down(destroy_volumes=True)
    c.up("minio", "cockroach")

    mzs = {
        "mz_first": workload.persist_txn_first,
        "mz_second": workload.persist_txn_second,
    }

    with c.override(
        *[
            Materialized(
                name=mz_name,
                external_cockroach=True,
                external_minio=True,
                sanity_restart=False,
                additional_system_parameter_defaults={
                    "persist_txn_tables": mzs[mz_name]
                },
                catalog_store=catalog_store,
            )
            for mz_name in mzs
        ]
    ):
        c.up("mz_first")

        c.sql(
            """
                ALTER SYSTEM SET max_tables = 1000;
                ALTER SYSTEM SET max_materialized_views = 1000;
            """,
            port=6877,
            user="mz_system",
            service="mz_first",
        )

        print("+++ Creating database objects ...")
        for table_id in range(workload.tables):
            c.sql(
                f"""
                    CREATE TABLE IF NOT EXISTS table{table_id}(id INTEGER, subid INTEGER, mz_service STRING);
                    CREATE MATERIALIZED VIEW view{table_id} AS SELECT DISTINCT id, subid, mz_service FROM table{table_id};
                """,
                service="mz_first",
            )

        print("+++ Running workload ...")
        start = time.time()

        # Schedule the start of the second Mz instance
        operations = [(c, workload, Operation.START_SECOND_MZ, 0)]

        # As well as all the other operations in the workload
        operations = operations + [
            (c, workload, workload.operation, id)
            for id in range(workload.operation_count)
        ]

        with futures.ThreadPoolExecutor(
            workload.concurrency,
        ) as executor:
            commits = executor.map(execute_operation, operations)

        elapsed = time.time() - start
        # The second Mz instance can come up slightly faster
        assert elapsed > (
            workload.second_mz_delay * 2
        ), f"Workload completed too soon - elapsed {elapsed}s is less than 2 x second_mz_delay({workload.second_mz_delay}s)"

        print(
            f"Workload completed in {elapsed} seconds, with second_mz_delay being {workload.second_mz_delay} seconds."
        )

        # Confirm that the first Mz has properly given up the ghost
        mz_first_log = c.invoke("logs", "mz_first", capture=True)
        assert (
            "unable to confirm leadership" in mz_first_log.stdout
            or "unexpected fence epoch" in mz_first_log.stdout
            or "fenced by new catalog upper" in mz_first_log.stdout
        )

        print("+++ Verifying committed transactions ...")
        cursor = c.sql_cursor(service="mz_second")
        for commit in commits:
            if commit is None:
                continue
            for target in ["table", "view"]:
                cursor.execute(
                    f"""
                    SELECT id, COUNT(*) AS transaction_size
                    FROM {target}{commit.table_id}
                    WHERE id = {commit.row_id}
                    GROUP BY id
                    """
                )
                result = cursor.fetchall()
                assert len(result) == 1
                assert (
                    result[0][0] == commit.row_id
                ), f"Unexpected result {result}; commit: {commit}; target {target}"
                assert (
                    result[0][1] == commit.transaction_size
                ), f"Unexpected result {result}; commit: {commit}; target {target}"

        print("Verification complete.")
