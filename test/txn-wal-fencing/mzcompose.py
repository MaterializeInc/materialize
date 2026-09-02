# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Introduce a second Mz instance while a concurrent workload is running for the
purpose of exercising fencing.
"""

import argparse
import random
import threading
import time
from concurrent import futures
from dataclasses import dataclass
from enum import Enum

from materialize import buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import CockroachOrPostgresMetadata
from materialize.mzcompose.services.minio import Minio


class Operation(Enum):
    START_SECOND_MZ = 0
    INSERT = 1


@dataclass
class Workload:
    name: str
    txn_wal_first: str = "off"
    txn_wal_second: str = "eager"
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
        txn_wal_first="off",
        txn_wal_second="lazy",
    ),
    Workload(
        name="eager_to_lazy_simple",
        txn_wal_first="eager",
        txn_wal_second="lazy",
    ),
    Workload(
        name="eager_to_off_simple",
        txn_wal_first="eager",
        txn_wal_second="off",
    ),
    Workload(name="off_to_eager_many_tables", tables=100),
    Workload(name="off_to_eager_many_connections", concurrency=512),
    Workload(
        name="eager_to_lazy_many_tables",
        tables=100,
        txn_wal_first="eager",
        txn_wal_second="lazy",
    ),
    Workload(
        name="eager_to_lazy_many_connections",
        concurrency=512,
        txn_wal_first="eager",
        txn_wal_second="lazy",
    ),
]

SERVICES = [
    Minio(setup_materialize=True),
    Azurite(),
    CockroachOrPostgresMetadata(),
    # Overriden below
    Materialized(name="mz_first"),
    Materialized(name="mz_second"),
]

# Selects how a process sequences DELETE/UPDATE/INSERT ... SELECT: `false` keeps
# them on the Coordinator behind in-process write locks, `true` sequences them
# from the session task under optimistic concurrency control. The value is
# sampled once at process startup, so two processes in one environment can hold
# different values.
OCC_FLAG = "enable_adapter_frontend_occ_read_then_write"

# Observed once per read-then-write that the OCC path sequenced, so the
# histogram's sample count identifies which path a process took.
OCC_METRIC = "mz_occ_read_then_write_retry_count_count"


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--azurite", action="store_true", help="Use Azurite as blob store instead of S3"
    )
    args = parser.parse_args()

    workloads = buildkite.shard_list(WORKLOADS, lambda w: w.name)
    print(
        f"Workloads in shard with index {buildkite.get_parallelism_index()}: {[w.name for w in workloads]}"
    )

    for workload in workloads:
        run_workload(c, workload, args)


def execute_operation(
    args: tuple[Composition, Workload, Operation, int],
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
                        f"INSERT INTO table{table_id} VALUES ({id}, {i}, '{mz_service}')".encode()
                    )
                cursor.execute("COMMIT")
            else:
                cursor.execute(
                    f"INSERT INTO table{table_id} VALUES ({id}, 0, '{mz_service}')".encode()
                )
        except Exception as e:
            str_e = str(e)
            if "running docker compose failed" in str_e:
                # The query targeted a Mz container that is not up
                return None
            elif "server closed the connection unexpectedly" in str_e:
                # Container died while query was in progress
                return None
            elif "Connection refused" in str_e:
                # Container died before the SQL connection was established
                return None
            elif "Connection reset by peer" in str_e:
                # Fenced container: the port is still published but environmentd
                # has already exited, so the connection is accepted and reset.
                return None
            elif "Connection timed out" in str_e:
                # Fenced container that stopped responding mid-connect.
                return None
            else:
                raise RuntimeError(f"unexpected exception: {e}")

        # No error, so we assume the INSERT successfully committed
        return SuccessfulCommit(
            table_id=table_id, row_id=id, transaction_size=transaction_size
        )


def run_workload(c: Composition, workload: Workload, args: argparse.Namespace) -> None:
    print(f"+++ Running workload {workload.name} ...")
    c.silent = True

    c.down(destroy_volumes=True)
    c.up(c.metadata_store())

    mzs = {
        "mz_first": workload.txn_wal_first,
        "mz_second": workload.txn_wal_second,
    }

    with c.override(
        *[
            Materialized(
                name=mz_name,
                external_metadata_store=True,
                external_blob_store=True,
                blob_store_is_azure=args.azurite,
                sanity_restart=False,
                support_external_clusterd=True,
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
            "unable to advance catalog upper" in mz_first_log.stdout
            or "unexpected fence epoch" in mz_first_log.stdout
            or "fenced by new catalog upper" in mz_first_log.stdout
            or "fenced by envd" in mz_first_log.stdout
        )

        print("+++ Verifying committed transactions ...")
        cursor = c.sql_cursor(service="mz_second")
        for commit in commits:
            if commit is None:
                continue
            for target in ["table", "view"]:
                cursor.execute(f"""
                    SELECT id, COUNT(*) AS transaction_size
                    FROM {target}{commit.table_id}
                    WHERE id = {commit.row_id}
                    GROUP BY id
                    """.encode())
                result = cursor.fetchall()
                assert len(result) == 1
                assert (
                    result[0][0] == commit.row_id
                ), f"Unexpected result {result}; commit: {commit}; target {target}"
                assert (
                    result[0][1] == commit.transaction_size
                ), f"Unexpected result {result}; commit: {commit}; target {target}"

        print("Verification complete.")


# Connections driving increments.
MIXED_MODE_CONCURRENCY = 16

# The statement never reached a server, or a server refused it, so it wrote
# nothing. Keeping these apart from the indeterminate ones below matters: a
# fenced instance produces plenty, and each one widens the band the counter is
# checked against.
NOT_APPLIED = [
    # Targeted a container that is not up, or one that died before the
    # connection was established, or one that stopped responding mid-connect.
    "running docker compose failed",
    "Connection refused",
    "Connection timed out",
    "canceling statement due to statement timeout",
    # How the OCC path reports sustained contention.
    "read-then-write exceeded maximum retry attempts under contention",
]

# The connection went away with the statement in flight, so the increment may or
# may not be durable. A fenced container still publishes its port, so the
# connection is accepted and reset.
INDETERMINATE = [
    "server closed the connection unexpectedly",
    "Connection reset by peer",
]


class Increment(Enum):
    ACKED = 0
    REJECTED = 1
    UNKNOWN = 2


def occ_sequenced_writes(c: Composition, service: str) -> int:
    """How many read-then-writes `service` has sequenced through the OCC path."""
    metrics = c.exec(
        service, "curl", "--silent", "localhost:6878/metrics", capture=True
    ).stdout
    for line in metrics.splitlines():
        if line.startswith(f"{OCC_METRIC} "):
            return int(float(line.split()[1]))
    # The histogram is registered unconditionally, so a missing line means the
    # scrape itself did not land.
    raise RuntimeError(f"{OCC_METRIC} not found in {service} metrics")


def increment_counter(args: tuple[Composition, str, bool]) -> Increment:
    """Increments the shared counter by one through a read-then-write.

    `slow` stretches the read phase, so that the operation can straddle the
    moment the second instance comes up. The sleep takes its duration from the
    `delay` column rather than from a literal, so that it is not folded away at
    plan time and instead runs while the selection is read.
    """
    c, mz_service, slow = args
    sleep = " AND mz_unsafe.mz_sleep(delay) IS NULL" if slow else ""
    try:
        c.sql_cursor(service=mz_service).execute(
            f"UPDATE counter SET v = v + 1 WHERE k = 1{sleep}".encode()
        )
    except Exception as e:
        if any(msg in str(e) for msg in NOT_APPLIED):
            return Increment.REJECTED
        if any(msg in str(e) for msg in INDETERMINATE):
            return Increment.UNKNOWN
        raise RuntimeError(f"unexpected exception: {e}")
    return Increment.ACKED


def workflow_mixed_mode_read_then_write(
    c: Composition, parser: WorkflowArgumentParser
) -> None:
    """Two instances in one environment sequencing read-then-write differently.

    `OCC_FLAG` is sampled once per process, so a rolling restart or a newly added
    serving process can leave one instance on the Coordinator's write-lock path
    and another on the OCC path. The two do not synchronize: the lock path
    excludes concurrent writers, the OCC path detects them afterwards from the
    write timestamp. A blind write from the lock path landing on top of an OCC
    write leaves the row with a negative copy of the stale value and two copies
    of the new one.

    Both orderings run, because they put the lock path on opposite sides of the
    handover. Each one goes red on a lost update or a broken multiplicity, and
    also on the precondition for either: the two instances committing at the same
    time.
    """
    parser.add_argument(
        "--azurite", action="store_true", help="Use Azurite as blob store instead of S3"
    )
    args = parser.parse_args()

    # Every increment opens its own connection, so leave out the per-invocation
    # Docker Compose echo.
    c.silent = True

    for first_occ, second_occ in [("false", "true"), ("true", "false")]:
        print(
            f"+++ Running with {OCC_FLAG} {first_occ} on 'mz_first', {second_occ} on 'mz_second' ..."
        )
        run_mixed_mode(c, args.azurite, first_occ, second_occ)


def run_mixed_mode(
    c: Composition, azurite: bool, first_occ: str, second_occ: str
) -> None:
    """Runs one ordering: 'mz_first' comes up first, then 'mz_second' displaces it."""
    c.down(destroy_volumes=True)
    c.up(c.metadata_store())

    with c.override(
        *[
            Materialized(
                name=mz_name,
                external_metadata_store=True,
                external_blob_store=True,
                blob_store_is_azure=azurite,
                sanity_restart=False,
                support_external_clusterd=True,
                additional_system_parameter_defaults={OCC_FLAG: occ},
            )
            for mz_name, occ in [("mz_first", first_occ), ("mz_second", second_occ)]
        ]
    ):
        c.up("mz_first")

        # Idempotent, because a retried connection re-runs the whole batch after
        # the statements it already applied. `mz_sleep` blocks the timely worker
        # it runs on, so `delay` stays short enough that the slow increments do
        # not starve the replica.
        c.sql(
            """
            CREATE TABLE IF NOT EXISTS counter (k int, v bigint, delay double precision);
            DELETE FROM counter;
            INSERT INTO counter VALUES (1, 0, 0.5);
            """,
            service="mz_first",
        )

        print("--- Confirming the instances disagree on how to sequence")
        assert (
            increment_counter((c, "mz_first", False)) == Increment.ACKED
        ), "baseline increment on 'mz_first' did not commit"
        occ_writes = occ_sequenced_writes(c, "mz_first")
        assert (occ_writes > 0) == (first_occ == "true"), (
            f"'mz_first' sequenced {occ_writes} read-then-writes through OCC, "
            f"which does not match {OCC_FLAG}={first_occ}"
        )

        print("--- Driving increments across both instances")
        stop = threading.Event()

        def drive(worker: int) -> list[tuple[str, Increment, float, float]]:
            # Worker 0 keeps a slow read-then-write in flight on 'mz_first' for
            # the whole run, so that 'mz_second' comes up while an operation
            # there sits between its read and its write. The workers aimed at
            # 'mz_second' start before it can serve and take their rejections
            # until it can.
            mz_service = "mz_first" if worker % 2 == 0 else "mz_second"
            op = (c, mz_service, worker == 0)
            outcomes = []
            while not stop.is_set():
                issued = time.time()
                outcomes.append(
                    (mz_service, increment_counter(op), issued, time.time())
                )
            return outcomes

        with futures.ThreadPoolExecutor(MIXED_MODE_CONCURRENCY) as executor:
            drivers = [
                executor.submit(drive, worker)
                for worker in range(MIXED_MODE_CONCURRENCY)
            ]
            try:
                time.sleep(2)
                c.up("mz_second")
                # The fence lands while 'mz_second' opens the catalog, well
                # before it reports healthy, so keep driving past that point to
                # cover the handover from both sides.
                time.sleep(15)
            finally:
                stop.set()
            outcomes = [outcome for driver in drivers for outcome in driver.result()]

        # The baseline increment on 'mz_first' counts too.
        acked = 1 + sum(
            1 for _, outcome, _, _ in outcomes if outcome == Increment.ACKED
        )
        unknown = sum(
            1 for _, outcome, _, _ in outcomes if outcome == Increment.UNKNOWN
        )
        print(f"acked: {acked}, unknown: {unknown}")

        occ_writes = occ_sequenced_writes(c, "mz_second")
        assert (occ_writes > 0) == (second_occ == "true"), (
            f"'mz_second' sequenced {occ_writes} read-then-writes through OCC, "
            f"which does not match {OCC_FLAG}={second_occ}"
        )

        # Without a commit from each side the run exercised one instance only.
        acks = {
            mz_service: [
                (issued, at)
                for service, outcome, issued, at in outcomes
                if service == mz_service and outcome == Increment.ACKED
            ]
            for mz_service in ["mz_first", "mz_second"]
        }
        for mz_service, times in acks.items():
            assert times, f"'{mz_service}' committed no increment"

        # What keeps the two modes from corrupting the row today is that they
        # never commit at the same time: both paths advance the catalog upper
        # before every write, so an instance stops committing once the other has
        # fenced it, and the fence lands before the other instance reads anything.
        # Overlapping commit windows are the bug's precondition, because a
        # lock-path write can then land on top of an OCC write and leave the row
        # with a negative copy of the stale value and two copies of the new one.
        # Sampling the flag once per process does not prevent that, so a red
        # assertion here means the modes have to be fenced against each other
        # rather than merely fixed per process.
        # Comparing when 'mz_first' last *issued* a statement that went on to
        # commit against when 'mz_second' first committed keeps a slow response
        # from reading as an overlap: a statement issued after the other instance
        # had already committed cannot have committed before it.
        gap = min(at for _, at in acks["mz_second"]) - max(
            issued for issued, _ in acks["mz_first"]
        )
        print(f"'mz_first' stopped committing {gap:.1f}s before 'mz_second' started")
        assert gap > 0, (
            f"'mz_first' committed a statement it issued {-gap:.1f}s after "
            f"'mz_second' had committed, so both sequencing modes were live at once"
        )

        print("--- Verifying the counter")
        cursor = c.sql_cursor(service="mz_second")

        # A negative copy of the stale value has no rendering, so a broken
        # multiplicity surfaces here as a missing row, an extra row, or a
        # retraction error.
        cursor.execute("SELECT v FROM counter WHERE k = 1")
        rows = cursor.fetchall()
        assert len(rows) == 1, f"counter holds {rows}, expected exactly one row"

        # An acked increment is durable, one whose connection died may or may not
        # be. Anything below `acked` is a lost update.
        v = rows[0][0]
        assert (
            acked <= v <= acked + unknown
        ), f"counter is {v}, expected between {acked} and {acked + unknown}"

        # Checked last so that a lost update is reported as such rather than as a
        # missing fence. The fence bounds the window in which the two modes
        # overlap: both paths advance the catalog upper before every write, so an
        # instance stops writing once the other one has fenced it.
        log = c.invoke("logs", "mz_first", capture=True).stdout
        assert (
            "unable to advance catalog upper" in log or "fenced by envd" in log
        ), "'mz_first' was never fenced, so the two instances never overlapped"
