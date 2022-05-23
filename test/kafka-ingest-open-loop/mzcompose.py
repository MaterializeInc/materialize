# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
import time

from pg8000.dbapi import InterfaceError

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Kgen,
    Materialized,
    Testdrive,
    Zookeeper,
)

prerequisites = ["zookeeper", "kafka"]

SERVICES = [
    Zookeeper(),
    Kafka(),
    Materialized(),
    # This instance of Testdrive is used to initialize the benchmark, and needs to have a
    # known seed.
    Testdrive(no_reset=True, seed=1),
    Kgen(),
]


def query_materialize(
    c: Composition,
) -> int:
    with c.sql_cursor() as cursor:
        try:
            cursor.execute("SELECT * FROM load_test_materialization_frontier")
            row = cursor.fetchone()
            if row is None or len(row) != 1 or row[0] is None:
                return 0
            timestamp = int(row[0])
            if timestamp == 0:
                return 0
            cursor.execute(f"SELECT * FROM records_ingested AS OF {timestamp}")
            row = cursor.fetchone()
            if row is None or len(row) != 1 or row[0] is None:
                return 0
            return int(row[0])
        except InterfaceError as e:
            print(f"C> error while querying materialized: {e}")
            return 0


def send_records(
    c: Composition,
    num_records: int,
    num_keys: int,
    value_bytes: int,
    timeout_secs: int,
) -> None:
    c.run(
        "kgen",
        f"--num-records={num_records}",
        "--keys=random",
        "--key-min=0",
        f"--key-max={num_keys}",
        "--values=bytes",
        f"--max-message-size={value_bytes+1}",
        f"--min-message-size={value_bytes}",
        "--topic=testdrive-load-test-1",
        "--quiet",
    )


# This workflow runs an open loop benchmark where it tries to send a steady rate
# of records to Kafka, and tracks how many records Materialize has ingested up to.
# Crucially, the rate of insertion into Kafka is as independent as possible from the
# rate of those records are ingested into Materialize, to try to avoid the
# "coordinated ommission" problem [1].
#
# Repeated runs of this benchmark can answer the question "what's the peak messages/sec
# Materialize can ingest for this workload" where the "workload" could be "persisted
# upsert Kafka sources with 10 MM unique keys and 2kb values", and comparing the peak QPS
# observed across different such workloads (along with the corresponding dataflow timing
# info) can give insight into the relative overheads of different workloads and what
# might be causing them.
#
# [1]: https://www.scylladb.com/2021/04/22/on-coordinated-omission/
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--num-seconds",
        type=int,
        default=100,
        help="number of seconds to write records to Kafka",
    )
    parser.add_argument(
        "--records-per-second",
        type=int,
        default=10000,
        help="throughput of writes to maintain during testing",
    )
    parser.add_argument(
        "--num-keys", type=int, default=1000000000, help="number of distinct keys"
    )
    parser.add_argument(
        "--value-bytes", type=int, default=500, help="record payload size in bytes"
    )
    parser.add_argument(
        "--upsert",
        action="store_true",
        help="whether to use envelope UPSERT (True) or NONE (False)",
    )
    parser.add_argument(
        "--timeout-secs", type=int, default=120, help="timeout to send records to Kafka"
    )
    parser.add_argument(
        "--enable-persistence",
        action="store_true",
        help="whether or not to enable persistence on materialized",
    )
    parser.add_argument(
        "--s3-storage",
        type=str,
        default=None,
        help="enables s3 persist storage, pointed at the given subpath of our internal testing bucket",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="number of dataflow workers to use in materialized",
    )
    args = parser.parse_args()

    envelope = "NONE"
    if args.upsert:
        envelope = "UPSERT"

    options = []
    if args.enable_persistence:
        options = [
            "--persistent-user-tables",
            "--persistent-kafka-sources",
            "--disable-persistent-system-tables-test=true",
        ]

    if args.s3_storage == "":
        print("--s3-storage value must be non-empty", file=sys.stderr)
        sys.exit(1)
    elif args.s3_storage:
        if args.enable_persistence is not True:
            print(
                "cannot specifiy --s3-storage without --enable-persistence",
                file=sys.stderr,
            )
            sys.exit(1)
        options.extend(
            [
                "--persist-storage-enabled",
                f"--persist-storage=s3://mtlz-test-persist-1d-lifecycle-delete/{args.s3_storage}",
            ]
        )

    override = [
        Materialized(
            timestamp_frequency="1s",
            options=options,
        )
    ]

    with c.override(*override):
        c.start_and_wait_for_tcp(services=prerequisites)

        c.up("materialized")
        c.wait_for_materialized("materialized")

        c.run(
            "testdrive",
            f"--var=envelope={envelope}",
            "setup.td",
        )

        start = time.monotonic()
        records_sent = 0
        total_records_to_send = args.records_per_second * args.num_seconds
        # Maximum observed delta between records sent by the benchmark and ingested by
        # Materialize.
        max_lag = 0
        last_reported_time = 0.0

        while True:
            elapsed = time.monotonic() - start
            records_ingested = query_materialize(c)

            lag = records_sent - records_ingested

            if lag > max_lag:
                max_lag = lag

            # Report our findings back once per second.
            if elapsed - last_reported_time > 1:
                print(
                    f"C> after {elapsed:.3f}s sent {records_sent} records, and ingested {records_ingested}. max observed lag {max_lag} records, most recent lag {lag} records"
                )
                last_reported_time = elapsed

            # Determine how many records we are scheduled to send, based on how long
            # the benchmark has been running and the desired QPS.
            records_scheduled = int(
                min(elapsed, args.num_seconds) * args.records_per_second
            )
            records_to_send = records_scheduled - records_sent

            if records_to_send > 0:
                send_records(
                    c,
                    num_records=records_to_send,
                    num_keys=args.num_keys,
                    value_bytes=args.value_bytes,
                    timeout_secs=args.timeout_secs,
                )
                records_sent = records_scheduled

            # Exit once we've sent all the records we need to send, and confirmed that
            # Materialize has ingested them.
            if records_sent == total_records_to_send == records_ingested:
                print(
                    f"C> Finished after {elapsed:.3f}s sent and ingested {records_sent} records. max observed lag {max_lag} records."
                )
                break


def workflow_smoke_test(c: Composition) -> None:
    for arg in ["--upsert", "--enable-persistence"]:
        c.workflow(
            "default",
            "--num-seconds=15",
            "--records-per-second=1000",
            arg,
        )
        c.kill("materialized")
        c.rm("materialized", "testdrive", "kafka", destroy_volumes=True)
        c.rm_volumes("mzdata", "pgdata")
