# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from typing import List

import pg8000

from materialize import ui
from materialize.mzcompose import (
    Kafka,
    Kgen,
    Materialized,
    Testdrive,
    Workflow,
    WorkflowArgumentParser,
    Zookeeper,
    say,
)
from materialize.ui import UIError

mz_persist = Materialized(
    name="materialized_persist",
    hostname="materialized",
    options="--persistent-user-tables --persistent-kafka-upsert-source --disable-persistent-system-tables-test",
)

mz_default = Materialized()

prerequisites = ["zookeeper", "kafka"]

services = [
    Zookeeper(),
    Kafka(),
    mz_default,
    mz_persist,
    # This instance of Testdrive is used to initialize the benchmark, and needs to have a
    # known seed.
    Testdrive(no_reset=True, seed=1),
    Kgen(),
]


def kafka_upsert_benchmark_step(
    w: Workflow,
    mz_service: str,
    restart_mz: bool,
    records_sent: int,
    num_records: int,
    num_keys: int,
    value_bytes: int,
    timeout_secs: int,
):
    if restart_mz:
        w.kill_services(services=[mz_service], signal="SIGKILL")
    w.run_service(
        service="kgen",
        command=" ".join(
            [
                f"--num-records={num_records}",
                "--keys=random",
                "--key-min=0",
                f"--key-max={num_keys}",
                "--values=bytes",
                f"--max-message-size={value_bytes+1}",
                f"--min-message-size={value_bytes}",
                "--topic=testdrive-load-test-1",
                "--quiet",
            ]
        ),
    )

    if restart_mz:
        w.start_services(services=[mz_service])
        w.wait_for_mz(service=mz_service)
    expected = records_sent + num_records

    host_port = int(w.composition.find_host_ports(mz_service)[0])
    query = "SELECT * FROM num_records_ingested"
    ui.progress(f"waiting for materialize to handle {query!r}", "C")
    error = None
    start_time = time.monotonic()

    # Default to polling Materialize once a second because that is the current default
    # timestamp frequency and polling any more frequently doesn't give new data.
    for remaining in ui.timeout_loop(timeout_secs):
        try:
            conn = pg8000.connect(
                host="localhost", port=host_port, user="materialize", timeout=1
            )
            conn.autocommit = True

            cursor = conn.cursor()
            cursor.execute("SELECT * FROM num_records_ingested")
            n = cursor.fetchone()[0]
            curr_time = time.monotonic()
            elapsed = curr_time - start_time
            if n == expected:
                say(f"query result: {n} after {elapsed}")
                ui.progress("success!", finish=True)
                return
            else:
                say(f"materialize has not ingested {expected} records, got: {n}")
        except Exception as e:
            ui.progress(" " + str(int(remaining)))
            error = e
    ui.progress(finish=True)
    raise UIError(f"never got correct result {expected}: {error}")


def workflow_kafka_upsert_benchmark(w: Workflow, args: List[str]):
    parser = WorkflowArgumentParser(w)
    parser.add_argument("--num-steps", type=int, default=10)
    parser.add_argument("--records-per-step", type=int, default=50000)
    parser.add_argument("--num-keys", type=int, default=1000000)
    parser.add_argument("--value-bytes", type=int, default=500)
    parser.add_argument("--initial-records", type=int, default=200000)
    parser.add_argument("--timeout-secs", type=int, default=120)
    parser.add_argument("--restart-mz", action="store_true")
    parser.add_argument("--enable-persistence", action="store_true")
    args = parser.parse_args(args)

    w.start_and_wait_for_tcp(services=prerequisites)
    mz_service = "materialized"
    if args.enable_persistence:
        mz_service = "materialized_persist"

    w.start_services(services=[mz_service])
    w.wait_for_mz(service=mz_service)

    w.run_service(
        service="testdrive-svc",
        command="setup.td",
    )

    kafka_upsert_benchmark_step(
        w,
        mz_service,
        True,
        0,
        args.initial_records,
        args.num_keys,
        args.value_bytes,
        args.timeout_secs,
    )
    records_sent = args.initial_records

    for i in range(0, args.num_steps):
        kafka_upsert_benchmark_step(
            w,
            mz_service,
            args.restart_mz,
            records_sent,
            args.records_per_step,
            args.num_keys,
            args.value_bytes,
            args.timeout_secs,
        )
        records_sent += args.records_per_step
