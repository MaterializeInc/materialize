# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

import requests

from materialize.mzcompose import Workflow, WorkflowArgumentParser
from materialize.mzcompose.services import PrometheusSQLExporter


def workflow_demo(w: Workflow, args: List[str]) -> None:
    """Run CH-benCHmark without any load on Materialize"""

    # Parse arguments.
    parser = WorkflowArgumentParser(w)
    parser.add_argument(
        "--wait", action="store_true", help="wait for the load generator to exit"
    )
    args, unknown_args = parser.parse_known_args(args)

    # Start Materialize.
    w.start_services(services=["materialized"])
    w.wait_for_mz()

    # Start MySQL and Debezium.
    w.start_services(services=["mysql", "connect"])
    w.wait_for_tcp(host="mysql", port=3306)
    w.wait_for_tcp(host="connect", port=8083)

    # Generate initial data.
    w.run_service(
        service="chbench",
        command="gen --config-file-path=/etc/chbenchmark/mz-default-mysql.cfg --warehouses=1",
    )

    # Start Debezium.
    connect_port = w.composition.find_host_ports("connect")[0]
    response = requests.post(
        f"http://localhost:{connect_port}/connectors",
        json={
            "name": "mysql-connector",
            "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "mysql",
                "database.port": "3306",
                "database.user": "debezium",
                "database.password": "debezium",
                "database.server.name": "debezium",
                "database.server.id": "1234",
                "database.history.kafka.bootstrap.servers": "kafka:9092",
                "database.history.kafka.topic": "mysql-history",
                "database.allowPublicKeyRetrieval": "true",
                "time.precision.mode": "connect",
            },
        },
    )
    # Don't error if the connector already exists.
    if response.status_code != requests.codes.conflict:
        response.raise_for_status()

    # Run load generator.
    w.run_service(
        service="chbench",
        command=[
            "run",
            "--config-file-path=/etc/chbenchmark/mz-default-mysql.cfg",
            "--dsn=mysql",
            "--gen-dir=/var/lib/mysql-files",
            "--analytic-threads=0",
            "--transactional-threads=1",
            "--run-seconds=86400",
            "--mz-sources",
            *unknown_args,
        ],
        daemon=not args.wait,
    )


def workflow_load_test(w: Workflow) -> None:
    """Run CH-benCHmark with a selected amount of load against Materialize."""
    w.start_services(services=["prometheus-sql-exporter"])
    workflow_demo(
        w,
        [
            "--peek-conns=1",
            "--mz-views=q01,q02,q05,q06,q08,q09,q12,q14,q17,q19",
            "--transactional-threads=2",
        ],
    )


services = [
    PrometheusSQLExporter(),
]
