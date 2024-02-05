# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(
        additional_system_parameter_defaults={
            "log_filter": "mz_storage::source::mysql=trace,info"
        }
    ),
    MySql(
        additional_args=[
            "--log-bin=mysql-bin",
            "--gtid_mode=ON",
            "--enforce_gtid_consistency=ON",
            "--binlog-format=row",
            "--log-slave-updates",
            "--binlog-row-image=full",
            "--server-id=1",
        ]
    ),
    MySql(
        name="mysql-replica",
        additional_args=[
            "--gtid_mode=ON",
            "--enforce_gtid_consistency=ON",
            "--skip-replica-start",
            "--server-id=2",
        ],
    ),
    Testdrive(default_timeout="60s"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )
    args = parser.parse_args()

    c.up("materialized", "mysql")

    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        *args.filter,
    )


def workflow_replica_connection(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.up("materialized", "mysql", "mysql-replica")
    c.run_testdrive_files(
        f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        "override/10-replica-connection.td",
    )
