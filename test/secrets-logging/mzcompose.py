# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Various ways of putting simulated secret data into Materialize, make sure it doesn't end up in any logs.
"""

import glob

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.azurite import Azurite
from materialize.mzcompose.services.fivetran_destination import FivetranDestination
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Redpanda(),
    Postgres(),
    MySql(),
    Azurite(),
    Mz(app_password=""),
    Minio(setup_materialize=True, additional_directories=["copytos3"]),
    Materialized(),
    FivetranDestination(volumes_extra=["tmp:/share/tmp"]),
    Testdrive(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Run testdrive."""
    parser.add_argument(
        "files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )
    (args, passthrough_args) = parser.parse_known_args()

    dependencies = [
        "fivetran-destination",
        "materialized",
        "postgres",
        "mysql",
        "minio",
        "zookeeper",
        "kafka",
        "schema-registry",
    ]
    c.up(*dependencies)

    def process(file: str) -> None:
        c.run_testdrive_files(
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            *passthrough_args,
            file,
            quiet=True,  # Don't print out the secret here
        )

    files = buildkite.shard_list(
        sorted(
            [
                file
                for pattern in args.files
                for file in glob.glob(
                    pattern, root_dir=MZ_ROOT / "test" / "secrets-logging"
                )
            ]
        ),
        lambda file: file,
    )
    c.test_parts(files, process)
    c.sanity_restart_mz()
