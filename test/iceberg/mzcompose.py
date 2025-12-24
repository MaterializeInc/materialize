# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition
from materialize.mzcompose.helpers.iceberg import setup_polaris_for_iceberg
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Mc, Minio
from materialize.mzcompose.services.polaris import Polaris, PolarisBootstrap
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Postgres(),
    Minio(),
    PolarisBootstrap(),
    Polaris(),
    Materialized(
        depends_on=["minio"],
        sanity_restart=False,
        system_parameter_defaults={"enable_iceberg_sink": "true"},
    ),
    Testdrive(),
    Mc(),
]


def workflow_default(c: Composition) -> None:
    # Start fresh
    c.down(destroy_volumes=True)
    c.up("postgres", "materialized")

    _, key = setup_polaris_for_iceberg(c)

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "*.td",
    )
