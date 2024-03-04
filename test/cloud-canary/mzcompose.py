# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import ssl
import time
import urllib.parse

import pg8000

from materialize.mzcompose import (
    _wait_for_pg,
)
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.ui import UIError

REGION = "aws/us-east-1"
ENVIRONMENT = os.getenv("ENVIRONMENT", "staging")
USERNAME = os.getenv("NIGHTLY_CANARY_USERNAME", "infra+nightly-canary@materialize.com")
APP_PASSWORD = os.environ["NIGHTLY_CANARY_APP_PASSWORD"]
VERSION = "devel-" + os.environ["BUILDKITE_COMMIT"]

# The DevEx account in the Confluent Cloud is used to provide Kafka services
KAFKA_BOOTSTRAP_SERVER = "pkc-n00kk.us-east-1.aws.confluent.cloud:9092"
SCHEMA_REGISTRY_ENDPOINT = "https://psrc-8kz20.us-east-2.aws.confluent.cloud"
# The actual values are stored in the i2 repository
CONFLUENT_API_KEY = os.environ["CONFLUENT_CLOUD_DEVEX_KAFKA_USERNAME"]
CONFLUENT_API_SECRET = os.environ["CONFLUENT_CLOUD_DEVEX_KAFKA_PASSWORD"]

SERVICES = [
    Cockroach(setup_materialize=True),
    Materialized(
        # We use materialize/environmentd and not materialize/materialized here
        # in order to ensure a perfect match to the container that should be
        # deployed to the cloud
        image=f"materialize/environmentd:{VERSION}",
        external_cockroach=True,
        persist_blob_url="file:///mzdata/persist/blob",
        options=[
            "--orchestrator-process-secrets-directory=/mzdata/secrets",
            "--orchestrator-process-scratch-directory=/scratch",
        ],
        # We can not restart this container at will, as it does not have clusterd
        sanity_restart=False,
    ),
    Testdrive(),  # Overriden below
    Mz(
        region=REGION,
        environment=ENVIRONMENT,
        app_password=APP_PASSWORD,
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Deploy the current source to the cloud and run tests."""

    parser.add_argument(
        "--cleanup",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Destroy the region at the end of the workflow.",
    )
    parser.add_argument(
        "--version-check",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Perform a version check.",
    )

    parser.add_argument(
        "td_files",
        nargs="*",
        default=["*.td"],
        help="run against the specified files",
    )

    args = parser.parse_args()

    if args.cleanup:
        disable_region(c)

    test_failed = True
    try:
        print(f"Enabling region using Mz version {VERSION} ...")
        try:
            c.run("mz", "region", "enable", "--version", VERSION)
        except UIError:
            # Work around https://github.com/MaterializeInc/materialize/issues/17219
            pass

        time.sleep(10)

        assert "materialize.cloud" in c.cloud_hostname()
        wait_for_cloud(c)

        if args.version_check:
            version_check(c)

        print("Running .td files ...")
        td(c, *args.td_files)
        test_failed = False
    finally:
        # Clean up
        if args.cleanup:
            disable_region(c)

    assert not test_failed


def disable_region(c: Composition) -> None:
    print(f"Shutting down region {REGION} ...")

    c.run("mz", "region", "disable")


def wait_for_cloud(c: Composition) -> None:
    print(f"Waiting for cloud cluster to come up with username {USERNAME} ...")
    _wait_for_pg(
        host=c.cloud_hostname(),
        user=USERNAME,
        password=APP_PASSWORD,
        port=6875,
        query="SELECT 1",
        expected=[[1]],
        timeout_secs=900,
        dbname="materialize",
        ssl_context=ssl.SSLContext(),
        # print_result=True
    )


def version_check(c: Composition) -> None:
    print("Obtaining mz_version() string from local instance ...")
    c.up("materialized")
    local_version = c.sql_query("SELECT mz_version();")[0][0]

    print("Obtaining mz_version() string from the cloud ...")
    cloud_cursor = pg8000.connect(
        host=c.cloud_hostname(),
        user=USERNAME,
        password=APP_PASSWORD,
        port=6875,
        ssl_context=ssl.SSLContext(),
    ).cursor()
    cloud_cursor.execute("SELECT mz_version()")
    result = cloud_cursor.fetchone()
    assert result is not None
    cloud_version = result[0]
    assert (
        local_version == cloud_version
    ), f"local version: {local_version} is not identical to cloud version: {cloud_version}"


def td(c: Composition, *args: str) -> None:
    materialize_url = f"postgres://{urllib.parse.quote(USERNAME)}:{urllib.parse.quote(APP_PASSWORD)}@{urllib.parse.quote(c.cloud_hostname())}:6875"

    with c.override(
        Testdrive(
            default_timeout="1200s",
            materialize_url=materialize_url,
            no_reset=True,  # Required so that admin port 6877 is not used
            seed=1,  # Required for predictable Kafka topic names
            kafka_url=KAFKA_BOOTSTRAP_SERVER,
            schema_registry_url=SCHEMA_REGISTRY_ENDPOINT,
            no_consistency_checks=True,
            environment=[
                "KAFKA_OPTION="
                + ",".join(
                    [
                        "security.protocol=SASL_SSL",
                        "sasl.mechanisms=PLAIN",
                        f"sasl.username={CONFLUENT_API_KEY}",
                        f"sasl.password={CONFLUENT_API_SECRET}",
                    ]
                ),
                "VAR="
                + ",".join(
                    [
                        f"confluent-api-key={CONFLUENT_API_KEY}",
                        f"confluent-api-secret={CONFLUENT_API_SECRET}",
                    ]
                ),
            ],
        )
    ):
        c.run_testdrive_files(
            *args,
            rm=True,
        )
