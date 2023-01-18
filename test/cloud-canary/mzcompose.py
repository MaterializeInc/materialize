# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import ssl
import time
import urllib.parse

import pg8000

from materialize.mzcompose import Composition, _wait_for_pg
from materialize.mzcompose.services import Materialized, Mz, Testdrive
from materialize.ui import UIError

REGION = "aws/us-east-1"
ENVIRONMENT = "staging"
USERNAME = "infra+nightly-canary@materialize.com"
APP_PASSWORD = os.environ["NIGHTLY_CANARY_APP_PASSWORD"]
VERSION = "devel-" + os.environ["BUILDKITE_COMMIT"]

SERVICES = [
    Materialized(),
    Testdrive(),
    Mz(
        region=REGION,
        environment=ENVIRONMENT,
        username=USERNAME,
        app_password=APP_PASSWORD,
    ),
]


def workflow_default(c: Composition) -> None:
    """Deploy the current source to the cloud and run a smoke test."""

    print("Obtaining mz_version() string from local instance ...")
    c.start_and_wait_for_tcp(services=["materialized"])
    c.wait_for_materialized()
    local_version = c.sql_query("SELECT mz_version();")[0][0]

    print(f"Shutting down region {REGION} ...")
    c.run("mz", "region", "disable", REGION)

    test_failed = True
    try:
        print(f"Enabling region using Mz version {VERSION} ...")
        try:
            c.run("mz", "region", "enable", REGION, "--version", VERSION)
        except UIError:
            # Work around https://github.com/MaterializeInc/materialize/issues/17219
            pass

        time.sleep(10)

        print("Obtaining hostname of cloud instance ...")
        region_status = c.run("mz", "region", "status", REGION, capture=True)
        sql_line = region_status.stdout.split("\n")[1]
        cloud_hostname = sql_line.split(":")[1].strip()
        assert "materialize.cloud" in cloud_hostname

        materialize_url = f"postgres://{urllib.parse.quote(USERNAME)}:{urllib.parse.quote(APP_PASSWORD)}@{cloud_hostname}:6875"

        print("Waiting for cloud cluster to come up ...")
        _wait_for_pg(
            host=cloud_hostname,
            user=USERNAME,
            password=APP_PASSWORD,
            port=6875,
            query="SELECT 1",
            expected=[[1]],
            timeout_secs=600,
            dbname="materialize",
            ssl_context=ssl.SSLContext(),
        )

        print("Obtaining mz_version() string from the cloud ...")
        cloud_cursor = pg8000.connect(
            host=cloud_hostname,
            user=USERNAME,
            password=APP_PASSWORD,
            port=6875,
            ssl_context=ssl.SSLContext(),
        ).cursor()
        cloud_cursor.execute("SELECT mz_version()")
        cloud_version = cloud_cursor.fetchone()[0]
        assert (
            local_version == cloud_version
        ), f"local version: {local_version} is not identical to cloud version: {cloud_version}"

        print("Running smoke tests ...")
        with c.override(
            Testdrive(
                default_timeout="600s",
                materialize_url=materialize_url,
                no_reset=True,  # Required so that admin port 6877 is not used
            )
        ):
            c.run("testdrive", "*.td", rm=True)

        test_failed = False
    finally:
        # Clean up
        print(f"Shutting down region {REGION} ...")
        c.run("mz", "region", "disable", REGION)

    assert not test_failed
