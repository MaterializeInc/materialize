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
import subprocess
import time
from pathlib import Path
from textwrap import dedent

import pg8000

from materialize.mzcompose import Composition, _wait_for_pg
from materialize.mzcompose.services import Materialized, Testdrive

SERVICES = [Materialized(), Testdrive()]

REGION = "aws/us-east-1"
ENVIRONMENT = "staging"
USERNAME = "infra+nightly-canary@materialize.com"
APP_PASSWORD = os.environ["NIGHTLY_CANARY_APP_PASSWORD"]
VERSION = "unstable-" + os.environ["BUILDKITE_COMMIT"]


def disable_region() -> None:
    print(f"Shuting down region {REGION} ...")

    subprocess.run(
        ["bin/ci-builder", "run", "stable", "bin/mz", "region", "disable", REGION]
    )


def workflow_default(c: Composition) -> None:
    """Deploy the current source to the cloud and run a smoke test."""

    print("Obtaining mz_version() string from local instance ...")
    c.start_and_wait_for_tcp(services=["materialized"])
    c.wait_for_materialized()
    local_version = c.sql_query("SELECT mz_version();")[0][0]

    print("Prepare configuration file for bin/mz")
    mz_profiles = Path.home() / ".config" / "mz" / "profiles.toml"
    mz_profiles.parent.mkdir(parents=True, exist_ok=True)
    with open(mz_profiles, "w+") as o:
        o.write(
            dedent(
                f"""
                current_profile = 'default'
                [profiles.default]
                email = '{USERNAME}'
                app-password = '{APP_PASSWORD}'
                region = '{REGION}'
                endpoint = 'https://{ENVIRONMENT}.cloud.materialize.com/'
                """
            )
        )

    disable_region()

    test_failed = True
    try:
        print(f"Enabling region using Mz version {VERSION} ...")
        subprocess.run(
            [
                "bin/ci-builder",
                "run",
                "stable",
                "bin/mz",
                "region",
                "enable",
                REGION,
                f"--version={VERSION}",
            ]
        )
        time.sleep(10)

        print("Obtaining hostname of cloud instance ...")
        hostname = subprocess.check_output(
            f"bin/ci-builder run stable bin/mz region status {REGION} | sed -n '2p' | cut -d ':' -f 2",
            shell=True,
            encoding="utf-8",
        ).strip()
        assert "materialize.cloud" in hostname

        print("Waiting for cloud cluster to come up ...")
        _wait_for_pg(
            host=hostname,
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
        # Fetch the version string on the cloud and confirm it is the same as the desired one
        cloud_cursor = pg8000.connect(
            host=hostname,
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
                default_timeout="300s",
                materialize_url="postgres://{hostname}:6875",
                materialize_username=USERNAME,
                materialize_password=APP_PASSWORD,
                no_reset=True,
            )
        ):
            c.run("testdrive", "*.td", rm=True)
        test_failed = False

    finally:
        disable_region()
        # Clean up
        assert not test_failed
