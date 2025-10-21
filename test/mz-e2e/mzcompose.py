# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Tests the mz command line tool against a real Cloud instance
"""

import argparse
import csv
import json
import os
import time

import psycopg

from materialize.mzcompose import (
    _wait_for_pg,
)
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.mz import Mz
from materialize.ui import UIError

REGION = "aws/us-west-2"
ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
USERNAME = os.getenv("NIGHTLY_MZ_USERNAME", "infra+bot@materialize.com")
APP_PASSWORD = os.getenv("MZ_CLI_APP_PASSWORD")

# The DevEx account in the Confluent Cloud is used to provide Kafka services
KAFKA_BOOTSTRAP_SERVER = "pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092"
# The actual values are stored in the i2 repository
CONFLUENT_API_KEY = os.getenv("CONFLUENT_CLOUD_QA_CANARY_KAFKA_USERNAME")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_CLOUD_QA_CANARY_KAFKA_PASSWORD")

SERVICES = [
    Mz(
        region=REGION,
        environment=ENVIRONMENT,
        app_password=APP_PASSWORD or "",
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

    args = parser.parse_args()

    # Look for the '.psqlrc' file in the home dir.
    home_dir = os.path.expanduser("~")
    psql_config_path = os.path.join(home_dir, ".psqlrc-mz")

    if args.cleanup:
        disable_region(c)
        if os.path.exists(psql_config_path):
            os.remove(psql_config_path)

    test_failed = True
    try:
        print("Enabling region using Mz ...")
        c.run("mz", "region", "enable")

        time.sleep(10)

        assert "materialize.cloud" in c.cloud_hostname()
        wait_for_cloud(c)

        # Test - `mz app-password`
        # Assert `mz app-password create`
        new_app_password_name = "Materialize CLI (mz) - Nightlies"
        output = c.run(
            "mz", "app-password", "create", new_app_password_name, capture=True
        )
        new_app_password = output.stdout.strip()
        assert "mzp_" in new_app_password

        psycopg.connect(
            host=c.cloud_hostname(),
            user=USERNAME,
            password=new_app_password,
            port=6875,
            sslmode="require",
        )

        # Assert `mz app-password list`
        output = c.run("mz", "app-password", "list", capture=True)
        assert new_app_password_name in output.stdout

        # // Test - `mz secrets`
        # Drop secret if exists
        output = c.run(
            "mz",
            "sql",
            "--",
            "-q",
            "-c",
            "DROP SECRET IF EXISTS CI_SECRET;",
            capture=True,
        )

        secret = "decode('c2VjcmV0Cg==', 'base64')"
        output = c.run(
            "mz", "secret", "create", "CI_SECRET", stdin=secret, capture=True
        )
        assert output.returncode == 0

        output = c.run(
            "mz",
            "secret",
            "create",
            "confluent_username",
            stdin=CONFLUENT_API_KEY,
            capture=True,
        )
        assert output.returncode == 0

        output = c.run(
            "mz",
            "secret",
            "create",
            "confluent_password",
            stdin=CONFLUENT_API_SECRET,
            capture=True,
        )
        assert output.returncode == 0

        output = c.run(
            "mz",
            "sql",
            "--",
            "-q",
            "-c",
            f"""
            CREATE CONNECTION confluent_cloud TO KAFKA (
                BROKER '{KAFKA_BOOTSTRAP_SERVER}',
                SASL MECHANISMS = 'PLAIN',
                SASL USERNAME = SECRET confluent_username,
                SASL PASSWORD = SECRET confluent_password
            );""",
            capture=True,
        )

        output = c.run(
            "mz",
            "sql",
            "--",
            "-q",
            "-c",
            """VALIDATE CONNECTION confluent_cloud;""",
            capture=True,
        )
        assert output.returncode == 0

        output = c.run("mz", "sql", "--", "-q", "-c", "SHOW SECRETS;", capture=True)
        assert "ci_secret" in output.stdout

        # Assert using force
        output = c.run(
            "mz", "secret", "create", "CI_SECRET", "--force", stdin=secret, capture=True
        )
        assert output.returncode == 0

        # Test - `mz user`
        user_email = "mz_ci_e2e_test_nightlies@materialize.com"

        output = c.run("mz", "user", "list", capture=True)
        if user_email in output.stdout:
            # Try to remove the username if it exist before trying to create one.
            c.run("mz", "user", "remove", user_email, check=False)

        output = c.run("mz", "user", "create", user_email, "MZ_CI", capture=True)
        assert output.returncode == 0

        output = c.run("mz", "user", "list", capture=True)
        assert output.returncode == 0
        assert user_email in output.stdout

        output = c.run("mz", "user", "remove", user_email, capture=True)
        assert output.returncode == 0

        output = c.run(
            "mz",
            "user",
            "list",
            "--format",
            "csv",
            capture=True,
        )
        # assert username is not in output.stdout
        user_list = csv.DictReader(output.stdout.split("\n"))
        for user in user_list:
            assert user["email"] != user_email

        # Test - `mz region list`
        # Enable, disable and show are already tested.
        output = c.run("mz", "region", "list", "--format", "json", capture=True)
        regions = json.loads(output.stdout)
        us_region = None
        for region in regions:
            if region["region"] == REGION:
                us_region = region
        assert us_region is not None and "enabled" == us_region["status"]

        # Verify the content is ok
        print(f"Path: {psql_config_path}")
        if os.path.exists(psql_config_path):
            with open(psql_config_path):
                # content = file.read()
                output = c.run("cat {psql_config_path}", capture=True)
                if output.stdout != "\\timing\n\\include ~/.psqlrc":
                    # TODO:
                    print("Content is not equal. Fix this.")
                    # raise ValueError("Incorrect content in the '.psqlrc-mz' file.")
        else:
            print("The configuration file '.psqlrc-mz' does not exist.")
            # TODO:
            # raise FileNotFoundError(
            #     "The configuration file '.psqlrc-mz' does not exist."
            # )

        test_failed = False
    finally:
        # Clean up
        if args.cleanup:
            disable_region(c)

    assert not test_failed


def disable_region(c: Composition) -> None:
    print(f"Shutting down region {REGION} ...")

    try:
        c.run("mz", "region", "disable", "--hard")
    except UIError:
        # Can return: status 404 Not Found
        pass


def wait_for_cloud(c: Composition) -> None:
    print(f"Waiting for cloud cluster to come up with username {USERNAME} ...")
    _wait_for_pg(
        host=c.cloud_hostname(),
        user=USERNAME,
        password=APP_PASSWORD,
        port=6875,
        query="SELECT 1",
        expected=[(1,)],
        timeout_secs=900,
        dbname="materialize",
        sslmode="require",
        # print_result=True
    )
