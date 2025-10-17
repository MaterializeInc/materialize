# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Deploy the current version on a real Staging Cloud, and run some basic
verifications, like ingesting data from Kafka and Redpanda Cloud using AWS
Privatelink. Runs only on main and release branches.
"""

import argparse
import glob
import itertools
import os
import secrets
import string
import time
import urllib.parse

import psycopg

from materialize import MZ_ROOT
from materialize.mz_version import MzVersion
from materialize.mzcompose import (
    _wait_for_pg,
)
from materialize.mzcompose.composition import (
    Composition,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import CockroachOrPostgresMetadata
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.redpanda_cloud import RedpandaCloud
from materialize.ui import UIError

REDPANDA_RESOURCE_GROUP = "ci-resource-group"
REDPANDA_NETWORK = "ci-network"
REDPANDA_CLUSTER = "ci-cluster"
REDPANDA_USER = "ci-user"

REGION = "aws/us-east-1"
ENVIRONMENT = os.getenv("ENVIRONMENT", "staging")
USERNAME = os.getenv("NIGHTLY_CANARY_USERNAME", "infra+nightly-canary@materialize.com")
APP_PASSWORD = os.getenv("NIGHTLY_CANARY_APP_PASSWORD")
VERSION = f"{MzVersion.parse_cargo()}--pr.g{os.getenv('BUILDKITE_COMMIT')}"

# The DevEx account in the Confluent Cloud is used to provide Kafka services
KAFKA_BOOTSTRAP_SERVER = "pkc-n00kk.us-east-1.aws.confluent.cloud:9092"
SCHEMA_REGISTRY_ENDPOINT = "https://psrc-8kz20.us-east-2.aws.confluent.cloud"
# The actual values are stored in the i2 repository
CONFLUENT_API_KEY = os.getenv("CONFLUENT_CLOUD_QA_CANARY_KAFKA_USERNAME")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_CLOUD_QA_CANARY_KAFKA_PASSWORD")


class Redpanda:
    def __init__(self, c: Composition, cleanup: bool):
        self.cloud = RedpandaCloud()

        if cleanup:
            self.delete()

        self.resource_group_id = self.cloud.create(
            "resource-groups", {"name": REDPANDA_RESOURCE_GROUP}
        )["resource_group"]["id"]

        result = self.cloud.create(
            "networks",
            {
                "name": REDPANDA_NETWORK,
                "region": "us-east-1",
                "resource_group_id": self.resource_group_id,
                "cluster_type": "TYPE_DEDICATED",
                "cloud_provider": "CLOUD_PROVIDER_AWS",
                "cidr_block": "10.0.0.0/20",
            },
        )
        self.network_id = self.cloud.wait(result)["resource_id"]

        result = self.cloud.create(
            "clusters",
            {
                "name": REDPANDA_CLUSTER,
                "cloud_provider": "CLOUD_PROVIDER_AWS",
                "connection_type": "CONNECTION_TYPE_PUBLIC",
                "resource_group_id": self.resource_group_id,
                "network_id": self.network_id,
                "region": "us-east-1",
                "throughput_tier": "tier-1-aws-v3-arm",
                "type": "TYPE_DEDICATED",
                "zones": ["use1-az2"],
                "aws_private_link": {
                    "enabled": True,
                    "connect_console": True,
                    "allowed_principals": [],
                },
            },
        )
        self.cluster_info = self.cloud.wait(result)["response"]["cluster"]

        self.aws_private_link = self.cloud.get(f"clusters/{self.cluster_info['id']}")[
            "cluster"
        ]["aws_private_link"]["status"]["service_name"]

        redpanda_cluster = self.cloud.get_cluster(self.cluster_info)

        self.password = "".join(
            secrets.choice(string.ascii_letters + string.digits) for i in range(32)
        )
        redpanda_cluster.create(
            "users",
            {
                "mechanism": "SASL_MECHANISM_SCRAM_SHA_512",
                "name": REDPANDA_USER,
                "password": self.password,
            },
        )

        redpanda_cluster.create(
            "acls",
            {
                "host": "*",
                "operation": "OPERATION_ALL",
                "permission_type": "PERMISSION_TYPE_ALLOW",
                "principal": f"User:{REDPANDA_USER}",
                "resource_name": "*",
                "resource_pattern_type": "RESOURCE_PATTERN_TYPE_LITERAL",
                "resource_type": "RESOURCE_TYPE_TOPIC",
            },
        )

        cloud_conn = psycopg.connect(
            host=c.cloud_hostname(),
            user=USERNAME,
            password=APP_PASSWORD,
            dbname="materialize",
            port=6875,
            sslmode="require",
        )
        cloud_conn.autocommit = True
        cloud_cursor = cloud_conn.cursor()
        cloud_cursor.execute(
            f"""CREATE CONNECTION privatelink_conn
            TO AWS PRIVATELINK (
                SERVICE NAME '{self.aws_private_link}',
                AVAILABILITY ZONES ('use1-az2')
            );""".encode()
        )
        cloud_cursor.execute(
            """SELECT principal
            FROM mz_aws_privatelink_connections plc
            JOIN mz_connections c on plc.id = c.id
            WHERE c.name = 'privatelink_conn';"""
        )
        results = cloud_cursor.fetchone()
        assert results
        privatelink_principal = results[0]
        cloud_cursor.close()
        cloud_conn.close()

        # Redpanda API sometimes returns a 404 for a while, ignore
        while True:
            try:
                result = self.cloud.patch(
                    f"clusters/{self.cluster_info['id']}",
                    {
                        "aws_private_link": {
                            "enabled": True,
                            "allowed_principals": [privatelink_principal],
                        }
                    },
                )
            except ValueError as e:
                print(f"Failure, retrying in 10s: {e}")
                time.sleep(10)
                continue
            break
        self.cloud.wait(result)

    def delete(self):
        for cluster in self.cloud.get("clusters")["clusters"]:
            if cluster["name"] == REDPANDA_CLUSTER:
                result = self.cloud.delete("clusters", cluster["id"])
                self.cloud.wait(result)
                break

        for network in self.cloud.get("networks")["networks"]:
            if network["name"] == REDPANDA_NETWORK:
                result = self.cloud.delete("networks", network["id"])
                self.cloud.wait(result)

        for resource_group in self.cloud.get("resource-groups")["resource_groups"]:
            if resource_group["name"] == REDPANDA_RESOURCE_GROUP:
                self.cloud.delete("resource-groups", resource_group["id"])


SERVICES = [
    CockroachOrPostgresMetadata(),
    Materialized(
        # We use materialize/environmentd and not materialize/materialized here
        # in order to ensure a perfect match to the container that should be
        # deployed to the cloud
        image=f"materialize/environmentd:{VERSION}",
        external_metadata_store=True,
        persist_blob_url="file:///mzdata/persist/blob",
        options=[
            "--orchestrator=process",
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

    files = list(
        itertools.chain.from_iterable(
            [
                glob.glob(file_glob, root_dir=MZ_ROOT / "test" / "cloud-canary")
                for file_glob in args.td_files
            ]
        )
    )

    if args.cleanup:
        disable_region(c)

    try:
        test_failed = True
        print(f"Enabling region using Mz version {VERSION} ...")
        try:
            c.run("mz", "region", "enable", "--version", VERSION)
        except UIError:
            # Work around https://github.com/MaterializeInc/database-issues/issues/4989
            pass

        time.sleep(10)

        assert "materialize.cloud" in c.cloud_hostname()
        wait_for_cloud(c)

        if args.version_check:
            version_check(c)

        # Takes about 40 min to spin up
        print("--- Spinnung up Redpanda Cloud (takes ~40 min)")
        redpanda = (
            Redpanda(c, cleanup=args.cleanup)
            if any(["redpanda" in filename for filename in files])
            else None
        )

        try:
            print("--- Running .td files ...")
            td(c, text="> CREATE CLUSTER canary_sources SIZE '25cc'")

            def process(filename: str) -> None:
                td(c, filename, redpanda=redpanda)

            c.test_parts(files, process)
            test_failed = False
        finally:
            if args.cleanup and redpanda is not None:
                print("--- Deleting Redpanda Cloud")
                redpanda.delete()
    finally:
        if args.cleanup:
            # Clean up
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


def version_check(c: Composition) -> None:
    print("Obtaining mz_version() string from local instance ...")
    c.up("materialized")
    # Remove the ($HASH) suffix because it can be different. The reason is that
    # the dockerhub devel-$HASH tag is just a link to the mzbuild-$CODEHASH. So
    # if the code has not changed, the environmentd image of the previous
    # version will be used.
    local_version = c.sql_query("SELECT mz_version();")[0][0].split(" ")[0]

    print("Obtaining mz_version() string from the cloud ...")
    cloud_cursor = psycopg.connect(
        host=c.cloud_hostname(),
        user=USERNAME,
        password=APP_PASSWORD,
        dbname="materialize",
        port=6875,
        sslmode="require",
    ).cursor()
    cloud_cursor.execute("SELECT mz_version()")
    result = cloud_cursor.fetchone()
    assert result is not None
    cloud_version = result[0].split(" ")[0]
    assert (
        local_version == cloud_version
    ), f"local version: {local_version} is not identical to cloud version: {cloud_version}"


def td(
    c: Composition,
    filename: str | None = None,
    text: str | None = None,
    redpanda: Redpanda | None = None,
) -> None:
    assert APP_PASSWORD is not None
    materialize_url = f"postgres://{urllib.parse.quote(USERNAME)}:{urllib.parse.quote(APP_PASSWORD)}@{urllib.parse.quote(c.cloud_hostname())}:6875"

    assert bool(filename) != bool(text)

    testdrive = Testdrive(
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
        ],
        entrypoint_extra=[
            f"--var=confluent-api-key={CONFLUENT_API_KEY}",
            f"--var=confluent-api-secret={CONFLUENT_API_SECRET}",
        ],
    )

    if redpanda and "redpanda" in str(filename):
        testdrive = Testdrive(
            default_timeout="1200s",
            materialize_url=materialize_url,
            no_reset=True,  # Required so that admin port 6877 is not used
            seed=1,  # Required for predictable Kafka topic names
            kafka_url=redpanda.cluster_info["kafka_api"]["seed_brokers"][0],
            schema_registry_url=redpanda.cluster_info["schema_registry"]["url"],
            no_consistency_checks=True,
            environment=[
                "KAFKA_OPTION="
                + ",".join(
                    [
                        "security.protocol=SASL_SSL",
                        "sasl.mechanisms=SCRAM-SHA-512",
                        f"sasl.username={REDPANDA_USER}",
                        f"sasl.password={redpanda.password}",
                    ]
                ),
            ],
            entrypoint_extra=[
                f"--var=redpanda-username={REDPANDA_USER}",
                f"--var=redpanda-password={redpanda.password}",
            ],
        )

    with c.override(testdrive):
        if text:
            c.testdrive(text)
        if filename:
            c.run_testdrive_files(
                filename,
            )
