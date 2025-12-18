# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import json
import random
import string

from materialize.mzcompose.composition import (
    Composition,
    Service,
)
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

ALLOW_ALL_ACL = (
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::*"],
            }
        ],
    },
)[0]


def make_random_key(n: int):
    return "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(n)
    )


def make_user(name: str, minio_alias: str, c: Composition) -> str:
    key = make_random_key(10)
    c.exec(
        "mc",
        "mc",
        "admin",
        "user",
        "add",
        minio_alias,
        name,
        key,
    )

    c.exec("mc", "cp", "/dev/stdin", f"/tmp/{name}", stdin=json.dumps(ALLOW_ALL_ACL))

    c.exec(
        "mc",
        "mc",
        "admin",
        "policy",
        "create",
        minio_alias,
        name,
        f"/tmp/{name}",
    )
    c.exec(
        "mc",
        "mc",
        "admin",
        "policy",
        "attach",
        minio_alias,
        name,
        "--user",
        name,
    )

    return key


def workflow_default(c: Composition) -> None:
    # Start fresh
    c.down(destroy_volumes=True)
    c.up(
        "postgres",
        "minio",
        "materialized",
        Service("mc", idle=True),
    )

    minio_alias = "s3test"
    c.exec(
        "mc",
        "mc",
        "alias",
        "set",
        minio_alias,
        "http://minio:9000/",
        "minioadmin",
        "minioadmin",
    )

    # Create a bucket
    c.exec(
        "mc",
        "mc",
        "mb",
        f"{minio_alias}/test-bucket",
    )

    key = make_user("tduser", minio_alias, c)

    with c.override(
        Polaris(
            extra_environment=[
                "AWS_ACCESS_KEY_ID=tduser",
                f"AWS_SECRET_ACCESS_KEY={key}",
            ],
        )
    ):
        c.up("polaris")

    access_token = None
    token_resp = c.exec(
        "polaris",
        "curl",
        "-s",
        "-X",
        "POST",
        "http://localhost:8181/api/catalog/v1/oauth/tokens",
        "-d",
        "grant_type=client_credentials&client_id=root&client_secret=root&scope=PRINCIPAL_ROLE:ALL",
        capture=True,
    )
    try:
        access_token = json.loads(token_resp.stdout)["access_token"]
    except Exception as e:
        raise RuntimeError(
            f"failed to parse access token response: {token_resp.stdout}: {e}"
        )

    catalog_payload = {
        "name": "default_catalog",
        "type": "INTERNAL",
        "properties": {
            "default-base-location": "s3://test-bucket/",
            "s3.endpoint": "http://minio:9000",
            "s3.path-style-access": "true",
            "s3.access-key-id": "tduser",
            "s3.secret-access-key": key,
            "s3.region": "minio",
        },
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": ["s3://test-bucket/*"],
            "endpoint": "http://minio:9000",
            "endpointInternal": "http://minio:9000",
            "pathStyleAccess": True,
        },
    }

    c.exec(
        "polaris",
        "curl",
        "-sS",
        "-i",
        "-X",
        "POST",
        "-H",
        f"Authorization: Bearer {access_token}",
        "-H",
        "Content-Type: application/json",
        "http://localhost:8181/api/management/v1/catalogs",
        "--data-binary",
        json.dumps(catalog_payload),
    )

    # Create a namespace
    namespace_payload = {
        "namespace": ["default_namespace"],
    }

    c.exec(
        "polaris",
        "curl",
        "-sS",
        "-i",
        "-X",
        "POST",
        "-H",
        f"Authorization: Bearer {access_token}",
        "-H",
        "Content-Type: application/json",
        "http://localhost:8181/api/catalog/v1/default_catalog/namespaces",
        "--data-binary",
        json.dumps(namespace_payload),
    )

    c.run_testdrive_files(
        f"--var=s3-access-key={key}",
        "--var=aws-endpoint=minio:9000",
        "*.td",
    )
