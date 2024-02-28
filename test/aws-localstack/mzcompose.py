# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests of AWS functionality that run against localstack."""

import uuid
from datetime import datetime
from typing import Any, cast

import boto3

from materialize.mzcompose import (
    DEFAULT_CLOUD_REGION,
    DEFAULT_MZ_ENVIRONMENT_ID,
    DEFAULT_ORDINAL,
    DEFAULT_ORG_ID,
)
from materialize.mzcompose.composition import (
    Composition,
)
from materialize.mzcompose.services.localstack import Localstack
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

ENVIRONMENT_NAME = f"environment-{DEFAULT_ORG_ID}-{DEFAULT_ORDINAL}"
NAMESPACE = ENVIRONMENT_NAME
SERVICE_ACCOUNT_NAME = ENVIRONMENT_NAME
OIDC_SUB = f"system:serviceaccount:{NAMESPACE}:{SERVICE_ACCOUNT_NAME}"
PURPOSE = "test-aws"
STACK = "mzcompose"
KMS_KEY_ALIAS_NAME = f"alias/customer_key_{DEFAULT_MZ_ENVIRONMENT_ID}"
AWS_CONNECTION_ROLE_ARN = "arn:aws:iam::123456789000:role/MaterializeConnection"
AWS_EXTERNAL_ID_PREFIX = "eb5cb59b-e2fe-41f3-87ca-d2176a495345"

AWS_ACCESS_KEY_ID = "LSIAQAAAAAAVNCBMPNSG"
AWS_SECRET_ACCESS_KEY = "secret"
AWS_ENDPOINT_URL_MZ = "http://localstack:4566"

SERVICES = [
    Localstack(),
    Materialized(
        depends_on=["localstack"],
        environment_extra=[
            f"AWS_ENDPOINT_URL={AWS_ENDPOINT_URL_MZ}",
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}",
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}",
        ],
        options=[
            "--secrets-controller=aws-secrets-manager",
            f"--aws-secrets-controller-tags=Owner={OIDC_SUB}",
            f"--aws-secrets-controller-tags=Environment={ENVIRONMENT_NAME}",
            f"--aws-secrets-controller-tags=Purpose={PURPOSE}",
            f"--aws-secrets-controller-tags=Stack={STACK}",
            f"--aws-connection-role-arn={AWS_CONNECTION_ROLE_ARN}",
            f"--aws-external-id-prefix={AWS_EXTERNAL_ID_PREFIX}",
        ],
    ),
    Testdrive(default_timeout="5s"),
]


def workflow_default(c: Composition) -> None:
    for name in ["secrets-manager", "aws-connection", "copy-to-s3"]:
        with c.test_case(name):
            c.workflow(name)


def workflow_secrets_manager(c: Composition) -> None:
    c.up("localstack")

    aws_endpoint_url = f"http://localhost:{c.port('localstack', 4566)}"

    kms_client = boto3.client(
        "kms",
        endpoint_url=aws_endpoint_url,
        region_name=DEFAULT_CLOUD_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    key_id = kms_client.create_key()["KeyMetadata"]["KeyId"]
    kms_client.create_alias(
        AliasName=KMS_KEY_ALIAS_NAME,
        TargetKeyId=key_id,
    )

    sm_client = boto3.client(
        "secretsmanager",
        endpoint_url=aws_endpoint_url,
        region_name=DEFAULT_CLOUD_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    expected_tags = [
        {
            "Key": "Owner",
            "Value": OIDC_SUB,
        },
        {
            "Key": "Environment",
            "Value": ENVIRONMENT_NAME,
        },
        {
            "Key": "Purpose",
            "Value": PURPOSE,
        },
        {
            "Key": "Stack",
            "Value": STACK,
        },
    ]

    # Create an orphaned secret that should get deleted when starting environmentd.
    orphan_1_name = f"/user-managed/{DEFAULT_MZ_ENVIRONMENT_ID}/u1234"
    sm_client.create_secret(
        Name=orphan_1_name,
        KmsKeyId=KMS_KEY_ALIAS_NAME,
        SecretString="I'm an orphan, delete me!",
        Tags=expected_tags,
    )
    # Create an orphaned secret without the correct tags, so it should be ignored.
    orphan_2_name = f"/user-managed/{DEFAULT_MZ_ENVIRONMENT_ID}/u5678"
    sm_client.create_secret(
        Name=orphan_2_name,
        KmsKeyId=KMS_KEY_ALIAS_NAME,
        SecretString="I'm an orphan, but I shouldn't be deleted because of my missing tags!",
    )
    # Create a secret for a different environment, so it should be ignored.
    other_environment_name = "environment-11111111-2222-3333-4444-555555555555-0"
    other_oidc_sub = (
        f"system:serviceaccount:{other_environment_name}:{other_environment_name}"
    )
    other_name = f"/user-managed/{other_environment_name}/u9876"
    sm_client.create_secret(
        Name=other_name,
        KmsKeyId=KMS_KEY_ALIAS_NAME,
        SecretString="I belong to a different environment, so leave me alone!",
        Tags=[
            {
                "Key": "Owner",
                "Value": other_oidc_sub,
            },
            {
                "Key": "Environment",
                "Value": other_environment_name,
            },
            {
                "Key": "Purpose",
                "Value": PURPOSE,
            },
            {
                "Key": "Stack",
                "Value": STACK,
            },
        ],
    )

    def list_secrets() -> dict[str, dict[str, Any]]:
        return {
            secret["Name"]: secret for secret in sm_client.list_secrets()["SecretList"]
        }

    def secret_name(_id: str) -> str:
        return f"/user-managed/{DEFAULT_MZ_ENVIRONMENT_ID}/{_id}"

    def get_secret_value(_id: str) -> bytes:
        return cast(
            bytes,
            sm_client.get_secret_value(SecretId=secret_name(_id))["SecretBinary"],
        )

    c.up("materialized")
    secrets = list_secrets()
    assert orphan_1_name not in secrets
    assert orphan_2_name in secrets
    assert other_name in secrets
    # Should include migrated secrets and secrets for other environments
    assert len(secrets) == 2

    c.sql("CREATE SECRET secret AS 's3cret'")
    secrets = list_secrets()

    # New secret should exist with specified contents
    assert secret_name("u1") in secrets
    assert b"s3cret" == get_secret_value("u1")

    # Secrets should have expected tags
    secret_u1 = secrets[secret_name("u1")]
    for tag in expected_tags:
        assert tag in secret_u1["Tags"]

    # Check that alter secret gets reflected in Secrets Manager
    c.sql("ALTER SECRET secret AS 'tops3cret'")
    assert b"tops3cret" == get_secret_value("u1")

    # Rename should not change the contents in Secrets Manager
    c.sql("ALTER SECRET secret RENAME TO renamed_secret")
    assert b"tops3cret" == get_secret_value("u1")

    c.sql("DROP SECRET renamed_secret")
    # Check that the file has been deleted from Secrets Manager
    secrets = list_secrets()
    assert secret_name("u1") not in secrets


def workflow_aws_connection(c: Composition) -> None:
    c.up("localstack", "materialized")
    c.run_testdrive_files("aws-connection/aws-connection.td")


def workflow_copy_to_s3(c: Composition) -> None:
    with c.override(
        Materialized(
            depends_on=["localstack"],
            environment_extra=[
                f"AWS_ENDPOINT_URL={AWS_ENDPOINT_URL_MZ}",
                f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}",
                f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}",
            ],
        )
    ):
        c.up("localstack", "materialized")
        localhost_aws_endpoint_url = f"http://localhost:{c.port('localstack', 4566)}"
        s3_client = boto3.client(
            "s3",
            endpoint_url=localhost_aws_endpoint_url,
            region_name=DEFAULT_CLOUD_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        bucket_name = "copy-to-s3"
        s3_client.create_bucket(Bucket=bucket_name)
        path_prefix = str(uuid.uuid4())
        c.run_testdrive_files(
            f"--var=endpoint={AWS_ENDPOINT_URL_MZ}",
            f"--var=access-key={AWS_ACCESS_KEY_ID}",
            f"--var=secret-key={AWS_SECRET_ACCESS_KEY}",
            f"--var=s3-prefix={bucket_name}/{path_prefix}",
            f"--var=region={DEFAULT_CLOUD_REGION}",
            "copy-to-s3/copy-to-s3.td",
        )

        # asserting the uploaded files
        date = datetime.now().strftime("%Y-%m-%d")
        assert (
            s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=f"{path_prefix}/1/{date}/"
            )["Contents"][0]["Key"]
            == f"{path_prefix}/1/{date}/part-0001.csv"
        )

        assert (
            s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{path_prefix}/2/")[
                "Contents"
            ][0]["Key"]
            == f"{path_prefix}/2/part-0001.csv"
        )

        assert (
            s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{path_prefix}/3/")[
                "Contents"
            ][0]["Key"]
            == f"{path_prefix}/3/part-0001.csv"
        )
