# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import boto3

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    DEFAULT_CLOUD_REGION,
    DEFAULT_MZ_ENVIRONMENT_ID,
    DEFAULT_ORDINAL,
    DEFAULT_ORG_ID,
    Localstack,
    Materialized,
    Testdrive,
)

ENVIRONMENT_NAME = f"environment-{DEFAULT_ORG_ID}-{DEFAULT_ORDINAL}"
NAMESPACE = ENVIRONMENT_NAME
SERVICE_ACCOUNT_NAME = ENVIRONMENT_NAME
OIDC_SUB = f"system:serviceaccount:{NAMESPACE}:{SERVICE_ACCOUNT_NAME}"
PURPOSE = "Customer Secrets"
STACK = "mzcompose"
KMS_KEY_ALIAS_NAME = f"alias/customer_key_{DEFAULT_MZ_ENVIRONMENT_ID}"

AWS_ACCESS_KEY_ID = "LSIAQAAAAAAVNCBMPNSG"
AWS_SECRET_ACCESS_KEY = "secret"

SERVICES = [
    Localstack(),
    Materialized(
        depends_on=["localstack"],
        environment_extra=[
            "AWS_ENDPOINT_URL=http://localstack:4566",
            f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}",
            f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}",
        ],
        options=[
            "--secrets-controller=aws-secrets-manager",
            f"--aws-secrets-controller-tags=Owner={OIDC_SUB}",
            f"--aws-secrets-controller-tags=Environment={ENVIRONMENT_NAME}",
            f"--aws-secrets-controller-tags=Purpose={PURPOSE}",
            f"--aws-secrets-controller-tags=Stack={STACK}",
        ],
    ),
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
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

    c.up("materialized")
    secrets = {secret["Name"] for secret in sm_client.list_secrets()["SecretList"]}
    assert orphan_1_name not in secrets
    assert orphan_2_name in secrets
    assert other_name in secrets

    c.sql("CREATE SECRET secret AS 's3cret'")
    secret_name = f"/user-managed/{DEFAULT_MZ_ENVIRONMENT_ID}/u1"
    secret = next(
        s for s in sm_client.list_secrets()["SecretList"] if s["Name"] == secret_name
    )
    for tag in expected_tags:
        assert tag in secret["Tags"]
    secret_value = sm_client.get_secret_value(SecretId=secret_name)["SecretBinary"]
    assert secret_value == b"s3cret"

    # Check that alter secret gets reflected in Secrets Manager
    c.sql("ALTER SECRET secret AS 'tops3cret'")
    secret_value = sm_client.get_secret_value(SecretId=secret_name)["SecretBinary"]
    assert secret_value == b"tops3cret"

    # Rename should not change the contents in Secrets Manager
    c.sql("ALTER SECRET secret RENAME TO renamed_secret")
    secret_value = sm_client.get_secret_value(SecretId=secret_name)["SecretBinary"]
    assert secret_value == b"tops3cret"

    c.sql("DROP SECRET renamed_secret")
    # Check that the file has been deleted from Secrets Manager
    secrets = {secret["Name"] for secret in sm_client.list_secrets()["SecretList"]}
    assert secret_name not in secrets
