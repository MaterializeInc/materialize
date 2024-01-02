# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests of AWS functionality that run against AWS.

To run these tests locally:

  $ cd test/aws
  $ AWS_PROFILE=mz-scratch-admin ./mzcompose --dev run default
"""

import codecs
import json
import random

import boto3
from pg8000.dbapi import ProgrammingError

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized

AWS_EXTERNAL_ID_PREFIX = "eb5cb59b-e2fe-41f3-87ca-d2176a495345"

SERVICES = [
    Materialized(),
]


class TestContext:
    def __init__(self, iam_propagation_seconds: int):
        self.iam_propagation_seconds = iam_propagation_seconds
        self.seed = random.getrandbits(32)
        self.sts = boto3.client("sts")
        self.iam = boto3.client("iam")

        # Get the IAM principal that we're running as.
        caller = self.sts.get_caller_identity()
        self.account_id = caller["Account"]
        self.materialized_principal = caller["Arn"]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    # Sleeping to wait for IAM to propagate is ugly and somewhat flaky, but
    # there isn't an obviously better solution. This only runs in the nightly
    # pipeline, so flakes are more tolerable than they would be if this ran in
    # the PR pipeline.
    parser.add_argument(
        "--iam-propagation-seconds",
        type=int,
        default=10,
        help="how long to wait for IAM policies to propagate",
    )
    args = parser.parse_args()

    # Set up.
    ctx = TestContext(iam_propagation_seconds=args.iam_propagation_seconds)

    # Create the "jump role" that Materialize will use to assume each
    # connection's role.
    connection_role = f"testdrive-{ctx.seed}-MaterializeConnection"
    connection_role_arn = f"arn:aws:iam::{ctx.account_id}:role/{connection_role}"
    ctx.iam.create_role(
        RoleName=connection_role,
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": ctx.materialized_principal,
                        },
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )

    # Start Materialize.
    materialized = Materialized(
        environment_extra=[
            "AWS_DEFAULT_REGION=us-east-1",
            "AWS_ACCESS_KEY_ID",
            "AWS_PROFILE",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ],
        volumes_extra=[
            # Mounting the .aws directory in the container allows Materialize to
            # use SSO credentials, which makes it easier to run this composition
            # locally. CI doesn't need this.
            "~/.aws:/home/materialize/.aws",
        ],
        options=[
            f"--aws-connection-role-arn={connection_role_arn}",
            f"--aws-external-id-prefix={AWS_EXTERNAL_ID_PREFIX}",
        ],
    )
    with c.override(materialized):
        # (Re)start Materialize and enable AWS connections.
        c.down()
        c.up("materialized")
        c.sql(
            port=6877,
            user="mz_system",
            sql="""
            ALTER SYSTEM SET enable_aws_connection = true;
            ALTER SYSTEM SET enable_connection_validation_syntax = true;
            """,
        )

        for fn in [test_credentials, test_assume_role]:
            with c.test_case(fn.__name__):
                fn(c, ctx)


def test_credentials(c: Composition, ctx: TestContext):
    # Create a user with an access key.
    customer_user = f"testdrive-{ctx.seed}-Customer"
    ctx.iam.create_user(UserName=customer_user)
    access_key = ctx.iam.create_access_key(UserName=customer_user)
    access_key_id = access_key["AccessKey"]["AccessKeyId"]
    secret_access_key = access_key["AccessKey"]["SecretAccessKey"]

    # Creating a connection with those credentials should work.
    c.sql(
        f"""
        CREATE SECRET aws_secret_access_key AS '{secret_access_key}';
        CREATE CONNECTION aws_credentials TO AWS (
            ACCESS KEY ID = '{access_key_id}',
            SECRET ACCESS KEY = SECRET aws_secret_access_key
        );
    """
    )
    # Wait for IAM to propagate.
    c.sleep(ctx.iam_propagation_seconds)
    c.sql("VALIDATE CONNECTION aws_credentials")

    # Corrupting the secret access key should cause authentication to fail with
    # an invalid signature error.
    bad_secret_access_key = codecs.encode(secret_access_key, "rot13")
    c.sql(f"ALTER SECRET aws_secret_access_key AS '{bad_secret_access_key}'")
    try:
        c.sql("VALIDATE CONNECTION aws_credentials")
    except ProgrammingError as e:
        assert "SignatureDoesNotMatch" in e.args[0]["M"]
    else:
        assert False, "connection validation unexpectedly succeeded"

    # Changing the access key to a nonexistent access key should fail with an
    # invalid client ID error.
    c.sql(
        "ALTER CONNECTION aws_credentials SET (ACCESS KEY ID = 'AKIAV2KIV5LP3RAKAZUY')"
    )
    try:
        c.sql("VALIDATE CONNECTION aws_credentials")
    except ProgrammingError as e:
        assert "InvalidClientTokenId" in e.args[0]["M"]
    else:
        assert False, "connection validation unexpectedly succeeded"


def test_assume_role(c: Composition, ctx: TestContext):
    # Create a connection to a not-yet-existing customer role.
    customer_role = f"testdrive-{ctx.seed}-Customer"
    customer_role_arn = f"arn:aws:iam::{ctx.account_id}:role/{customer_role}"
    c.sql(
        f"CREATE CONNECTION aws_assume_role TO AWS (ASSUME ROLE ARN '{customer_role_arn}')"
    )
    connection_id = c.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'aws_assume_role'"
    )[0][0]

    # Ensure that validating the connection fails.
    try:
        c.sql("VALIDATE CONNECTION aws_assume_role")
    except ProgrammingError as e:
        assert "AccessDenied" in e.args[0]["M"]
    else:
        assert False, "connection validation unexpectedly succeeded"

    # Create the customer role, but incorrectly fail to constrain the
    # external ID.
    principal = c.sql_query(
        f"SELECT principal FROM mz_internal.mz_aws_connections WHERE id = '{connection_id}'"
    )[0][0]
    ctx.iam.create_role(
        RoleName=customer_role,
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": principal,
                        },
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )

    # Wait for IAM to propagate.
    c.sleep(ctx.iam_propagation_seconds)

    try:
        c.sql("VALIDATE CONNECTION aws_assume_role")
    except ProgrammingError as e:
        # Ensure the top line error message is exactly what we expect.
        assert "role trust policy does not require an external ID" == e.args[0]["M"]
        # We're not as prescriptive about the detail/hint fields. Just ensure
        # that the details include the exact ARN of the connection's role and
        # that the hint includes a link to further documentation.
        assert customer_role_arn in e.args[0]["D"]
        assert (
            "https://materialize.com/s/aws-connection-role-trust-policy"
            in e.args[0]["H"]
        )
    else:
        assert False, "connection validation unexpectedly succeeded"

    # Update the customer role's trust policy to use Materialize's example.
    trust_policy = c.sql_query(
        f"SELECT example_trust_policy FROM mz_internal.mz_aws_connections WHERE id = '{connection_id}'"
    )[0][0]
    ctx.iam.update_assume_role_policy(
        RoleName=customer_role,
        PolicyDocument=json.dumps(trust_policy),
    )

    # Wait for IAM to propagate.
    c.sleep(ctx.iam_propagation_seconds)

    # Ensure that connection validation now succeeds.
    c.sql("VALIDATE CONNECTION aws_assume_role")
