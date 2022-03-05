# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Test loading AWS credentials from various credential sources."""

import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from mypy_boto3_iam import IAMClient
from mypy_boto3_sts import STSClient

from materialize.mzcompose import Composition, UIError
from materialize.mzcompose.services import Materialized, Testdrive

EXTERNAL_ID = str(random.randrange(0, 2**64))
SEED = random.randrange(0, 2**32)
DISCARD = "http://127.0.0.1:9"

# == Services ==

# In buildkite this script is executed in a read-only directory so we can't use
# cwd. Directories in `/tmp` end up empty inside of the running docker
# container, perhaps because of docker-in-docker and external permissions.
#
# This works in both local dev and buildkite.
LOCAL_DIR = (Path(__file__).parent / f"mzcompose-aws-config-{SEED}").resolve()

AWS_VOLUME = [f"{LOCAL_DIR}:/root/.aws"]

SERVICES = [
    Materialized(
        forward_aws_credentials=False,
        environment_extra=[f"AWS_EC2_METADATA_SERVICE_ENDPOINT={DISCARD}"],
        volumes_extra=AWS_VOLUME,
    ),
    Testdrive(
        materialized_url=f"postgres://materialize@materialized:6875",
        seed=SEED,
    ),
]

# Service overrides for specifying external id

MZ_EID = Materialized(
    forward_aws_credentials=False,
    options=f"--aws-external-id={EXTERNAL_ID}",
    environment_extra=[f"AWS_EC2_METADATA_SERVICE_ENDPOINT={DISCARD}"],
    volumes_extra=AWS_VOLUME,
)


def workflow_default(c: Composition) -> None:
    "Test that materialize can use a multitude of auth schemes to connect to AWS"
    LOCAL_DIR.mkdir()

    session = boto3.Session()
    sts: STSClient = session.client("sts")
    iam: IAMClient = session.client("iam")

    identity = sts.get_caller_identity()
    current_user = identity["Arn"]

    aws_region = session.region_name

    created_roles: List[CreatedRole] = []
    try:
        allowed = create_role(iam, "Allow", current_user, created_roles)
        denied = create_role(iam, "Deny", current_user, created_roles)
        requires_eid = create_role(
            iam, "Allow", current_user, created_roles, external_id=EXTERNAL_ID
        )
        profile_contents = gen_profile_text(
            session, allowed.arn, requires_eid.arn, denied.arn
        )

        wait_for_role(sts, allowed.arn)

        td_args = [
            f"--aws-region={aws_region}",
            f"--var=allowed-role-arn={allowed.arn}",
            f"--var=denied-role-arn={denied.arn}",
            f"--var=role-requires-eid={requires_eid.arn}",
        ]

        # == Run core tests ==

        c.up("materialized")

        write_aws_config(LOCAL_DIR, profile_contents)

        c.wait_for_materialized("materialized")
        c.run(
            "testdrive-svc",
            *td_args,
            "test.td",
        )
        c.run(
            "testdrive-svc",
            *td_args,
            # no reset because the next test wants to validate behavior with
            # the previous catalog
            "--no-reset",
            "test-externalid-missing.td",
        )

        # == Tests that restarting materialized without a profile doesn't bork mz ==

        print("+++ Test Restarts with and without profile files")

        # Historically, a missing aws config file would cause all SQL
        # commands to hang entirely after a restart, this no longer happens
        # but this step restarts to catch it if it comes back.
        c.stop("materialized")

        rm_aws_config(LOCAL_DIR)

        c.up("materialized")

        c.run(
            "testdrive-svc",
            "--no-reset",
            "test-restart-no-creds.td",
        )

        # now test that with added credentials things can be done
        write_aws_config(LOCAL_DIR, profile_contents)
        c.run("testdrive-svc", *td_args, "test-restart-with-creds.td")

        # == Test that requires --aws-external-id has been supplied ==
        print("+++ Test AWS External IDs")
        c.stop("materialized")
        c.rm("materialized")

        with c.override(MZ_EID):
            c.up("materialized")
            c.wait_for_materialized("materialized")
            write_aws_config(LOCAL_DIR, profile_contents)
            c.run("testdrive-svc", *td_args, "test-externalid-present.td")
    finally:
        errored = False
        for role in created_roles:
            try:
                iam.delete_role_policy(RoleName=role.name, PolicyName=role.policy_name)
            except Exception as e:
                errored = True
                print(
                    f"> Unable to delete role policy {role.name}/{role.policy_name}: {e}"
                )

            try:
                iam.delete_role(RoleName=role.name)
                print(f"> Deleted IAM role {role.name}")
            except Exception as e:
                errored = True
                print(f"> Unable to delete role {role.name}: {e}")

        rm_aws_config(LOCAL_DIR)
        LOCAL_DIR.rmdir()

        if errored:
            raise UIError("Unable to completely clean up AWS resources")


@dataclass
class CreatedRole:
    name: str
    arn: str
    policy_name: str


def create_role(
    iam: IAMClient,
    effect: str,
    current_user: str,
    created: List[CreatedRole],
    external_id: Optional[str] = None,
) -> CreatedRole:
    effect_name = effect.lower()
    eid_name = "-externalid" if external_id else ""
    role_name = f"testdrive-aws-config-{effect_name}{eid_name}-{SEED}"
    policy_name = f"{effect_name}{eid_name}-{SEED}"

    assume_role_policy: Dict[str, Any] = {
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Principal": {"AWS": [current_user]},
            "Action": "sts:AssumeRole",
        },
    }
    if external_id is not None:
        assume_role_policy["Statement"]["Condition"] = {
            "StringEquals": {"sts:ExternalId": external_id}
        }

    create_response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy),
    )
    role_arn = create_response["Role"]["Arn"]
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": effect,
                        "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                        "Resource": "arn:aws:s3:::testdrive*",
                    },
                    {
                        "Effect": effect,
                        "Action": ["s3:GetObject", "s3:GetObjectAcl"],
                        "Resource": "arn:aws:s3:::testdrive*/*",
                    },
                ],
            }
        ),
    )

    role = CreatedRole(name=role_name, arn=role_arn, policy_name=policy_name)
    created.append(role)
    return role


def gen_profile_text(
    session: boto3.Session,
    allowed_role_arn: str,
    allowed_eid_role_arn: str,
    denied_role_arn: str,
) -> str:
    creds = session.get_credentials()
    region = session.region_name

    return f"""\
[profile credentials]
aws_access_key_id = {creds.access_key}
aws_secret_access_key = {creds.secret_key}
aws_session_token = {creds.token}
region = {region}

[profile allowed]
source_profile = credentials
role_arn = {allowed_role_arn}
region = {region}

# The region should be discovered in the source_profile, but it is not.
[profile allowed-no-region]
source_profile = credentials
role_arn = {allowed_role_arn}

# expect fail in first case

[profile allowed-eid]
source_profile = credentials
role_arn = {allowed_eid_role_arn}
region = {region}

# expected failure profiles

[profile no-region]
role_arn = {allowed_role_arn}
aws_access_key_id = {creds.access_key}
aws_secret_access_key = {creds.secret_key}
aws_session_token = {creds.token}

[profile denied]
source_profile = credentials
role_arn = {denied_role_arn}
region = {region}

[profile no-credentials]
region = {region}
"""


def wait_for_role(sts: STSClient, role_arn: str) -> None:
    """
    Verify that it is possible to assume the given role

    In practice this always seems to take less than 10 seconds, but give it up
    to 90 to reduce any chance of flakiness.
    """
    for i in range(90, 0, -1):
        try:
            sts.assume_role(
                RoleArn=role_arn, RoleSessionName="mzcomposevalidatecreated"
            )
        except Exception as e:
            if i % 10 == 0:
                print(f"Unable to assume role, {i} seconds remaining: {e}")
            time.sleep(1)
            continue
        print(f"Successfully assumed role {role_arn}")
        break
    else:
        raise UIError("Never able to assume role")


def write_aws_config(local_dir: Path, text: str) -> None:
    config_file = local_dir / "config"
    with open(config_file, "w") as fh:
        fh.write(text)


def rm_aws_config(local_dir: Path) -> None:
    conf_file = local_dir / "config"
    conf_file.unlink()
