# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for launching and interacting with scratch ec2 instances
"""

import os
import boto3
from subprocess import CalledProcessError
from typing import Any, Dict, Optional

from materialize import git
from materialize import ui
from materialize import spawn
from materialize import ssh


def check_required_vars() -> None:
    """Set reasonable default values for the
    environment variables necessary to interact with AWS."""
    if not os.environ.get("AWS_PROFILE"):
        os.environ["AWS_PROFILE"] = "mz-scratch-admin"
    if not os.environ.get("AWS_DEFAULT_REGION"):
        os.environ["AWS_DEFAULT_REGION"] = "us-east-2"


SPEAKER = ui.speaker("scratch> ")
ROOT = os.environ["MZ_ROOT"]


def launch(
    *,
    key_name: str,
    instance_type: str,
    ami: str,
    tags: Dict[str, str],
    name: Optional[str] = None,
) -> Any:
    """Launch and configure an ec2 instance with the given properties."""

    def is_ready(i: Any) -> bool:
        return bool(
            i.public_ip_address and i.state and i.state.get("Name") == "running"
        )

    if name:
        tags["Name"] = name

    SPEAKER(f"launching instance {name or '(unnamed)'}")
    ec2 = boto3.resource("ec2")
    with open(ROOT + "/misc/load-tests/provision.bash") as f:
        provisioning_script = f.read()
    i = ec2.create_instances(
        MinCount=1,
        MaxCount=1,
        ImageId=ami,
        InstanceType=instance_type,
        KeyName=key_name,
        UserData=provisioning_script,
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [{"Key": k, "Value": v} for (k, v) in tags.items()],
            }
        ],
    )[0]
    for remaining in ui.timeout_loop(60, 5):
        SPEAKER(f"Waiting for instance to be ready: {remaining}s remaining")
        i.reload()
        if is_ready(i):
            break

    if not is_ready(i):
        raise RuntimeError(
            "Instance did not become ready in a reasonable amount of time"
        )

    done = False
    for remaining in ui.timeout_loop(180, 5):
        SPEAKER(f"Waiting for instance to finish setup: {remaining}s remaining")
        try:
            ssh.runv(["[", "-f", "/DONE", "]"], "ubuntu", i.public_ip_address)
        except CalledProcessError:
            continue
        done = True
        break

    if not done:
        raise RuntimeError(
            "Instance did not finish setup in a reasonable amount of time"
        )

    return i


def mkrepo(i: Any) -> None:
    """Create a Materialize repository on the remote ec2 instance and push the present repository to it."""
    ssh.runv(
        ["git", "init", "--bare", "/home/ubuntu/materialize/.git"],
        "ubuntu",
        i.public_ip_address,
    )
    os.chdir(ROOT)
    git.push(f"ubuntu@{i.public_ip_address}:~/materialize/.git")
    head_rev = git.rev_parse("HEAD")
    ssh.runv(
        ["git", "-C", "/home/ubuntu/materialize", "config", "core.bare", "false"],
        "ubuntu",
        i.public_ip_address,
    )
    ssh.runv(
        ["git", "-C", "/home/ubuntu/materialize", "checkout", "head_rev"],
        "ubuntu",
        i.public_ip_address,
    )
