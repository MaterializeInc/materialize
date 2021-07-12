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
import json
import argparse
import asyncio
import random
import shlex
import sys
import boto3
from subprocess import CalledProcessError
from typing import Any, Dict, List, NamedTuple, Optional
from datetime import datetime

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

EC2: Any  # "boto3.resources.factory.ec2.ServiceResource"


def launch(
    *,
    key_name: str,
    instance_type: str,
    ami: str,
    tags: Dict[str, str],
    display_name: Optional[str] = None,
    subnet_id: Optional[str] = None,
    size_gb: int,
    security_group_id: str,
    nonce: str,
) -> Any:
    """Launch and configure an ec2 instance with the given properties."""

    if display_name:
        tags["Name"] = display_name
    tags["scratch-created"] = str(datetime.now().timestamp())
    tags["nonce"] = nonce
    tags["git_ref"] = git.describe()
    import pprint

    pprint.pprint(tags)

    SPEAKER(f"launching instance {display_name or '(unnamed)'}")
    with open(ROOT + "/misc/load-tests/provision.bash") as f:
        provisioning_script = f.read()
    i = EC2.create_instances(
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
        NetworkInterfaces=[
            {
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "Groups": [security_group_id],
                "SubnetId": subnet_id,
            }
        ],
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": size_gb,
                    "VolumeType": "gp3",
                },
            }
        ],
    )[0]

    return i


async def setup(i: Any, subnet_id: Optional[str]) -> None:
    def is_ready(i: Any) -> bool:
        return bool(
            i.public_ip_address and i.state and i.state.get("Name") == "running"
        )

    done = False
    print("In setup")
    async for remaining in ui.async_timeout_loop(60, 5):
        print("In loop")
        SPEAKER(f"Waiting for instance to become ready: {remaining}s remaining")
        i.reload()
        if is_ready(i):
            done = True
            break

    if not done:
        raise RuntimeError(
            f"Instance {i} did not become ready in a reasonable amount of time"
        )

    done = False
    async for remaining in ui.async_timeout_loop(180, 5):
        try:
            ssh.runv(["[", "-f", "/DONE", "]"], "ubuntu", i.public_ip_address)
            done = True
            break
        except CalledProcessError:
            continue

    if not done:
        raise RuntimeError(
            "Instance did not finish setup in a reasonable amount of time"
        )

    mkrepo(i)


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
        ["git", "-C", "/home/ubuntu/materialize", "checkout", head_rev],
        "ubuntu",
        i.public_ip_address,
    )


class MachineDesc(NamedTuple):
    name: str
    launch_script: Optional[str]
    instance_type: str
    ami: str
    tags: Dict[str, str]
    size_gb: int


async def setup_all(instances: List[Any], subnet_id: str) -> None:
    await asyncio.gather(*(setup(i, subnet_id) for i in instances))


def launch_cluster(
    descs: List[MachineDesc],
    nonce: str,
    subnet_id: str,
    key_name: str,
    security_group_id: str,
    extra_tags: Dict[str, str],
) -> List[Any]:
    """Launch a cluster of instances with a given nonce"""
    instances = [
        launch(
            key_name=key_name,
            instance_type=d.instance_type,
            ami=d.ami,
            tags={**d.tags, **extra_tags},
            display_name=f"{nonce}-{d.name}",
            size_gb=d.size_gb,
            subnet_id=subnet_id,
            security_group_id=security_group_id,
            nonce=nonce,
        )
        for d in descs
    ]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup_all(instances, subnet_id))
    loop.close()

    hosts_str = "".join(
        (f"{i.private_ip_address}\t{d.name}\n" for (i, d) in zip(instances, descs))
    )

    for i in instances:
        ssh.runv(
            [f"echo {shlex.quote(hosts_str)} | sudo tee -a /etc/hosts"],
            "ubuntu",
            i.public_ip_address,
        )

    for (i, d) in zip(instances, descs):
        if d.launch_script:
            ssh.runv(
                [
                    "cd",
                    "~/materialize",
                    ";",
                    "nohup",
                    "bash",
                    "-c",
                    shlex.quote(d.launch_script),
                    ">~/mzscratch-startup.out",
                    "2>~/mzscratch-startup.err",
                    "&",
                ],
                "ubuntu",
                i.public_ip_address,
            )

    return instances


def multi_json(s: str) -> List[Dict[Any, Any]]:
    """Read zero or more JSON objects from a string,
    without requiring each of them to be on its own line.

    For example:
    {
        "name": "First Object"
    }{"name": "Second Object"}
    """

    decoder = json.JSONDecoder()
    idx = 0
    result = []
    while idx < len(s):
        if s[idx] in " \t\n\r":
            idx += 1
        else:
            (obj, idx) = decoder.raw_decode(s, idx)
            result.append(obj)

    return result


def main() -> None:
    # Sane defaults for internal Materialize use in the scratch account
    DEFAULT_SUBNET_ID = "subnet-0b47df5733387582b"
    DEFAULT_SG_ID = "sg-0f2d62ae0f39f93cc"

    parser = argparse.ArgumentParser()
    parser.add_argument("--subnet-id", type=str, default=DEFAULT_SUBNET_ID)
    parser.add_argument("--key-name", type=str, required=True)
    parser.add_argument("--security-group-id", type=str, default=DEFAULT_SG_ID)
    parser.add_argument("--extra-tags", type=str, required=False)
    args = parser.parse_args()
    extra_tags = {}
    if args.extra_tags:
        extra_tags = json.loads(args.extra_tags)
        if not isinstance(extra_tags, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in extra_tags.items()
        ):
            raise RuntimeError(
                "extra-tags must be a JSON dictionary of strings to strings"
            )

    check_required_vars()
    global EC2
    EC2 = boto3.resource("ec2")

    descs = [
        MachineDesc(
            name=obj["name"],
            launch_script=obj.get("launch_script"),
            instance_type=obj["instance_type"],
            ami=obj["ami"],
            tags=obj.get("tags", dict()),
            size_gb=obj["size_gb"],
        )
        for obj in multi_json(sys.stdin.read())
    ]

    nonce = "".join(random.choice("0123456789abcdef") for n in range(8))

    launch_cluster(
        descs, nonce, args.subnet_id, args.key_name, args.security_group_id, extra_tags
    )


if __name__ == "__main__":
    main()
