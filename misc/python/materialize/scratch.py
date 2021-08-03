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
import time
from subprocess import CalledProcessError
from typing import Any, Dict, List, NamedTuple, Optional
from datetime import datetime

from materialize import git
from materialize import ui
from materialize import spawn
from materialize import ssh

SPEAKER = ui.speaker("scratch> ")
ROOT = os.environ["MZ_ROOT"]


def launch(
    *,
    key_name: Optional[str],
    instance_type: str,
    ami: str,
    tags: Dict[str, str],
    display_name: Optional[str] = None,
    subnet_id: Optional[str] = None,
    size_gb: int,
    security_group_id: str,
    instance_profile: Optional[str],
    nonce: str,
) -> Any:
    """Launch and configure an ec2 instance with the given properties."""

    if display_name:
        tags["Name"] = display_name
    tags["scratch-created"] = str(datetime.now().timestamp())
    tags["nonce"] = nonce
    tags["git_ref"] = git.describe()

    SPEAKER(f"launching instance {display_name or '(unnamed)'}")
    with open(ROOT + "/misc/load-tests/provision.bash") as f:
        provisioning_script = f.read()
    kwargs = {
        "MinCount": 1,
        "MaxCount": 1,
        "ImageId": ami,
        "InstanceType": instance_type,
        "UserData": provisioning_script,
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": k, "Value": v} for (k, v) in tags.items()],
            }
        ],
        "NetworkInterfaces": [
            {
                "AssociatePublicIpAddress": True,
                "DeviceIndex": 0,
                "Groups": [security_group_id],
                "SubnetId": subnet_id,
            }
        ],
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": size_gb,
                    "VolumeType": "gp3",
                },
            }
        ],
    }
    if key_name:
        kwargs["KeyName"] = key_name
    if instance_profile:
        kwargs["IamInstanceProfile"] = {"Name": instance_profile}
    i = boto3.resource("ec2").create_instances(**kwargs)[0]

    return i


class CommandResult(NamedTuple):
    status: str
    stdout: str
    stderr: str


async def run_ssm(i: Any, commands: List[str], timeout: int = 60) -> CommandResult:
    id = boto3.client("ssm").send_command(
        InstanceIds=[i.instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
    )["Command"]["CommandId"]

    async for remaining in ui.async_timeout_loop(timeout, 5):
        invocation_dne = boto3.client("ssm").exceptions.InvocationDoesNotExist
        SPEAKER(f"Waiting for commands to finish running: {remaining}s remaining")
        try:
            result = boto3.client("ssm").get_command_invocation(
                CommandId=id, InstanceId=i.instance_id
            )
        except invocation_dne:
            continue
        if result["Status"] != "InProgress":
            return CommandResult(
                status=result["Status"],
                stdout=result["StandardOutputContent"],
                stderr=result["StandardErrorContent"],
            )

    raise RuntimeError(
        f"Command {commands} on instance {i} did not run in a reasonable amount of time"
    )


async def setup(i: Any, subnet_id: Optional[str], local_pub_key: str) -> None:
    def is_ready(i: Any) -> bool:
        return bool(
            i.public_ip_address and i.state and i.state.get("Name") == "running"
        )

    done = False
    async for remaining in ui.async_timeout_loop(60, 5):
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
    invalid_instance = boto3.client("ssm").exceptions.InvalidInstanceId

    commands = [
        "mkdir -p ~ubuntu/.ssh",
        f"echo {local_pub_key} >> ~ubuntu/.ssh/authorized_keys",
    ]
    import pprint

    print("Running commands:")
    pprint.pprint(commands)
    async for remaining in ui.async_timeout_loop(180, 5):
        try:
            await run_ssm(i, commands, 180)
            done = True
            break
        except invalid_instance:
            pass

    if not done:
        raise RuntimeError(f"Failed to run SSM commands on instance {i}")

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


async def setup_all(instances: List[Any], subnet_id: str, local_pub_key: str) -> None:
    await asyncio.gather(*(setup(i, subnet_id, local_pub_key) for i in instances))


def launch_cluster(
    descs: List[MachineDesc],
    nonce: str,
    subnet_id: str,
    key_name: Optional[str],
    security_group_id: str,
    instance_profile: Optional[str],
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
            instance_profile=instance_profile,
            nonce=nonce,
        )
        for d in descs
    ]

    with open(f"{os.environ['HOME']}/.ssh/id_rsa.pub") as pk:
        local_pub_key = pk.read().strip()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup_all(instances, subnet_id, local_pub_key))
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


def get_old_instances(threshold_seconds: int) -> List[Any]:
    def is_running(i: Any) -> bool:
        return bool(i["State"]["Name"] == "running")

    def is_old(i: Any) -> bool:
        tags_dict = {tag["Key"]: tag["Value"] for tag in i["Tags"]}
        created = tags_dict.get("scratch-created")
        if created is None:
            return False
        created = float(created)
        return time.time() - created > threshold_seconds

    return [
        i
        for r in boto3.client("ec2").describe_instances()["Reservations"]
        for i in r["Instances"]
        if is_running(i) and is_old(i)
    ]
