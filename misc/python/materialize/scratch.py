# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for launching and interacting with scratch EC2 instances."""

import asyncio
import csv
import os
import shlex
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from subprocess import CalledProcessError
from typing import Dict, List, NamedTuple, Optional

import boto3
from mypy_boto3_ec2.service_resource import Instance
from mypy_boto3_ec2.type_defs import (
    InstanceNetworkInterfaceSpecificationTypeDef,
    InstanceTypeDef,
    RunInstancesRequestRequestTypeDef,
)
from prettytable import PrettyTable

from materialize import git, spawn, ssh, ui

SPEAKER = ui.speaker("scratch> ")
ROOT = os.environ["MZ_ROOT"]


def tags(i: Instance) -> Dict[str, str]:
    if not i.tags:
        return {}
    return {t["Key"]: t["Value"] for t in i.tags}


def instance_typedef_tags(i: InstanceTypeDef) -> Dict[str, str]:
    return {t["Key"]: t["Value"] for t in i.get("Tags", [])}


def name(tags: Dict[str, str]) -> Optional[str]:
    return tags.get("Name")


def launched_by(tags: Dict[str, str]) -> Optional[str]:
    return tags.get("LaunchedBy")


def delete_after(tags: Dict[str, str]) -> Optional[datetime]:
    unix = tags.get("scratch-delete-after")
    if not unix:
        return None
    unix = int(float(unix))
    return datetime.fromtimestamp(unix)


def print_instances(ists: List[Instance], format: str) -> None:
    field_names = [
        "Name",
        "Instance ID",
        "Public IP Address",
        "Private IP Address",
        "Launched By",
        "Delete After",
        "State",
    ]
    rows = [
        [
            name(tags),
            i.instance_id,
            i.public_ip_address,
            i.private_ip_address,
            launched_by(tags),
            delete_after(tags),
            i.state["Name"],
        ]
        for (i, tags) in [(i, tags(i)) for i in ists]
    ]
    if format == "table":
        pt = PrettyTable()
        pt.field_names = field_names
        pt.add_rows(rows)
        print(pt)
    elif format == "csv":
        w = csv.writer(sys.stdout)
        w.writerow(field_names)
        w.writerows(rows)
    else:
        raise RuntimeError("Unknown format passed to print_instances")


def now_plus(offset: timedelta) -> int:
    """Return a Unix timestamp representing the current time plus a given offset"""
    return int(datetime.now(timezone.utc).timestamp() + offset.total_seconds())


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
    delete_after: int,
) -> Instance:
    """Launch and configure an ec2 instance with the given properties."""

    if display_name:
        tags["Name"] = display_name
    tags["scratch-delete-after"] = str(delete_after)
    tags["nonce"] = nonce
    tags["git_ref"] = git.describe()

    network_interface: InstanceNetworkInterfaceSpecificationTypeDef = {
        "AssociatePublicIpAddress": True,
        "DeviceIndex": 0,
        "Groups": [security_group_id],
    }
    if subnet_id:
        network_interface["SubnetId"] = subnet_id

    SPEAKER(f"launching instance {display_name or '(unnamed)'}")
    with open(ROOT + "/misc/load-tests/provision.bash") as f:
        provisioning_script = f.read()
    kwargs: RunInstancesRequestRequestTypeDef = {
        "MinCount": 1,
        "MaxCount": 1,
        "ImageId": ami,
        "InstanceType": instance_type,  # type: ignore
        "UserData": provisioning_script,
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": k, "Value": v} for (k, v) in tags.items()],
            }
        ],
        "NetworkInterfaces": [network_interface],
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": size_gb,
                    "VolumeType": "gp3",
                },
            }
        ],
        "MetadataOptions": {
            # Allow Docker containers to access IMDSv2.
            "HttpPutResponseHopLimit": 2,
        },
    }
    if key_name:
        kwargs["KeyName"] = key_name
    if instance_profile:
        kwargs["IamInstanceProfile"] = {"Name": instance_profile}
    i = boto3.resource("ec2").create_instances(**kwargs)[0]
    print(i.tags)

    return i


class CommandResult(NamedTuple):
    status: str
    stdout: str
    stderr: str


async def run_ssm(
    instance_id: str, commands: List[str], timeout: int = 60
) -> CommandResult:
    id = boto3.client("ssm").send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
    )["Command"]["CommandId"]

    async for remaining in ui.async_timeout_loop(timeout, 5):
        invocation_dne = boto3.client("ssm").exceptions.InvocationDoesNotExist
        SPEAKER(f"Waiting for commands to finish running: {remaining}s remaining")
        try:
            result = boto3.client("ssm").get_command_invocation(
                CommandId=id, InstanceId=instance_id
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
        f"Command {commands} on instance {instance_id} did not run in a reasonable amount of time"
    )


async def setup(
    i: Instance,
    subnet_id: Optional[str],
    local_pub_key: str,
    identity_file: str,
    git_rev: str,
) -> None:
    def is_ready(i: Instance) -> bool:
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
            await run_ssm(i.instance_id, commands, 180)
            done = True
            break
        except invalid_instance:
            pass

    if not done:
        raise RuntimeError(f"Failed to run SSM commands on instance {i}")

    done = False
    async for remaining in ui.async_timeout_loop(180, 5):
        try:
            ssh.runv(
                ["[", "-f", "/DONE", "]"],
                "ubuntu",
                i.public_ip_address,
                identity_file=identity_file,
            )
            done = True
            break
        except CalledProcessError:
            continue

    if not done:
        raise RuntimeError(
            "Instance did not finish setup in a reasonable amount of time"
        )

    mkrepo(i, identity_file, git_rev)


def mkrepo(i: Instance, identity_file: str, rev: str) -> None:
    """Create a Materialize repository on the remote ec2 instance and push the present repository to it."""
    ssh.runv(
        ["git", "init", "--bare", "/home/ubuntu/materialize/.git"],
        "ubuntu",
        i.public_ip_address,
        identity_file=identity_file,
    )
    os.chdir(ROOT)
    os.environ["GIT_SSH_COMMAND"] = f"ssh -i {identity_file}"
    head_rev = git.rev_parse(rev)
    git.push(
        f"ubuntu@{i.public_ip_address}:~/materialize/.git",
        f"refs/heads/scratch_{head_rev}",
    )
    ssh.runv(
        ["git", "-C", "/home/ubuntu/materialize", "config", "core.bare", "false"],
        "ubuntu",
        i.public_ip_address,
        identity_file=identity_file,
    )
    ssh.runv(
        ["git", "-C", "/home/ubuntu/materialize", "checkout", head_rev],
        "ubuntu",
        i.public_ip_address,
        identity_file=identity_file,
    )


class MachineDesc(NamedTuple):
    name: str
    launch_script: Optional[str]
    instance_type: str
    ami: str
    tags: Dict[str, str]
    size_gb: int
    checkout: bool = True


async def setup_all(
    instances: List[Instance],
    subnet_id: str,
    local_pub_key: str,
    identity_file: str,
    git_rev: str,
) -> None:
    await asyncio.gather(
        *(setup(i, subnet_id, local_pub_key, identity_file, git_rev) for i in instances)
    )


def launch_cluster(
    descs: List[MachineDesc],
    nonce: str,
    subnet_id: str,
    key_name: Optional[str],
    security_group_id: str,
    instance_profile: Optional[str],
    extra_tags: Dict[str, str],
    delete_after: int,  # Unix timestamp.
    git_rev: str,
    extra_env: Dict[str, str],
) -> List[Instance]:
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
            delete_after=delete_after,
        )
        for d in descs
    ]

    # Generate temporary ssh key for running commands remotely
    tmpdir = tempfile.TemporaryDirectory()

    identity_file = f"{tmpdir.name}/id_rsa"
    spawn.runv(["ssh-keygen", "-t", "rsa", "-N", "", "-f", identity_file])
    with open(f"{tmpdir.name}/id_rsa.pub") as pk:
        local_pub_key = pk.read().strip()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            *(
                setup(
                    i,
                    subnet_id,
                    local_pub_key,
                    identity_file,
                    (git_rev if d.checkout else "HEAD"),
                )
                for (i, d) in zip(instances, descs)
            )
        )
    )
    hosts_str = "".join(
        (f"{i.private_ip_address}\t{d.name}\n" for (i, d) in zip(instances, descs))
    )

    env = [f"{k}={v}" for k, v in extra_env.items()]

    for i in instances:
        ssh.runv(
            [f"echo {shlex.quote(hosts_str)} | sudo tee -a /etc/hosts"],
            "ubuntu",
            i.public_ip_address,
            identity_file=identity_file,
        )

    for (i, d) in zip(instances, descs):
        if d.launch_script:
            ssh.runv(
                [
                    "cd",
                    "~/materialize",
                    ";",
                ]
                + env
                + [
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
                identity_file=identity_file,
            )

    tmpdir.cleanup()
    return instances


def whoami() -> str:
    return boto3.client("sts").get_caller_identity()["UserId"].split(":")[1]


def get_instances_by_tag(k: str, v: str) -> List[InstanceTypeDef]:
    return [
        i
        for r in boto3.client("ec2").describe_instances()["Reservations"]
        for i in r["Instances"]
        if instance_typedef_tags(i).get(k) == v
    ]


def get_old_instances() -> List[InstanceTypeDef]:
    def is_running(i: InstanceTypeDef) -> bool:
        return i["State"]["Name"] == "running"

    def is_old(i: InstanceTypeDef) -> bool:
        delete_after = instance_typedef_tags(i).get("scratch-delete-after")
        if delete_after is None:
            return False
        delete_after = float(delete_after)
        return datetime.now(timezone.utc).timestamp() > delete_after

    return [
        i
        for r in boto3.client("ec2").describe_instances()["Reservations"]
        for i in r["Instances"]
        if is_running(i) and is_old(i)
    ]
