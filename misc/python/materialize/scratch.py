# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Utilities for launching and interacting with scratch instances."""

import asyncio
import csv
import datetime
import os
import shlex
import subprocess
import sys
from subprocess import CalledProcessError
from typing import NamedTuple, cast

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_ec2.literals import InstanceTypeType
from mypy_boto3_ec2.service_resource import Instance
from mypy_boto3_ec2.type_defs import (
    FilterTypeDef,
    InstanceNetworkInterfaceSpecificationTypeDef,
    InstanceTypeDef,
    RunInstancesRequestServiceResourceCreateInstancesTypeDef,
)
from prettytable import PrettyTable
from pydantic import BaseModel

from materialize import MZ_ROOT, git, spawn, ui, util

# Sane defaults for internal Materialize use in the scratch account
DEFAULT_SECURITY_GROUP_NAME = "scratch-security-group"
DEFAULT_INSTANCE_PROFILE_NAME = "admin-instance"

SSH_COMMAND = ["mssh", "-o", "StrictHostKeyChecking=off"]
SFTP_COMMAND = ["msftp", "-o", "StrictHostKeyChecking=off"]

say = ui.speaker("scratch> ")


class MachineDesc(BaseModel):
    name: str
    provider: str | None = None  # "aws" or "hetzner"; inferred if not set
    launch_script: str | None = None
    # AWS-specific fields
    instance_type: str | None = None
    ami: str | None = None
    # Hetzner-specific fields
    server_type: str | None = None
    image: str | None = None
    location: str | None = None
    # Common fields
    tags: dict[str, str] = {}
    size_gb: int = 50
    checkout: bool = True
    ami_user: str = "ubuntu"

    def get_provider(self) -> str:
        """Return the provider, inferring from fields if not explicitly set."""
        if self.provider:
            return self.provider
        if self.server_type:
            return "hetzner"
        if self.instance_type:
            return "aws"
        raise RuntimeError(
            f"Cannot infer provider for machine '{self.name}': "
            "set 'provider' explicitly or provide 'instance_type' (AWS) / 'server_type' (Hetzner)"
        )


class ScratchInstance(BaseModel):
    """Provider-neutral representation of a scratch instance."""

    provider: str  # "aws" or "hetzner"
    instance_id: str
    name: str | None = None
    public_ip: str | None = None
    private_ip: str | None = None
    state: str = "unknown"
    launched_by: str | None = None
    delete_after_ts: datetime.datetime | None = None
    ami_user: str = "ubuntu"
    tags: dict[str, str] = {}
    ssh_command: list[str] = []
    sftp_command: list[str] = []

    @property
    def ssh_host(self) -> str:
        if self.provider == "aws":
            return f"{self.ami_user}@{self.instance_id}"
        return f"{self.ami_user}@{self.public_ip}"


# --- EC2 tag helpers ---


def tags(i: Instance) -> dict[str, str]:
    if not i.tags:
        return {}
    return {t["Key"]: t["Value"] for t in i.tags}


def instance_typedef_tags(i: InstanceTypeDef) -> dict[str, str]:
    return {t["Key"]: t["Value"] for t in i.get("Tags", [])}


def name(tags: dict[str, str]) -> str | None:
    return tags.get("Name")


def launched_by(tags: dict[str, str]) -> str | None:
    return tags.get("LaunchedBy")


def ami_user(tags: dict[str, str]) -> str | None:
    return tags.get("ami-user", "ubuntu")


def delete_after(tags: dict[str, str]) -> datetime.datetime | None:
    unix = tags.get("scratch-delete-after")
    if not unix:
        return None
    unix = int(float(unix))
    return datetime.datetime.fromtimestamp(unix)


def ec2_to_scratch(i: Instance) -> ScratchInstance:
    """Convert an EC2 Instance to a provider-neutral ScratchInstance."""
    t = tags(i)
    return ScratchInstance(
        provider="aws",
        instance_id=i.instance_id,
        name=name(t),
        public_ip=i.public_ip_address,
        private_ip=i.private_ip_address,
        state=i.state["Name"] if i.state else "unknown",
        launched_by=launched_by(t),
        delete_after_ts=delete_after(t),
        ami_user=ami_user(t) or "ubuntu",
        tags=t,
        ssh_command=list(SSH_COMMAND),
        sftp_command=list(SFTP_COMMAND),
    )


# --- Provider-neutral functions ---


def instance_host(instance: ScratchInstance) -> str:
    return instance.ssh_host


def print_instances(
    instances: list[ScratchInstance], format: str = "table", numbered: bool = False
) -> None:
    field_names = [
        "Provider",
        "Name",
        "Instance ID",
        "Public IP Address",
        "Private IP Address",
        "Launched By",
        "Delete After",
        "State",
    ]
    if numbered:
        field_names = ["#"] + field_names
    rows = []
    for idx, i in enumerate(instances, 1):
        row = [
            i.provider,
            i.name,
            i.instance_id,
            i.public_ip,
            i.private_ip,
            i.launched_by,
            i.delete_after_ts,
            i.state,
        ]
        if numbered:
            row = [idx] + row
        rows.append(row)
    if format == "table":
        pt = PrettyTable()
        pt.field_names = field_names
        pt.align = "l"
        pt.add_rows(rows)
        print(pt)
    elif format == "csv":
        w = csv.writer(sys.stdout)
        w.writerow(field_names)
        w.writerows(rows)
    else:
        raise RuntimeError("Unknown format passed to print_instances")


def mssh(
    instance: ScratchInstance,
    command: str,
    *,
    extra_ssh_args: list[str] = [],
    input: bytes | None = None,
    quiet: bool = False,
) -> None:
    """Runs a command over SSH."""
    host = instance.ssh_host
    if command:
        if not quiet:
            print(f"{host}$ {command}", file=sys.stderr)
        if instance.provider == "aws":
            # Quote to work around:
            # https://github.com/aws/aws-ec2-instance-connect-cli/pull/26
            command = shlex.quote(command)
    else:
        print(f"$ ssh {host}")

    result = subprocess.run(
        [
            *instance.ssh_command,
            *extra_ssh_args,
            host,
            command,
        ],
        input=input,
        stdout=subprocess.DEVNULL if quiet else None,
        stderr=subprocess.DEVNULL if quiet else None,
    )
    # Exit code 130 = SIGINT (Ctrl-C) — exit cleanly
    if result.returncode == 130:
        sys.exit(130)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, result.args)


def msftp(
    instance: ScratchInstance,
) -> None:
    """Connects over SFTP."""
    host = instance.ssh_host
    spawn.runv([*instance.sftp_command, host])


def setup_ai_tools(instance: ScratchInstance) -> None:
    """Transfer local Claude Code and Codex configs to a scratch instance
    and install the materialize-docs skill."""
    import io
    import pathlib
    import tarfile

    home = pathlib.Path.home()

    # Collect only essential config files — skip session logs, caches, etc.
    items: list[tuple[str, pathlib.Path]] = []

    # Claude Code: settings, credentials, and project memory files
    claude_dir = home / ".claude"
    if claude_dir.is_dir():
        for name in ("settings.json", ".credentials.json"):
            p = claude_dir / name
            if p.is_file():
                items.append((f".claude/{name}", p))
        # Transfer memory directories from all projects
        projects_dir = claude_dir / "projects"
        if projects_dir.is_dir():
            for proj in projects_dir.iterdir():
                if not proj.is_dir():
                    continue
                memory_dir = proj / "memory"
                if memory_dir.is_dir():
                    items.append((f".claude/projects/{proj.name}/memory", memory_dir))

    if (home / ".claude.json").is_file():
        items.append((".claude.json", home / ".claude.json"))

    # Codex: only config, not session logs
    codex_dir = home / ".codex"
    if codex_dir.is_dir():
        for name in ("config.json", "instructions.md"):
            p = codex_dir / name
            if p.is_file():
                items.append((f".codex/{name}", p))

    if items:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for arcname, path in items:
                tar.add(str(path), arcname=arcname)
        mssh(instance, "tar xzf - -C ~", input=buf.getvalue(), quiet=True)

    mssh(
        instance,
        "cd materialize && "
        "NPM_CONFIG_UPDATE_NOTIFIER=false "
        "npx -q -y skills add MaterializeInc/agent-skills -g -a claude-code --copy -y --skill materialize-docs 2>/dev/null || true",
        quiet=True,
    )

    mssh(
        instance,
        "cd materialize && "
        "NPM_CONFIG_UPDATE_NOTIFIER=false "
        "npx -q -y skills add MaterializeInc/agent-skills -g -a codex --copy -y --skill materialize-docs 2>/dev/null || true",
        quiet=True,
    )


def mkrepo(
    instance: ScratchInstance, rev: str, init: bool = True, force: bool = False
) -> None:
    if init:
        mssh(instance, "git clone https://github.com/MaterializeInc/materialize.git")

    rev = git.rev_parse(rev)

    cmd: list[str] = [
        "git",
        "push",
        "--no-verify",
        f"{instance.ssh_host}:materialize/.git",
        # Explicit refspec is required if the host repository is in detached
        # HEAD mode.
        f"{rev}:refs/heads/scratch",
    ]
    if force:
        cmd.append("--force")

    spawn.runv(
        cmd,
        cwd=MZ_ROOT,
        env=dict(os.environ, GIT_SSH_COMMAND=" ".join(instance.ssh_command)),
    )
    git_config_cmds = f"git checkout -f {rev}"

    # Propagate local git user.name and user.email to the scratch instance.
    if git_name := git.get_user_name():
        git_config_cmds += f" && git config user.name {shlex.quote(git_name)}"
    if git_email := git.get_user_email():
        git_config_cmds += f" && git config user.email {shlex.quote(git_email)}"

    mssh(
        instance,
        f"cd materialize && {git_config_cmds}",
    )


# --- AWS-specific functions ---


def launch(
    *,
    key_name: str | None,
    instance_type: str,
    ami: str,
    ami_user: str,
    tags: dict[str, str],
    display_name: str | None = None,
    size_gb: int,
    security_group_name: str,
    instance_profile: str | None,
    nonce: str,
    delete_after: datetime.datetime,
) -> Instance:
    """Launch and configure an ec2 instance with the given properties."""

    if display_name:
        tags["Name"] = display_name
    tags["scratch-delete-after"] = str(delete_after.timestamp())
    tags["nonce"] = nonce
    tags["git_ref"] = git.describe()
    tags["ami-user"] = ami_user

    ec2 = boto3.client("ec2")
    groups = ec2.describe_security_groups()
    security_group_id = None
    for group in groups["SecurityGroups"]:
        if group["GroupName"] == security_group_name:
            security_group_id = group["GroupId"]
            break

    if security_group_id is None:
        vpcs = ec2.describe_vpcs()
        vpc_id = None
        for vpc in vpcs["Vpcs"]:
            if vpc["IsDefault"] == True:
                vpc_id = vpc["VpcId"]
                break
        if vpc_id is None:
            default_vpc = ec2.create_default_vpc()
            vpc_id = default_vpc["Vpc"]["VpcId"]
        securitygroup = ec2.create_security_group(
            GroupName=security_group_name,
            Description="Allows all.",
            VpcId=vpc_id,
        )
        security_group_id = securitygroup["GroupId"]
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            CidrIp="0.0.0.0/0",
            IpProtocol="tcp",
            FromPort=22,
            ToPort=22,
        )

    network_interface: InstanceNetworkInterfaceSpecificationTypeDef = {
        "AssociatePublicIpAddress": True,
        "DeviceIndex": 0,
        "Groups": [security_group_id],
    }

    say(f"launching instance {display_name or '(unnamed)'}")
    with open(MZ_ROOT / "misc" / "scratch" / "provision.bash") as f:
        provisioning_script = f.read()
    kwargs: RunInstancesRequestServiceResourceCreateInstancesTypeDef = {
        "MinCount": 1,
        "MaxCount": 1,
        "ImageId": ami,
        "InstanceType": cast(InstanceTypeType, instance_type),
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

    return i


class CommandResult(NamedTuple):
    status: str
    stdout: str
    stderr: str


async def setup(
    i: Instance,
    git_rev: str,
) -> None:
    def is_ready(i: Instance) -> bool:
        return bool(
            i.public_ip_address and i.state and i.state.get("Name") == "running"
        )

    done = False
    async for remaining in ui.async_timeout_loop(60, 2):
        print(
            f"\rscratch> Waiting for instance to become ready: {remaining:0.0f}s remaining\033[K",
            end="",
            flush=True,
            file=sys.stderr,
        )
        try:
            i.reload()
            if is_ready(i):
                done = True
                break
        except ClientError:
            pass
    print(file=sys.stderr)
    if not done:
        raise RuntimeError(
            f"Instance {i} did not become ready in a reasonable amount of time"
        )

    # Convert to ScratchInstance for SSH operations
    si = ec2_to_scratch(i)

    # Wait for SSH to be available
    done = False
    async for remaining in ui.async_timeout_loop(120, 2):
        print(
            f"\rscratch> Waiting for SSH access: {remaining:0.0f}s remaining\033[K",
            end="",
            flush=True,
            file=sys.stderr,
        )
        try:
            mssh(si, "true", quiet=True)
            done = True
            break
        except CalledProcessError:
            continue
    print(file=sys.stderr)
    if not done:
        raise RuntimeError(
            "SSH did not become available in a reasonable amount of time"
        )

    # Start git clone while provisioning continues
    say("Cloning repo (provisioning continues in background)...")
    mkrepo(si, git_rev)

    # Wait for provisioning to finish
    done = False
    async for remaining in ui.async_timeout_loop(300, 2):
        print(
            f"\rscratch> Waiting for provisioning to complete: {remaining:0.0f}s remaining\033[K",
            end="",
            flush=True,
            file=sys.stderr,
        )
        try:
            mssh(si, "[[ -f /opt/provision/done ]]", quiet=True)
            done = True
            break
        except CalledProcessError:
            continue
    print(file=sys.stderr)
    if not done:
        raise RuntimeError(
            "Instance did not finish setup in a reasonable amount of time"
        )


def launch_cluster(
    descs: list[MachineDesc],
    *,
    nonce: str | None = None,
    key_name: str | None = None,
    security_group_name: str = DEFAULT_SECURITY_GROUP_NAME,
    instance_profile: str | None = DEFAULT_INSTANCE_PROFILE_NAME,
    extra_tags: dict[str, str] = {},
    delete_after: datetime.datetime,
    git_rev: str = "HEAD",
    extra_env: dict[str, str] = {},
) -> list[ScratchInstance]:
    """Launch a cluster of instances with a given nonce"""

    if not nonce:
        nonce = util.nonce(8)

    ec2_instances = [
        launch(
            key_name=key_name,
            instance_type=cast(str, d.instance_type),
            ami=cast(str, d.ami),
            ami_user=d.ami_user,
            tags={**d.tags, **extra_tags},
            display_name=f"{nonce}-{d.name}",
            size_gb=d.size_gb,
            security_group_name=security_group_name,
            instance_profile=instance_profile,
            nonce=nonce,
            delete_after=delete_after,
        )
        for d in descs
    ]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            *(
                setup(i, git_rev if d.checkout else "HEAD")
                for (i, d) in zip(ec2_instances, descs)
            )
        )
    )

    # Reload and convert to ScratchInstances
    instances: list[ScratchInstance] = []
    for i in ec2_instances:
        i.reload()
        instances.append(ec2_to_scratch(i))

    hosts_str = "".join(
        f"{si.private_ip}\t{d.name}\n" for (si, d) in zip(instances, descs)
    )
    for si in instances:
        mssh(si, "sudo tee -a /etc/hosts", input=hosts_str.encode())

    for si in instances:
        setup_ai_tools(si)

    env = " ".join(f"{k}={shlex.quote(v)}" for k, v in extra_env.items())
    for si, d in zip(instances, descs):
        if d.launch_script:
            mssh(
                si,
                f"(cd materialize && {env} nohup bash -c {shlex.quote(d.launch_script)}) &> mzscratch.log &",
            )

    return instances


def whoami() -> str:
    return boto3.client("sts").get_caller_identity()["UserId"].split(":")[1]


def get_instance(instance_name: str) -> ScratchInstance:
    """
    Get an instance by instance id. The special name 'mine' resolves to a
    unique running owned instance, if there is one; otherwise the name is
    assumed to be an instance id.
    """
    if instance_name == "mine":
        filters: list[FilterTypeDef] = [
            {"Name": "tag:LaunchedBy", "Values": [whoami()]},
            {"Name": "instance-state-name", "Values": ["pending", "running"]},
        ]
        instances = [i for i in boto3.resource("ec2").instances.filter(Filters=filters)]
        if not instances:
            raise RuntimeError("can't understand 'mine': no owned instance?")
        if len(instances) > 1:
            raise RuntimeError(
                f"can't understand 'mine': too many owned instances ({', '.join(i.id for i in instances)})"
            )
        instance = instances[0]
        say(f"understanding 'mine' as unique owned instance {instance.id}")
        return ec2_to_scratch(instance)
    return ec2_to_scratch(boto3.resource("ec2").Instance(instance_name))


def list_instances(
    owners: list[str] | None = None, all: bool = False
) -> list[ScratchInstance]:
    """List AWS instances, optionally filtered by owner."""
    filters: list[FilterTypeDef] = []
    if not all:
        if not owners:
            owners = [whoami()]
        filters.append({"Name": "tag:LaunchedBy", "Values": owners})
    ec2_instances = list(boto3.resource("ec2").instances.filter(Filters=filters))
    return [ec2_to_scratch(i) for i in ec2_instances]


def terminate_instances(instances: list[ScratchInstance]) -> None:
    """Terminate AWS EC2 instances."""
    for inst in instances:
        boto3.resource("ec2").Instance(inst.instance_id).terminate()


def get_instances_by_tag(k: str, v: str) -> list[InstanceTypeDef]:
    return [
        i
        for r in boto3.client("ec2").describe_instances()["Reservations"]
        for i in r["Instances"]
        if instance_typedef_tags(i).get(k) == v
    ]


def get_old_instances() -> list[InstanceTypeDef]:
    def exists(i: InstanceTypeDef) -> bool:
        return i["State"]["Name"] != "terminated"

    def is_old(i: InstanceTypeDef) -> bool:
        delete_after = instance_typedef_tags(i).get("scratch-delete-after")
        if delete_after is None:
            return False
        delete_after = float(delete_after)
        return datetime.datetime.utcnow().timestamp() > delete_after

    return [
        i
        for r in boto3.client("ec2").describe_instances()["Reservations"]
        for i in r["Instances"]
        if exists(i) and is_old(i)
    ]
