# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Hetzner Cloud provider for scratch instances."""

import asyncio
import datetime
import os
import pathlib
import shlex
import sys
from subprocess import CalledProcessError

from materialize import MZ_ROOT, git, ui, util
from materialize.scratch import (
    MachineDesc,
    ScratchInstance,
    mkrepo,
    mssh,
    say,
    setup_ai_tools,
)

HETZNER_SSH_COMMAND = [
    "ssh",
    "-o",
    "StrictHostKeyChecking=off",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "BatchMode=yes",
]
HETZNER_SFTP_COMMAND = [
    "sftp",
    "-o",
    "StrictHostKeyChecking=off",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "BatchMode=yes",
]

DEFAULT_LOCATION = "fsn1"
DEFAULT_FIREWALL_NAME = "scratch-firewall"


def _sanitize_label_value(value: str) -> str:
    """Sanitize a string for use as a Hetzner Cloud label value.

    Hetzner labels allow: [a-zA-Z0-9._-], max 63 chars.
    """
    value = value.replace("@", "_at_")
    return "".join(c for c in value if c.isalnum() or c in "_.-")[:63]


def get_token() -> str:
    """Get the Hetzner Cloud API token from env or hcloud CLI config."""
    token = os.environ.get("HETZNER_SCRATCH_TOKEN")
    if token:
        return token

    # Try reading from hcloud CLI config
    config_path = pathlib.Path.home() / ".config" / "hcloud" / "cli.toml"
    if config_path.exists():
        import toml

        config = toml.load(config_path)
        active_context = config.get("active_context")
        if active_context and "contexts" in config:
            for ctx in config["contexts"]:
                if ctx.get("name") == active_context:
                    token = ctx.get("token", "")
                    if token:
                        return token

    raise RuntimeError(
        "Hetzner Cloud API token not found. Set HETZNER_SCRATCH_TOKEN or run `hcloud context create` (https://console.hetzner.cloud -> Scratch -> Security -> API Tokens)."
    )


def get_client():
    """Get an authenticated hcloud Client."""
    from hcloud import Client

    return Client(token=get_token())


def whoami() -> str:
    """Identify the current user.

    Hetzner tokens are per-project, not per-user, so we use
    the local git user email or system username, sanitized for
    Hetzner label constraints.
    """
    email = git.get_user_email()
    if email:
        return _sanitize_label_value(email)
    return _sanitize_label_value(os.environ.get("USER", "unknown"))


def _ensure_ssh_key(client):
    """Ensure the user's SSH public key is registered with Hetzner Cloud."""
    home = pathlib.Path.home()
    key_paths = [
        home / ".ssh" / "id_ed25519.pub",
        home / ".ssh" / "id_rsa.pub",
        home / ".ssh" / "id_ecdsa.pub",
    ]
    pub_key = None
    for kp in key_paths:
        if kp.exists():
            pub_key = kp.read_text().strip()
            break

    if not pub_key:
        raise RuntimeError(
            "No SSH public key found (~/.ssh/id_ed25519.pub, id_rsa.pub, or id_ecdsa.pub).\n"
            "Create one with: ssh-keygen -t ed25519"
        )

    key_name = f"scratch-{whoami()}"
    existing = client.ssh_keys.get_by_name(key_name)
    if existing:
        if existing.public_key.strip() != pub_key:
            client.ssh_keys.delete(existing)
            return client.ssh_keys.create(name=key_name, public_key=pub_key)
        return existing

    return client.ssh_keys.create(name=key_name, public_key=pub_key)


def _ensure_firewall(client):
    """Ensure a scratch firewall exists allowing inbound SSH."""
    from hcloud.firewalls.domain import FirewallRule

    existing = client.firewalls.get_by_name(DEFAULT_FIREWALL_NAME)
    if existing:
        return existing

    rules = [
        FirewallRule(
            direction=FirewallRule.DIRECTION_IN,
            protocol=FirewallRule.PROTOCOL_TCP,
            port="22",
            source_ips=["0.0.0.0/0", "::/0"],
        ),
    ]
    response = client.firewalls.create(
        name=DEFAULT_FIREWALL_NAME,
        rules=rules,
    )
    return response.firewall


def _server_to_scratch(server) -> ScratchInstance:
    """Convert a Hetzner Server to a ScratchInstance."""
    labels = server.labels or {}
    delete_after_ts = None
    if "scratch-delete-after" in labels:
        unix = int(float(labels["scratch-delete-after"]))
        delete_after_ts = datetime.datetime.fromtimestamp(unix)

    public_ip = None
    if server.public_net and server.public_net.ipv4:
        public_ip = server.public_net.ipv4.ip

    return ScratchInstance(
        provider="hetzner",
        instance_id=str(server.id),
        name=labels.get("Name", server.name),
        public_ip=public_ip,
        private_ip=None,
        state=server.status,
        launched_by=labels.get("LaunchedBy"),
        delete_after_ts=delete_after_ts,
        ami_user=labels.get("ami-user", "ubuntu"),
        tags=labels,
        ssh_command=list(HETZNER_SSH_COMMAND),
        sftp_command=list(HETZNER_SFTP_COMMAND),
    )


def launch(
    *,
    server_type: str,
    image: str,
    location: str,
    ami_user: str,
    labels: dict[str, str],
    display_name: str | None = None,
    size_gb: int,
    nonce: str,
    delete_after: datetime.datetime,
    ssh_key,
    firewall,
):
    """Launch a Hetzner Cloud server."""
    from hcloud.images import Image
    from hcloud.locations import Location
    from hcloud.server_types import ServerType

    if display_name:
        labels["Name"] = _sanitize_label_value(display_name)
    labels["scratch-delete-after"] = str(delete_after.timestamp())
    labels["nonce"] = nonce
    labels["git_ref"] = git.describe()
    labels["ami-user"] = ami_user

    # Hetzner server names must be RFC 1123 compliant
    server_name = (display_name or f"scratch-{nonce}").lower().replace(" ", "-")
    server_name = "".join(c for c in server_name if c.isalnum() or c == "-")
    server_name = server_name.strip("-")[:63]

    say(f"launching server {display_name or '(unnamed)'}")

    with open(MZ_ROOT / "misc" / "scratch" / "provision-hetzner.bash") as f:
        provisioning_script = f.read()

    # cloud-init treats user_data starting with #! as a shell script
    user_data = provisioning_script

    client = get_client()
    response = client.servers.create(
        name=server_name,
        server_type=ServerType(name=server_type),
        image=Image(name=image),
        location=Location(name=location),
        ssh_keys=[ssh_key],
        user_data=user_data,
        labels=labels,
        firewalls=[firewall],
    )

    return response.server


async def setup(server, git_rev: str) -> None:
    """Wait for a Hetzner server to be ready and set up the git repo."""
    client = get_client()

    server_id: int = server.id  # type: ignore[assignment]

    done = False
    async for remaining in ui.async_timeout_loop(120, 2):
        print(
            f"\rscratch> Waiting for server to become ready: {remaining:0.0f}s remaining\033[K",
            end="",
            flush=True,
            file=sys.stderr,
        )
        server = client.servers.get_by_id(server_id)
        if server.status == "running" and server.public_net and server.public_net.ipv4:
            done = True
            break
    print(file=sys.stderr)  # newline after inline countdown
    if not done:
        raise RuntimeError(
            f"Server {server.name} did not become ready in a reasonable amount of time"
        )

    si = _server_to_scratch(server)

    # Wait for SSH to be available (ubuntu user created), then start git clone
    # in parallel with the rest of provisioning.
    done = False
    async for remaining in ui.async_timeout_loop(300, 2):
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

    # Start git clone in the background while provisioning continues
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
        raise RuntimeError("Server did not finish setup in a reasonable amount of time")


def launch_cluster(
    descs: list[MachineDesc],
    *,
    nonce: str | None = None,
    extra_tags: dict[str, str] = {},
    delete_after: datetime.datetime,
    git_rev: str = "HEAD",
    extra_env: dict[str, str] = {},
    location: str = DEFAULT_LOCATION,
) -> list[ScratchInstance]:
    """Launch a cluster of Hetzner servers."""

    if not nonce:
        nonce = util.nonce(8)

    client = get_client()
    ssh_key = _ensure_ssh_key(client)
    firewall = _ensure_firewall(client)

    servers = [
        launch(
            server_type=d.server_type or "",
            image=d.image or "ubuntu-24.04",
            location=d.location or location,
            ami_user=d.ami_user,
            labels={**d.tags, **extra_tags},
            display_name=f"{nonce}-{d.name}",
            size_gb=d.size_gb,
            nonce=nonce,
            delete_after=delete_after,
            ssh_key=ssh_key,
            firewall=firewall,
        )
        for d in descs
    ]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        asyncio.gather(
            *(
                setup(s, git_rev if d.checkout else "HEAD")
                for (s, d) in zip(servers, descs)
            )
        )
    )

    # Reload and convert
    instances: list[ScratchInstance] = []
    for s in servers:
        sid: int = s.id  # type: ignore[assignment]
        s = client.servers.get_by_id(sid)
        instances.append(_server_to_scratch(s))

    hosts_str = "".join(
        f"{si.public_ip}\t{d.name}\n" for (si, d) in zip(instances, descs)
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


def get_instance(instance_name: str) -> ScratchInstance:
    """Get a Hetzner server by ID or 'mine'."""
    client = get_client()

    if instance_name == "mine":
        me = whoami()
        servers = client.servers.get_all(label_selector=f"LaunchedBy={me}")
        running = [s for s in servers if s.status == "running"]
        if not running:
            raise RuntimeError("can't understand 'mine': no owned server?")
        if len(running) > 1:
            raise RuntimeError(
                f"can't understand 'mine': too many owned servers ({', '.join(str(s.id) for s in running)})"
            )
        say(f"understanding 'mine' as unique owned server {running[0].id}")
        return _server_to_scratch(running[0])

    server = client.servers.get_by_id(int(instance_name))
    return _server_to_scratch(server)


def list_instances(
    owners: list[str] | None = None, all: bool = False
) -> list[ScratchInstance]:
    """List Hetzner servers."""
    client = get_client()

    if all:
        servers = client.servers.get_all()
    elif owners:
        servers = []
        for owner in owners:
            owner = _sanitize_label_value(owner)
            servers.extend(client.servers.get_all(label_selector=f"LaunchedBy={owner}"))
    else:
        me = whoami()
        servers = client.servers.get_all(label_selector=f"LaunchedBy={me}")

    return [_server_to_scratch(s) for s in servers]


def terminate_instances(instances: list[ScratchInstance]) -> None:
    """Terminate Hetzner servers."""
    client = get_client()
    for inst in instances:
        server = client.servers.get_by_id(int(inst.instance_id))
        client.servers.delete(server)
