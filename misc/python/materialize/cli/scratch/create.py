# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import datetime
import json
from typing import Any

from materialize.cli.scratch import check_required_vars
from materialize.scratch import (
    DEFAULT_INSTANCE_PROFILE_NAME,
    DEFAULT_SECURITY_GROUP_NAME,
    MZ_ROOT,
    MachineDesc,
    launch_cluster,
    mssh,
    print_instances,
    render_table,
    whoami,
)

MAX_AGE_DAYS = 1.5


def _natsort_key(s: str) -> list:
    """Split a string into text and integer parts for natural sorting."""
    import re

    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", s)]


def pick_machine() -> str:
    """Show available machine configs and let the user pick one."""
    scratch_dir = MZ_ROOT / "misc" / "scratch"
    configs: list[tuple[str, str, str]] = []
    for f in sorted(scratch_dir.glob("*.json"), key=lambda f: _natsort_key(f.stem)):
        with open(f) as fh:
            descs = [MachineDesc.model_validate(obj) for obj in multi_json(fh.read())]
        for d in descs:
            configs.append((f.stem, d.instance_type, d.name))

    render_table(
        ["#", "CONFIG", "INSTANCE TYPE", "NAME"],
        [
            [str(i), stem, instance_type, name]
            for i, (stem, instance_type, name) in enumerate(configs, 1)
        ],
    )

    while True:
        choice = input("Select a machine config [#]: ").strip()
        try:
            idx = int(choice)
            if 1 <= idx <= len(configs):
                return configs[idx - 1][0]
        except ValueError:
            # Allow typing the config name directly
            for stem, _, _ in configs:
                if choice == stem:
                    return stem
        print(
            f"Invalid choice: {choice!r}. Enter a number 1-{len(configs)} or a config name."
        )


def multi_json(s: str) -> list[dict[Any, Any]]:
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
            obj, idx = decoder.raw_decode(s, idx)
            result.append(obj)

    return result


def load_machine_descs(
    machine: str | None,
    *,
    instance_type: str | None = None,
    size_gb: int | None = None,
) -> list[MachineDesc]:
    """Load machine configs, applying optional overrides.

    With no `machine` but an explicit `instance_type`, synthesize a single
    default config so callers can launch an arbitrary instance type without a
    preset file. With no `machine` and no `instance_type`, prompt for one."""
    if machine is None and instance_type is not None:
        desc = MachineDesc(name=instance_type, instance_type=instance_type)
        if size_gb is not None:
            desc.size_gb = size_gb
        return [desc]

    if machine is None:
        machine = pick_machine()

    with open(MZ_ROOT / "misc" / "scratch" / f"{machine}.json") as f:
        print(f"Reading machine configs from {f.name}")
        descs = [MachineDesc.model_validate(obj) for obj in multi_json(f.read())]

    if instance_type is not None:
        if len(descs) != 1:
            raise RuntimeError(
                f"--instance-type requires a single-machine config, got {len(descs)}"
            )
        descs[0].instance_type = instance_type
        # An explicit type override invalidates any pinned AMI; re-derive it.
        descs[0].ami = None
    if size_gb is not None:
        for d in descs:
            d.size_gb = size_gb
    return descs


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--key-name", type=str, required=False, help="Optional EC2 Key Pair name"
    )
    parser.add_argument(
        "--security-group-name",
        type=str,
        default=DEFAULT_SECURITY_GROUP_NAME,
        help="EC2 Security Group name. Defaults to Materialize scratch account.",
    )
    parser.add_argument(
        "--extra-tags",
        type=str,
        required=False,
        help='Additional tags/labels for created instance. Format: {"key": "value"}',
    )
    parser.add_argument(
        "--instance-profile",
        type=str,
        default=DEFAULT_INSTANCE_PROFILE_NAME,
        help="EC2 instance profile / IAM role. Defaults to `%s`."
        % DEFAULT_INSTANCE_PROFILE_NAME,
    )
    parser.add_argument("--output-format", choices=["table", "csv"], default="table")
    parser.add_argument(
        "--git-rev",
        type=str,
        default="HEAD",
        help="Git revision of `materialize` codebase to push to scratch instance. Defaults to `HEAD`",
    )
    parser.add_argument(
        "--ssh",
        action="store_true",
        help=(
            "ssh into the machine after the launch script is run. "
            "Only works if a single instance was started"
        ),
    )
    parser.add_argument(
        "machine",
        nargs="?",
        const=None,
        help=(
            "Use a config from {machine}.json in `misc/scratch`. "
            "Hint: `dev-box` is a good starter!"
        ),
    )
    parser.add_argument(
        "--instance-type",
        type=str,
        help=(
            "Override the EC2 instance type. The AMI is derived from its "
            "architecture. May be used without a machine config."
        ),
    )
    parser.add_argument(
        "--size-gb",
        type=int,
        help="Override the root volume size in GiB.",
    )
    parser.add_argument(
        "--max-age-days",
        type=float,
        default=MAX_AGE_DAYS,
        help="Maximum age for scratch instance in days. Defaults to 1.5",
    )


def run(args: argparse.Namespace) -> None:
    extra_tags = {}
    if args.extra_tags:
        extra_tags = json.loads(args.extra_tags)
        if not isinstance(extra_tags, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in extra_tags.items()
        ):
            raise RuntimeError(
                "extra-tags must be a JSON dictionary of strings to strings"
            )

    descs = load_machine_descs(
        args.machine,
        instance_type=args.instance_type,
        size_gb=args.size_gb,
    )

    check_required_vars()
    extra_tags["LaunchedBy"] = whoami()

    if args.ssh and len(descs) != 1:
        raise RuntimeError(f"Cannot use `--ssh` with {len(descs)} instances")

    if args.max_age_days <= 0:
        raise RuntimeError(f"max_age_days must be positive, got {args.max_age_days}")
    max_age = datetime.timedelta(days=args.max_age_days)

    instances = launch_cluster(
        descs,
        key_name=args.key_name,
        security_group_name=args.security_group_name,
        instance_profile=args.instance_profile,
        extra_tags=extra_tags,
        delete_after=datetime.datetime.now(datetime.timezone.utc) + max_age,
        git_rev=args.git_rev,
        extra_env={},
    )

    print("Launched instances:")
    print_instances(instances, args.output_format)

    if args.ssh:
        print(f"ssh-ing into: {instances[0].instance_id}")
        mssh(instances[0], "")
