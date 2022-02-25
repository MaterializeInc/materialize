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
import sys
from typing import Any, Dict, List

from materialize.cli.scratch import check_required_vars
from materialize.scratch import (
    DEFAULT_INSTANCE_PROFILE_NAME,
    DEFAULT_SECURITY_GROUP_ID,
    DEFAULT_SUBNET_ID,
    ROOT,
    MachineDesc,
    launch_cluster,
    mssh,
    print_instances,
    whoami,
)

MAX_AGE_DAYS = 1.5


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


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--subnet-id",
        type=str,
        default=DEFAULT_SUBNET_ID,
        help="EC2 Subnet ID. Defaults to Materialize scratch account.",
    )
    parser.add_argument(
        "--key-name", type=str, required=False, help="Optional EC2 Key Pair name"
    )
    parser.add_argument(
        "--security-group-id",
        type=str,
        default=DEFAULT_SECURITY_GROUP_ID,
        help="EC2 Security Group ID. Defaults to Materialize scratch account.",
    )
    parser.add_argument(
        "--extra-tags",
        type=str,
        required=False,
        help='Additional EC2 tags for created instance. Format: {"key", "value"}',
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

    check_required_vars()

    extra_tags["LaunchedBy"] = whoami()

    if args.machine:
        with open(ROOT / "misc" / "scratch" / "{}.json".format(args.machine)) as f:

            print("Reading machine configs from {}".format(f.name))
            descs = [MachineDesc.parse_obj(obj) for obj in multi_json(f.read())]
    else:
        print("Reading machine configs from stdin...")
        descs = [MachineDesc.parse_obj(obj) for obj in multi_json(sys.stdin.read())]

    if args.ssh and len(descs) != 1:
        raise RuntimeError("Cannot use `--ssh` with {} instances".format(len(descs)))

    if args.max_age_days <= 0:
        raise RuntimeError(f"max_age_days must be positive, got {args.max_age_days}")
    max_age = datetime.timedelta(days=args.max_age_days)

    instances = launch_cluster(
        descs,
        subnet_id=args.subnet_id,
        key_name=args.key_name,
        security_group_id=args.security_group_id,
        instance_profile=args.instance_profile,
        extra_tags=extra_tags,
        delete_after=datetime.datetime.utcnow() + max_age,
        git_rev=args.git_rev,
        extra_env={},
    )

    print("Launched instances:")
    print_instances(instances, args.output_format)

    if args.ssh:
        print("ssh-ing into: {}".format(instances[0].instance_id))
        mssh(instances[0], "")
