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
    DEFAULT_SG_ID,
    DEFAULT_SUBNET_ID,
    MachineDesc,
    launch_cluster,
    print_instances,
    whoami,
)

MAX_AGE = datetime.timedelta(weeks=1)


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
    parser.add_argument("--subnet-id", type=str, default=DEFAULT_SUBNET_ID)
    parser.add_argument("--key-name", type=str, required=False)
    parser.add_argument("--security-group-id", type=str, default=DEFAULT_SG_ID)
    parser.add_argument("--extra-tags", type=str, required=False)
    parser.add_argument("--instance-profile", type=str)
    parser.add_argument("--output-format", choices=["table", "csv"], default="table")
    parser.add_argument("--git-rev", type=str, default="HEAD")


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

    descs = [MachineDesc.parse_obj(obj) for obj in multi_json(sys.stdin.read())]

    instances = launch_cluster(
        descs,
        subnet_id=args.subnet_id,
        key_name=args.key_name,
        security_group_id=args.security_group_id,
        instance_profile=args.instance_profile,
        extra_tags=extra_tags,
        delete_after=datetime.datetime.utcnow() + MAX_AGE,
        git_rev=args.git_rev,
        extra_env={},
    )

    print("Launched instances:")
    print_instances(instances, args.output_format)
