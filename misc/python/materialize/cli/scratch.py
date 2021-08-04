# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import json
import os
import random
import sys
from typing import Any, Dict, List

from materialize.scratch import MachineDesc, launch_cluster


def check_required_vars() -> None:
    """Set reasonable default values for the
    environment variables necessary to interact with AWS."""
    if not os.environ.get("AWS_PROFILE"):
        os.environ["AWS_PROFILE"] = "mz-scratch-admin"
    if not os.environ.get("AWS_DEFAULT_REGION"):
        os.environ["AWS_DEFAULT_REGION"] = "us-east-2"


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
    DEFAULT_INSTPROF_NAME = "ssm-instance-profile"

    parser = argparse.ArgumentParser()
    parser.add_argument("--subnet-id", type=str, default=DEFAULT_SUBNET_ID)
    parser.add_argument("--key-name", type=str, required=False)
    parser.add_argument("--security-group-id", type=str, default=DEFAULT_SG_ID)
    parser.add_argument("--extra-tags", type=str, required=False)
    parser.add_argument("--instance-profile", type=str, default=DEFAULT_INSTPROF_NAME)
    parser.add_argument("--no-instance-profile", action="store_const", const=True)
    args = parser.parse_args()
    instance_profile = None if args.no_instance_profile else args.instance_profile
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

    instances = launch_cluster(
        descs,
        nonce,
        args.subnet_id,
        args.key_name,
        args.security_group_id,
        instance_profile,
        extra_tags,
    )
    print(
        "launched instances: {}".format(
            [
                f"{d.name} (instance id: {i.instance_id}, ip: {i.public_ip_address})"
                for (i, d) in zip(instances, descs)
            ]
        )
    )


if __name__ == "__main__":
    main()
