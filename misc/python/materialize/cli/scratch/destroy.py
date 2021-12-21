# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import sys
from typing import List

import boto3
from mypy_boto3_ec2.type_defs import FilterTypeDef

from materialize.cli.scratch import check_required_vars
from materialize.scratch import print_instances, whoami


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "instances",
        nargs="*",
        help="Instance IDs to destroy",
    )
    parser.add_argument(
        "--all-mine",
        action="store_true",
        help="Destroy all of your instances (incompatible with specifying instance IDs)",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Don't actually destroy any instances",
    )
    parser.add_argument("--output-format", choices=["table", "csv"], default="table")


def run(args: argparse.Namespace) -> None:
    check_required_vars()
    instance_ids = []
    filters: List[FilterTypeDef] = [
        {
            "Name": "instance-state-name",
            "Values": ["pending", "running", "shutting-down", "stopping", "stopped"],
        }
    ]
    if args.all_mine:
        if args.instances:
            print(
                "scratch: error: cannot specify --all-mine and instance IDs",
                file=sys.stderr,
            )
            sys.exit(1)
        filters.append({"Name": "tag:LaunchedBy", "Values": [whoami()]})
    elif not args.instances:
        print(
            "scratch: error: must supply at least one instance ID to destroy",
            file=sys.stderr,
        )
        sys.exit(1)
    else:
        instance_ids.extend(args.instances)

    instances = list(
        boto3.resource("ec2").instances.filter(
            Filters=filters, InstanceIds=instance_ids
        )
    )

    if args.dry_run:
        print("Would destroy instances:")
        print_instances(instances, args.output_format)
    else:
        for instance in instances:
            instance.terminate()
        print("Destroyed instances:")
        print_instances(instances, args.output_format)
