# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
from typing import List

import boto3
from mypy_boto3_ec2.type_defs import FilterTypeDef

from materialize.cli.scratch import check_required_vars
from materialize.scratch import print_instances, whoami


def configure_parser(parser: argparse.ArgumentParser) -> None:
    check_required_vars()
    parser.add_argument(
        "who",
        nargs="*",
        help="Whose instances to show (defaults to yourself)",
    )
    parser.add_argument("--all", help="Show all instances", action="store_true")
    parser.add_argument("--output-format", choices=["table", "csv"], default="table")


def run(args: argparse.Namespace) -> None:
    filters: List[FilterTypeDef] = []
    if not args.all:
        filters.append({"Name": "tag:LaunchedBy", "Values": args.who or [whoami()]})
    print_instances(
        list(boto3.resource("ec2").instances.filter(Filters=filters)),
        args.output_format,
    )
