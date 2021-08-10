# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
from typing import Callable

import boto3
from materialize.cli.scratch import check_required_vars
from materialize.scratch import launched_by, print_instances, tags, whoami
from mypy_boto3_ec2.service_resource import Instance

def configure_parser(parser: argparse.ArgumentParser):
    check_required_vars()
    parser.add_argument(
        "who",
        nargs="*",
        help="Whose instances to show (defaults to yourself)",
        default=[whoami()],
    )
    parser.add_argument("--all", help="Show all instances", action="store_true")

def run(args: argparse.Namespace) -> None:
    filter: Callable[[Instance], bool] = (
        (lambda _i: True) if args.all else (lambda i: launched_by(tags(i)) in args.who)
    )

    ists = [i for i in boto3.resource("ec2").instances.all() if filter(i)]

    print_instances(ists)
