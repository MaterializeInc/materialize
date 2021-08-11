# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

import boto3
from materialize.cli.scratch import check_required_vars
from materialize.scratch import print_instances


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "instances",
        nargs="+",
        help="Instance IDs to destroy",
    )
    parser.add_argument("--output-format", choices=["table", "csv"], default="table")


def run(args: argparse.Namespace) -> None:
    check_required_vars()
    ists = list(boto3.resource("ec2").instances.filter(InstanceIds=args.instances))
    for i in ists:
        i.terminate()
    print("Destroyed instances:")
    print_instances(ists, args.output_format)
