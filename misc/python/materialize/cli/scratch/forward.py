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
from materialize.scratch import mssh


def configure_parser(parser: argparse.ArgumentParser) -> None:
    check_required_vars()

    parser.add_argument("instance", help="The ID of the instance to connect to"),
    parser.add_argument("ports", nargs="*", help="The remote ports to forward locally")


def run(args: argparse.Namespace) -> None:
    instance = boto3.resource("ec2").Instance(args.instance)
    ssh_args = []
    for port in args.ports:
        ssh_args.extend(["-L", f"{port}:127.0.0.1:{port}"])

    mssh(instance, "sleep infinity", extra_ssh_args=ssh_args)
