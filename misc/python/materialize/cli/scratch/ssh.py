# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

from materialize.cli.scratch import get_instance, pick_instance
from materialize.scratch import mssh


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "instance",
        nargs="?",
        default=None,
        help="The ID of the instance to connect to, or 'mine' to specify your only live instance",
    )
    parser.add_argument("command", nargs="*", help="The command to run via SSH, if any")


def run(args: argparse.Namespace) -> None:
    if args.instance:
        instance = get_instance(args.instance)
    else:
        instance = pick_instance()
    mssh(instance, " ".join(args.command))
