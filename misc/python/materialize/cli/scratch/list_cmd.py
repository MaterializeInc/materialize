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

from materialize.cli.scratch import list_all_instances
from materialize.scratch import print_instances


def configure_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "owner",
        nargs="*",
        help="Whose instances to show (defaults to yourself)",
    )
    parser.add_argument("--all", help="Show all instances", action="store_true")
    parser.add_argument("--output-format", choices=["table", "csv"], default="table")


def run(args: argparse.Namespace) -> None:
    provider = getattr(args, "provider", None)
    instances, warnings = list_all_instances(
        provider=provider, owners=args.owner or None, all=args.all
    )
    print_instances(instances, args.output_format)
    for w in warnings:
        print(f"WARNING: Could not list instances from {w}", file=sys.stderr)
