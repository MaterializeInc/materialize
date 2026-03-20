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

from materialize.cli.scratch import get_instance, list_all_instances, pick_instance
from materialize.scratch import print_instances, ui


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
        "-y",
        "--yes",
        action="store_true",
        help="Don't ask for confirmation before destroying",
    )
    parser.add_argument("--output-format", choices=["table", "csv"], default="table")


def run(args: argparse.Namespace) -> None:
    provider = getattr(args, "provider", None)

    if args.all_mine and args.instances:
        print(
            "scratch: error: cannot specify --all-mine and instance IDs",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.all_mine:
        instances, warnings = list_all_instances(provider=provider)
        instances = [
            i
            for i in instances
            if i.state in ("pending", "running", "stopping", "stopped")
        ]
    elif args.instances:
        instances = [get_instance(id, provider) for id in args.instances]
        warnings = []
    else:
        instances = [pick_instance(provider)]
        warnings = []

    if not instances:
        print("No instances to destroy.")
        for w in warnings:
            print(f"WARNING: Could not list instances from {w}", file=sys.stderr)
        return

    print("Destroying instances:")
    print_instances(instances, args.output_format)

    if not args.yes and not ui.confirm("Would you like to continue?"):
        sys.exit(0)

    aws_instances = [i for i in instances if i.provider == "aws"]
    hetzner_instances = [i for i in instances if i.provider == "hetzner"]

    if aws_instances:
        from materialize.scratch import terminate_instances

        terminate_instances(aws_instances)

    if hetzner_instances:
        from materialize import scratch_hetzner

        scratch_hetzner.terminate_instances(hetzner_instances)

    print("Instances destroyed.")

    for w in warnings:
        print(f"WARNING: Could not list instances from {w}", file=sys.stderr)
