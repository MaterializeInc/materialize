# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzimage.py — builds Materialize-specific Docker images.

import argparse
import os
import sys
from pathlib import Path
from typing import Any, List

from materialize import git
from materialize import mzbuild
from materialize import ui


def main() -> int:
    args = parse_args()
    ui.Verbosity.init_from_env(explicit=None)
    root = Path(os.environ["MZ_ROOT"])
    repo = mzbuild.Repository(root)

    if args.command == "list":
        for image in repo:
            print(image.name)
    else:
        if args.image not in repo.images:
            print(f"fatal: unknown image: {args.image}", file=sys.stderr)
            return 1
        deps = repo.resolve_dependencies([repo.images[args.image]])
        rimage = deps[args.image]
        if args.command == "build":
            deps.acquire(force_build=True)
        elif args.command == "run":
            deps.acquire()
            rimage.run(args.image_args)
        elif args.command == "acquire":
            deps.acquire()
        elif args.command == "fingerprint":
            print(rimage.fingerprint())
        elif args.command == "spec":
            print(rimage.spec())
        elif args.command == "describe":
            inputs = sorted(rimage.inputs(args.transitive))
            dependencies = sorted(rimage.list_dependencies(args.transitive))

            print(f"Image: {rimage.name}")
            print(f"Fingerprint: {rimage.fingerprint()}")

            print("Input files:")
            for inp in inputs:
                print(f"    {inp}")

            print("Dependencies:")
            for d in dependencies:
                print(f"    {d}")
            if not dependencies:
                print("    (none)")
        else:
            raise RuntimeError("unreachable")
    return 0


def parse_args() -> argparse.Namespace:
    """Parse known args, or exit the program"""
    parser = argparse.ArgumentParser(
        prog="mzimage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Swiss army knife for mzbuild images.",
        epilog="For additional help on a subcommand, run:\n\n  %(prog)s <command> -h",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")

    def add_subcommand(name: str, **kwargs: Any) -> argparse.ArgumentParser:
        subparser = subparsers.add_parser(name, **kwargs)
        return subparser

    def add_image_subcommand(name: str, **kwargs: Any) -> argparse.ArgumentParser:
        subparser = add_subcommand(name, **kwargs)
        subparser.add_argument(
            "image", help="the name of an mzbuild image in this repository"
        )
        return subparser

    add_subcommand(
        "list",
        description="List all images in this repository.",
        help="list all images",
    )

    add_image_subcommand(
        "build", description="Unconditionally build an image.", help="build an image"
    )

    add_image_subcommand(
        "acquire", description="Download or build an image.", help="acquire an image"
    )

    run_parser = add_image_subcommand(
        "run", description="Acquire and run an image.", help="run an image"
    )
    run_parser.add_argument("image_args", nargs=argparse.REMAINDER)

    add_image_subcommand(
        "fingerprint",
        description="Compute the fingerprint for an image.",
        help="fingerprint an image",
    )

    add_image_subcommand(
        "spec",
        description="Compute the Docker Hub specification for an image.",
        help="compute image spec",
    )

    describe_parser = add_image_subcommand(
        "describe",
        description="Print information about an image.",
        help="show image details",
    )
    describe_parser.add_argument(
        "--transitive",
        action="store_true",
        help="compute transitive inputs and dependencies",
    )

    args = parser.parse_args()
    if args.command is None:
        # TODO(benesch): we can set `required=True` in the call to
        # `add_subparsers` when we upgrade to Python v3.7+.
        parser.print_help(file=sys.stderr)
        sys.exit(1)
    return args


if __name__ == "__main__":
    sys.exit(main())
