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

from materialize import mzbuild
from pathlib import Path
from typing import Any, List
import argparse
import os
import sys


def main() -> int:
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

    root = Path(os.environ["MZ_ROOT"])
    repo = mzbuild.Repository(root)

    if args.command is None:
        # TODO(benesch): we can set `required=True` in the call to
        # `add_subparsers` when we upgrade to Python v3.7+.
        parser.print_help(file=sys.stderr)
        return 1
    elif args.command == "list":
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
        elif args.command == "describe":
            print(f"Image: {rimage.name}")
            print(f"Fingerprint: {rimage.fingerprint()}")
            print("Inputs:")
            for inp in sorted(rimage.inputs(args.transitive)):
                print(f"    {inp.decode()}")
            print("Dependencies:")
            dependencies = sorted(rimage.list_dependencies(args.transitive))
            for d in dependencies:
                print(f"    {d}")
            if not dependencies:
                print("    (none)")
        else:
            raise RuntimeError("unreachable")
    return 0


if __name__ == "__main__":
    sys.exit(main())
