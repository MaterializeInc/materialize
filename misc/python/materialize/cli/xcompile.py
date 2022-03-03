# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# xcompile.py — builds Materialize-specific Docker images.

import argparse
import sys

from materialize import mzbuild, spawn, xcompile


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="xcompile",
        description="Facilitates cross compilation of Rust binaries.",
        epilog="For additional help on a subcommand, run:\n\n  %(prog)s <command> -h",
    )
    parser.add_argument(
        "--arch",
        default=mzbuild.Arch.X86_64,
        help="the CPU architecture to build for",
        type=mzbuild.Arch,
        choices=mzbuild.Arch,
    )

    parser.add_argument(
        "--channel",
        default=None,
        help="Rust compiler channel to use",
    )

    subparsers = parser.add_subparsers(
        dest="command", metavar="<command>", required=True
    )

    cargo_parser = subparsers.add_parser(
        "cargo", help="run a cross-compiling cargo command"
    )
    cargo_parser.add_argument(
        "--rustflags",
        action="append",
        default=[],
        help="override the default flags to the Rust compiler",
    )

    cargo_parser.add_argument("subcommand", help="the cargo subcommand to invoke")
    cargo_parser.add_argument(
        "subargs", nargs=argparse.REMAINDER, help="the arguments to pass to cargo"
    )

    tool_parser = subparsers.add_parser(
        "tool", help="run a cross-compiling binutils tool"
    )
    tool_parser.add_argument("tool", metavar="TOOL", help="the binutils tool to invoke")
    tool_parser.add_argument(
        "subargs", nargs=argparse.REMAINDER, help="the arguments to pass to the tool"
    )

    args = parser.parse_args()
    if args.command == "cargo":
        spawn.runv(
            [
                *xcompile.cargo(
                    arch=args.arch,
                    channel=args.channel,
                    subcommand=args.subcommand,
                    rustflags=args.rustflags,
                ),
                *args.subargs,
            ]
        )
    elif args.command == "tool":
        spawn.runv(
            [
                *xcompile.tool(args.arch, args.tool),
                *args.subargs,
            ]
        )
    else:
        raise RuntimeError("unreachable")

    return 0


if __name__ == "__main__":
    sys.exit(main())
