# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import types
from typing import Callable, NamedTuple


class Subcommand(NamedTuple):
    configure_parser: Callable[[argparse.ArgumentParser], None]
    name: str
    run: Callable[[argparse.Namespace], None]


def subcommand_from_module(m: types.ModuleType) -> Subcommand:
    return Subcommand(m.configure_parser, m.__name__.split(".")[-1], m.run)  # type: ignore


def main() -> None:
    from materialize.cli.scratch import create, mine

    modules = [subcommand_from_module(m) for m in [create, mine]]

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")
    for m in modules:
        s = subparsers.add_parser(m.name)
        m.configure_parser(s)
        s.set_defaults(mod=m)

    args = parser.parse_args()
    # TODO - Pass `required=True` to parser.add_subparsers once we support 3.7
    if not "mod" in args:
        print("Must specify a command")
        return

    args.mod.run(args)


if __name__ == "__main__":
    main()
