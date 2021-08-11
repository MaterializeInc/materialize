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

from materialize.cli.scratch import create, mine

from materialize import errors


def main() -> None:
    from materialize.cli.scratch import create, destroy, mine

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcommand")
    for name, configure, run in [
        ("create", create.configure_parser, create.run),
        ("mine", mine.configure_parser, mine.run),
        ("destroy", destroy.configure_parser, destroy.run),
    ]:
        s = subparsers.add_parser(name)
        configure(s)
        s.set_defaults(run=run)

    args = parser.parse_args()
    # TODO - Pass `required=True` to parser.add_subparsers once we support 3.7
    if not "run" in args:
        raise errors.BadSpec("Must specify a command")

    args.run(args)


if __name__ == "__main__":
    main()
