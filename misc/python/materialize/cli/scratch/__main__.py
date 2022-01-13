# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

from materialize.cli.scratch import create, destroy, mine, push, ssh


def main() -> None:
    parser = argparse.ArgumentParser("scratch")
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    for name, configure, run in [
        ("create", create.configure_parser, create.run),
        ("mine", mine.configure_parser, mine.run),
        ("ssh", ssh.configure_parser, ssh.run),
        ("destroy", destroy.configure_parser, destroy.run),
        ("push", push.configure_parser, push.run),
    ]:
        s = subparsers.add_parser(name)
        configure(s)
        s.set_defaults(run=run)

    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
