# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse

import shtab

from materialize.cli.scratch import create, destroy, login, mine, push, sftp, ssh


def main() -> None:
    parser = argparse.ArgumentParser("scratch")
    subparsers = parser.add_subparsers(
        dest="subcommand", required=True, description="", help="subparsers"
    )
    for name, configure, run, description in [
        ("login", login.configure_parser, login.run, "Log in to AWS SSO"),
        (
            "create",
            create.configure_parser,
            create.run,
            "Create a new scratch instance",
        ),
        ("mine", mine.configure_parser, mine.run, "Show active scratch instance"),
        ("ssh", ssh.configure_parser, ssh.run, "Connect to scratch instance via ssh"),
        (
            "sftp",
            sftp.configure_parser,
            sftp.run,
            "Connect to scratch instance via sftp",
        ),
        (
            "destroy",
            destroy.configure_parser,
            destroy.run,
            "Destroy a scratch instance",
        ),
        (
            "push",
            push.configure_parser,
            push.run,
            "Push current HEAD (or a specific git commit) to scratch instance",
        ),
        (
            "completion",
            lambda p: shtab.add_argument_to(p, "shell", parent=parser),
            lambda: None,
            "Generate shell completion script",
        ),
    ]:
        s = subparsers.add_parser(name, description=description, help=description)
        configure(s)
        s.set_defaults(run=run)

    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
