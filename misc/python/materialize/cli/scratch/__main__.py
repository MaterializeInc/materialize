# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import sys

import shtab

from materialize.cli.scratch import (
    claude,
    create,
    destroy,
    forward,
    go,
    list_cmd,
    login,
    push,
    sftp,
    ssh,
)


def main() -> None:
    parser = argparse.ArgumentParser("scratch")
    parser.add_argument(
        "--provider",
        choices=["aws", "hetzner"],
        default=os.environ.get("MZ_SCRATCH_PROVIDER"),
        help="Cloud provider (default: both for list/destroy, aws for other commands; or MZ_SCRATCH_PROVIDER env var)",
    )
    commands = [
        ("login", login.configure_parser, login.run, "Log in to cloud provider"),
        (
            "create",
            create.configure_parser,
            create.run,
            "Create a new scratch instance",
        ),
        (
            "list",
            list_cmd.configure_parser,
            list_cmd.run,
            "List scratch instances",
            ["ls"],
        ),
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
            ["rm"],
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
        (
            "forward",
            forward.configure_parser,
            forward.run,
            "Forward local ports remotely",
        ),
        (
            "go",
            go.configure_parser,
            go.run,
            "Create-or-connect: reuse existing instance or create a new one and SSH in",
        ),
        (
            "claude",
            claude.configure_parser,
            claude.run,
            "Create-or-reuse a scratch instance and start Claude Code",
        ),
    ]
    primary_names = [entry[0] for entry in commands]
    subparsers = parser.add_subparsers(
        dest="subcommand",
        required=True,
        metavar="{" + ",".join(primary_names) + "}",
    )
    for entry in commands:
        name, configure, run, description = entry[:4]
        aliases = entry[4] if len(entry) > 4 else []
        s = subparsers.add_parser(
            name, aliases=aliases, description=description, help=description
        )
        configure(s)
        s.set_defaults(run=run)

    args, extra = parser.parse_known_args()
    args.extra_args = extra
    args.run(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        print(f"scratch: error: {e}", file=sys.stderr)
        sys.exit(1)
