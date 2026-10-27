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
    commands = [
        ("login", login.configure_parser, login.run, "Log in to AWS SSO"),
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

    # Use parse_known_args only when needed (e.g. `claude` passes extra args
    # through to claude-code).  First parse just enough to find the subcommand.
    args, extra = parser.parse_known_args()
    if extra and args.subcommand not in ("claude",):
        # Re-parse strictly so the user sees an error for unknown flags.
        parser.parse_args()
    args.extra_args = extra
    args.run(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        msg = str(e)
        if (
            "SSO" in msg
            or "expired" in msg
            or "UnauthorizedSSOTokenError" in type(e).__name__
        ):
            print(
                f"scratch: error: {msg}\nhint: run `bin/scratch login` to refresh your SSO session",
                file=sys.stderr,
            )
        else:
            print(f"scratch: error: {msg}", file=sys.stderr)
        sys.exit(1)
