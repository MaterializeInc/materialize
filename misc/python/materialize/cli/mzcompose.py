# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzcompose.py — runs Docker Compose with Materialize customizations.

from pathlib import Path
from typing import IO, List, Tuple, Text, Optional, Sequence
from typing_extensions import NoReturn
import argparse
import os
import sys

from materialize import errors
from materialize import mzbuild
from materialize import mzcompose
from materialize import spawn
from materialize import ui


announce = ui.speaker("==> ")
say = ui.speaker("")

MIN_COMPOSE_VERSION = (1, 24, 0)


def assert_docker_compose_version() -> None:
    """Check the version of docker-compose installed.

    Raises `MzRuntimeError` if the version is not recent enough.
    """
    cmd = ["docker-compose", "version", "--short"]
    output = spawn.capture(cmd, unicode=True).strip()
    version = tuple(int(i) for i in output.split("."))
    if version < MIN_COMPOSE_VERSION:
        msg = f"Unsupported docker-compose version: {version}, min required: {MIN_COMPOSE_VERSION}"
        raise errors.MzConfigurationError(msg)


def main(argv: List[str]) -> None:
    # Lightly parse the arguments so we know what to do.
    args, unknown_args = ArgumentParser().parse_known_args(argv)
    if not args.file:
        config_files = ["mzcompose.yml"]
    else:
        config_files = args.file

    ui.Verbosity.init_from_env(args.mz_quiet)

    root = Path(os.environ["MZ_ROOT"])
    repo = mzbuild.Repository(root)

    if args.command == "gen-shortcuts":
        return gen_shortcuts(repo)

    assert_docker_compose_version()

    announce("Collecting mzbuild dependencies")
    composition = mzcompose.Composition(
        repo, args.project_directory or str(Path(config_files[0]).parent),
    )
    for config_file in config_files:
        composition.load_file(config_file)
    deps = repo.resolve_dependencies(composition.images)
    for d in deps:
        say(d.spec())

    # Check if the command is going to create or start containers, and if so
    # build the dependencies. This can be slow, so we don't want to do it if we
    # can help it (e.g., for `down` or `ps`).
    if args.command in ["create", "run", "start", "up"]:
        deps.acquire()

    # Hand over control to Docker Compose.
    announce("Delegating to Docker Compose")
    composition.run(
        [
            *unknown_args,
            *([args.command] if args.command is not None else []),
            *args.extra,
        ]
    )


def gen_shortcuts(repo: mzbuild.Repository) -> None:
    template = """#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mzcompose — runs Docker Compose with Materialize customizations.

exec "$(dirname "$0")/{}/bin/mzcompose" "$@"
"""
    for path in repo.compose_dirs:
        mzcompose_path = path / "mzcompose"
        with open(mzcompose_path, "w") as f:
            f.write(template.format(os.path.relpath(repo.root, path)))
        mzbuild.chmod_x(mzcompose_path)


# We subclass `argparse.ArgumentParser` so that we can override its default
# behavior of exiting on error. We want Docker Compose to be responsible for
# generating option-parsing errors.
class ArgumentParser(argparse.ArgumentParser):
    def __init__(self) -> None:
        super().__init__(add_help=False)
        self.add_argument("--mz-quiet", action="store_true", default=None)
        self.add_argument("-f", "--file", action="append")
        self.add_argument("--project-directory")
        self.add_argument("command", nargs="?")
        self.add_argument("extra", nargs=argparse.REMAINDER)

    def parse_known_args(
        self,
        args: Optional[Sequence[Text]] = None,
        namespace: Optional[argparse.Namespace] = None,
    ) -> Tuple[argparse.Namespace, List[str]]:
        ns = argparse.Namespace()
        try:
            return super().parse_known_args(args, namespace=ns)
        except ValueError:
            return (ns, [])

    def error(self, message: str) -> NoReturn:
        raise ValueError(message)


if __name__ == "__main__":
    with errors.error_handler(lambda *args: print(*args, file=sys.stderr)):
        main(sys.argv[1:])
