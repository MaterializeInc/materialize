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
import json
import os
import sys
import webbrowser

from materialize import errors
from materialize import mzbuild
from materialize import mzcompose
from materialize import spawn
from materialize import ui


announce = ui.speaker("==> ")
say = ui.speaker("")

MIN_COMPOSE_VERSION = (1, 24, 0)


def main(argv: List[str]) -> int:
    # Lightly parse the arguments so we know what to do.
    args, unknown_args = ArgumentParser().parse_known_args(argv)
    if args.file:
        raise errors.MzConfigurationError("-f/--file option not supported")
    elif args.project_directory:
        raise errors.MzConfigurationError("--project-directory option not supported")

    ui.Verbosity.init_from_env(args.mz_quiet)

    # Load repository.
    root = Path(os.environ["MZ_ROOT"])
    repo = mzbuild.Repository(root, release_mode=(args.mz_build_mode == "release"))

    # Handle special mzcompose commands that apply to the repo.
    if args.command == "gen-shortcuts":
        return gen_shortcuts(repo)
    elif args.command == "lint":
        return lint(repo)
    elif args.command == "list-compositions":
        return list_compositions(repo)

    # Load composition.
    try:
        composition = mzcompose.Composition(repo, args.mz_find or Path.cwd().name)
    except errors.UnknownComposition:
        if args.mz_find:
            print(f"unknown composition {args.mz_find!r}", file=sys.stderr)
            print("hint: available compositions:", file=sys.stderr)
            for name in repo.compositions:
                print(f"    {name}", file=sys.stderr)
        else:
            print("error: directory does not contain mzcompose.yml", file=sys.stderr)
            print(
                "hint: enter one of the following directories and run ./mzcompose:",
                file=sys.stderr,
            )
            for path in repo.compositions.values():
                print(f"    {path.relative_to(Path.cwd())}", file=sys.stderr)
        return 1

    # Handle special mzcompose commands that apply to the composition.
    if args.command == "list-workflows":
        return list_workflows(composition)
    elif args.command == "list-ports":
        return list_ports(composition, args.first_command_arg)
    elif args.command == "web":
        return web(composition, args.first_command_arg)

    # From here on out we're definitely invoking Docker Compose, so make sure
    # it's new enough.
    output = spawn.capture(
        ["docker-compose", "version", "--short"], unicode=True
    ).strip()
    version = tuple(int(i) for i in output.split("."))
    if version < MIN_COMPOSE_VERSION:
        msg = f"Unsupported docker-compose version: {version}, min required: {MIN_COMPOSE_VERSION}"
        raise errors.MzConfigurationError(msg)

    announce("Collecting mzbuild dependencies")
    deps = repo.resolve_dependencies(composition.images)
    for d in deps:
        say(d.spec())

    # Check if the command is going to create or start containers, and if so
    # build the dependencies. This can be slow, so we don't want to do it if we
    # can help it (e.g., for `down` or `ps`).
    if args.command in ["create", "run", "start", "up"]:
        deps.acquire()

    # The `run` command requires special handling.
    if args.command == "run":
        try:
            workflow = composition.get_workflow(
                dict(os.environ), args.first_command_arg
            )
        except KeyError:
            # Restart any dependencies whose definitions have changed. This is
            # Docker Compose's default behavior for `up`, but not for `run`,
            # which is a constant irritation that we paper over here. The trick,
            # taken from Buildkite's Docker Compose plugin, is to run an `up`
            # command that requests zero instances of the requested service.
            composition.run(
                [
                    "up",
                    "-d",
                    "--scale",
                    f"{args.first_command_arg}=0",
                    args.first_command_arg,
                ]
            )
        else:
            # The user has specified a workflow rather than a service. Run the
            # workflow instead of Docker Compose.
            if args.remainder:
                raise errors.MzRuntimeError(
                    f"cannot specify extra arguments ({' '.join(args.remainder)}) "
                    "when specifying a workflow (rather than a container)"
                )
            workflow.run()
            return 0

    # Hand over control to Docker Compose.
    announce("Delegating to Docker Compose")
    proc = composition.run(
        [
            *unknown_args,
            *([args.command] if args.command is not None else []),
            *([args.first_command_arg] if args.first_command_arg is not None else []),
            *args.remainder,
        ],
        check=False,
    )
    return proc.returncode


def gen_shortcuts(repo: mzbuild.Repository) -> int:
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
    for path in repo.compositions.values():
        mzcompose_path = path.parent / "mzcompose"
        with open(mzcompose_path, "w") as f:
            f.write(template.format(os.path.relpath(repo.root, path.parent)))
        mzbuild.chmod_x(mzcompose_path)

    return 0


def lint(repo: mzbuild.Repository) -> int:
    errors = []
    for name in repo.compositions:
        errors += mzcompose.Composition.lint(repo, name)
    for error in sorted(errors):
        print(error)
    return 1 if errors else 0


def list_compositions(repo: mzbuild.Repository) -> int:
    for name in sorted(repo.compositions):
        print(name)
    return 0


def list_workflows(composition: mzcompose.Composition) -> int:
    for name in sorted(composition.workflows):
        print(name)
    return 0


def list_ports(composition: mzcompose.Composition, service: Optional[str]) -> int:
    if service is None:
        raise errors.MzRuntimeError(f"list-ports command requires a service argument")
    for port in composition.find_host_ports(service):
        print(port)
    return 0


def web(composition: mzcompose.Composition, service: Optional[str]) -> int:
    if service is None:
        raise errors.MzRuntimeError(f"web command requires a service argument")
    ports = composition.find_host_ports(service)
    if len(ports) == 1:
        webbrowser.open(f"http://localhost:{ports[0]}")
    elif not ports:
        raise errors.MzRuntimeError(f"No running services matched {service!r}")
    else:
        raise errors.MzRuntimeError(
            f"Too many ports matched {service!r}, found: {ports}"
        )
    return 0


# We subclass `argparse.ArgumentParser` so that we can override its default
# behavior of exiting on error. We want Docker Compose to be responsible for
# generating option-parsing errors.
class ArgumentParser(argparse.ArgumentParser):
    def __init__(self) -> None:
        super().__init__(add_help=False)
        self.add_argument("--mz-quiet", action="store_true", default=None)
        self.add_argument("--mz-find")
        self.add_argument("--mz-build-mode", default="release")
        self.add_argument("-f", "--file")
        self.add_argument("--project-directory")
        self.add_argument("command", nargs="?")
        self.add_argument("first_command_arg", nargs="?")
        self.add_argument("remainder", nargs=argparse.REMAINDER)

    def parse_known_args(
        self,
        args: Optional[Sequence[Text]] = None,
        namespace: Optional[argparse.Namespace] = None,
    ) -> Tuple[argparse.Namespace, List[str]]:
        ns = argparse.Namespace()
        try:
            (pargs, unknown_args) = super().parse_known_args(args, namespace=ns)
            if pargs.mz_build_mode not in ["dev", "release"]:
                raise errors.BadSpec(
                    f'unknown build mode {pargs.mz_build_mode!r} (expected "dev" or "release")'
                )
            return (pargs, unknown_args)
        except ValueError:
            return (ns, [])

    def error(self, message: str) -> NoReturn:
        raise ValueError(message)


if __name__ == "__main__":
    with errors.error_handler(lambda *args: print(*args, file=sys.stderr)):
        sys.exit(main(sys.argv[1:]))
