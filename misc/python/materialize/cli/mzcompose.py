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
from tempfile import TemporaryFile
from typing import IO, List, Tuple, Text, Optional, Sequence
from typing_extensions import NoReturn
import argparse
import os
import subprocess
import sys
import yaml

from materialize import errors
from materialize import ui
from materialize import mzbuild


announce = ui.speaker("==>")
say = ui.speaker("")


def main(argv: List[str]) -> int:
    # Lightly parse the arguments so we know what to do.
    args, unknown_args = ArgumentParser().parse_known_args(argv)
    if not args.file:
        config_files = ["mzcompose.yml"]
    else:
        config_files = args.file

    ui.Verbosity.init_from_env(args.mz_quiet)
    # TODO: we should propagate this down to subprocesses by explicit command-line flags, probably
    if ui.Verbosity.quiet:
        os.environ["MZ_QUIET"] = "yes"

    root = Path(os.environ["MZ_ROOT"])
    repo = mzbuild.Repository(root)

    if args.command == "gen-shortcuts":
        return gen_shortcuts(repo)

    announce("Collecting mzbuild dependencies")
    composes = []
    for config_file in config_files:
        composes.append(build_compose_file(repo, args.command, config_file))

    # Hand over control to Docker Compose.
    announce("Delegating to Docker Compose")
    dc_args = [
        "docker-compose",
        *[f"-f/dev/fd/{comp.fileno()}" for comp in composes],
        "--project-directory",
        args.project_directory or str(Path(config_file).parent),
        *unknown_args,
        *([args.command] if args.command is not None else []),
        *args.extra,
    ]
    os.execvp("docker-compose", dc_args)


def build_compose_file(
    repo: mzbuild.Repository, command: str, config_file: str
) -> IO[bytes]:
    """Substitute known keys with mzbuild-provided values

    * Replace `mzimage` with fingerprinted image names
    """
    images = []
    default = os.getenv(f"MZBUILD_DOCKER_TAG", None)
    with open(config_file) as f:
        compose = yaml.safe_load(f)
        # strip mzconduct top-level key, if it exists
        compose.pop("mzconduct", None)
        for config in compose["services"].values():
            if "mzbuild" in config:
                image_name = config["mzbuild"]

                if image_name not in repo.images:
                    raise errors.BadSpec(f"mzcompose: unknown image {image_name}")

                image = repo.images[image_name]
                override_tag = os.getenv(f"MZBUILD_{image.env_var_name()}_TAG", default)
                if override_tag is not None:
                    config["image"] = image.docker_name(override_tag)
                    print(
                        f"mzcompose: warning: overriding {image_name} image to tag {override_tag}",
                        file=sys.stderr,
                    )
                    del config["mzbuild"]
                else:
                    images.append(image)

            if "propagate-uid-gid" in config:
                config["user"] = f"{os.getuid()}:{os.getgid()}"
                del config["propagate-uid-gid"]

    deps = repo.resolve_dependencies(images)
    for d in deps:
        say(d.spec())

    for config in compose["services"].values():
        if "mzbuild" in config:
            config["image"] = deps[config["mzbuild"]].spec()
            del config["mzbuild"]

    # Check if the command is going to create or start containers, and if so
    # build the dependencies. This can be slow, so we don't want to do it if we
    # can help it (e.g., for `down` or `ps`).
    if command in ["create", "run", "start", "up"]:
        deps.acquire()

    # Construct a configuration that will point Docker Compose at the correct
    # images.
    tempfile = TemporaryFile()
    os.set_inheritable(tempfile.fileno(), True)
    yaml.dump(compose, tempfile, encoding="utf-8")  # type: ignore
    tempfile.flush()
    tempfile.seek(0)
    return tempfile


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
    for path in repo.compose_dirs:
        mzcompose_path = path / "mzcompose"
        with open(mzcompose_path, "w") as f:
            f.write(template.format(os.path.relpath(repo.root, path)))
        mzbuild.chmod_x(mzcompose_path)
    return 0


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
        sys.exit(main(sys.argv[1:]))
