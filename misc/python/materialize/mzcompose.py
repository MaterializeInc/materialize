# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The implementation of the mzcompose system for Docker compositions.

For an overview of what mzcompose is and why it exists, see the [user-facing
documentation][user-docs].

[user-docs]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/mzbuild.md
"""

from tempfile import TemporaryFile
from typing import IO, List
import os
import subprocess
import sys
import yaml

from materialize import errors
from materialize import mzbuild


class Composition:
    """A set of parsed mzcompose.yml files."""

    def __init__(self, repo: mzbuild.Repository, project_dir: str):
        self.files: List[IO[bytes]] = []
        self.images: List[mzbuild.Image] = []
        self.repo = repo
        self.project_dir = project_dir

    def load_file(self, path: str) -> None:
        """Loads a single file into the composition.

        Args:
            path: The path to the file to load.
        """
        default_tag = os.getenv(f"MZBUILD_TAG", None)

        with open(path) as f:
            compose = yaml.safe_load(f)

        # Remove mzconduct configuration that Docker Compose doesn't know how
        # to handle.
        compose.pop("mzconduct", None)

        # Resolve all services that reference an `mzbuild` image to a specific
        # `image` reference.

        for config in compose["services"].values():
            if "mzbuild" in config:
                image_name = config["mzbuild"]

                if image_name not in self.repo.images:
                    raise errors.BadSpec(f"mzcompose: unknown image {image_name}")

                image = self.repo.images[image_name]
                override_tag = os.getenv(
                    f"MZBUILD_{image.env_var_name()}_TAG", default_tag
                )
                if override_tag is not None:
                    config["image"] = image.docker_name(override_tag)
                    print(
                        f"mzcompose: warning: overriding {image_name} image to tag {override_tag}",
                        file=sys.stderr,
                    )
                    del config["mzbuild"]
                else:
                    self.images.append(image)

                if "propagate-uid-gid" in config:
                    config["user"] = f"{os.getuid()}:{os.getgid()}"
                    del config["propagate-uid-gid"]

        deps = self.repo.resolve_dependencies(self.images)
        for config in compose["services"].values():
            if "mzbuild" in config:
                config["image"] = deps[config["mzbuild"]].spec()
                del config["mzbuild"]

        # Emit the munged configuration to a temporary file so that we can later
        # pass it to Docker Compose.
        tempfile = TemporaryFile()
        os.set_inheritable(tempfile.fileno(), True)
        yaml.dump(compose, tempfile, encoding="utf-8")  # type: ignore
        tempfile.flush()
        self.files.append(tempfile)

    def run(self, args: List[str]) -> None:
        """Invokes docker-compose on the composition.

        Arguments to specify the files in the composition and the project
        directory are added automatically.

        Args:
            args: Additional arguments to pass to docker-compose.
        """
        for file in self.files:
            file.seek(0)
        subprocess.check_call(
            [
                "docker-compose",
                *[f"-f/dev/fd/{file.fileno()}" for file in self.files],
                "--project-directory",
                self.project_dir,
                *args,
            ],
            close_fds=False,
        )
