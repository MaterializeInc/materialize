# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The implementation of the mzbuild system for Docker images.
For an overview of what mzbuild is and why it exists, see the [user-facing
documentation][user-docs].

[user-docs]: https://github.com/MaterializeInc/materialize/blob/master/doc/developer/mzbuild.md
"""

from collections import OrderedDict
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryFile
from typing import (
    cast,
    Any,
    Dict,
    List,
    Set,
    Sequence,
    Optional,
    Union,
    Iterable,
    Iterator,
    IO,
    overload,
)
from typing_extensions import Literal
import base64
import contextlib
import enum
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time

import yaml

from materialize import cargo
from materialize import git
from materialize import spawn
from materialize import ui

announce = ui.speaker("==>")


class Fingerprint(bytes):
    """A SHA-1 hash of the inputs to an `Image`.

    The string representation uses base32 encoding to distinguish mzbuild
    fingerprints from Git's hex encoded SHA-1 hashes while still being
    URL safe.
    """

    def __str__(self) -> str:
        return base64.b32encode(self).decode()


class AcquiredFrom(enum.Enum):
    """Where an `Image` was acquired from."""

    REGISTRY = "registry"
    """The image was downloaded from Docker Hub."""

    LOCAL_BUILD = "local-build"
    """The image was built from source locally."""


class RepositoryDetails:
    """Immutable details about a `Repository`.

    Used internally by mzbuild.

    Attributes:
        root: The path to the root of the repository.
        cargo_workspace: The `cargo.Workspace` associated with the repository.
    """

    def __init__(self, root: Path):
        self.root = root
        self.cargo_workspace = cargo.Workspace(root)

    def xcargo(self) -> str:
        """Determine the path to a Cargo executable that targets linux/amd64."""
        return str(self.root / "bin" / "xcompile")

    def xcargo_target_dir(self) -> Path:
        """Determine the path to the target directory for Cargo."""
        return self.root / "target" / "x86_64-unknown-linux-gnu"


def xbinutil(tool: str) -> str:
    """Determine the name of a binutils tool for linux/amd64."""
    if sys.platform == "linux":
        return tool
    else:
        return f"x86_64-unknown-linux-gnu-{tool}"


xobjcopy = xbinutil("objcopy")
xstrip = xbinutil("strip")


def docker_images() -> Set[str]:
    """List the Docker images available on the local machine."""
    return set(
        spawn.capture(
            ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"], unicode=True,
        )
        .strip()
        .split("\n")
    )


def chmod_x(path: Path) -> None:
    """Set the executable bit on a file or directory."""
    # https://stackoverflow.com/a/30463972/1122351
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2  # copy R bits to X
    os.chmod(path, mode)


class PreImage:
    """An action to run before building a Docker image.

    Args:
        rd: The `RepositoryDetails` for the repository.
        path: The path to the `Image` associated with this action.
    """

    def __init__(self, rd: RepositoryDetails, path: Path):
        self.rd = rd
        self.path = path

    def run(self) -> None:
        """Perform the action."""
        spawn.runv(["git", "clean", "-ffdX", self.path])

    def inputs(self) -> Set[str]:
        """Return the files which are considered inputs to the action."""


class CargoPreImage(PreImage):
    """A `PreImage` action that uses Cargo."""

    def inputs(self) -> Set[str]:
        return {
            "ci/builder/stable.stamp",
            "ci/builder/nightly.stamp",
            "Cargo.toml",
            # TODO(benesch): we could in theory fingerprint only the subset of
            # Cargo.lock that applies to the crates at hand, but that is a
            # *lot* of work.
            "Cargo.lock",
            ".cargo/config",
        }


class CargoBuild(CargoPreImage):
    """A pre-image action that builds a single binary with Cargo."""

    def __init__(self, rd: RepositoryDetails, path: Path, config: Dict[str, Any]):
        super().__init__(rd, path)
        self.bin = config.pop("bin", None)
        self.strip = config.pop("strip", True)
        self.extract = config.pop("extract", {})
        if self.bin is None:
            raise ValueError("mzbuild config is missing pre-build target")

    def build(self) -> None:
        cargo_build = [self.rd.xcargo(), "build", "--release", "--bin", self.bin]
        spawn.runv(
            cargo_build, cwd=self.rd.root,
        )
        shutil.copy(self.rd.xcargo_target_dir() / "release" / self.bin, self.path)
        if self.strip:
            # NOTE(benesch): the debug information is large enough that it slows
            # down CI, since we're packaging these binaries up into Docker
            # images and shipping them around. A bit unfortunate, since it'd be
            # nice to have useful backtraces if the binary crashes.
            spawn.runv([xstrip, self.path / self.bin])
        else:
            # Even if we've been asked not to strip the binary, remove the
            # `.debug_pubnames` and `.debug_pubtypes` sections. These are just
            # indexes that speed up launching a debugger against the binary,
            # and we're happy to have slower debugger start up in exchange for
            # smaller binaries. Plus the sections have been obsoleted by a
            # `.debug_names` section in DWARF 5, and so debugger support for
            # `.debug_pubnames`/`.debug_pubtypes` is minimal anyway.
            # See: https://github.com/rust-lang/rust/issues/46034
            spawn.runv(
                [
                    xobjcopy,
                    "-R",
                    ".debug_pubnames",
                    "-R",
                    ".debug_pubtypes",
                    self.path / self.bin,
                ]
            )
        if self.extract:
            output = spawn.capture(
                cargo_build + ["--message-format=json"], unicode=True
            )
            for line in output.split("\n"):
                if line.strip() == "" or not line.startswith("{"):
                    continue
                message = json.loads(line)
                if message["reason"] != "build-script-executed":
                    continue
                package = message["package_id"].split()[0]
                for d in self.extract.get(package, []):
                    shutil.copy(Path(message["out_dir"]) / d, self.path / Path(d).name)

    def run(self) -> None:
        super().run()
        self.build()

    def inputs(self) -> Set[str]:
        crate = self.rd.cargo_workspace.crate_for_bin(self.bin)
        deps = self.rd.cargo_workspace.transitive_path_dependencies(crate)
        return super().inputs() | set(inp for dep in deps for inp in dep.inputs())


# TODO(benesch): make this less hardcoded and custom.
class CargoTest(CargoPreImage):
    """A pre-image action that builds all test binaries in the Cargo workspace.

    .. todo:: This action does not generalize well.
        Its implementation currently hardcodes many details about the
        dependencies of various Rust crates. Ideally these dependencies would
        instead be captured by configuration in `mzbuild.yml`.
    """

    def __init__(self, rd: RepositoryDetails, path: Path, config: Dict[str, Any]):
        super().__init__(rd, path)

    def run(self) -> None:
        super().run()
        CargoBuild(self.rd, self.path, {"bin": "testdrive", "strip": True}).build()
        CargoBuild(self.rd, self.path, {"bin": "materialized", "strip": True}).build()

        # NOTE(benesch): The two invocations of `cargo test --no-run` here
        # deserve some explanation. The first invocation prints error messages
        # to stdout in a human readable form. If that succeeds, the second
        # invocation instructs Cargo to dump the locations of the test binaries
        # it built in a machine readable form. Without the first invocation, the
        # error messages would also be sent to the output file in JSON, and the
        # user would only see a vague "could not compile <package>" error.
        args = [
            self.rd.xcargo(),
            "test",
            "--locked",
            "--no-run",
        ]
        spawn.runv(args)
        output = spawn.capture(args + ["--message-format=json"], unicode=True)

        tests = []
        for line in output.split("\n"):
            if line.strip() == "":
                continue
            message = json.loads(line)
            if message.get("profile", {}).get("test", False):
                crate_name = message["package_id"].split()[0]
                target_kind = "".join(message["target"]["kind"])
                slug = crate_name + "." + target_kind
                if target_kind != "lib":
                    slug += "." + message["target"]["name"]
                crate_path_match = re.search(
                    r"\(path\+file://(.*)\)", message["package_id"]
                )
                if not crate_path_match:
                    raise ValueError(f'invalid package_id: {message["package_id"]}')
                crate_path = Path(crate_path_match.group(1)).relative_to(
                    self.rd.root.resolve()
                )
                tests.append((message["executable"], slug, crate_path))

        os.makedirs(self.path / "tests" / "examples")
        with open(self.path / "tests" / "manifest", "w") as manifest:
            for (executable, slug, crate_path) in tests:
                shutil.copy(executable, self.path / "tests" / slug)
                spawn.runv([xstrip, self.path / "tests" / slug])
                manifest.write(f"{slug} {crate_path}\n")
        shutil.move(str(self.path / "materialized"), self.path / "tests")
        shutil.move(str(self.path / "testdrive"), self.path / "tests")
        shutil.copy(
            self.rd.xcargo_target_dir() / "debug" / "examples" / "pingpong",
            self.path / "tests" / "examples",
        )
        shutil.copytree(self.rd.root / "misc" / "shlib", self.path / "shlib")

    def inputs(self) -> Set[str]:
        crates = self.rd.cargo_workspace.crates.values()
        return super().inputs() | set(inp for crate in crates for inp in crate.inputs())


class Image:
    """A Docker image whose build and dependencies are managed by mzbuild.

    An image corresponds to a directory in a repository that contains a
    `mzbuild.yml` file. This directory is called an "mzbuild context."

    Attributes:
        name: The name of the image.
        publish: Whether the image should be pushed to Docker Hub.
        depends_on: The names of the images upon which this image depends.
        root: The path to the root of the associated `Repository`.
        path: The path to the directory containing the `mzbuild.yml`
            configuration file.
        pre_image: An optional action to perform before running `docker build`.
        build_args: An optional list of --build-arg to pass to the dockerfile
    """

    _DOCKERFILE_MZFROM_RE = re.compile(rb"^MZFROM\s*(\S+)")

    def __init__(self, rd: RepositoryDetails, path: Path):
        self.rd = rd
        self.path = path
        self.pre_image: Optional[PreImage] = None
        with open(self.path / "mzbuild.yml") as f:
            data = yaml.safe_load(f)
            self.name: str = data.pop("name")
            self.publish: bool = data.pop("publish", True)
            pre_image = data.pop("pre-image", None)
            if pre_image is not None:
                typ = pre_image.pop("type", None)
                if typ == "cargo-build":
                    self.pre_image = CargoBuild(self.rd, self.path, pre_image)
                elif typ == "cargo-test":
                    self.pre_image = CargoTest(self.rd, self.path, pre_image)
                else:
                    raise ValueError(
                        f"mzbuild config in {self.path} has unknown pre-image type"
                    )
            self.build_args = data.pop("build-args", {})

        if re.search(r"[^A-Za-z0-9\-]", self.name):
            raise ValueError(
                f"mzbuild image name {self.name} contains invalid character; only alphanumerics and hyphens allowed"
            )

        self.depends_on = []
        with open(self.path / "Dockerfile", "rb") as f:
            for line in f:
                match = self._DOCKERFILE_MZFROM_RE.match(line)
                if match:
                    self.depends_on.append(match.group(1).decode())

    def env_var_name(self) -> str:
        """Return the image name formatted for use in an environment variable.

        The name is capitalized and all hyphens are replaced with underscores.
        """
        return self.name.upper().replace("-", "_")

    def docker_name(self, tag: str) -> str:
        """Return the name of the image on Docker Hub at the given tag."""
        return f"materialize/{self.name}:{tag}"


class ResolvedImage:
    """An `Image` whose dependencies have been resolved.

    Attributes:
        image: The underlying `Image`.
        dependencies: A mapping from dependency name to `ResolvedImage` for
            each of the images that `image` depends upon.
    """

    def __init__(self, image: Image, dependencies: Iterable["ResolvedImage"]):
        self.image = image
        self.dependencies = {}
        for d in dependencies:
            self.dependencies[d.name] = d

    def __repr__(self) -> str:
        return f"ResolvedImage<{self.spec()}>"

    @property
    def name(self) -> str:
        """The name of the underlying image."""
        return self.image.name

    @property
    def publish(self) -> bool:
        """Whether the underlying image should be pushed to Docker Hub."""
        return self.image.publish

    def spec(self) -> str:
        """Return the "spec" for the image.

        A spec is the unique identifier for the image given its current
        fingerprint. It is a valid Docker Hub name.
        """
        return self.image.docker_name(f"mzbuild-{self.fingerprint()}")

    def write_dockerfile(self) -> IO[bytes]:
        """Render the Dockerfile without mzbuild directives.

        Returns:
            file: A handle to a temporary file containing the adjusted
                Dockerfile."""
        with open(self.image.path / "Dockerfile", "rb") as f:
            lines = f.readlines()
        f = TemporaryFile()
        for line in lines:
            match = Image._DOCKERFILE_MZFROM_RE.match(line)
            if match:
                image = match.group(1).decode()
                spec = self.dependencies[image].spec()
                line = Image._DOCKERFILE_MZFROM_RE.sub(b"FROM %b" % spec.encode(), line)
            f.write(line)
        f.seek(0)
        return f

    def build(self) -> None:
        """Build the image from source."""
        if self.image.pre_image is not None:
            self.image.pre_image.run()
        f = self.write_dockerfile()
        cmd: Sequence[str] = [
            "docker",
            "build",
            "-f",
            "-",
            *(f"--build-arg={k}={v}" for k, v in self.image.build_args.items()),
            "-t",
            self.spec(),
            str(self.image.path),
        ]
        spawn.runv(cmd, stdin=f)

    def acquire(self) -> AcquiredFrom:
        """Download or build the image.

        Returns:
            acquired_from: How the image was acquired.
        """
        if self.image.publish:
            while True:
                try:
                    spawn.runv(["docker", "pull", self.spec()])
                    return AcquiredFrom.REGISTRY
                except subprocess.CalledProcessError:
                    if not ui.env_is_truthy("MZBUILD_WAIT_FOR_IMAGE"):
                        break
                    print(f"waiting for mzimage to become available", file=sys.stderr)
                    time.sleep(10)
        self.build()
        return AcquiredFrom.LOCAL_BUILD

    def push(self) -> None:
        """Push the image to Docker Hub.

        The image is pushed under the Docker identifier returned by
        `ResolvedImage.spec`.
        """
        spawn.runv(["docker", "push", self.spec()])

    def pushed(self) -> bool:
        """Check whether the image is pushed to Docker Hub.

        Note that this operation requires a rather slow network request.
        """
        proc = subprocess.run(
            ["docker", "manifest", "inspect", self.spec()],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=dict(os.environ, DOCKER_CLI_EXPERIMENTAL="enabled"),
        )
        return proc.returncode == 0

    def run(self, args: List[str] = []) -> None:
        """Run a command in the image.

        Creates a container from the image and runs the command described by
        `args` in the image.
        """
        spawn.runv(["docker", "run", "--tty", "--rm", "--init", self.spec(), *args])

    def list_dependencies(self, transitive: bool = False) -> Set[str]:
        out = set()
        for dep in self.dependencies.values():
            out.add(dep.name)
            if transitive:
                out |= dep.list_dependencies(transitive)
        return out

    def inputs(self, transitive: bool = False) -> Set[str]:
        """List the files tracked as inputs to the image.

        These files are used to compute the fingerprint for the image. See
        `ResolvedImage.fingerprint` for details.

        Returns:
            inputs: A list of input files, relative to the root of the
                repository.
        """
        paths = set(git.expand_globs(self.image.rd.root, f"{self.image.path}/**"))
        if self.image.pre_image is not None:
            paths |= self.image.pre_image.inputs()
        if transitive:
            for dep in self.dependencies.values():
                paths |= dep.inputs(transitive)
        return paths

    @lru_cache(maxsize=None)
    def fingerprint(self) -> Fingerprint:
        """Fingerprint the inputs to the image.

        Compute the fingerprint of the image. Changing the contents of any of
        the files or adding or removing files to the image will change the
        fingerprint, as will modifying the inputs to any of its dependencies.

        The image considers all non-gitignored files in its mzbuild context to
        be inputs. If it has a pre-image action, that action may add additional
        inputs via `PreImage.inputs`.
        """
        self_hash = hashlib.sha1()
        for rel_path in sorted(
            set(git.expand_globs(self.image.rd.root, *self.inputs()))
        ):
            abs_path = self.image.rd.root / rel_path
            file_hash = hashlib.sha1()
            raw_file_mode = os.lstat(abs_path).st_mode
            # Compute a simplified file mode using the same rules as Git.
            # https://github.com/git/git/blob/3bab5d562/Documentation/git-fast-import.txt#L610-L616
            if stat.S_ISLNK(raw_file_mode):
                file_mode = 0o120000
            elif raw_file_mode & stat.S_IXUSR:
                file_mode = 0o100755
            else:
                file_mode = 0o100644
            with open(abs_path, "rb") as f:
                file_hash.update(f.read())
            self_hash.update(file_mode.to_bytes(2, byteorder="big"))
            self_hash.update(rel_path.encode())
            self_hash.update(file_hash.digest())
            self_hash.update(b"\0")

        full_hash = hashlib.sha1()
        full_hash.update(self_hash.digest())
        for dep in sorted(self.dependencies.values(), key=lambda d: d.name):
            full_hash.update(dep.name.encode())
            full_hash.update(dep.fingerprint())
            full_hash.update(b"\0")

        return Fingerprint(full_hash.digest())


class DependencySet:
    """A topologically-sorted, transitively-closed list of `Image`s.

    .. warning:: These guarantees are not enforced.
        Dependency sets constructed by `Repository.resolve_dependencies` will
        uphold the topological sort and transitive closure guarantees, but
        it is your responsibility to provide these guarantees for any dependency
        sets you create yourself.

    Iterating over a dependency set yields the contained images in the stored
    order, with the caveat noted above. Indexing a dependency set yields the
    _i_ th item in the stored order.
    """

    def __init__(self, dependencies: Iterable[Image]):
        self.dependencies: OrderedDict[str, ResolvedImage] = OrderedDict()
        for d in dependencies:
            self.dependencies[d.name] = ResolvedImage(
                d, (self.dependencies[d0] for d0 in d.depends_on)
            )

    def acquire(self, force_build: bool = False) -> None:
        """Download or build all of the images in the dependency set.

        Args:
            force_build: Whether to force all images that are not already
                available locally to be built from source, regardless of whether
                the image is available for download. Note that this argument has
                no effect if the image is already available locally.
            push: Whether to push any images that will built locally to Docker
                Hub.
        """
        known_images = docker_images()
        for d in self:
            spec = d.spec()
            if spec not in known_images:
                if force_build:
                    announce(f"Force-building {spec}")
                    d.build()
                else:
                    announce(f"Acquiring {spec}")
                    acquired_from = d.acquire()
            else:
                announce(f"Already built {spec}")

    def __iter__(self) -> Iterator[ResolvedImage]:
        return iter(self.dependencies.values())

    def __getitem__(self, key: str) -> ResolvedImage:
        return self.dependencies[key]


class Repository:
    """A collection of mzbuild `Image`s.

    Creating a repository will walk the filesystem beneath `root` to
    automatically discover all contained `Image`s.

    Iterating over a repository yields the contained images in an arbitrary
    order.

    Args:
        root: The path to the root of the repository.

    Attributes:
        images: A mapping from image name to `Image` for all contained images.
        compose_dirs: The set of directories containing an `mzcompose.yml` file.
    """

    def __init__(self, root: Path):
        self.rd = RepositoryDetails(root)
        self.images: Dict[str, Image] = {}
        self.compose_dirs = set()
        for (path, dirs, files) in os.walk(self.root, topdown=True):
            # Filter out some particularly massive ignored directories to keep
            # things snappy. Not required for correctness.
            dirs[:] = set(dirs) - {".git", "target", "mzdata", "node_modules"}
            if "mzbuild.yml" in files:
                image = Image(self.rd, Path(path))
                if not image.name:
                    raise ValueError(f"config at {path} missing name")
                if image.name in self.images:
                    raise ValueError(f"image {image.name} exists twice")
                self.images[image.name] = image
            if "mzcompose.yml" in files:
                self.compose_dirs.add(Path(path))

        # Validate dependencies.
        for image in self.images.values():
            for d in image.depends_on:
                if d not in self.images:
                    raise ValueError(
                        f"image {image.name} depends on non-existent image {d}"
                    )

    @property
    def root(self) -> Path:
        """The path to the root directory for the repository."""
        return self.rd.root

    def resolve_dependencies(self, targets: Iterable[Image]) -> DependencySet:
        """Compute the dependency set necessary to build target images.

         The dependencies of `targets` will be crawled recursively until the
         complete set of transitive dependencies is determined or a circular
         dependency is discovered. The returned dependency set will be sorted
         in topological order.

         Raises:
            ValueError: A circular dependency was discovered in the images
                in the repository.
        """
        resolved = OrderedDict()
        visiting = set()

        def visit(image: Image, path: List[str] = []) -> None:
            if image.name in resolved:
                return
            if image.name in visiting:
                diagram = " -> ".join(path + [image.name])
                raise ValueError(f"circular dependency in mzbuild: {diagram}")

            visiting.add(image.name)
            for d in sorted(image.depends_on):
                visit(self.images[d], path + [image.name])
            resolved[image.name] = image

        for target in sorted(targets, key=lambda image: image.name):
            visit(target)

        return DependencySet(resolved.values())

    def __iter__(self) -> Iterator[Image]:
        return iter(self.images.values())
