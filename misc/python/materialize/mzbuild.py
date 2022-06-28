# Copyright Materialize, Inc. and contributors. All rights reserved.
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

[user-docs]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/mzbuild.md
"""

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from collections import OrderedDict
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryFile
from typing import IO, Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set

import yaml

from materialize import cargo, git, spawn, ui, xcompile
from materialize.xcompile import Arch


class Fingerprint(bytes):
    """A SHA-1 hash of the inputs to an `Image`.

    The string representation uses base32 encoding to distinguish mzbuild
    fingerprints from Git's hex encoded SHA-1 hashes while still being
    URL safe.
    """

    def __str__(self) -> str:
        return base64.b32encode(self).decode()


class RepositoryDetails:
    """Immutable details about a `Repository`.

    Used internally by mzbuild.

    Attributes:
        root: The path to the root of the repository.
        arch: The CPU architecture to build for.
        release_mode: Whether the repository is being built in release mode.
        coverage: Whether the repository has code coverage instrumentation
            enabled.
        cargo_workspace: The `cargo.Workspace` associated with the repository.
    """

    def __init__(self, root: Path, arch: Arch, release_mode: bool, coverage: bool):
        self.root = root
        self.arch = arch
        self.release_mode = release_mode
        self.coverage = coverage
        self.cargo_workspace = cargo.Workspace(root)

    def cargo(
        self, subcommand: str, rustflags: List[str], channel: Optional[str] = None
    ) -> List[str]:
        """Start a cargo invocation for the configured architecture."""
        return xcompile.cargo(
            arch=self.arch, channel=channel, subcommand=subcommand, rustflags=rustflags
        )

    def tool(self, name: str) -> List[str]:
        """Start a binutils tool invocation for the configured architecture."""
        return xcompile.tool(self.arch, name)

    def cargo_target_dir(self) -> Path:
        """Determine the path to the target directory for Cargo."""
        return self.root / "target-xcompile" / xcompile.target(self.arch)

    def rewrite_builder_path_for_host(self, path: Path) -> Path:
        """Rewrite a path that is relative to the target directory inside the
        builder to a path that is relative to the target directory on the host.

        If path does is not relative to the target directory inside the builder,
        it is returned unchanged.
        """
        builder_target_dir = Path("/mnt/build") / xcompile.target(self.arch)
        try:
            return self.cargo_target_dir() / path.relative_to(builder_target_dir)
        except ValueError:
            return path


def docker_images() -> Set[str]:
    """List the Docker images available on the local machine."""
    return set(
        spawn.capture(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"])
        .strip()
        .split("\n")
    )


def is_docker_image_pushed(name: str) -> bool:
    """Check whether the named image is pushed to Docker Hub.

    Note that this operation requires a rather slow network request.
    """
    proc = subprocess.run(
        ["docker", "manifest", "inspect", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=dict(os.environ, DOCKER_CLI_EXPERIMENTAL="enabled"),
    )
    return proc.returncode == 0


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
        pass

    def inputs(self) -> Set[str]:
        """Return the files which are considered inputs to the action."""
        raise NotImplementedError

    def extra(self) -> str:
        """Returns additional data for incorporation in the fingerprint."""
        return ""


class Copy(PreImage):
    """A `PreImage` action which copies files from a directory.

    The contents of the specified `source` directory are copied into the
    `destination` directory. The `source` directory is relative to the
    repository root. The `destination` directory is relative to the mzbuild
    context. Files in the `source` directory are matched against the glob
    specified by the `matching` argument.
    """

    def __init__(self, rd: RepositoryDetails, path: Path, config: Dict[str, Any]):
        super().__init__(rd, path)

        self.source = config.pop("source", None)
        if self.source is None:
            raise ValueError("mzbuild config is missing 'source' argument")

        self.destination = config.pop("destination", None)
        if self.destination is None:
            raise ValueError("mzbuild config is missing 'destination' argument")

        self.matching = config.pop("matching", "*")

    def run(self) -> None:
        super().run()
        for src in self.inputs():
            dst = self.path / self.destination / Path(src).relative_to(self.source)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(self.rd.root / src, dst)

    def inputs(self) -> Set[str]:
        return set(git.expand_globs(self.rd.root, f"{self.source}/{self.matching}"))


class CargoPreImage(PreImage):
    """A `PreImage` action that uses Cargo."""

    def inputs(self) -> Set[str]:
        return {
            "ci/builder",
            "Cargo.toml",
            # TODO(benesch): we could in theory fingerprint only the subset of
            # Cargo.lock that applies to the crates at hand, but that is a
            # *lot* of work.
            "Cargo.lock",
            ".cargo/config",
        }

    def extra(self) -> str:
        # Cargo images depend on the release mode and whether coverage is
        # enabled.
        flags: List[str] = []
        if self.rd.release_mode:
            flags += "release"
        if self.rd.coverage:
            flags += "coverage"
        flags.sort()
        return ",".join(flags)


class CargoBuild(CargoPreImage):
    """A pre-image action that builds a single binary with Cargo."""

    def __init__(self, rd: RepositoryDetails, path: Path, config: Dict[str, Any]):
        super().__init__(rd, path)
        bin = config.pop("bin", [])
        self.bins = bin if isinstance(bin, list) else [bin]
        example = config.pop("example", [])
        self.examples = example if isinstance(example, list) else [example]
        self.strip = config.pop("strip", True)
        self.extract = config.pop("extract", {})
        self.rustflags = config.pop("rustflags", [])
        self.channel = None
        if rd.coverage:
            self.rustflags += [
                "-Zinstrument-coverage",
                # Nix generates some unresolved symbols that -Zinstrument-coverage
                # somehow causes the linker to complain about, so just disable
                # warnings about unresolved symbols and hope it all works out.
                # See: https://github.com/nix-rust/nix/issues/1116
                "-Clink-arg=-Wl,--warn-unresolved-symbols",
            ]
            self.channel = "nightly"
        if len(self.bins) == 0 and len(self.examples) == 0:
            raise ValueError("mzbuild config is missing pre-build target")

    def build(self) -> None:
        cargo_build = [
            *self.rd.cargo("build", channel=self.channel, rustflags=self.rustflags)
        ]

        for bin in self.bins:
            cargo_build.extend(["--bin", bin])
        for example in self.examples:
            cargo_build.extend(["--example", example])

        if self.rd.release_mode:
            cargo_build.append("--release")
        spawn.runv(cargo_build, cwd=self.rd.root)
        cargo_profile = "release" if self.rd.release_mode else "debug"

        def copy(exe: Path) -> None:
            (self.path / exe).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(
                self.rd.cargo_target_dir() / cargo_profile / exe, self.path / exe
            )

            if self.strip:
                # NOTE(benesch): the debug information is large enough that it slows
                # down CI, since we're packaging these binaries up into Docker
                # images and shipping them around. A bit unfortunate, since it'd be
                # nice to have useful backtraces if the binary crashes.
                spawn.runv(
                    [*self.rd.tool("strip"), "--strip-debug", self.path / exe],
                    cwd=self.rd.root,
                )
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
                        *self.rd.tool("objcopy"),
                        "-R",
                        ".debug_pubnames",
                        "-R",
                        ".debug_pubtypes",
                        self.path / exe,
                    ],
                    cwd=self.rd.root,
                )

        for bin in self.bins:
            copy(Path(bin))
        for example in self.examples:
            copy(Path("examples") / example)

        if self.extract:
            output = spawn.capture(
                cargo_build + ["--message-format=json"],
                cwd=self.rd.root,
            )
            target_dir = self.rd.cargo_target_dir()
            for line in output.split("\n"):
                if line.strip() == "" or not line.startswith("{"):
                    continue
                message = json.loads(line)
                if message["reason"] != "build-script-executed":
                    continue
                out_dir = self.rd.rewrite_builder_path_for_host(
                    Path(message["out_dir"])
                )
                if not out_dir.is_relative_to(target_dir):
                    # Some crates are built for both the host and the target.
                    # Ignore the built-for-host out dir.
                    continue
                package = message["package_id"].split()[0]
                for src, dst in self.extract.get(package, {}).items():
                    spawn.runv(["cp", "-R", out_dir / src, self.path / dst])

    def run(self) -> None:
        super().run()
        self.build()

    def inputs(self) -> Set[str]:
        deps = set()

        for bin in self.bins:
            crate = self.rd.cargo_workspace.crate_for_bin(bin)
            deps |= self.rd.cargo_workspace.transitive_path_dependencies(crate)

        for example in self.examples:
            crate = self.rd.cargo_workspace.crate_for_example(example)
            deps |= self.rd.cargo_workspace.transitive_path_dependencies(
                crate, dev=True
            )

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

        # NOTE(benesch): The two invocations of `cargo test --no-run` here
        # deserve some explanation. The first invocation prints error messages
        # to stdout in a human readable form. If that succeeds, the second
        # invocation instructs Cargo to dump the locations of the test binaries
        # it built in a machine readable form. Without the first invocation, the
        # error messages would also be sent to the output file in JSON, and the
        # user would only see a vague "could not compile <package>" error.
        args = [*self.rd.cargo("test", rustflags=[]), "--locked", "--no-run"]
        spawn.runv(args, cwd=self.rd.root)
        output = spawn.capture(args + ["--message-format=json"], cwd=self.rd.root)

        tests = []
        for line in output.split("\n"):
            if line.strip() == "":
                continue
            message = json.loads(line)
            if message.get("profile", {}).get("test", False):
                crate_name = message["package_id"].split()[0]
                target_kind = "".join(message["target"]["kind"])
                if target_kind == "proc-macro":
                    continue
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
                executable = self.rd.rewrite_builder_path_for_host(
                    Path(message["executable"])
                )
                tests.append((executable, slug, crate_path))

        os.makedirs(self.path / "tests" / "examples")
        with open(self.path / "tests" / "manifest", "w") as manifest:
            for (executable, slug, crate_path) in tests:
                shutil.copy(executable, self.path / "tests" / slug)
                spawn.runv(
                    [*self.rd.tool("strip"), self.path / "tests" / slug],
                    cwd=self.rd.root,
                )
                package = slug.replace(".", "::")
                manifest.write(f"{slug} {package} {crate_path}\n")
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
        pre_images: Optional actions to perform before running `docker build`.
        build_args: An optional list of --build-arg to pass to the dockerfile
    """

    _DOCKERFILE_MZFROM_RE = re.compile(rb"^MZFROM\s*(\S+)")

    def __init__(self, rd: RepositoryDetails, path: Path):
        self.rd = rd
        self.path = path
        self.pre_images: List[PreImage] = []
        with open(self.path / "mzbuild.yml") as f:
            data = yaml.safe_load(f)
            self.name: str = data.pop("name")
            self.publish: bool = data.pop("publish", True)
            for pre_image in data.pop("pre-image", []):
                typ = pre_image.pop("type", None)
                if typ == "cargo-build":
                    self.pre_images.append(CargoBuild(self.rd, self.path, pre_image))
                elif typ == "cargo-test":
                    self.pre_images.append(CargoTest(self.rd, self.path, pre_image))
                elif typ == "copy":
                    self.pre_images.append(Copy(self.rd, self.path, pre_image))
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

    def docker_name(self, tag: str) -> str:
        """Return the name of the image on Docker Hub at the given tag."""
        return f"materialize/{self.name}:{tag}"


class ResolvedImage:
    """An `Image` whose dependencies have been resolved.

    Attributes:
        image: The underlying `Image`.
        acquired: Whether the image is available locally.
        dependencies: A mapping from dependency name to `ResolvedImage` for
            each of the images that `image` depends upon.
    """

    def __init__(self, image: Image, dependencies: Iterable["ResolvedImage"]):
        self.image = image
        self.acquired = False
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
        return f"materialize/{self.name}:mzbuild-{self.fingerprint()}"

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
        for dep in self.dependencies.values():
            dep.acquire()
        spawn.runv(["git", "clean", "-ffdX", self.image.path])
        for pre_image in self.image.pre_images:
            pre_image.run()
        build_args = {
            **self.image.build_args,
            "ARCH_GCC": str(self.image.rd.arch),
            "ARCH_GO": self.image.rd.arch.go_str(),
        }
        f = self.write_dockerfile()
        cmd: Sequence[str] = [
            "docker",
            "build",
            "-f",
            "-",
            *(f"--build-arg={k}={v}" for k, v in build_args.items()),
            "-t",
            self.spec(),
            f"--platform=linux/{self.image.rd.arch.go_str()}",
            str(self.image.path),
        ]
        spawn.runv(cmd, stdin=f, stdout=sys.stderr.buffer)

    def acquire(self) -> None:
        """Download or build the image if it does not exist locally."""
        if self.acquired:
            return

        ui.header(f"Acquiring {self.spec()}")
        try:
            spawn.runv(
                ["docker", "pull", self.spec()],
                stdout=sys.stderr.buffer,
            )
        except subprocess.CalledProcessError:
            self.build()
        self.acquired = True

    def ensure(self) -> None:
        """Ensure the image exists on Docker Hub if it is publishable.

        The image is pushed using its spec as its tag.
        """
        if self.publish and is_docker_image_pushed(self.spec()):
            ui.say(f"{self.spec()} already exists")
            return
        self.build()
        spawn.runv(["docker", "push", self.spec()])

    def run(self, args: List[str] = [], docker_args: List[str] = []) -> None:
        """Run a command in the image.

        Creates a container from the image and runs the command described by
        `args` in the image.
        """
        spawn.runv(
            [
                "docker",
                "run",
                "--tty",
                "--rm",
                "--init",
                *docker_args,
                self.spec(),
                *args,
            ]
        )

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
        if not paths:
            # While we could find an `mzbuild.yml` file for this service, expland_globs didn't
            # return any files that matched this service. At the very least, the `mzbuild.yml`
            # file itself should have been returned. We have a bug if paths is empty.
            raise AssertionError(
                f"{self.image.name} mzbuild exists but its files are unknown to git"
            )
        for pre_image in self.image.pre_images:
            paths |= pre_image.inputs()
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

        for pre_image in self.image.pre_images:
            self_hash.update(pre_image.extra().encode())
            self_hash.update(b"\0")

        self_hash.update(f"arch={self.image.rd.arch}".encode())
        self_hash.update(f"coverage={self.image.rd.coverage}".encode())

        full_hash = hashlib.sha1()
        full_hash.update(self_hash.digest())
        for dep in sorted(self.dependencies.values(), key=lambda d: d.name):
            full_hash.update(dep.name.encode())
            full_hash.update(dep.fingerprint())
            full_hash.update(b"\0")

        return Fingerprint(full_hash.digest())


class DependencySet:
    """A set of `ResolvedImage`s.

    Iterating over a dependency set yields the contained images in an arbitrary
    order. Indexing a dependency set yields the image with the specified name.
    """

    def __init__(self, dependencies: Iterable[Image]):
        """Construct a new `DependencySet`.

        The provided `dependencies` must be topologically sorted.
        """
        self._dependencies: Dict[str, ResolvedImage] = {}
        known_images = docker_images()
        for d in dependencies:
            image = ResolvedImage(
                image=d,
                dependencies=(self._dependencies[d0] for d0 in d.depends_on),
            )
            image.acquired = image.spec() in known_images
            self._dependencies[d.name] = image

    def acquire(self) -> None:
        """Download or build all of the images in the dependency set that do not
        already exist locally.
        """
        for dep in self:
            dep.acquire()

    def ensure(self) -> None:
        """Ensure all publishable images in this dependency set exist on Docker
        Hub.

        Images are pushed using their spec as their tag.
        """
        for dep in self:
            dep.ensure()

    def __iter__(self) -> Iterator[ResolvedImage]:
        return iter(self._dependencies.values())

    def __getitem__(self, key: str) -> ResolvedImage:
        return self._dependencies[key]


class Repository:
    """A collection of mzbuild `Image`s.

    Creating a repository will walk the filesystem beneath `root` to
    automatically discover all contained `Image`s.

    Iterating over a repository yields the contained images in an arbitrary
    order.

    Args:
        root: The path to the root of the repository.
        arch: The CPU architecture to build for.
        release_mode: Whether to build the repository in release mode.
        coverage: Whether to enable code coverage instrumentation.

    Attributes:
        images: A mapping from image name to `Image` for all contained images.
        compose_dirs: The set of directories containing a `mzcompose.py` file.
    """

    def __init__(
        self,
        root: Path,
        arch: Arch = Arch.host(),
        release_mode: bool = True,
        coverage: bool = False,
    ):
        self.rd = RepositoryDetails(root, arch, release_mode, coverage)
        self.images: Dict[str, Image] = {}
        self.compositions: Dict[str, Path] = {}
        for (path, dirs, files) in os.walk(self.root, topdown=True):
            if path == str(root / "misc"):
                dirs.remove("python")
            # Filter out some particularly massive ignored directories to keep
            # things snappy. Not required for correctness.
            dirs[:] = set(dirs) - {
                ".git",
                "target",
                "target-ra",
                "target-xcompile",
                "mzdata",
                "node_modules",
                "venv",
            }
            if "mzbuild.yml" in files:
                image = Image(self.rd, Path(path))
                if not image.name:
                    raise ValueError(f"config at {path} missing name")
                if image.name in self.images:
                    raise ValueError(f"image {image.name} exists twice")
                self.images[image.name] = image
            if "mzcompose.py" in files:
                name = Path(path).name
                if name in self.compositions:
                    raise ValueError(f"composition {name} exists twice")
                self.compositions[name] = Path(path)

        # Validate dependencies.
        for image in self.images.values():
            for d in image.depends_on:
                if d not in self.images:
                    raise ValueError(
                        f"image {image.name} depends on non-existent image {d}"
                    )

    @staticmethod
    def install_arguments(parser: argparse.ArgumentParser) -> None:
        """Install options to configure a repository into an argparse parser.

        This function installs the following options:

          * The mutually-exclusive `--dev`/`--release` options to control the
            `release_mode` repository attribute.
          * The `--coverage` booelan option to control the `coverage` repository
            attribute.

        Use `Repository.from_arguments` to construct a repository from the
        parsed command-line arguments.
        """
        build_mode = parser.add_mutually_exclusive_group()
        build_mode.add_argument(
            "--dev",
            dest="release",
            action="store_false",
            help="build Rust binaries with the dev profile",
        )
        build_mode.add_argument(
            "--release",
            action="store_true",
            help="build Rust binaries with the release profile (default)",
        )
        parser.add_argument(
            "--coverage",
            help="whether to enable code coverage compilation flags",
            action="store_true",
        )
        parser.add_argument(
            "--arch",
            default=Arch.host(),
            help="the CPU architecture to build for",
            type=Arch,
            choices=Arch,
        )

    @classmethod
    def from_arguments(cls, root: Path, args: argparse.Namespace) -> "Repository":
        """Construct a repository from command-line arguments.

        The provided namespace must contain the options installed by
        `Repository.install_arguments`.
        """
        return cls(
            root, release_mode=args.release, coverage=args.coverage, arch=args.arch
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


def publish_multiarch_images(tag: str, dependency_sets: List[DependencySet]) -> None:
    """Publishes a set of docker images under a given tag."""
    for images in zip(*dependency_sets):
        names = set(image.image.name for image in images)
        assert len(names) == 1, "dependency sets did not contain identical images"
        name = f"materialize/{next(iter(names))}:{tag}"
        spawn.runv(
            ["docker", "manifest", "create", name, *(image.spec() for image in images)]
        )
        spawn.runv(["docker", "manifest", "push", name])
    print(f"--- Nofifying for tag {tag}")
    markdown = f"""Pushed images with Docker tag `{tag}`"""
    spawn.runv(
        [
            "buildkite-agent",
            "annotate",
            "--style=info",
            f"--context=build-tags-{tag}",
        ],
        stdin=markdown.encode(),
    )
